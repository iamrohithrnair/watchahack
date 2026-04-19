"""Corporate HQ ingestor — identifies grid cells containing major corporate headquarters.

Uses the Companies House API to look up registered addresses of major corporations
(especially media outlets) and maps them to grid cells. This enables the system to
flag data streams from cells containing global news agencies as 'non-local',
preventing the geospatial aliasing problem where worldwide news gets attributed
to the cell containing the publisher's HQ.

Companies House API: free, requires API key.
Postcodes.io: free, no key needed (geocodes UK postcodes to lat/lon).
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.corporate_hq")

_CH_SEARCH_URL = "https://api.company-information.service.gov.uk/search/companies"
_CH_COMPANY_URL = "https://api.company-information.service.gov.uk/company/{number}"
_POSTCODES_URL = "https://api.postcodes.io/postcodes/{postcode}"

# Major media outlets and news agencies with significant London presence.
# The Brain flagged geospatial aliasing: news about global events gets tagged
# to the cell containing the publisher's HQ. This list targets the biggest offenders.
_MEDIA_CORPS = [
    "BBC",
    "Sky News",
    "Reuters",
    "Press Association",
    "Associated Newspapers",  # Daily Mail
    "News Group Newspapers",  # The Sun / News UK
    "Telegraph Media Group",
    "Guardian Media Group",
    "Financial Times",
    "The Economist",
    "ITV PLC",
    "Channel Four Television",
    "Global Media",
    "Bloomberg LP",
]

# Other major corporations whose HQ presence is useful context.
_OTHER_CORPS = [
    "HSBC Holdings",
    "Barclays PLC",
    "BP PLC",
    "Shell International",
    "Unilever PLC",
    "GlaxoSmithKline",
    "AstraZeneca",
    "London Stock Exchange Group",
    "Transport for London",
]

_ALL_CORPS = _MEDIA_CORPS + _OTHER_CORPS

# Regex to extract postcode from address strings
_POSTCODE_RE = re.compile(r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.IGNORECASE)


class CorporateHQIngestor(BaseIngestor):
    source_name = "corporate_hq"
    rate_limit_name = "default"

    def _get_api_key(self) -> str | None:
        return os.environ.get("COMPANIES_HOUSE_API_KEY")

    async def fetch_data(self) -> Any:
        api_key = self._get_api_key()
        if not api_key:
            self.log.info("COMPANIES_HOUSE_API_KEY not set — using fallback known HQs only")
            return {"fallback": True}

        results = []
        for corp_name in _ALL_CORPS:
            data = await self.fetch(
                _CH_SEARCH_URL,
                params={"q": corp_name, "items_per_page": "1"},
                headers={"Authorization": api_key},
            )
            if data and isinstance(data, dict):
                items = data.get("items", [])
                if items:
                    item = items[0]
                    results.append({
                        "name": item.get("title", corp_name),
                        "number": item.get("company_number"),
                        "address": item.get("address", {}),
                        "is_media": corp_name in _MEDIA_CORPS,
                        "query": corp_name,
                    })
        return results

    async def _geocode_postcode(self, postcode: str) -> tuple[float, float] | None:
        """Use Postcodes.io to convert a UK postcode to lat/lon."""
        clean = postcode.strip().replace(" ", "")
        data = await self.fetch(
            _POSTCODES_URL.format(postcode=clean),
        )
        if data and isinstance(data, dict) and data.get("status") == 200:
            result = data.get("result", {})
            lat = result.get("latitude")
            lon = result.get("longitude")
            if lat is not None and lon is not None:
                return float(lat), float(lon)
        return None

    async def process(self, data: Any) -> None:
        if isinstance(data, dict) and data.get("fallback"):
            await self._process_fallback()
            return

        if not data:
            self.log.info("No corporate HQ data to process")
            return

        processed = 0
        for corp in data:
            address = corp.get("address", {})
            postcode = address.get("postal_code", "")
            if not postcode:
                # Try to extract from address string
                addr_str = " ".join(str(v) for v in address.values() if v)
                match = _POSTCODE_RE.search(addr_str)
                postcode = match.group(0) if match else ""

            lat, lon, cell_id = None, None, None
            if postcode:
                coords = await self._geocode_postcode(postcode)
                if coords:
                    lat, lon = coords
                    cell_id = self.graph.latlon_to_cell(lat, lon)

            if cell_id is None:
                continue

            corp_type = "media_hq" if corp.get("is_media") else "corporate_hq"

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"Corporate HQ: {corp['name']} ({corp_type})",
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "company_name": corp["name"],
                    "company_number": corp.get("number"),
                    "postcode": postcode,
                    "hq_type": corp_type,
                    "is_media": corp.get("is_media", False),
                    "query": corp.get("query", ""),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Corporate HQ mapped: {corp['name']} → {cell_id} "
                    f"[{corp_type}] ({postcode})"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "company_name": corp["name"],
                    "cell_id": cell_id,
                    "hq_type": corp_type,
                    "is_media": corp.get("is_media", False),
                    "postcode": postcode,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Corporate HQ: processed=%d companies", processed)

    async def _process_fallback(self) -> None:
        """Use hardcoded known HQ locations when no API key is available."""
        # Well-known London media/corporate HQ locations
        known_hqs = [
            ("BBC", 51.5188, -0.1441, True),           # Broadcasting House, W1A 1AA
            ("Sky News", 51.4964, -0.1731, True),       # Osterley, TW7 5QD (approx)
            ("Reuters", 51.5137, -0.0893, True),        # Canary Wharf area
            ("Associated Newspapers", 51.5175, -0.1076, True),  # Northcliffe House, W8
            ("News UK", 51.4985, -0.0196, True),        # London Bridge
            ("Guardian Media Group", 51.5347, -0.1217, True),  # Kings Cross
            ("Financial Times", 51.5078, -0.0986, True), # Bracken House, EC4
            ("ITV PLC", 51.5078, -0.1131, True),        # South Bank
            ("Channel 4", 51.4925, -0.1265, True),      # Horseferry Road
            ("Bloomberg LP", 51.5138, -0.0890, True),   # City of London
            ("HSBC Holdings", 51.5047, -0.0198, True),  # Canary Wharf
            ("Barclays PLC", 51.5050, -0.0186, False),  # Canary Wharf
            ("BP PLC", 51.5027, -0.1460, False),        # St James's Square
            ("London Stock Exchange", 51.5155, -0.0922, False),  # Paternoster Square
            ("Transport for London", 51.5029, -0.1135, False),  # Palestra, SE1
        ]

        processed = 0
        for name, lat, lon, is_media in known_hqs:
            cell_id = self.graph.latlon_to_cell(lat, lon)
            if cell_id is None:
                continue

            corp_type = "media_hq" if is_media else "corporate_hq"

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"Corporate HQ: {name} ({corp_type})",
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "company_name": name,
                    "hq_type": corp_type,
                    "is_media": is_media,
                    "source_method": "fallback_known_locations",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Corporate HQ mapped: {name} → {cell_id} [{corp_type}]"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "company_name": name,
                    "cell_id": cell_id,
                    "hq_type": corp_type,
                    "is_media": is_media,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Corporate HQ (fallback): processed=%d known HQs", processed)


async def ingest_corporate_hq(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for corporate HQ mapping."""
    ingestor = CorporateHQIngestor(board, graph, scheduler)
    await ingestor.run()
