"""Companies House ingestor — recent filings, incorporations, and officer changes in London.

Uses the free Companies House API (https://developer.company-information.service.gov.uk/)
to track corporate activity: new incorporations, dissolutions, director appointments,
and significant filing events. Requires a COMPANIES_HOUSE_API_KEY env var.
"""

from __future__ import annotations

import base64
import logging
import os
from datetime import datetime, timedelta
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.companies_house")

_BASE_URL = "https://api.company-information.service.gov.uk"

# City of London coordinates as default
_LONDON_LAT = 51.5074
_LONDON_LON = -0.1278

# Filing categories of interest for urban intelligence
_INTERESTING_CATEGORIES = {
    "incorporation",
    "change-of-name",
    "liquidation",
    "dissolution",
    "officers",
    "insolvency",
    "accounts",
    "capital",
    "confirmation-statement",
    "resolution",
}


class CompaniesHouseIngestor(BaseIngestor):
    source_name = "companies_house"
    rate_limit_name = "default"
    required_env_vars = ["COMPANIES_HOUSE_API_KEY"]

    def _auth_header(self) -> dict[str, str]:
        key = os.environ.get("COMPANIES_HOUSE_API_KEY", "")
        # Companies House uses HTTP Basic auth: key as username, empty password
        token = base64.b64encode(f"{key}:".encode()).decode()
        return {"Authorization": f"Basic {token}"}

    async def _api_get(self, path: str, params: dict | None = None) -> Any:
        """Make an authenticated GET to the Companies House API."""
        url = f"{_BASE_URL}{path}"
        return await self.fetch(url, params=params, headers=self._auth_header())

    async def fetch_data(self) -> Any:
        """Fetch recent filings for London-registered companies."""
        # Search for recently filed items — use advanced company search
        # to find London-based companies with recent activity
        today = datetime.utcnow().strftime("%Y-%m-%d")
        yesterday = (datetime.utcnow() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Search for recently incorporated London companies
        incorporations = await self._api_get(
            "/advanced-search/companies",
            params={
                "location": "london",
                "incorporated_from": yesterday,
                "incorporated_to": today,
                "size": "20",
            },
        )

        # Search for recently dissolved London companies
        dissolutions = await self._api_get(
            "/advanced-search/companies",
            params={
                "location": "london",
                "dissolved_from": yesterday,
                "dissolved_to": today,
                "size": "20",
            },
        )

        return {
            "incorporations": incorporations,
            "dissolutions": dissolutions,
        }

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Companies House data: %s", type(data))
            return

        cell_id = self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON)
        processed = 0

        # Process incorporations
        inc_data = data.get("incorporations")
        if isinstance(inc_data, dict):
            items = inc_data.get("items", [])
            for company in items[:15]:
                processed += await self._process_company(
                    company, "incorporation", cell_id
                )

        # Process dissolutions
        dis_data = data.get("dissolutions")
        if isinstance(dis_data, dict):
            items = dis_data.get("items", [])
            for company in items[:15]:
                processed += await self._process_company(
                    company, "dissolution", cell_id
                )

        # If we got incorporations, fetch officers for the first few
        # to map director relationships
        if isinstance(inc_data, dict):
            items = inc_data.get("items", [])
            for company in items[:5]:
                number = company.get("company_number")
                if number:
                    processed += await self._fetch_officers(number, company, cell_id)

        self.log.info("Companies House: processed=%d events", processed)

    async def _process_company(
        self, company: dict, event_type: str, cell_id: str | None
    ) -> int:
        """Process a single company record."""
        try:
            name = company.get("company_name", "Unknown")
            number = company.get("company_number", "")
            status = company.get("company_status", "")
            company_type = company.get("company_type", "")
            date_of_creation = company.get("date_of_creation", "")
            date_of_cessation = company.get("date_of_cessation", "")
            sic_codes = company.get("sic_codes", [])
            address = company.get("registered_office_address", {})
            locality = address.get("locality", "") if isinstance(address, dict) else ""

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"Company {event_type}: {name} ({number})",
                location_id=cell_id,
                lat=_LONDON_LAT,
                lon=_LONDON_LON,
                metadata={
                    "company_name": name,
                    "company_number": number,
                    "event_type": event_type,
                    "company_status": status,
                    "company_type": company_type,
                    "date_of_creation": date_of_creation,
                    "date_of_cessation": date_of_cessation,
                    "sic_codes": sic_codes[:5] if sic_codes else [],
                    "locality": locality,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Companies House [{event_type}]: {name} ({number}) — "
                    f"type={company_type}, status={status}"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "company_name": name,
                    "company_number": number,
                    "event_type": event_type,
                    "company_type": company_type,
                    "sic_codes": sic_codes[:5] if sic_codes else [],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            return 1
        except Exception:
            self.log.exception("Error processing company: %s", company.get("company_name"))
            return 0

    async def _fetch_officers(
        self, company_number: str, company: dict, cell_id: str | None
    ) -> int:
        """Fetch officers for a company and post director relationship data."""
        officers_data = await self._api_get(
            f"/company/{company_number}/officers",
            params={"items_per_page": "10"},
        )
        if not isinstance(officers_data, dict):
            return 0

        items = officers_data.get("items", [])
        if not items:
            return 0

        company_name = company.get("company_name", "Unknown")
        processed = 0

        for officer in items[:5]:
            try:
                name = officer.get("name", "Unknown")
                role = officer.get("officer_role", "")
                appointed = officer.get("appointed_on", "")
                resigned = officer.get("resigned_on", "")
                nationality = officer.get("nationality", "")
                occupation = officer.get("occupation", "")

                # Only report active officers
                if resigned:
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"Officer: {name} is {role} at {company_name}",
                    location_id=cell_id,
                    lat=_LONDON_LAT,
                    lon=_LONDON_LON,
                    metadata={
                        "officer_name": name,
                        "officer_role": role,
                        "company_name": company_name,
                        "company_number": company_number,
                        "appointed_on": appointed,
                        "nationality": nationality,
                        "occupation": occupation,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Companies House [officer]: {name} — {role} at "
                        f"{company_name} ({company_number})"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "officer_name": name,
                        "officer_role": role,
                        "company_name": company_name,
                        "company_number": company_number,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1
            except Exception:
                self.log.exception("Error processing officer for %s", company_number)

        return processed


async def ingest_companies_house(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Companies House fetch cycle."""
    ingestor = CompaniesHouseIngestor(board, graph, scheduler)
    ok, missing = CompaniesHouseIngestor.check_keys()
    if not ok:
        log.warning("Companies House API key not set (%s) — skipping", missing)
        return
    await ingestor.run()
