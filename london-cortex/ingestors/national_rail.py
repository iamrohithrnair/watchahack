"""National Rail Darwin LDBWS ingestor — real-time train departures at London terminals.

Uses the Darwin OpenLDBWS SOAP API to fetch departure boards for major London
rail terminals.  Extracts delay minutes, cancellation counts, and on-time
percentages as numeric observations, providing a parallel transport data stream
to corroborate or refute TfL anomalies.

Requires a free Darwin API token:
  https://realtime.nationalrail.co.uk/OpenLDBWSRegistration
Set DARWIN_API_KEY in .env.
"""

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.national_rail")

DARWIN_LDBWS_URL = "https://lite.realtime.nationalrail.co.uk/OpenLDBWS/ldb12.asmx"

# Huxley2 — free, keyless REST proxy to the same Darwin data (JSON instead of SOAP)
HUXLEY2_BASE = "https://huxley2.azurewebsites.net"

# Major London terminal stations: (CRS code, name, lat, lon)
LONDON_TERMINALS = [
    ("WAT", "London Waterloo", 51.5031, -0.1132),
    ("VIC", "London Victoria", 51.4952, -0.1439),
    ("LBG", "London Bridge", 51.5052, -0.0861),
    ("LST", "London Liverpool Street", 51.5178, -0.0823),
    ("PAD", "London Paddington", 51.5154, -0.1755),
    ("EUS", "London Euston", 51.5282, -0.1337),
    ("KGX", "London King's Cross", 51.5320, -0.1240),
    ("STP", "London St Pancras", 51.5322, -0.1260),
]

# SOAP envelope template for GetDepartureBoard
_SOAP_TEMPLATE = """<?xml version="1.0" encoding="utf-8"?>
<soap:Envelope xmlns:soap="http://www.w3.org/2003/05/soap-envelope"
               xmlns:typ="http://thalesgroup.com/RTTI/2013-11-28/Token/types"
               xmlns:ldb="http://thalesgroup.com/RTTI/2017-10-01/ldb/">
  <soap:Header>
    <typ:AccessToken>
      <typ:TokenValue>{token}</typ:TokenValue>
    </typ:AccessToken>
  </soap:Header>
  <soap:Body>
    <ldb:GetDepartureBoardRequest>
      <ldb:numRows>20</ldb:numRows>
      <ldb:crs>{crs}</ldb:crs>
    </ldb:GetDepartureBoardRequest>
  </soap:Body>
</soap:Envelope>"""

# XML namespaces in the Darwin response
_NS = {
    "soap": "http://www.w3.org/2003/05/soap-envelope",
    "lt7": "http://thalesgroup.com/RTTI/2017-10-01/ldb/types",
    "lt4": "http://thalesgroup.com/RTTI/2015-11-27/ldb/types",
}


def _parse_delay_minutes(etd: str) -> float | None:
    """Parse estimated departure into delay minutes.

    Returns 0 for 'On time', None for 'Cancelled'/'Delayed'/'No report',
    and parsed minutes for 'HH:MM' strings compared to scheduled.
    """
    etd_lower = etd.strip().lower()
    if etd_lower == "on time":
        return 0.0
    if etd_lower in ("cancelled", "delayed", "no report", ""):
        return None
    return None  # We can't compute delay without scheduled time in simple mode


class NationalRailIngestor(BaseIngestor):
    source_name = "national_rail"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self._api_key = os.environ.get("DARWIN_API_KEY", "")

    async def fetch_data(self) -> Any:
        if not self._api_key:
            self.log.info(
                "DARWIN_API_KEY not set — using Huxley2 free REST proxy"
            )
            return await self._fetch_huxley2()

        results = {}
        for crs, name, lat, lon in LONDON_TERMINALS:
            soap_body = _SOAP_TEMPLATE.format(token=self._api_key, crs=crs)
            try:
                resp_text = await self._soap_request(soap_body)
                if resp_text:
                    results[crs] = {
                        "xml": resp_text,
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                    }
            except Exception:
                self.log.exception("Failed to fetch departures for %s (%s)", name, crs)

        return results if results else None

    async def _fetch_huxley2(self) -> dict | None:
        """Fetch departure data via Huxley2 — free Darwin REST proxy, no key."""
        results = {}
        for crs, name, lat, lon in LONDON_TERMINALS:
            url = f"{HUXLEY2_BASE}/departures/{crs}/10"
            try:
                data = await self.fetch(url)
                if data and isinstance(data, dict):
                    results[crs] = {
                        "huxley2": data,
                        "name": name,
                        "lat": lat,
                        "lon": lon,
                    }
            except Exception:
                self.log.exception("Huxley2 failed for %s (%s)", name, crs)
        return results if results else None

    async def _soap_request(self, body: str) -> str | None:
        """Send SOAP request and return raw XML text."""
        import aiohttp

        headers = {"Content-Type": "application/soap+xml; charset=utf-8"}
        timeout = aiohttp.ClientTimeout(total=15, connect=10)

        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.post(DARWIN_LDBWS_URL, data=body, headers=headers) as resp:
                if resp.status == 200:
                    return await resp.text()
                self.log.warning("Darwin SOAP HTTP %d", resp.status)
                return None

    @staticmethod
    def _parse_xml_services(xml_text: str) -> tuple[int, int, int, int] | None:
        """Parse Darwin SOAP XML into (on_time, delayed, cancelled, total)."""
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None

        services = root.findall(
            ".//lt7:trainServices/lt7:service", _NS
        ) or root.findall(
            ".//lt4:trainServices/lt4:service", _NS
        )

        on_time = delayed = cancelled = 0
        total = len(services)
        for svc in services:
            etd_el = svc.find("lt7:etd", _NS) or svc.find("lt4:etd", _NS)
            etd = etd_el.text.strip() if etd_el is not None and etd_el.text else ""
            etd_lower = etd.lower()
            if etd_lower == "on time":
                on_time += 1
            elif etd_lower == "cancelled":
                cancelled += 1
            else:
                delayed += 1
        return on_time, delayed, cancelled, total

    @staticmethod
    def _parse_huxley2_services(data: dict) -> tuple[int, int, int, int]:
        """Parse Huxley2 JSON departure board into (on_time, delayed, cancelled, total)."""
        services = data.get("trainServices") or []
        on_time = delayed = cancelled = 0
        total = len(services)
        for svc in services:
            etd = (svc.get("etd") or "").strip().lower()
            if etd == "on time":
                on_time += 1
            elif etd == "cancelled":
                cancelled += 1
            else:
                delayed += 1
        return on_time, delayed, cancelled, total

    async def process(self, data: Any) -> None:
        total_processed = 0

        for crs, station_data in data.items():
            name = station_data["name"]
            lat = station_data["lat"]
            lon = station_data["lon"]
            cell_id = self.graph.latlon_to_cell(lat, lon)

            if "xml" in station_data:
                parsed = self._parse_xml_services(station_data["xml"])
                if parsed is None:
                    self.log.warning("Failed to parse XML for %s", name)
                    continue
                on_time, delayed, cancelled, total = parsed
                via = "darwin"
            elif "huxley2" in station_data:
                on_time, delayed, cancelled, total = self._parse_huxley2_services(
                    station_data["huxley2"]
                )
                via = "huxley2"
            else:
                continue

            if total == 0:
                continue

            on_time_pct = (on_time / total) * 100 if total > 0 else 0.0

            # Store on-time percentage as main observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=on_time_pct,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "station": name,
                    "crs": crs,
                    "metric": "on_time_pct",
                    "total_services": total,
                    "on_time": on_time,
                    "delayed": delayed,
                    "cancelled": cancelled,
                    "via": via,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"National Rail [{name}] {on_time}/{total} on-time "
                    f"({on_time_pct:.0f}%), {delayed} delayed, {cancelled} cancelled"
                ),
                data={
                    "station": name,
                    "crs": crs,
                    "on_time_pct": on_time_pct,
                    "total_services": total,
                    "on_time": on_time,
                    "delayed": delayed,
                    "cancelled": cancelled,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            total_processed += 1

            # Also store cancellation count if non-zero
            if cancelled > 0:
                cancel_obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=float(cancelled),
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "station": name,
                        "crs": crs,
                        "metric": "cancellations",
                        "total_services": total,
                    },
                )
                await self.board.store_observation(cancel_obs)

        self.log.info("National Rail: processed %d stations", total_processed)


async def ingest_national_rail(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one National Rail fetch cycle."""
    ingestor = NationalRailIngestor(board, graph, scheduler)
    await ingestor.run()
