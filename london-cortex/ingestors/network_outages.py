"""Cloudflare Radar network outage ingestor — internet/ISP disruptions affecting London.

Uses the free Cloudflare Radar API to fetch:
1. Confirmed outages (annotations) filtered to GB
2. Traffic anomalies for major UK ISPs/mobile networks

This provides an independent data layer to help classify sensor anomalies as
either 'real-world event' or 'data transmission failure'.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.network_outages")

# Cloudflare Radar API — free, CC BY-NC 4.0
_BASE = "https://api.cloudflare.com/client/v4/radar"
_OUTAGES_URL = f"{_BASE}/annotations/outages"
_ANOMALIES_URL = f"{_BASE}/traffic_anomalies"

# Major UK ISPs and mobile carriers by ASN
_UK_ASNS = {
    "2856": "BT",
    "5089": "Virgin Media",
    "6871": "Sky Broadband",
    "12576": "EE",
    "15169": "Google (UK)",
    "29518": "Bredband2 (Three UK)",
    "34984": "Three UK",
    "25135": "Vodafone UK",
    "20712": "Andrews & Arnold",
    "13285": "TalkTalk",
}

# Central London coordinates for observations without specific location
_LONDON_CENTER = (51.5074, -0.1278)


class NetworkOutageIngestor(BaseIngestor):
    source_name = "network_outages"
    rate_limit_name = "default"
    required_env_vars = ["CLOUDFLARE_RADAR_API_TOKEN"]

    def _headers(self) -> dict[str, str]:
        token = os.environ.get("CLOUDFLARE_RADAR_API_TOKEN", "")
        return {"Authorization": f"Bearer {token}"}

    async def fetch_data(self) -> Any:
        """Fetch both outages and traffic anomalies for UK."""
        outages = await self.fetch(
            _OUTAGES_URL,
            params={"location": "GB", "dateRange": "1d", "format": "json", "limit": "20"},
            headers=self._headers(),
        )
        anomalies = await self.fetch(
            _ANOMALIES_URL,
            params={"location": "GB", "dateRange": "1d", "format": "json", "limit": "20"},
            headers=self._headers(),
        )
        if outages is None and anomalies is None:
            return None
        return {"outages": outages, "anomalies": anomalies}

    async def process(self, data: Any) -> None:
        processed = 0

        # --- Confirmed outages ---
        outages_resp = data.get("outages")
        if outages_resp and isinstance(outages_resp, dict):
            annotations = (
                outages_resp.get("result", {}).get("annotations", [])
            )
            for ann in annotations:
                outage_type = ann.get("outageType", "unknown")
                cause = ann.get("outageCause", "unknown")
                start = ann.get("startDate", "")
                end = ann.get("endDate", "")
                scope = ann.get("scope", "")
                description = ann.get("description", "")
                locations = ann.get("locations", [])
                asns = ann.get("asns", [])

                # Build human-readable scope
                asn_names = []
                for asn_info in asns:
                    asn_num = str(asn_info.get("asn", ""))
                    name = asn_info.get("name", _UK_ASNS.get(asn_num, f"AS{asn_num}"))
                    asn_names.append(name)

                location_names = [loc.get("name", loc.get("code", "")) for loc in locations]

                severity = self._outage_severity(outage_type, cause)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=severity,
                    location_id=self.graph.latlon_to_cell(*_LONDON_CENTER),
                    lat=_LONDON_CENTER[0],
                    lon=_LONDON_CENTER[1],
                    metadata={
                        "event_type": "outage",
                        "outage_type": outage_type,
                        "cause": cause,
                        "scope": scope,
                        "start": start,
                        "end": end,
                        "asns": asn_names,
                        "locations": location_names,
                        "description": description,
                    },
                )
                await self.board.store_observation(obs)
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Network outage [GB]: {outage_type} caused by {cause} | "
                        f"ASNs: {', '.join(asn_names) or 'unknown'} | "
                        f"Scope: {scope} | {start}"
                    ),
                    data={
                        "event_type": "outage",
                        "outage_type": outage_type,
                        "cause": cause,
                        "asns": asn_names,
                        "severity": severity,
                        "observation_id": obs.id,
                    },
                    location_id=self.graph.latlon_to_cell(*_LONDON_CENTER),
                ))
                processed += 1

        # --- Traffic anomalies ---
        anomalies_resp = data.get("anomalies")
        if anomalies_resp and isinstance(anomalies_resp, dict):
            anomaly_list = (
                anomalies_resp.get("result", {}).get("trafficAnomalies", [])
            )
            for anom in anomaly_list:
                status = anom.get("status", "unverified")
                start = anom.get("startDate", "")
                end = anom.get("endDate", "")
                anom_type = anom.get("type", "unknown")  # location or asn

                asn_details = anom.get("asnDetails", {})
                location_details = anom.get("locationDetails", {})

                asn_num = str(asn_details.get("asn", ""))
                asn_name = asn_details.get("name", _UK_ASNS.get(asn_num, ""))
                loc_name = location_details.get("name", "")
                loc_code = location_details.get("code", "")

                entity = asn_name or loc_name or loc_code or "UK network"

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=3 if status == "verified" else 1,
                    location_id=self.graph.latlon_to_cell(*_LONDON_CENTER),
                    lat=_LONDON_CENTER[0],
                    lon=_LONDON_CENTER[1],
                    metadata={
                        "event_type": "traffic_anomaly",
                        "anomaly_type": anom_type,
                        "entity": entity,
                        "asn": asn_num,
                        "status": status,
                        "start": start,
                        "end": end,
                    },
                )
                await self.board.store_observation(obs)
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Traffic anomaly [GB]: {entity} | "
                        f"Type: {anom_type} | Status: {status} | {start}"
                    ),
                    data={
                        "event_type": "traffic_anomaly",
                        "entity": entity,
                        "status": status,
                        "asn": asn_num,
                        "observation_id": obs.id,
                    },
                    location_id=self.graph.latlon_to_cell(*_LONDON_CENTER),
                ))
                processed += 1

        self.log.info("Network outages: processed=%d events", processed)

    @staticmethod
    def _outage_severity(outage_type: str, cause: str) -> float:
        """Map outage type/cause to a severity score (1-5)."""
        type_scores = {
            "NATIONWIDE": 5,
            "REGIONAL": 3,
            "NETWORK": 2,
        }
        cause_scores = {
            "POWER_OUTAGE": 4,
            "NATURAL_DISASTER": 5,
            "GOVERNMENT_ORDER": 4,
            "WEATHER": 3,
            "CABLE_CUT": 3,
            "TECHNICAL": 2,
            "MAINTENANCE": 1,
        }
        t = type_scores.get(outage_type.upper(), 2)
        c = cause_scores.get(cause.upper(), 2)
        return max(t, c)


async def ingest_network_outages(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Cloudflare Radar fetch cycle."""
    ingestor = NetworkOutageIngestor(board, graph, scheduler)
    await ingestor.run()
