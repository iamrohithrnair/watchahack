"""TfL Disruption Incidents ingestor — per-incident cause and impact detail.

Fetches active disruptions from TfL's Line Disruption API, providing granular
per-incident data including cause descriptions, affected routes/stops, and
disruption categories. By parsing the description text, we extract structured
cause types (signal failure, train fault, track defect, etc.) that enable
downstream agents to forecast disruption duration and second-order effects.

Complements planned_works.py (which uses /Line/.../Status for line-level status)
by providing individual incident records with richer cause detail.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl_disruption_incidents")

# TfL Line Disruption endpoint — no API key required
# Returns per-incident disruptions (richer than /Status which gives line summaries)
TFL_DISRUPTION_URL = (
    "https://api.tfl.gov.uk/Line/Mode/"
    "tube,dlr,overground,elizabeth-line,tram/Disruption"
)

# Regex patterns to extract cause from free-text description
_CAUSE_PATTERNS = [
    (re.compile(r"signal\s+(?:failure|problem|fault)", re.I), "signal_failure"),
    (re.compile(r"track\s+(?:fault|defect|failure|problem)", re.I), "track_defect"),
    (re.compile(r"train\s+(?:fault|failure|defect)", re.I), "train_fault"),
    (re.compile(r"points?\s+failure", re.I), "points_failure"),
    (re.compile(r"power\s+(?:failure|supply|cut|outage)", re.I), "power_failure"),
    (re.compile(r"staff\s+(?:shortage|unavailab)", re.I), "staff_shortage"),
    (re.compile(r"person\s+(?:on|under|hit|struck)", re.I), "person_on_track"),
    (re.compile(r"trespass", re.I), "trespass"),
    (re.compile(r"security\s+(?:alert|incident)", re.I), "security_alert"),
    (re.compile(r"customer\s+(?:incident|action)", re.I), "customer_incident"),
    (re.compile(r"(?:fire\s+alert|fire\s+alarm|fire\b)", re.I), "fire_alert"),
    (re.compile(r"flood", re.I), "flooding"),
    (re.compile(r"engineering\s+work", re.I), "engineering_works"),
    (re.compile(r"planned\s+closure", re.I), "planned_closure"),
    (re.compile(r"overcrowding", re.I), "overcrowding"),
    (re.compile(r"(?:late\s+finish|overrun)", re.I), "overrun"),
]

# Map disruption type/category to severity
_CATEGORY_SEVERITY = {
    "RealTime": 4,
    "PlannedWork": 2,
    "Information": 1,
}


def _extract_cause(description: str) -> str:
    """Extract structured cause type from free-text description."""
    for pattern, cause in _CAUSE_PATTERNS:
        if pattern.search(description):
            return cause
    return "unknown"


def _extract_affected_stations(description: str) -> list[str]:
    """Extract station names mentioned as affected from description text."""
    # Look for patterns like "between X and Y" or "at X station"
    stations: list[str] = []
    between = re.search(r"between\s+(.+?)\s+and\s+(.+?)(?:\s+station|\s*[.,])", description, re.I)
    if between:
        stations.extend([between.group(1).strip(), between.group(2).strip()])
    at_station = re.findall(r"at\s+(\w[\w\s]*?)\s+(?:station|Street)", description, re.I)
    stations.extend(s.strip() for s in at_station)
    return stations[:10]


class TflDisruptionIncidentIngestor(BaseIngestor):
    source_name = "tfl_disruption_incidents"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_DISRUPTION_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        processed = 0

        for incident in data:
            category = incident.get("category", "")
            category_desc = incident.get("categoryDescription", category)
            description = incident.get("description", "")
            closure_text = incident.get("closureText", "")
            disruption_type = incident.get("type", "")

            severity_score = _CATEGORY_SEVERITY.get(category, 3)
            cause = _extract_cause(description)
            affected_stations = _extract_affected_stations(description)

            # Extract affected routes/stops from structured fields
            affected_routes = []
            affected_stops = []
            for route in incident.get("affectedRoutes", []) or []:
                for section in route.get("routeSections", []) or []:
                    orig = section.get("originator", "")
                    dest = section.get("destination", "")
                    if orig and dest:
                        affected_routes.append(f"{orig} to {dest}")

            for stop in incident.get("affectedStops", []) or []:
                name = stop.get("commonName", "")
                if name:
                    affected_stops.append(name)

            # Combine structured and text-extracted stations
            all_affected = list(dict.fromkeys(affected_stops + affected_stations))

            is_realtime = category == "RealTime"

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity_score,
                location_id=None,  # Line-level disruption
                lat=None,
                lon=None,
                metadata={
                    "category": category,
                    "category_description": category_desc,
                    "disruption_type": disruption_type,
                    "cause": cause,
                    "closure_text": closure_text,
                    "description": description[:500],
                    "is_realtime": is_realtime,
                    "affected_routes": affected_routes[:10],
                    "affected_stops": all_affected[:20],
                },
            )
            await self.board.store_observation(obs)

            tag = " [REALTIME]" if is_realtime else " [PLANNED]"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Disruption incident{tag} cause={cause} "
                    f"type={disruption_type} severity={severity_score}"
                    + (f" — {description[:200]}" if description else "")
                ),
                data={
                    "category": category,
                    "cause": cause,
                    "disruption_type": disruption_type,
                    "severity_score": severity_score,
                    "closure_text": closure_text,
                    "is_realtime": is_realtime,
                    "affected_stops": all_affected[:10],
                    "observation_id": obs.id,
                },
                location_id=None,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL disruption incidents: processed=%d active incidents", processed
        )


async def ingest_tfl_disruption_incidents(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL disruption incident fetch cycle."""
    ingestor = TflDisruptionIncidentIngestor(board, graph, scheduler)
    await ingestor.run()
