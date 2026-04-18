"""TfL Road Disruption ingestor — accidents, closures, roadworks, planned events.

Fetches active disruptions from TfL's Road API, providing real-time data on the
*causes* of traffic disruption (accidents, hazards, roadworks) rather than just
observing effects through cameras.

Planned works are explicitly tagged (is_planned=True) with time windows so that
downstream agents can cross-reference infrastructure outages (tunnel closures,
camera blackouts) against scheduled maintenance — reducing false positives for
malicious or anomalous activity.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.road_disruptions")

# TfL Road Disruption endpoint — no API key required
TFL_DISRUPTIONS_URL = "https://api.tfl.gov.uk/Road/all/Disruption"

# Map TfL severity to a numeric 1-5 scale for anomaly detection
_SEVERITY_MAP = {
    "Minimal": 1,
    "Moderate": 2,
    "Serious": 3,
    "Severe": 4,
}

# Categories we care about most (leading indicators for traffic)
_PRIORITY_CATEGORIES = {
    "Accident",
    "RealTime",
    "PlannedWork",
    "Closure",
    "EmergencyWorks",
    "SpecialEvent",
}

# Categories that represent scheduled/planned maintenance (vs realtime incidents)
_PLANNED_CATEGORIES = {"PlannedWork"}
_PLANNED_SUBCATEGORIES = {
    "Maintenance",
    "TfL Planned Works",
    "Utility Works",
    "HighwaysEngland Planned Works",
    "PlannedWorks",
}


class RoadDisruptionIngestor(BaseIngestor):
    source_name = "tfl_road_disruptions"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_DISRUPTIONS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        processed = 0
        skipped = 0

        for disruption in data:
            category = disruption.get("category", "")
            severity = disruption.get("severity", "")
            severity_score = _SEVERITY_MAP.get(severity, 2)

            # Extract location — point is "[lon,lat]" format
            point_str = disruption.get("point", "")
            dlat, dlon = None, None

            if isinstance(point_str, str) and point_str.startswith("["):
                try:
                    coords = point_str.strip("[]").split(",")
                    dlon = float(coords[0])
                    dlat = float(coords[1])
                except (ValueError, IndexError):
                    dlat = dlon = None

            cell_id = self.graph.latlon_to_cell(dlat, dlon) if dlat and dlon else None

            location_desc = disruption.get("location", "Unknown location")
            corridor = disruption.get("corridorIds", [])
            road_name = corridor[0] if corridor else ""
            sub_category = disruption.get("subCategory", "")

            is_planned = (
                category in _PLANNED_CATEGORIES
                or sub_category in _PLANNED_SUBCATEGORIES
            )
            is_realtime = category in ("Accident", "RealTime", "EmergencyWorks")

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity_score,
                location_id=cell_id,
                lat=dlat,
                lon=dlon,
                metadata={
                    "category": category,
                    "sub_category": sub_category,
                    "severity": severity,
                    "road": road_name,
                    "location": location_desc,
                    "description": (disruption.get("comments") or "")[:500],
                    "is_realtime": is_realtime,
                    "is_planned": is_planned,
                    "start_time": disruption.get("startDateTime"),
                    "end_time": disruption.get("endDateTime"),
                    "streets": [
                        s.get("name", "") for s in disruption.get("streets", [])
                        if isinstance(s, dict)
                    ][:10],
                },
            )
            await self.board.store_observation(obs)

            # More descriptive messages for priority categories
            if is_realtime:
                priority_tag = " [REALTIME]"
            elif is_planned:
                priority_tag = " [PLANNED]"
            else:
                priority_tag = ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Road disruption{priority_tag} [{road_name or location_desc}] "
                    f"{category}/{sub_category} severity={severity}"
                ),
                data={
                    "category": category,
                    "sub_category": sub_category,
                    "severity": severity,
                    "severity_score": severity_score,
                    "road": road_name,
                    "location": location_desc,
                    "lat": dlat,
                    "lon": dlon,
                    "observation_id": obs.id,
                    "is_realtime": is_realtime,
                    "is_planned": is_planned,
                    "start_time": disruption.get("startDateTime"),
                    "end_time": disruption.get("endDateTime"),
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Road disruptions: processed=%d total active disruptions", processed
        )


async def ingest_road_disruptions(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL road disruption fetch cycle."""
    ingestor = RoadDisruptionIngestor(board, graph, scheduler)
    await ingestor.run()
