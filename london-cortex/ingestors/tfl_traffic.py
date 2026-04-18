"""TfL Road Corridor traffic status ingestor — live status for major London roads.

Fetches real-time traffic status (statusSeverity) for all TfL-managed road
corridors (A-roads, tunnels, ring roads). Complements:
- tfl_jamcam: visual confirmation of conditions
- tfl_road_disruptions: specific incidents/causes

This provides the quantitative corridor-level status that enables predictions
like "A102 approach experiencing delays" with automated verification.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl_traffic")

# Returns all road corridors with live status — no API key required
TFL_ROAD_URL = "https://api.tfl.gov.uk/Road"

# Map TfL statusSeverity strings to numeric scores for anomaly detection
_STATUS_SCORE = {
    "Good": 0,
    "Minor Delays": 1,
    "Moderate Delays": 2,
    "Severe Delays": 3,
    "Closure": 4,
}


class TflTrafficIngestor(BaseIngestor):
    source_name = "tfl_traffic"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_ROAD_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected TfL Road response type: %s", type(data))
            return

        processed = 0

        for corridor in data:
            road_id = corridor.get("id", "unknown")
            display_name = corridor.get("displayName", road_id)
            status = corridor.get("statusSeverity", "Unknown")
            status_desc = corridor.get("statusSeverityDescription", "")
            score = _STATUS_SCORE.get(status, 2)

            # Extract centroid from bounds for spatial indexing
            bounds = corridor.get("bounds", "")
            lat, lon = self._centroid_from_bounds(bounds)
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "road_id": road_id,
                    "road_name": display_name,
                    "status": status,
                    "status_description": status_desc,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Traffic [{display_name}] status={status} ({status_desc})"
                ),
                data={
                    "road_id": road_id,
                    "road_name": display_name,
                    "status": status,
                    "status_description": status_desc,
                    "score": score,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("TfL traffic corridors: processed=%d", processed)

    @staticmethod
    def _centroid_from_bounds(bounds: Any) -> tuple[float | None, float | None]:
        """Compute centroid lat/lon from TfL bounds field.

        Bounds is a string like "[[-0.25,51.53],[-0.10,51.65]]" or a list.
        """
        try:
            if isinstance(bounds, str):
                import json
                bounds = json.loads(bounds)
            if isinstance(bounds, list) and len(bounds) >= 2:
                # bounds is [[lon_min, lat_min], [lon_max, lat_max]]
                lon_min, lat_min = float(bounds[0][0]), float(bounds[0][1])
                lon_max, lat_max = float(bounds[1][0]), float(bounds[1][1])
                return (lat_min + lat_max) / 2, (lon_min + lon_max) / 2
        except (ValueError, TypeError, IndexError, KeyError):
            pass
        return None, None


async def ingest_tfl_traffic(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL road traffic status cycle."""
    ingestor = TflTrafficIngestor(board, graph, scheduler)
    await ingestor.run()
