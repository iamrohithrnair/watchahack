"""Public events ingestor — PredictHQ API for festivals, sports, protests, concerts."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.events")

PREDICTHQ_BASE = "https://api.predicthq.com/v1/events/"

# Greater London bounding box (same as config LONDON_BBOX)
LONDON_WITHIN = "51.28,-0.51,51.70,0.33"

# Categories relevant to urban disruption detection
CATEGORIES = [
    "community",
    "concerts",
    "conferences",
    "expos",
    "festivals",
    "performing-arts",
    "politics",
    "protests",
    "sports",
]


class EventsIngestor(BaseIngestor):
    source_name = "public_events"
    rate_limit_name = "default"
    required_env_vars = ["PREDICTHQ_API_KEY"]

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("PREDICTHQ_API_KEY", "")
        if not api_key:
            self.log.warning("PREDICTHQ_API_KEY not set, skipping events ingest")
            return None

        now = datetime.now(timezone.utc)
        # Fetch events happening today through next 3 days
        params = {
            "within": LONDON_WITHIN,
            "category": ",".join(CATEGORIES),
            "active.gte": now.strftime("%Y-%m-%d"),
            "active.lte": (now + timedelta(days=3)).strftime("%Y-%m-%d"),
            "sort": "start",
            "limit": 100,
        }
        headers = {"Authorization": f"Bearer {api_key}"}
        return await self.fetch(PREDICTHQ_BASE, params=params, headers=headers)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected PredictHQ response type: %s", type(data))
            return

        results = data.get("results", [])
        processed = 0

        for event in results:
            title = event.get("title", "Unknown event")
            category = event.get("category", "unknown")
            start = event.get("start", "")
            end = event.get("end", "")
            rank = event.get("rank", 0)  # 0-100 significance
            labels = event.get("labels", [])

            # Location: PredictHQ returns [lon, lat] in GeoJSON order
            location = event.get("location", [])
            if len(location) == 2:
                lon, lat = location[0], location[1]
            else:
                lat = lon = None

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            # Estimated attendance (PHQ attendance field)
            phq_attendance = event.get("phq_attendance", None)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=rank,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": title,
                    "category": category,
                    "start": start,
                    "end": end,
                    "rank": rank,
                    "labels": labels,
                    "phq_attendance": phq_attendance,
                    "event_id": event.get("id", ""),
                },
            )
            await self.board.store_observation(obs)

            attendance_str = f", ~{phq_attendance} attendees" if phq_attendance else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Planned event [{category}] \"{title}\" "
                    f"(rank={rank}{attendance_str}, {start[:10]})"
                ),
                data={
                    "title": title,
                    "category": category,
                    "start": start,
                    "end": end,
                    "rank": rank,
                    "labels": labels,
                    "phq_attendance": phq_attendance,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Public events: processed=%d from PredictHQ", processed)


async def ingest_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one PredictHQ fetch cycle."""
    ingestor = EventsIngestor(board, graph, scheduler)
    await ingestor.run()
