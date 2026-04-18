"""Open Plaques ingestor — blue plaques and commemorative markers across London."""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.open_plaques")

# Open Plaques API — London area
_BASE_URL = "https://openplaques.org"
_PLAQUES_URL = f"{_BASE_URL}/plaques.json"

# London bounding box pages to rotate through
# The API paginates, so we rotate pages to get different plaques each cycle
_MAX_PAGE = 50  # London has ~1000+ plaques


class OpenPlaquesIngestor(BaseIngestor):
    source_name = "open_plaques"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        page = random.randint(1, _MAX_PAGE)
        return await self.fetch(_PLAQUES_URL, params={"page": str(page)})

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.info("Unexpected Open Plaques response: %s", type(data))
            return

        if not data:
            self.log.info("Open Plaques returned 0 results")
            return

        processed = 0
        for plaque in data[:15]:
            try:
                plaque_id = plaque.get("id", "")
                inscription = plaque.get("inscription", "")
                colour = plaque.get("colour_name", "") or plaque.get("colour", "")
                erected_at = plaque.get("erected_at", "")
                is_current = plaque.get("is_current", True)

                # Location
                lat = None
                lon = None
                address = ""
                geo = plaque.get("geolocated", False)
                if plaque.get("latitude") and plaque.get("longitude"):
                    try:
                        lat = float(plaque["latitude"])
                        lon = float(plaque["longitude"])
                    except (ValueError, TypeError):
                        pass

                # People commemorated
                people = []
                for person in plaque.get("people", []):
                    if isinstance(person, dict):
                        people.append(person.get("full_name", ""))

                # Location details
                location = plaque.get("location", "")
                if isinstance(location, dict):
                    address = location.get("name", "")

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                # Trim inscription for display
                short_inscription = inscription[:150] + "..." if len(inscription) > 150 else inscription

                obs = Observation(
                    source="open_plaques",
                    obs_type=ObservationType.GEO,
                    value=short_inscription,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "plaque_id": plaque_id,
                        "colour": colour,
                        "erected_at": erected_at,
                        "people": people,
                        "is_current": is_current,
                        "full_inscription": inscription,
                    },
                )
                await self.board.store_observation(obs)

                people_str = ", ".join(people[:3]) if people else "unknown"
                msg = AgentMessage(
                    from_agent="open_plaques",
                    channel="#raw",
                    content=(
                        f"Blue plaque: {people_str} — "
                        f"{short_inscription}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "geo",
                        "plaque_id": plaque_id,
                        "people": people,
                        "colour": colour,
                        "erected_at": erected_at,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing plaque: %s", plaque.get("id"))

        self.log.info("Open Plaques: processed=%d", processed)


async def ingest_open_plaques(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = OpenPlaquesIngestor(board, graph, scheduler)
    await ingestor.run()
