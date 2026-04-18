"""V&A Museum collection ingestor — objects, exhibitions, and cultural data."""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.vam_museum")

# V&A API v2
_BASE_URL = "https://api.vam.ac.uk/v2"

# Interesting search terms to rotate through — keeps data diverse
_SEARCH_TERMS = [
    "London", "Thames", "Westminster", "Victorian", "Tudor",
    "railway", "textile", "ceramic", "photograph", "silver",
    "fashion", "jewellery", "architecture", "poster", "sculpture",
]

# V&A is at 51.4966, -0.1722 (South Kensington)
_VAM_LAT = 51.4966
_VAM_LON = -0.1722


class VAMMuseumIngestor(BaseIngestor):
    source_name = "vam_museum"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        term = random.choice(_SEARCH_TERMS)
        params = {
            "q": term,
            "page_size": "15",
            "order_sort": "asc",
            "images_exist": "true",
        }
        data = await self.fetch(f"{_BASE_URL}/objects/search", params=params)
        if isinstance(data, dict):
            data["_search_term"] = term
        return data

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.info("Unexpected V&A response type: %s", type(data))
            return

        records = data.get("records", [])
        search_term = data.get("_search_term", "unknown")
        if not records:
            self.log.info("V&A returned 0 records for '%s'", search_term)
            return

        cell_id = self.graph.latlon_to_cell(_VAM_LAT, _VAM_LON)
        processed = 0

        for rec in records[:15]:
            try:
                sys_num = rec.get("systemNumber", "")
                title_parts = rec.get("_primaryTitle", "") or rec.get("objectType", "")
                obj_type = rec.get("objectType", "")
                date_text = rec.get("_primaryDate", "")
                maker = rec.get("_primaryMaker", {}).get("name", "") if isinstance(rec.get("_primaryMaker"), dict) else ""
                place = rec.get("_primaryPlace", "")
                materials = rec.get("_primaryMaterial", "")
                categories = [c.get("text", "") for c in rec.get("categories", []) if isinstance(c, dict)]

                # Build image URL if available
                images = rec.get("_images", {})
                image_url = ""
                if isinstance(images, dict):
                    iiif = images.get("_iiif_image_base_url", "")
                    if iiif:
                        image_url = f"{iiif}/full/400,/0/default.jpg"

                obs = Observation(
                    source="vam_museum",
                    obs_type=ObservationType.TEXT,
                    value=f"{title_parts} ({obj_type}, {date_text})",
                    location_id=cell_id,
                    lat=_VAM_LAT,
                    lon=_VAM_LON,
                    metadata={
                        "system_number": sys_num,
                        "object_type": obj_type,
                        "date": date_text,
                        "maker": maker,
                        "place_of_origin": place,
                        "materials": materials,
                        "categories": categories,
                        "image_url": image_url,
                        "search_term": search_term,
                    },
                )
                await self.board.store_observation(obs)

                content = f"V&A object: {title_parts}"
                if maker:
                    content += f" by {maker}"
                if date_text:
                    content += f" ({date_text})"
                if place:
                    content += f" from {place}"

                msg = AgentMessage(
                    from_agent="vam_museum",
                    channel="#raw",
                    content=content,
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "system_number": sys_num,
                        "object_type": obj_type,
                        "date": date_text,
                        "maker": maker,
                        "place_of_origin": place,
                        "materials": materials,
                        "categories": categories,
                        "image_url": image_url,
                        "search_term": search_term,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing V&A record: %s", rec.get("systemNumber"))

        self.log.info("V&A Museum: processed=%d for search='%s'", processed, search_term)


async def ingest_vam_museum(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = VAMMuseumIngestor(board, graph, scheduler)
    await ingestor.run()
