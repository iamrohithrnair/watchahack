"""TfL JamCam ingestor — fetches camera locations and image URLs."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl")

TFL_JAMCAM_URL = "https://api.tfl.gov.uk/Place/Type/JamCam"


class TflJamCamIngestor(BaseIngestor):
    source_name = "tfl_jamcam"
    rate_limit_name = "tfl"

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        scheduler: AsyncScheduler,
        memory: MemoryManager | None = None,
    ) -> None:
        super().__init__(board, graph, scheduler)
        self.memory = memory

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_JAMCAM_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected TfL response type: %s", type(data))
            return

        # Load dead camera set for filtering
        dead_ids: set[str] = set()
        if self.memory:
            health = self.memory.get_camera_health()
            dead_ids = {cid for cid, info in health.items() if info.get("status") == "dead"}

        processed = 0
        skipped = 0
        skipped_dead = 0
        for camera in data:
            try:
                cam_id = camera.get("id", "unknown")

                # Skip dead cameras entirely
                if cam_id in dead_ids:
                    skipped_dead += 1
                    continue

                lat = camera.get("lat") or camera.get("Lat")
                lon = camera.get("lon") or camera.get("Lon")
                common_name = camera.get("commonName", cam_id)

                # Extract image URL from additionalProperties
                image_url: str | None = None
                add_props = camera.get("additionalProperties", [])
                for prop in add_props:
                    if prop.get("key") in ("imageUrl", "ImageUrl", "image"):
                        image_url = prop.get("value")
                        break

                if lat is None or lon is None:
                    skipped += 1
                    continue

                cell_id = self.graph.latlon_to_cell(float(lat), float(lon))

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.IMAGE,
                    value=image_url or "",
                    location_id=cell_id,
                    lat=float(lat),
                    lon=float(lon),
                    metadata={
                        "camera_id": cam_id,
                        "name": common_name,
                        "image_url": image_url,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"JamCam [{common_name}] lat={lat:.4f} lon={lon:.4f}"
                        + (f" image={image_url}" if image_url else " (no image)")
                    ),
                    data={
                        "camera_id": cam_id,
                        "lat": lat,
                        "lon": lon,
                        "image_url": image_url,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing JamCam entry: %s", camera.get("id"))
                skipped += 1

        self.log.info(
            "TfL JamCam: processed=%d skipped_dead=%d skipped_other=%d",
            processed, skipped_dead, skipped,
        )


async def ingest_tfl(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
    memory: MemoryManager | None = None,
) -> None:
    """Standalone async ingest function for one TfL JamCam fetch cycle."""
    ingestor = TflJamCamIngestor(board, graph, scheduler, memory=memory)
    await ingestor.run()
