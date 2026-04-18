"""Windy Webcams ingestor — fetches London-area webcam snapshots.

Image URLs from the Windy API expire after 10 minutes on the free tier,
so we download each image immediately and store it locally alongside the
observation.
"""

from __future__ import annotations

import logging
import os
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.image_store import ImageStore
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.windy_webcams")

WINDY_API_BASE = "https://api.windy.com/webcams/api/v3/webcams"

# Search 25km radius around central London
LONDON_LAT = 51.5074
LONDON_LON = -0.1278
SEARCH_RADIUS_KM = 25


class WindyWebcamsIngestor(BaseIngestor):
    source_name = "windy_webcam"
    rate_limit_name = "default"

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        super().__init__(board, graph, scheduler)
        self._api_key = os.environ.get("WINDY_API_KEY", "")
        self._image_store = ImageStore()
        if not self._api_key:
            self.log.warning("WINDY_API_KEY not set — windy webcams ingestor will be disabled")

    async def fetch_data(self) -> Any:
        if not self._api_key:
            return None

        headers = {"x-windy-api-key": self._api_key}
        params = {
            "nearby": f"{LONDON_LAT},{LONDON_LON},{SEARCH_RADIUS_KM}",
            "include": "images,location",
            "limit": "50",
        }
        return await self.fetch(
            WINDY_API_BASE,
            params=params,
            headers=headers,
        )

    async def process(self, data: Any) -> None:
        if not data:
            return

        webcams = data.get("webcams", [])
        if not webcams:
            self.log.info("No webcams returned from Windy API")
            return

        processed = 0
        skipped = 0

        for cam in webcams:
            try:
                cam_id = cam.get("webcamId") or cam.get("id", "unknown")
                title = cam.get("title", f"Webcam {cam_id}")
                status = cam.get("status", "")

                # Skip inactive cameras
                if status not in ("active", ""):
                    skipped += 1
                    continue

                # Extract location
                location = cam.get("location", {})
                lat = location.get("latitude")
                lon = location.get("longitude")
                city = location.get("city", "")
                country = location.get("country", "")

                if lat is None or lon is None:
                    skipped += 1
                    continue

                # Extract image URL — download immediately (expires in 10 min)
                images = cam.get("images", {})
                current = images.get("current", {})
                image_url = current.get("preview") or current.get("thumbnail") or ""

                # Try daylight image as fallback
                if not image_url:
                    daylight = images.get("daylight", {})
                    image_url = daylight.get("preview") or daylight.get("thumbnail") or ""

                image_bytes = None
                if image_url:
                    image_bytes = await self._download_image(image_url)

                cell_id = self.graph.latlon_to_cell(float(lat), float(lon))

                # Save image locally if we got it
                local_filename = None
                if image_bytes:
                    local_filename = f"windy_{cam_id}"
                    thumb = self._image_store.store_thumbnail(image_bytes, local_filename)
                    self._image_store.store_full(image_bytes, local_filename)
                    if not thumb:
                        local_filename = None

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.IMAGE,
                    value=image_url,
                    location_id=cell_id,
                    lat=float(lat),
                    lon=float(lon),
                    metadata={
                        "camera_id": str(cam_id),
                        "name": title,
                        "city": city,
                        "country": country,
                        "image_url": image_url,
                        "local_filename": local_filename,
                        "status": status,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Windy Webcam [{title}] lat={lat:.4f} lon={lon:.4f}"
                        + (f" image={'saved' if local_filename else 'no image'}")
                    ),
                    data={
                        "camera_id": str(cam_id),
                        "lat": lat,
                        "lon": lon,
                        "image_url": image_url,
                        "local_filename": local_filename,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing Windy webcam: %s", cam.get("webcamId"))
                skipped += 1

        self.log.info(
            "Windy Webcams: processed=%d skipped=%d total_from_api=%d",
            processed, skipped, len(webcams),
        )

    async def _download_image(self, url: str) -> bytes | None:
        """Download an image before the token expires."""
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        data = await resp.read()
                        if len(data) > 500:  # skip tiny/broken responses
                            return data
        except Exception as exc:
            self.log.debug("Failed to download webcam image: %s", exc)
        return None


async def ingest_windy_webcams(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Windy Webcams fetch cycle."""
    ingestor = WindyWebcamsIngestor(board, graph, scheduler)
    await ingestor.run()
