"""Sentinel-2 satellite imagery stub — searches Planetary Computer for recent imagery."""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.satellite")

# London bounding box [west, south, east, north]
LONDON_BBOX = [-0.51, 51.28, 0.33, 51.70]
PLANETARY_COMPUTER_CATALOG = "https://planetarycomputer.microsoft.com/api/stac/v1"
SENTINEL2_COLLECTION = "sentinel-2-l2a"

LONDON_LAT = 51.49  # centroid
LONDON_LON = -0.09


def _search_sentinel2_sync() -> list[dict[str, Any]]:
    """Search Planetary Computer for recent Sentinel-2 scenes (sync, runs in thread)."""
    try:
        import pystac_client  # type: ignore
    except ImportError:
        log.error("pystac-client not installed; skipping Sentinel-2 search")
        return []

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)
    date_range = f"{week_ago.strftime('%Y-%m-%d')}/{now.strftime('%Y-%m-%d')}"

    try:
        catalog = pystac_client.Client.open(PLANETARY_COMPUTER_CATALOG)
        search = catalog.search(
            collections=[SENTINEL2_COLLECTION],
            bbox=LONDON_BBOX,
            datetime=date_range,
            query={"eo:cloud_cover": {"lt": 50}},
            max_items=10,
        )
        items = list(search.items())
        results = []
        for item in items:
            results.append({
                "id": item.id,
                "datetime": item.datetime.isoformat() if item.datetime else None,
                "cloud_cover": item.properties.get("eo:cloud_cover"),
                "bbox": list(item.bbox) if item.bbox else None,
                "platform": item.properties.get("platform"),
                "thumbnail": item.assets.get("thumbnail", item.assets.get("overview", {})).get("href"),
            })
        return results
    except Exception as exc:
        log.warning("Planetary Computer search failed: %s", exc)
        return []


class SentinelIngestor(BaseIngestor):
    source_name = "sentinel2"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Search for Sentinel-2 imagery in a thread executor (pystac-client is sync)."""
        return await asyncio.to_thread(_search_sentinel2_sync)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected Sentinel-2 search result: %s", type(data))
            return

        if not data:
            self.log.info("No Sentinel-2 scenes found for London in the past 7 days")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        for scene in data:
            scene_id = scene.get("id", "unknown")
            scene_dt = scene.get("datetime", "")
            cloud_cover = scene.get("cloud_cover")
            platform = scene.get("platform", "")
            thumbnail = scene.get("thumbnail")

            self.log.info(
                "Sentinel-2 scene available: id=%s date=%s cloud=%.1f%% platform=%s",
                scene_id,
                scene_dt,
                cloud_cover if cloud_cover is not None else -1,
                platform,
            )

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.IMAGE,
                value=thumbnail or scene_id,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "scene_id": scene_id,
                    "datetime": scene_dt,
                    "cloud_cover": cloud_cover,
                    "platform": platform,
                    "bbox": scene.get("bbox"),
                    "thumbnail": thumbnail,
                    "status": "available_not_downloaded",
                },
            )
            await self.board.store_observation(obs)

            cloud_str = f"{cloud_cover:.0f}%" if cloud_cover is not None else "unknown"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Sentinel-2 [{scene_id}] date={scene_dt} cloud={cloud_str} "
                    f"platform={platform} [metadata only, not downloaded]"
                ),
                data={
                    "scene_id": scene_id,
                    "datetime": scene_dt,
                    "cloud_cover": cloud_cover,
                    "platform": platform,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        self.log.info(
            "Sentinel-2: found %d scenes over London in the past 7 days", len(data)
        )


async def ingest_satellite(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Sentinel-2 search cycle."""
    ingestor = SentinelIngestor(board, graph, scheduler)
    await ingestor.run()
