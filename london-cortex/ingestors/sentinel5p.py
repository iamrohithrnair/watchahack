"""Sentinel-5P TROPOMI atmospheric composition ingestor — NO2, SO2, O3 via S5P-PAL STAC.

Uses the free, no-auth S5P-PAL Level-3 STAC catalog to find recent gridded
atmospheric composition data over London.  Provides satellite-based air quality
readings independent of the ground-based LAQN sensor network.

Products ingested:
  - Tropospheric column NO2  (nitrogen dioxide)
  - Total column O3          (ozone)
  - Anthropogenic SO2 COBRA  (sulphur dioxide, planetary boundary layer)
"""

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

log = logging.getLogger("cortex.ingestors.sentinel5p")

# S5P-PAL STAC catalog for Level-3 gridded products
S5P_PAL_STAC_URL = "https://data-portal.s5p-pal.com/api/s5p-l3"

# London bounding box [west, south, east, north]
LONDON_BBOX = [-0.51, 51.28, 0.33, 51.70]
LONDON_LAT = 51.49
LONDON_LON = -0.09

# S5P-PAL L3 collection IDs for the species we want
COLLECTIONS = {
    "NO2": "s5p-l3-no2-trop",
    "O3": "s5p-l3-o3-tot",
    "SO2": "s5p-l3-so2-cobra-pbl",
}

# How far back to search (satellite revisit is ~daily, but clouds cause gaps)
SEARCH_DAYS = 3


def _search_s5p_products_sync() -> list[dict[str, Any]]:
    """Search S5P-PAL STAC for recent L3 products over London (sync, runs in thread)."""
    try:
        import pystac_client  # type: ignore
    except ImportError:
        log.error("pystac-client not installed; skipping Sentinel-5P search")
        return []

    now = datetime.now(timezone.utc)
    start = now - timedelta(days=SEARCH_DAYS)
    date_range = f"{start.strftime('%Y-%m-%d')}/{now.strftime('%Y-%m-%d')}"

    results: list[dict[str, Any]] = []

    try:
        catalog = pystac_client.Client.open(S5P_PAL_STAC_URL)
    except Exception as exc:
        log.warning("Failed to open S5P-PAL STAC catalog: %s", exc)
        return []

    for species, collection_id in COLLECTIONS.items():
        try:
            search = catalog.search(
                collections=[collection_id],
                bbox=LONDON_BBOX,
                datetime=date_range,
                max_items=3,  # latest few per species
            )
            items = list(search.items())
            for item in items:
                props = item.properties or {}
                results.append({
                    "species": species,
                    "collection": collection_id,
                    "item_id": item.id,
                    "datetime": item.datetime.isoformat() if item.datetime else props.get("datetime"),
                    "start_datetime": props.get("start_datetime"),
                    "end_datetime": props.get("end_datetime"),
                    "bbox": list(item.bbox) if item.bbox else None,
                    "platform": props.get("platform", "Sentinel-5P"),
                    "instrument": props.get("instruments", ["TROPOMI"])[0] if props.get("instruments") else "TROPOMI",
                    "processing_level": props.get("processing:level", "L3"),
                    "asset_count": len(item.assets),
                })
        except Exception as exc:
            log.warning("S5P-PAL STAC search failed for %s (%s): %s", species, collection_id, exc)

    return results


class Sentinel5PIngestor(BaseIngestor):
    source_name = "sentinel5p"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Search S5P-PAL STAC catalog in a thread (pystac_client is sync)."""
        return await asyncio.to_thread(_search_s5p_products_sync)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected Sentinel-5P data: %s", type(data))
            return

        if not data:
            self.log.info("No Sentinel-5P L3 products found over London in the past %d days", SEARCH_DAYS)
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # Group by species and take the most recent per species
        latest_by_species: dict[str, dict[str, Any]] = {}
        for product in data:
            species = product["species"]
            if species not in latest_by_species:
                latest_by_species[species] = product
            else:
                existing_dt = latest_by_species[species].get("datetime", "")
                new_dt = product.get("datetime", "")
                if new_dt > existing_dt:
                    latest_by_species[species] = product

        for species, product in latest_by_species.items():
            item_id = product.get("item_id", "unknown")
            sensing_dt = product.get("datetime", "unknown")

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=0.0,  # metadata-only; no pixel-level extraction yet
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "item_id": item_id,
                    "species": species,
                    "collection": product.get("collection"),
                    "datetime": sensing_dt,
                    "start_datetime": product.get("start_datetime"),
                    "end_datetime": product.get("end_datetime"),
                    "platform": product.get("platform"),
                    "instrument": product.get("instrument"),
                    "processing_level": product.get("processing_level"),
                    "status": "catalog_metadata_only",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Sentinel-5P TROPOMI [{species}] L3 product available: "
                    f"id={item_id} sensed={sensing_dt} "
                    f"[satellite atmospheric {species} over London]"
                ),
                data={
                    "item_id": item_id,
                    "species": species,
                    "datetime": sensing_dt,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        species_found = sorted(latest_by_species.keys())
        self.log.info(
            "Sentinel-5P: found L3 products for %s over London (total %d items from past %d days)",
            ", ".join(species_found),
            len(data),
            SEARCH_DAYS,
        )


async def ingest_sentinel5p(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Sentinel-5P search cycle."""
    ingestor = Sentinel5PIngestor(board, graph, scheduler)
    await ingestor.run()
