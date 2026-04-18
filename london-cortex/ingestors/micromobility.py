"""Micro-mobility GBFS ingestor — e-scooter/e-bike availability from Lime, Dott, TIER.

Fetches free-floating vehicle positions from GBFS (General Bikeshare Feed
Specification) feeds published by London micro-mobility operators. Aggregates
vehicle counts per grid cell.

Sudden drops in vehicle availability near transit hubs are a direct real-world
proxy for commuter displacement — when tube/rail disruptions hit, commuters
grab nearby e-scooters/bikes, depleting local supply.

No API key required (GBFS is open data).
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.micromobility")

# GBFS discovery endpoints for London micro-mobility operators.
# Each publishes free_bike_status per the GBFS spec.
# NOTE: Lime and TIER endpoints return 404 as of 2026-03. Kept for reference.
GBFS_PROVIDERS = [
    # ("lime", "https://data.lime.bike/api/partners/v2/gbfs/london/gbfs.json"),  # 404
    ("dott", "https://gbfs.api.ridedott.com/public/v2/london/gbfs.json"),
    # ("tier", "https://platform.tier-services.io/v2/gbfs/london/gbfs.json"),  # 404
]

# London bounding box for filtering stray vehicles
LON_BBOX = {"min_lat": 51.28, "max_lat": 51.70, "min_lon": -0.51, "max_lon": 0.33}


def _in_london(lat: float, lon: float) -> bool:
    return (LON_BBOX["min_lat"] <= lat <= LON_BBOX["max_lat"]
            and LON_BBOX["min_lon"] <= lon <= LON_BBOX["max_lon"])


class MicromobilityIngestor(BaseIngestor):
    source_name = "micromobility"
    rate_limit_name = "default"

    async def _get_free_bike_url(self, gbfs_url: str) -> str | None:
        """Resolve the free_bike_status URL from a GBFS discovery file."""
        data = await self.fetch(gbfs_url, retries=2)
        if not data or not isinstance(data, dict):
            return None
        # GBFS v2: data.en.feeds[] or data.feeds[]
        feeds_wrapper = data.get("data", {})
        feeds = feeds_wrapper.get("en", {}).get("feeds", [])
        if not feeds:
            feeds = feeds_wrapper.get("feeds", [])
        for feed in feeds:
            if feed.get("name") == "free_bike_status":
                return feed.get("url")
        return None

    async def fetch_data(self) -> Any:
        results = []
        for provider_name, gbfs_url in GBFS_PROVIDERS:
            bike_url = await self._get_free_bike_url(gbfs_url)
            if not bike_url:
                self.log.debug("No free_bike_status feed for %s", provider_name)
                continue
            data = await self.fetch(bike_url, retries=2)
            if data and isinstance(data, dict):
                bikes = data.get("data", {}).get("bikes", [])
                if bikes:
                    results.append((provider_name, bikes))

        return results if results else None

    async def process(self, data: Any) -> None:
        # Aggregate vehicles per grid cell across all providers
        cell_counts: dict[str, dict] = {}  # cell_id -> {provider: count, lat, lon, ...}

        for provider_name, bikes in data:
            for bike in bikes:
                lat = bike.get("lat")
                lon = bike.get("lon")
                if lat is None or lon is None:
                    continue
                try:
                    lat, lon = float(lat), float(lon)
                except (ValueError, TypeError):
                    continue
                if not _in_london(lat, lon):
                    continue

                cell_id = self.graph.latlon_to_cell(lat, lon)
                if not cell_id:
                    continue

                if cell_id not in cell_counts:
                    cell_counts[cell_id] = {
                        "total": 0, "providers": {}, "lat": lat, "lon": lon,
                    }
                cell_counts[cell_id]["total"] += 1
                cell_counts[cell_id]["providers"][provider_name] = (
                    cell_counts[cell_id]["providers"].get(provider_name, 0) + 1
                )

        processed = 0
        total_vehicles = sum(c["total"] for c in cell_counts.values())

        for cell_id, info in cell_counts.items():
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=info["total"],
                location_id=cell_id,
                lat=info["lat"],
                lon=info["lon"],
                metadata={
                    "vehicle_count": info["total"],
                    "providers": info["providers"],
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Micromobility [{cell_id}] vehicles={info['total']} "
                    f"({', '.join(f'{p}={c}' for p, c in info['providers'].items())})"
                ),
                data={
                    "cell_id": cell_id,
                    "vehicle_count": info["total"],
                    "providers": info["providers"],
                    "lat": info["lat"],
                    "lon": info["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Micromobility: %d vehicles across %d cells from %d providers",
            total_vehicles, processed, len(data),
        )


async def ingest_micromobility(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one micro-mobility fetch cycle."""
    ingestor = MicromobilityIngestor(board, graph, scheduler)
    await ingestor.run()
