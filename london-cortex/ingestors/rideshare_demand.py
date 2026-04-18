"""Ride-share demand proxy — Uber surge multipliers across key London corridors.

Queries the Uber Price Estimates API for a set of representative corridors
(major station-to-station routes). Surge multiplier > 1.0 indicates elevated
demand, which correlates with modal shift from public transport to PHVs —
especially during transit disruptions.

Requires: UBER_SERVER_TOKEN env var (free: https://developer.uber.com/)
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.rideshare_demand")

UBER_ESTIMATES_URL = "https://api.uber.com/v1.2/estimates/price"

# Representative London corridors: (name, start_lat, start_lon, end_lat, end_lon)
# Chosen to capture modal-shift hotspots near major rail/tube stations.
CORRIDORS = [
    ("Kings Cross → Liverpool St", 51.5308, -0.1238, 51.5178, -0.0823),
    ("Waterloo → Canary Wharf", 51.5032, -0.1132, 51.5054, -0.0235),
    ("Paddington → Victoria", 51.5154, -0.1755, 51.4952, -0.1439),
    ("London Bridge → Stratford", 51.5055, -0.0860, 51.5430, -0.0035),
    ("Euston → Brixton", 51.5284, -0.1331, 51.4613, -0.1146),
    ("Bank → Heathrow T5", 51.5133, -0.0886, 51.4723, -0.4889),
    ("Clapham Jn → Oxford Circus", 51.4640, -0.1703, 51.5152, -0.1418),
    ("Angel → Greenwich", 51.5322, -0.1058, 51.4769, -0.0005),
]


class RideshareDemandIngestor(BaseIngestor):
    source_name = "rideshare_demand"
    rate_limit_name = "default"
    required_env_vars = ["UBER_SERVER_TOKEN"]

    async def fetch_data(self) -> Any:
        token = os.environ.get("UBER_SERVER_TOKEN", "").strip()
        if not token:
            self.log.debug("UBER_SERVER_TOKEN not set, skipping rideshare demand ingest")
            return None

        headers = {"Authorization": f"Token {token}", "Accept-Language": "en_GB"}
        results = []

        for name, slat, slon, elat, elon in CORRIDORS:
            params = {
                "start_latitude": slat,
                "start_longitude": slon,
                "end_latitude": elat,
                "end_longitude": elon,
            }
            data = await self.fetch(
                UBER_ESTIMATES_URL, params=params, headers=headers, retries=2,
            )
            if data and isinstance(data, dict):
                results.append((name, slat, slon, elat, elon, data))

        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0

        for name, slat, slon, elat, elon, response in data:
            prices = response.get("prices", [])
            if not prices:
                continue

            # Aggregate across product types (UberX, Comfort, etc.)
            surges = []
            estimates = []
            for product in prices:
                surge = product.get("surge_multiplier", 1.0)
                surges.append(surge)
                estimates.append({
                    "product": product.get("display_name", "unknown"),
                    "surge": surge,
                    "estimate": product.get("estimate", ""),
                    "duration_s": product.get("duration", 0),
                    "distance_mi": product.get("distance", 0),
                })

            max_surge = max(surges) if surges else 1.0
            avg_surge = sum(surges) / len(surges) if surges else 1.0

            # Use corridor midpoint for spatial indexing
            mid_lat = (slat + elat) / 2
            mid_lon = (slon + elon) / 2
            cell_id = self.graph.latlon_to_cell(mid_lat, mid_lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=max_surge,
                location_id=cell_id,
                lat=mid_lat,
                lon=mid_lon,
                metadata={
                    "corridor": name,
                    "max_surge": max_surge,
                    "avg_surge": round(avg_surge, 2),
                    "products": estimates,
                    "start": {"lat": slat, "lon": slon},
                    "end": {"lat": elat, "lon": elon},
                },
            )
            await self.board.store_observation(obs)

            # Flag elevated demand (surge > 1.3) more prominently
            demand_level = (
                "HIGH" if max_surge >= 2.0
                else "ELEVATED" if max_surge >= 1.3
                else "NORMAL"
            )

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Rideshare demand [{name}] surge={max_surge:.1f}x "
                    f"avg={avg_surge:.2f}x ({demand_level})"
                ),
                data={
                    "corridor": name,
                    "max_surge": max_surge,
                    "avg_surge": round(avg_surge, 2),
                    "demand_level": demand_level,
                    "products": estimates,
                    "lat": mid_lat,
                    "lon": mid_lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Rideshare demand: processed=%d corridors", processed,
        )


async def ingest_rideshare_demand(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one rideshare demand fetch cycle."""
    ingestor = RideshareDemandIngestor(board, graph, scheduler)
    await ingestor.run()
