"""Elexon BMRS grid demand ingestor — half-hourly national electricity demand outturn.

Fetches Initial National Demand Outturn (INDO) from the Elexon BMRS API.
This captures the GB transmission-level demand in MW, updated every 30 minutes.
The morning demand ramp (~25 GW -> 40 GW+) directly correlates with TfL services
starting and the city waking up, enabling carbon-cost analysis of public transport.

Free API, no key required.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.grid_demand")

# Elexon BMRS v1 — Initial National Demand Outturn (half-hourly)
INDO_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/INDO"

LONDON_LAT = 51.5
LONDON_LON = -0.12


class GridDemandIngestor(BaseIngestor):
    source_name = "grid_demand"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        now = datetime.now(timezone.utc)
        params = {
            "from": (now - timedelta(hours=2)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "to": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "format": "json",
        }
        return await self.fetch(INDO_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected INDO response type: %s", type(data))
            return

        records = data.get("data", [])
        if not records:
            self.log.info("No INDO records returned")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # Process the most recent record for the current observation
        latest = records[-1] if isinstance(records, list) else None
        if latest is None:
            return

        demand_mw = latest.get("demand") or latest.get("initialDemand")
        settlement_period = latest.get("settlementPeriod")
        settlement_date = latest.get("settlementDate")
        publish_time = latest.get("publishTime", "")

        if demand_mw is None:
            self.log.warning("No demand value in latest INDO record")
            return

        try:
            demand_val = float(demand_mw)
        except (ValueError, TypeError):
            self.log.warning("Invalid demand value: %s", demand_mw)
            return

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=demand_val,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "national_demand_mw",
                "settlement_period": settlement_period,
                "settlement_date": settlement_date,
                "publish_time": publish_time,
            },
        )
        await self.board.store_observation(obs)

        # Build a recent trend from available records for richer context
        recent_demands = []
        for rec in records[-6:]:  # last 3 hours of half-hourly data
            d = rec.get("demand") or rec.get("initialDemand")
            if d is not None:
                try:
                    recent_demands.append(float(d))
                except (ValueError, TypeError):
                    pass

        trend = ""
        if len(recent_demands) >= 2:
            delta = recent_demands[-1] - recent_demands[0]
            direction = "rising" if delta > 0 else "falling" if delta < 0 else "stable"
            trend = f" trend={direction} ({delta:+.0f} MW over {len(recent_demands)} periods)"

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Grid demand: {demand_val:.0f} MW"
                f" (period {settlement_period}, {settlement_date})"
                f"{trend}"
            ),
            data={
                "demand_mw": demand_val,
                "settlement_period": settlement_period,
                "settlement_date": settlement_date,
                "recent_demands": recent_demands,
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Grid demand: %.0f MW (period %s)%s",
            demand_val, settlement_period, trend,
        )


async def ingest_grid_demand(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one grid demand fetch cycle."""
    ingestor = GridDemandIngestor(board, graph, scheduler)
    await ingestor.run()
