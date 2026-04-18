"""National Grid ESO real-time generation mix — MW output by fuel type via Elexon BMRS.

Fetches the FUELINST dataset: instantaneous generation outturn by fuel type,
updated every 5 minutes. No API key required.

This complements the existing carbon_intensity ingestor (which gives percentages
and gCO2/kWh) by providing absolute MW output per fuel type — enabling direct
correlation between observed wind speed (open_meteo) and wind farm output, and
quantifying which fossil fuels are being displaced.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.grid_generation")

# Elexon BMRS FUELINST — free, no API key
FUELINST_URL = "https://data.elexon.co.uk/bmrs/api/v1/datasets/FUELINST"

# Categorise fuel types for richer metadata
RENEWABLE_FUELS = {"WIND", "BIOMASS", "NPSHYD"}
FOSSIL_FUELS = {"CCGT", "COAL", "OCGT", "OIL"}
NUCLEAR_FUELS = {"NUCLEAR"}
INTERCONNECTORS = {
    "INTFR", "INTIRL", "INTNED", "INTNSL", "INTEW",
    "INTELEC", "INTGRNL", "INTIFA2", "INTNEM", "INTVKL",
}

# London coords for board posting (grid-level data, not location-specific)
LONDON_LAT = 51.5
LONDON_LON = -0.12


class GridGenerationIngestor(BaseIngestor):
    source_name = "grid_generation"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(FUELINST_URL, params={"format": "json"})

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected FUELINST response type: %s", type(data))
            return

        records = data.get("data", [])
        if not records:
            self.log.warning("No FUELINST records returned")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # Group by the latest startTime to get one snapshot
        latest_time = max(r.get("startTime", "") for r in records)
        latest_records = [r for r in records if r.get("startTime") == latest_time]

        totals = {"renewable": 0.0, "fossil": 0.0, "nuclear": 0.0, "interconnector": 0.0, "other": 0.0}
        fuel_mw: dict[str, float] = {}
        total_generation = 0.0

        for record in latest_records:
            fuel_type = record.get("fuelType", "")
            generation_mw = record.get("generation")
            if generation_mw is None:
                continue
            try:
                mw = float(generation_mw)
            except (ValueError, TypeError):
                continue

            fuel_mw[fuel_type] = mw
            total_generation += mw

            if fuel_type in RENEWABLE_FUELS:
                totals["renewable"] += mw
            elif fuel_type in FOSSIL_FUELS:
                totals["fossil"] += mw
            elif fuel_type in NUCLEAR_FUELS:
                totals["nuclear"] += mw
            elif fuel_type in INTERCONNECTORS:
                totals["interconnector"] += mw
            else:
                totals["other"] += mw

            # Store per-fuel observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=mw,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "generation_mw",
                    "fuel_type": fuel_type,
                    "snapshot_time": latest_time,
                },
            )
            await self.board.store_observation(obs)

        # Store category totals as observations for easier anomaly detection
        for category, mw in totals.items():
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=mw,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": f"generation_{category}_total_mw",
                    "snapshot_time": latest_time,
                },
            )
            await self.board.store_observation(obs)

        # Compute renewable share
        renewable_pct = (totals["renewable"] / total_generation * 100) if total_generation > 0 else 0
        wind_mw = fuel_mw.get("WIND", 0)

        # Summary message for the board
        top_fuels = sorted(fuel_mw.items(), key=lambda x: x[1], reverse=True)
        summary_parts = [f"{f}={mw:.0f}MW" for f, mw in top_fuels if mw > 100]
        summary = ", ".join(summary_parts)

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Grid generation [{latest_time}]: "
                f"total={total_generation:.0f}MW, "
                f"wind={wind_mw:.0f}MW, "
                f"renewable={renewable_pct:.1f}%, "
                f"fossil={totals['fossil']:.0f}MW. "
                f"Top: {summary}"
            ),
            data={
                "snapshot_time": latest_time,
                "total_mw": total_generation,
                "wind_mw": wind_mw,
                "renewable_mw": totals["renewable"],
                "fossil_mw": totals["fossil"],
                "nuclear_mw": totals["nuclear"],
                "interconnector_mw": totals["interconnector"],
                "renewable_pct": round(renewable_pct, 1),
                "fuel_breakdown_mw": fuel_mw,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "Grid generation: total=%.0fMW wind=%.0fMW renewable=%.1f%% fossil=%.0fMW (%d fuel types)",
            total_generation, wind_mw, renewable_pct, totals["fossil"], len(fuel_mw),
        )


async def ingest_grid_generation(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one grid generation fetch cycle."""
    ingestor = GridGenerationIngestor(board, graph, scheduler)
    await ingestor.run()
