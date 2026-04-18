"""Carbon Intensity ingestor — UK grid carbon intensity and generation mix."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.energy")

CARBON_INTENSITY_URL = "https://api.carbonintensity.org.uk/intensity"
GENERATION_MIX_URL = "https://api.carbonintensity.org.uk/generation"

LONDON_LAT = 51.5
LONDON_LON = -0.12


class CarbonIntensityIngestor(BaseIngestor):
    source_name = "carbon_intensity"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        intensity_data = await self.fetch(CARBON_INTENSITY_URL)
        generation_data = await self.fetch(GENERATION_MIX_URL)
        return {
            "intensity": intensity_data,
            "generation": generation_data,
        }

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # ── Intensity ────────────────────────────────────────────────────────
        intensity_resp = data.get("intensity")
        intensity_val: float | None = None
        forecast_val: float | None = None
        index_label: str = ""

        if isinstance(intensity_resp, dict):
            entries = intensity_resp.get("data", [])
            if isinstance(entries, list) and entries:
                entry = entries[0]
                inner = entry.get("intensity", {})
                try:
                    intensity_val = float(inner.get("actual") or inner.get("forecast") or 0) or None
                    forecast_val = float(inner.get("forecast") or 0) or None
                    index_label = inner.get("index", "")
                except (ValueError, TypeError):
                    pass

        if intensity_val is not None:
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=intensity_val,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "carbon_intensity_gco2_kwh",
                    "index": index_label,
                    "forecast": forecast_val,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Carbon intensity: {intensity_val} gCO2/kWh [{index_label}]"
                    + (f" forecast={forecast_val}" if forecast_val else "")
                ),
                data={
                    "intensity_gco2_kwh": intensity_val,
                    "forecast": forecast_val,
                    "index": index_label,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
        else:
            self.log.warning("Could not extract carbon intensity value from response")

        # ── Generation mix ────────────────────────────────────────────────────
        generation_resp = data.get("generation")
        if not isinstance(generation_resp, dict):
            self.log.info("No generation mix data")
            return

        gen_entries = generation_resp.get("data", [])
        if isinstance(gen_entries, list) and gen_entries:
            mix_data = gen_entries[0].get("generationmix", [])
        elif isinstance(gen_entries, dict):
            mix_data = gen_entries.get("generationmix", [])
        else:
            mix_data = []

        for fuel_entry in mix_data:
            fuel = fuel_entry.get("fuel", "")
            perc = fuel_entry.get("perc")
            if fuel and perc is not None:
                try:
                    perc_val = float(perc)
                except (ValueError, TypeError):
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=perc_val,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={"metric": "generation_mix_pct", "fuel": fuel},
                )
                await self.board.store_observation(obs)

        # Post a generation mix summary message
        if mix_data:
            mix_summary = ", ".join(
                f"{e['fuel']}={e['perc']:.1f}%"
                for e in mix_data
                if e.get("perc") is not None and float(e["perc"]) > 1.0
            )
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Generation mix: {mix_summary}",
                data={"generation_mix": mix_data},
                location_id=cell_id,
            )
            await self.board.post(msg)

        self.log.info(
            "Carbon intensity: %s gCO2/kWh [%s], generation mix: %d sources",
            intensity_val,
            index_label,
            len(mix_data),
        )


async def ingest_energy(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one carbon intensity fetch cycle."""
    ingestor = CarbonIntensityIngestor(board, graph, scheduler)
    await ingestor.run()
