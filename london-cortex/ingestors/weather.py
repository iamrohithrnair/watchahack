"""Open-Meteo weather ingestor — current conditions for central London."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.weather")

OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
LONDON_LAT = 51.5
LONDON_LON = -0.12

WEATHER_PARAMS = {
    "latitude": LONDON_LAT,
    "longitude": LONDON_LON,
    "current": "temperature_2m,rain,wind_speed_10m,relative_humidity_2m",
}

# Map variable names to human-readable labels and units
VARIABLE_META: dict[str, tuple[str, str]] = {
    "temperature_2m": ("temperature", "°C"),
    "rain": ("rain", "mm"),
    "wind_speed_10m": ("wind_speed", "km/h"),
    "relative_humidity_2m": ("humidity", "%"),
}


class WeatherIngestor(BaseIngestor):
    source_name = "open_meteo"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(OPEN_METEO_URL, params=WEATHER_PARAMS)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Open-Meteo response: %s", type(data))
            return

        current = data.get("current", {})
        if not current:
            self.log.warning("No 'current' section in Open-Meteo response")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        processed = 0
        for var_key, (label, unit) in VARIABLE_META.items():
            raw_val = current.get(var_key)
            if raw_val is None:
                continue

            try:
                value = float(raw_val)
            except (ValueError, TypeError):
                self.log.debug("Could not convert %s=%r to float", var_key, raw_val)
                continue

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "variable": var_key,
                    "label": label,
                    "unit": unit,
                    "interval": data.get("current_units", {}).get(var_key, ""),
                },
            )
            await self.board.store_observation(obs)
            processed += 1

        # Post a single summary message
        summary_parts = []
        for var_key, (label, unit) in VARIABLE_META.items():
            val = current.get(var_key)
            if val is not None:
                summary_parts.append(f"{label}={val}{unit}")

        summary = ", ".join(summary_parts) if summary_parts else "no data"
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"Weather [Central London] {summary}",
            data={
                "lat": LONDON_LAT,
                "lon": LONDON_LON,
                **{k: current.get(k) for k in VARIABLE_META},
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info("Open-Meteo weather: stored %d observations — %s", processed, summary)


async def ingest_weather(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Open-Meteo fetch cycle."""
    ingestor = WeatherIngestor(board, graph, scheduler)
    await ingestor.run()
