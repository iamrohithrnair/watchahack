"""NESO Embedded Wind & Solar Forecast ingestor — forward-looking renewable generation."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.embedded_generation")

# CKAN datastore API — free, no key required
# Resource: "embedded_solar_and_wind_forecast" (current forecast, up to 14 days ahead)
NESO_DATASTORE_URL = (
    "https://api.neso.energy/api/3/action/datastore_search"
)
RESOURCE_ID = "db6c038f-98af-4570-ab60-24d71ebd0ae5"

# Central London coordinates (national-level data, pinned to city centre)
LONDON_LAT = 51.5
LONDON_LON = -0.12


class EmbeddedGenerationIngestor(BaseIngestor):
    source_name = "neso_embedded_gen"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(
            NESO_DATASTORE_URL,
            params={"resource_id": RESOURCE_ID, "limit": 100},
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict) or not data.get("success"):
            self.log.warning("NESO API returned unsuccessful response")
            return

        records = data.get("result", {}).get("records", [])
        if not records:
            self.log.warning("No forecast records returned")
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        now = datetime.now(timezone.utc)
        processed = 0

        # Aggregate: find current period and near-future forecasts
        current_wind = None
        current_solar = None
        peak_wind = 0
        peak_solar = 0
        wind_capacity = None
        solar_capacity = None
        forecast_periods: list[dict] = []

        for rec in records:
            try:
                wind_mw = int(rec.get("EMBEDDED_WIND_FORECAST") or 0)
                solar_mw = int(rec.get("EMBEDDED_SOLAR_FORECAST") or 0)
                wind_cap = int(rec.get("EMBEDDED_WIND_CAPACITY") or 0)
                solar_cap = int(rec.get("EMBEDDED_SOLAR_CAPACITY") or 0)
                date_str = rec.get("DATE_GMT", "")
                time_str = rec.get("TIME_GMT", "")
                settlement_period = int(rec.get("SETTLEMENT_PERIOD") or 0)
            except (ValueError, TypeError):
                continue

            if wind_cap:
                wind_capacity = wind_cap
            if solar_cap:
                solar_capacity = solar_cap

            peak_wind = max(peak_wind, wind_mw)
            peak_solar = max(peak_solar, solar_mw)

            # Parse timestamp to find current/nearest period
            try:
                if "T" in str(date_str):
                    dt_str = str(date_str).split("T")[0]
                else:
                    dt_str = str(date_str).split(" ")[0]
                period_dt = datetime.strptime(
                    f"{dt_str} {time_str}", "%Y-%m-%d %H:%M"
                ).replace(tzinfo=timezone.utc)
            except (ValueError, TypeError):
                period_dt = None

            period_info = {
                "wind_mw": wind_mw,
                "solar_mw": solar_mw,
                "time": time_str,
                "date": dt_str if 'dt_str' in dir() else "",
                "settlement_period": settlement_period,
            }
            forecast_periods.append(period_info)

            # Track the closest-to-now period as "current"
            if period_dt and abs((period_dt - now).total_seconds()) < 1800:
                current_wind = wind_mw
                current_solar = solar_mw

        # Fall back to first record if we couldn't match a current period
        if current_wind is None and forecast_periods:
            current_wind = forecast_periods[0]["wind_mw"]
            current_solar = forecast_periods[0]["solar_mw"]

        # Store current embedded wind forecast
        if current_wind is not None:
            wind_util = (current_wind / wind_capacity * 100) if wind_capacity else None
            obs_wind = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(current_wind),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "embedded_wind_forecast_mw",
                    "capacity_mw": wind_capacity,
                    "utilisation_pct": round(wind_util, 1) if wind_util else None,
                    "peak_forecast_mw": peak_wind,
                },
            )
            await self.board.store_observation(obs_wind)
            processed += 1

        # Store current embedded solar forecast
        if current_solar is not None:
            solar_util = (current_solar / solar_capacity * 100) if solar_capacity else None
            obs_solar = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(current_solar),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "embedded_solar_forecast_mw",
                    "capacity_mw": solar_capacity,
                    "utilisation_pct": round(solar_util, 1) if solar_util else None,
                    "peak_forecast_mw": peak_solar,
                },
            )
            await self.board.store_observation(obs_solar)
            processed += 1

        # Post a summary message with forward-looking context
        total_embedded = (current_wind or 0) + (current_solar or 0)
        total_capacity = (wind_capacity or 0) + (solar_capacity or 0)
        total_util = (total_embedded / total_capacity * 100) if total_capacity else 0

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Embedded renewables forecast: wind={current_wind}MW "
                f"(peak {peak_wind}MW/{wind_capacity}MW cap), "
                f"solar={current_solar}MW "
                f"(peak {peak_solar}MW/{solar_capacity}MW cap), "
                f"combined utilisation {total_util:.1f}%"
            ),
            data={
                "wind_forecast_mw": current_wind,
                "solar_forecast_mw": current_solar,
                "wind_capacity_mw": wind_capacity,
                "solar_capacity_mw": solar_capacity,
                "peak_wind_mw": peak_wind,
                "peak_solar_mw": peak_solar,
                "total_embedded_mw": total_embedded,
                "combined_utilisation_pct": round(total_util, 1),
                "forecast_periods": len(forecast_periods),
                "observation_id": obs_wind.id if current_wind is not None else None,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        self.log.info(
            "NESO embedded forecast: wind=%sMW solar=%sMW periods=%d",
            current_wind, current_solar, len(forecast_periods),
        )


async def ingest_embedded_generation(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    ingestor = EmbeddedGenerationIngestor(board, graph, scheduler)
    await ingestor.run()
