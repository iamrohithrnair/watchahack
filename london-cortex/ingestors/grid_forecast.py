"""NESO day-ahead generation & demand forecasts — forward-looking grid context.

Fetches two datasets from the National Energy System Operator (NESO) CKAN portal:
1. Day-ahead wind generation forecast (half-hourly MW + capacity)
2. Day-ahead demand forecast (peak/trough/fixed cardinal points)

These provide the 'ground truth' explanation for grid behaviour: when combined
with the existing grid_generation (actual outturn) and grid_demand (actual demand)
ingestors, the system can detect forecast-vs-actual deviations and preemptively
classify routine balancing patterns, reducing false anomalies.

Free API, no key required.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.grid_forecast")

# NESO CKAN datastore API — free, no key
NESO_DATASTORE_URL = "https://api.neso.energy/api/3/action/datastore_search"

# Resource IDs for day-ahead forecasts
WIND_FORECAST_RESOURCE = "b2f03146-f05d-4824-a663-3a4f36090c71"
DEMAND_FORECAST_RESOURCE = "aec5601a-7f3e-4c4c-bf56-d8e4184d3c5b"

# Central London (national-level data, pinned to city centre)
LONDON_LAT = 51.5
LONDON_LON = -0.12


class GridForecastIngestor(BaseIngestor):
    source_name = "neso_grid_forecast"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        wind = await self.fetch(
            NESO_DATASTORE_URL,
            params={"resource_id": WIND_FORECAST_RESOURCE, "limit": 48},
        )
        demand = await self.fetch(
            NESO_DATASTORE_URL,
            params={"resource_id": DEMAND_FORECAST_RESOURCE, "limit": 20},
        )
        return {"wind": wind, "demand": demand}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        wind_processed = await self._process_wind(data.get("wind"), cell_id)
        demand_processed = await self._process_demand(data.get("demand"), cell_id)

        self.log.info(
            "Grid forecast: wind=%d periods, demand=%d points",
            wind_processed, demand_processed,
        )

    async def _process_wind(self, data: Any, cell_id: str | None) -> int:
        if not isinstance(data, dict) or not data.get("success"):
            self.log.warning("Wind forecast: unsuccessful or missing response")
            return 0

        records = data.get("result", {}).get("records", [])
        if not records:
            return 0

        processed = 0
        peak_forecast = 0.0
        min_forecast = float("inf")
        capacity_mw = 0.0
        forecasts: list[dict] = []

        for rec in records:
            try:
                forecast_mw = float(rec.get("Incentive_forecast") or 0)
                cap = float(rec.get("Capacity") or 0)
                period = int(rec.get("Settlement_period") or 0)
                dt_str = str(rec.get("Datetime_GMT", ""))
            except (ValueError, TypeError):
                continue

            if cap > 0:
                capacity_mw = cap
            peak_forecast = max(peak_forecast, forecast_mw)
            min_forecast = min(min_forecast, forecast_mw)

            forecasts.append({
                "period": period,
                "forecast_mw": forecast_mw,
                "datetime": dt_str,
            })

        if not forecasts:
            return 0

        # Store the current (first) period and summary stats
        current = forecasts[0]
        utilisation = (current["forecast_mw"] / capacity_mw * 100) if capacity_mw > 0 else 0

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=current["forecast_mw"],
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "wind_forecast_day_ahead_mw",
                "capacity_mw": capacity_mw,
                "utilisation_pct": round(utilisation, 1),
                "peak_forecast_mw": peak_forecast,
                "min_forecast_mw": min_forecast if min_forecast != float("inf") else 0,
                "periods_ahead": len(forecasts),
            },
        )
        await self.board.store_observation(obs)
        processed += 1

        # Post summary
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Day-ahead wind forecast: current={current['forecast_mw']:.0f}MW "
                f"(of {capacity_mw:.0f}MW cap, {utilisation:.1f}% util), "
                f"range={min_forecast:.0f}-{peak_forecast:.0f}MW over {len(forecasts)} periods"
            ),
            data={
                "forecast_type": "wind_day_ahead",
                "current_forecast_mw": current["forecast_mw"],
                "capacity_mw": capacity_mw,
                "utilisation_pct": round(utilisation, 1),
                "peak_mw": peak_forecast,
                "min_mw": min_forecast if min_forecast != float("inf") else 0,
                "periods": len(forecasts),
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        return processed

    async def _process_demand(self, data: Any, cell_id: str | None) -> int:
        if not isinstance(data, dict) or not data.get("success"):
            self.log.warning("Demand forecast: unsuccessful or missing response")
            return 0

        records = data.get("result", {}).get("records", [])
        if not records:
            return 0

        processed = 0
        peak_demand = 0.0
        trough_demand = float("inf")
        cardinal_points: list[dict] = []

        for rec in records:
            try:
                demand_mw = float(rec.get("FORECASTDEMAND") or 0)
                cp_type = str(rec.get("CP_TYPE", "")).strip()
                f_point = str(rec.get("F_Point", "")).strip()
                start_time = rec.get("CP_ST_TIME")
                target_date = str(rec.get("TARGETDATE", ""))
            except (ValueError, TypeError):
                continue

            if demand_mw <= 0:
                continue

            if "peak" in cp_type.lower() or "max" in f_point.lower():
                peak_demand = max(peak_demand, demand_mw)
            if "trough" in cp_type.lower() or "min" in f_point.lower():
                trough_demand = min(trough_demand, demand_mw)

            cardinal_points.append({
                "demand_mw": demand_mw,
                "type": cp_type,
                "f_point": f_point,
                "start_time": start_time,
                "target_date": target_date,
            })

        if not cardinal_points:
            return 0

        if trough_demand == float("inf"):
            trough_demand = min(cp["demand_mw"] for cp in cardinal_points)
        if peak_demand == 0:
            peak_demand = max(cp["demand_mw"] for cp in cardinal_points)

        swing = peak_demand - trough_demand

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=peak_demand,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "demand_forecast_day_ahead_peak_mw",
                "trough_mw": trough_demand,
                "swing_mw": swing,
                "cardinal_points": len(cardinal_points),
                "target_date": cardinal_points[0].get("target_date", ""),
            },
        )
        await self.board.store_observation(obs)
        processed += 1

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Day-ahead demand forecast: peak={peak_demand:.0f}MW, "
                f"trough={trough_demand:.0f}MW, swing={swing:.0f}MW "
                f"({len(cardinal_points)} cardinal points)"
            ),
            data={
                "forecast_type": "demand_day_ahead",
                "peak_mw": peak_demand,
                "trough_mw": trough_demand,
                "swing_mw": swing,
                "cardinal_points": len(cardinal_points),
                "target_date": cardinal_points[0].get("target_date", ""),
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)

        return processed


async def ingest_grid_forecast(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one grid forecast fetch cycle."""
    ingestor = GridForecastIngestor(board, graph, scheduler)
    await ingestor.run()
