"""NESO grid system status ingestor — operational margin, reserves, and planning adequacy.

Fetches two datasets from the NESO Data Portal (CKAN API):
1. System Operating Plan (SOP): real-time control room snapshot with operating margin
   surplus/shortfall, reserve availability, demand forecasts, and trigger levels.
2. Daily OPMR: 2-14 day ahead planning margin forecasts with national surplus.

This provides the official operational context that the Brain needs to distinguish
between genuine generation problems and normal data feed issues. When operating
margin drops or reserves are short, anomalies in energy/demand data are more
likely to reflect real grid stress.

Free API, no key required. Rate limit: 2 requests/min for datastore API.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.grid_status")

# NESO Data Portal CKAN datastore API
NESO_DATASTORE = "https://api.neso.energy/api/3/action/datastore_search"

# Resource IDs
SOP_RESOURCE_ID = "e51f2721-00ab-4182-9cae-3c973e854aa8"
OPMR_RESOURCE_ID = "0eede912-8820-4c66-a58a-f7436d36b95f"

LONDON_LAT = 51.5
LONDON_LON = -0.12


class GridStatusIngestor(BaseIngestor):
    source_name = "neso_grid_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Fetch latest SOP record (sorted by datetime descending)
        sop_data = await self.fetch(
            NESO_DATASTORE,
            params={
                "resource_id": SOP_RESOURCE_ID,
                "sort": "sop_datetime desc",
                "limit": "5",
            },
        )
        # Fetch latest OPMR records (next few days)
        opmr_data = await self.fetch(
            NESO_DATASTORE,
            params={
                "resource_id": OPMR_RESOURCE_ID,
                "sort": "Date desc",
                "limit": "5",
            },
        )
        return {"sop": sop_data, "opmr": opmr_data}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        sop_count = await self._process_sop(data.get("sop"), cell_id)
        opmr_count = await self._process_opmr(data.get("opmr"), cell_id)
        self.log.info(
            "Grid status: %d SOP records, %d OPMR records processed",
            sop_count, opmr_count,
        )

    async def _process_sop(self, raw: Any, cell_id: str | None) -> int:
        """Process System Operating Plan records."""
        if not isinstance(raw, dict) or not raw.get("success"):
            self.log.warning("SOP fetch failed or unexpected format")
            return 0

        records = raw.get("result", {}).get("records", [])
        if not records:
            self.log.warning("No SOP records returned")
            return 0

        # Use the most recent record
        record = records[0]
        sop_time = record.get("sop_datetime", "")
        cardinal = record.get("cardinal_point", "")

        # Key operational metrics
        demand_mw = _float(record.get("total_sop_demand"))
        margin_surplus = _float(record.get("operating_margin_surplus"))
        trigger_level = _float(record.get("trigger_level"))
        standing_reserve_req = _float(record.get("standing_reserve_requirement"))
        standing_reserve_avail = _float(record.get("standing_reserve_availability"))
        reserve_shortfall = _float(record.get("standing_reserve_shortfall"))
        reserve_excess = _float(record.get("standing_reserve_excess"))
        contingency_req = _float(record.get("contingency_requirement"))
        imbalance = _float(record.get("imbalance"))

        # Determine system stress level
        stress = "normal"
        if margin_surplus is not None and trigger_level is not None:
            if margin_surplus < 0:
                stress = "deficit"
            elif margin_surplus < trigger_level:
                stress = "tight"
        if reserve_shortfall is not None and reserve_shortfall > 0:
            stress = "reserve_short"

        # Store key numeric observations for anomaly detection
        metrics = {
            "operating_margin_surplus_mw": margin_surplus,
            "total_demand_mw": demand_mw,
            "standing_reserve_shortfall_mw": reserve_shortfall,
            "standing_reserve_excess_mw": reserve_excess,
            "imbalance_mw": imbalance,
        }

        stored = 0
        for metric_name, value in metrics.items():
            if value is None:
                continue
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": metric_name,
                    "cardinal_point": cardinal,
                    "sop_time": sop_time,
                },
            )
            await self.board.store_observation(obs)
            stored += 1

        # Summary message
        parts = [f"SOP [{sop_time}] cardinal={cardinal}"]
        if demand_mw is not None:
            parts.append(f"demand={demand_mw:.0f}MW")
        if margin_surplus is not None:
            parts.append(f"margin={margin_surplus:.0f}MW")
        if trigger_level is not None:
            parts.append(f"trigger={trigger_level:.0f}MW")
        if reserve_shortfall is not None and reserve_shortfall > 0:
            parts.append(f"reserve_short={reserve_shortfall:.0f}MW")
        parts.append(f"stress={stress}")

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"Grid status: {', '.join(parts)}",
            data={
                "dataset": "sop",
                "sop_time": sop_time,
                "cardinal_point": cardinal,
                "total_demand_mw": demand_mw,
                "operating_margin_surplus_mw": margin_surplus,
                "trigger_level_mw": trigger_level,
                "standing_reserve_requirement_mw": standing_reserve_req,
                "standing_reserve_availability_mw": standing_reserve_avail,
                "standing_reserve_shortfall_mw": reserve_shortfall,
                "standing_reserve_excess_mw": reserve_excess,
                "contingency_requirement_mw": contingency_req,
                "imbalance_mw": imbalance,
                "stress_level": stress,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        return stored

    async def _process_opmr(self, raw: Any, cell_id: str | None) -> int:
        """Process Operational Planning Margin Requirement records."""
        if not isinstance(raw, dict) or not raw.get("success"):
            self.log.warning("OPMR fetch failed or unexpected format")
            return 0

        records = raw.get("result", {}).get("records", [])
        if not records:
            self.log.warning("No OPMR records returned")
            return 0

        stored = 0
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        for record in records:
            date = record.get("Date", "")
            peak_demand = _float(record.get("Peak Demand Forecast"))
            gen_avail = _float(record.get("Generator Availability"))
            national_surplus = _float(record.get("National Surplus"))
            opmr_total = _float(record.get("OPMR total"))

            if national_surplus is None:
                continue

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=national_surplus,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "opmr_national_surplus_mw",
                    "forecast_date": date,
                    "peak_demand_mw": peak_demand,
                    "generator_availability_mw": gen_avail,
                },
            )
            await self.board.store_observation(obs)
            stored += 1

        # Post summary for the nearest forecast date
        r = records[0]
        date = r.get("Date", "")
        surplus = _float(r.get("National Surplus"))
        peak = _float(r.get("Peak Demand Forecast"))
        gen = _float(r.get("Generator Availability"))

        adequacy = "adequate"
        if surplus is not None:
            if surplus < 0:
                adequacy = "deficit"
            elif surplus < 2000:
                adequacy = "tight"

        parts = [f"OPMR [{date}]"]
        if peak is not None:
            parts.append(f"peak_demand={peak:.0f}MW")
        if gen is not None:
            parts.append(f"gen_avail={gen:.0f}MW")
        if surplus is not None:
            parts.append(f"surplus={surplus:.0f}MW")
        parts.append(f"adequacy={adequacy}")

        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"Grid planning margin: {', '.join(parts)}",
            data={
                "dataset": "opmr",
                "forecast_date": date,
                "peak_demand_forecast_mw": peak,
                "generator_availability_mw": gen,
                "national_surplus_mw": surplus,
                "adequacy": adequacy,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        return stored


def _float(val: Any) -> float | None:
    """Safely convert a value to float."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def ingest_grid_status(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one grid status fetch cycle."""
    ingestor = GridStatusIngestor(board, graph, scheduler)
    await ingestor.run()
