"""Smart energy ingestor — Octopus Agile tariff prices + UKPN smart meter consumption.

Combines two data sources to measure price-sensitive consumer electricity usage:

1. **Octopus Agile tariff** — half-hourly wholesale electricity prices for London
   (region C). Public API, no auth needed. Prices change every 30 min based on
   grid conditions (wind, demand, solar). Negative prices occur during oversupply.
   Source: https://api.octopus.energy/

2. **UKPN Smart Meter LV Feeder** — aggregated half-hourly consumption from smart
   meters (anonymised to ≥5 properties per feeder) in London Power Networks (LPN).
   Uses the same UKPN_API_KEY as ukpn_substation.py (optional, improves rate limits).
   Source: https://ukpowernetworks.opendatasoft.com/

Together these allow the system to detect behavioral responses to grid conditions:
price spikes → demand reduction, negative prices → demand shifting, etc.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.smart_energy")

# ── Octopus Agile API (public, no auth) ──────────────────────────────────────
# Product code for Agile Octopus; region C = South East (covers London)
AGILE_PRODUCT = "AGILE-FLEX-22-11-25"
AGILE_TARIFF = f"E-1R-{AGILE_PRODUCT}-C"
AGILE_RATES_URL = (
    f"https://api.octopus.energy/v1/products/{AGILE_PRODUCT}"
    f"/electricity-tariffs/{AGILE_TARIFF}/standard-unit-rates/"
)

# ── UKPN Smart Meter LV Feeder (OpenDataSoft v2.1) ──────────────────────────
UKPN_BASE = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1"
SMART_METER_DS = "ukpn-smart-meter-consumption-lv-feeder"

LONDON_LAT = 51.5
LONDON_LON = -0.12


class SmartEnergyIngestor(BaseIngestor):
    source_name = "smart_energy"
    rate_limit_name = "default"

    def _ukpn_headers(self) -> dict[str, str] | None:
        key = os.environ.get("UKPN_API_KEY", "")
        if key:
            return {"Authorization": f"Apikey {key}"}
        return None

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}

        # 1. Octopus Agile half-hourly prices (latest 48 periods = 24h)
        agile_data = await self.fetch(
            AGILE_RATES_URL,
            params={"page_size": 48},
        )
        if agile_data:
            results["agile"] = agile_data

        # 2. UKPN smart meter consumption — latest LPN records
        sm_url = f"{UKPN_BASE}/catalog/datasets/{SMART_METER_DS}/records"
        sm_data = await self.fetch(
            sm_url,
            params={
                "limit": 20,
                "where": "dno='LPN'",
                "order_by": "data_collection_log_timestamp desc",
            },
            headers=self._ukpn_headers(),
        )
        if sm_data:
            results["smart_meter"] = sm_data

        return results if results else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        agile_count = await self._process_agile(data.get("agile"))
        sm_count = await self._process_smart_meter(data.get("smart_meter"))

        self.log.info(
            "Smart energy: %d agile price periods, %d smart meter records",
            agile_count, sm_count,
        )

    async def _process_agile(self, data: Any) -> int:
        if not data or not isinstance(data, dict):
            return 0

        results = data.get("results", [])
        if not results:
            return 0

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        count = 0

        # Find current/most-recent price and any extremes
        now = datetime.now(timezone.utc)
        current_price = None
        min_price = float("inf")
        max_price = float("-inf")

        for entry in results:
            price = entry.get("value_inc_vat")
            if price is None:
                continue

            try:
                price_val = float(price)
            except (ValueError, TypeError):
                continue

            valid_from = entry.get("valid_from", "")
            valid_to = entry.get("valid_to", "")

            min_price = min(min_price, price_val)
            max_price = max(max_price, price_val)

            # Identify the current half-hour period
            if valid_from and valid_to:
                try:
                    t_from = datetime.fromisoformat(valid_from.replace("Z", "+00:00"))
                    t_to = datetime.fromisoformat(valid_to.replace("Z", "+00:00"))
                    if t_from <= now < t_to:
                        current_price = price_val
                except (ValueError, TypeError):
                    pass

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=price_val,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "metric": "agile_tariff_p_kwh",
                    "valid_from": valid_from,
                    "valid_to": valid_to,
                    "inc_vat": True,
                },
            )
            await self.board.store_observation(obs)
            count += 1

        # Post summary message
        if count > 0:
            price_display = f"{current_price:.2f}" if current_price is not None else "N/A"
            negative_flag = " [NEGATIVE PRICE]" if current_price is not None and current_price < 0 else ""
            spike_flag = " [PRICE SPIKE]" if current_price is not None and current_price > 35 else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Octopus Agile: current {price_display}p/kWh{negative_flag}{spike_flag}"
                    f" | 24h range: {min_price:.2f}-{max_price:.2f}p/kWh"
                    f" ({count} half-hour periods)"
                ),
                data={
                    "type": "agile_tariff",
                    "current_price_p_kwh": current_price,
                    "min_24h": min_price if min_price != float("inf") else None,
                    "max_24h": max_price if max_price != float("-inf") else None,
                    "periods": count,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        return count

    async def _process_smart_meter(self, data: Any) -> int:
        if not data or not isinstance(data, dict):
            return 0

        records = data.get("results", [])
        if not records:
            return 0

        count = 0
        total_consumption_wh = 0
        total_meters = 0

        for rec in records:
            fields = rec if "total_consumption_active_import" in rec else rec.get("fields", rec)

            consumption_wh = fields.get("total_consumption_active_import")
            meter_count = fields.get("aggregated_device_count_active", 0)
            substation_id = fields.get("secondary_substation_id", "unknown")
            feeder_id = fields.get("lv_feeder_id", "")
            timestamp = fields.get("data_collection_log_timestamp", "")

            if consumption_wh is None:
                continue

            try:
                consumption_val = float(consumption_wh)
            except (ValueError, TypeError):
                continue

            try:
                n_meters = int(meter_count) if meter_count else 0
            except (ValueError, TypeError):
                n_meters = 0

            # Extract geopoint if available
            geopoint = fields.get("geopoint")
            lat, lon = None, None
            if isinstance(geopoint, dict):
                lat = geopoint.get("lat")
                lon = geopoint.get("lon")
            elif isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
                lat, lon = geopoint[0], geopoint[1]

            if lat is None or lon is None:
                lat, lon = LONDON_LAT, LONDON_LON

            cell_id = self.graph.latlon_to_cell(lat, lon)

            # Per-meter average consumption (Wh)
            per_meter_wh = consumption_val / n_meters if n_meters > 0 else consumption_val

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=consumption_val,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "metric": "smart_meter_consumption_wh",
                    "substation_id": substation_id,
                    "feeder_id": str(feeder_id),
                    "meter_count": n_meters,
                    "per_meter_wh": round(per_meter_wh, 1),
                    "timestamp": timestamp,
                },
            )
            await self.board.store_observation(obs)

            total_consumption_wh += consumption_val
            total_meters += n_meters
            count += 1

        # Post summary message
        if count > 0:
            avg_per_meter = total_consumption_wh / total_meters if total_meters > 0 else 0
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"UKPN smart meters (LPN): {count} feeders, "
                    f"{total_meters} meters, "
                    f"avg {avg_per_meter:.0f} Wh/meter/period, "
                    f"total {total_consumption_wh / 1000:.1f} kWh"
                ),
                data={
                    "type": "smart_meter_consumption",
                    "feeder_count": count,
                    "total_meters": total_meters,
                    "total_consumption_wh": total_consumption_wh,
                    "avg_per_meter_wh": round(avg_per_meter, 1),
                },
                location_id=self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON),
            )
            await self.board.post(msg)

        return count


async def ingest_smart_energy(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one smart energy fetch cycle."""
    ingestor = SmartEnergyIngestor(board, graph, scheduler)
    await ingestor.run()
