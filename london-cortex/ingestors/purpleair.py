"""PurpleAir crowd-sourced PM2.5 ingestor — hyper-local air quality sensors.

Uses the PurpleAir API v1 to fetch real-time PM2.5 readings from low-cost
sensors within the Greater London bounding box. Provides thousands of
additional data points to cross-validate LAQN and Sensor.Community readings.

Requires PURPLEAIR_API_KEY (free: https://develop.purpleair.com).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.purpleair")

PURPLEAIR_API_URL = "https://api.purpleair.com/v1/sensors"

# Fields to request (minimise response size / API points cost)
# pm2.5_10minute: 10-min average PM2.5 (CF=1), plus humidity & temperature
FIELDS = "name,latitude,longitude,pm2.5_10minute,humidity,temperature,last_seen"


class PurpleAirIngestor(BaseIngestor):
    source_name = "purpleair"
    rate_limit_name = "default"
    required_env_vars = ["PURPLEAIR_API_KEY"]

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("PURPLEAIR_API_KEY", "")
        if not api_key:
            self.log.warning("PURPLEAIR_API_KEY not set — skipping")
            return None

        # Bounding box: NW corner (max_lat, min_lon), SE corner (min_lat, max_lon)
        params = {
            "fields": FIELDS,
            "nwlat": LONDON_BBOX["max_lat"],
            "nwlng": LONDON_BBOX["min_lon"],
            "selat": LONDON_BBOX["min_lat"],
            "selng": LONDON_BBOX["max_lon"],
            "location_type": "0",  # 0 = outdoor sensors only
        }
        headers = {"X-API-Key": api_key}
        return await self.fetch(PURPLEAIR_API_URL, params=params, headers=headers)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected response type: %s", type(data))
            return

        fields_list = data.get("fields", [])
        data_rows = data.get("data", [])

        if not fields_list or not data_rows:
            self.log.info("PurpleAir: no sensors returned for London bbox")
            return

        # Build column index for positional access
        col = {name: idx for idx, name in enumerate(fields_list)}

        processed = 0
        skipped = 0

        for row in data_rows:
            try:
                lat = row[col["latitude"]]
                lon = row[col["longitude"]]
                pm25 = row[col["pm2.5_10minute"]]
                name = row[col.get("name", -1)] if "name" in col else "unknown"
            except (IndexError, KeyError, TypeError):
                skipped += 1
                continue

            if lat is None or lon is None or pm25 is None:
                skipped += 1
                continue

            try:
                lat = float(lat)
                lon = float(lon)
                pm25 = float(pm25)
            except (ValueError, TypeError):
                skipped += 1
                continue

            # Sanity: discard negative or implausibly high readings
            if pm25 < 0 or pm25 > 1000:
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            if cell_id is None:
                skipped += 1
                continue

            humidity = None
            temperature = None
            if "humidity" in col:
                try:
                    humidity = float(row[col["humidity"]])
                except (ValueError, TypeError, IndexError):
                    pass
            if "temperature" in col:
                try:
                    # PurpleAir returns temperature in Fahrenheit
                    temp_f = float(row[col["temperature"]])
                    temperature = round((temp_f - 32) * 5 / 9, 1)
                except (ValueError, TypeError, IndexError):
                    pass

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=pm25,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "sensor_name": str(name),
                    "species": "PM2.5",
                    "unit": "ug/m3",
                    "humidity": humidity,
                    "temperature_c": temperature,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"PurpleAir [{name}] PM2.5={pm25:.1f} ug/m3"
                ),
                data={
                    "sensor_name": str(name),
                    "species": "PM2.5",
                    "value": pm25,
                    "unit": "ug/m3",
                    "humidity": humidity,
                    "temperature_c": temperature,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "PurpleAir: processed=%d skipped=%d total_rows=%d",
            processed, skipped, len(data_rows),
        )


async def ingest_purpleair(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one PurpleAir fetch cycle."""
    ingestor = PurpleAirIngestor(board, graph, scheduler)
    await ingestor.run()
