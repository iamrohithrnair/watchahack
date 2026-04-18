"""Sensor.Community (formerly Luftdaten) citizen air quality ingestor — PM2.5, PM10.

Free, no API key. Uses the area-filter endpoint to fetch only Greater London sensors.
Provides decentralized redundancy for official LAQN PM readings.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.sensor_community")

# Area filter: 25 km radius around central London (51.509, -0.118)
SENSOR_COMMUNITY_URL = (
    "https://data.sensor.community/airrohr/v1/filter/area=51.509,-0.118,25"
)

# Sensor.Community value_type mapping
# P1 = PM10, P2 = PM2.5, temperature, humidity also available
PARTICULATE_TYPES = {"P1": "PM10", "P2": "PM2.5"}


class SensorCommunityIngestor(BaseIngestor):
    source_name = "sensor_community"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(SENSOR_COMMUNITY_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected response type: %s", type(data))
            return

        processed = 0
        skipped = 0

        for entry in data:
            location = entry.get("location", {})
            lat_str = location.get("latitude")
            lon_str = location.get("longitude")

            try:
                lat = float(lat_str) if lat_str else None
                lon = float(lon_str) if lon_str else None
            except (ValueError, TypeError):
                skipped += 1
                continue

            if lat is None or lon is None:
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            if cell_id is None:
                # Outside our grid (beyond Greater London)
                skipped += 1
                continue

            sensor_id = entry.get("sensor", {}).get("id", "unknown")
            sensor_type = (
                entry.get("sensor", {}).get("sensor_type", {}).get("name", "unknown")
            )

            for sv in entry.get("sensordatavalues", []):
                value_type = sv.get("value_type", "")
                if value_type not in PARTICULATE_TYPES:
                    continue

                species = PARTICULATE_TYPES[value_type]

                try:
                    value = float(sv.get("value", ""))
                except (ValueError, TypeError):
                    skipped += 1
                    continue

                # Sanity: discard obviously broken readings (negative or > 1000 ug/m3)
                if value < 0 or value > 1000:
                    skipped += 1
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=value,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "sensor_id": sensor_id,
                        "sensor_type": sensor_type,
                        "species": species,
                        "unit": "ug/m3",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Citizen air sensor [{sensor_id}] {species}={value:.1f} ug/m3"
                    ),
                    data={
                        "sensor_id": sensor_id,
                        "sensor_type": sensor_type,
                        "species": species,
                        "value": value,
                        "unit": "ug/m3",
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "Sensor.Community: processed=%d skipped=%d total_entries=%d",
            processed, skipped, len(data),
        )


async def ingest_sensor_community(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Sensor.Community fetch cycle."""
    ingestor = SensorCommunityIngestor(board, graph, scheduler)
    await ingestor.run()
