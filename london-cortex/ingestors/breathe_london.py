"""Breathe London hyper-local air quality ingestor — NO2 and PM2.5.

Ingests real-time hourly readings from the Breathe London network of ~136
Airly sensors deployed across all 33 London boroughs by the GLA. Provides
hyper-local ground-truth data complementing LAQN, PurpleAir, and
Sensor.Community readings.

API docs: https://www.breathelondon.org/developers
Requires BREATHE_LONDON_API_KEY (free, request at developer portal).
Data licensed under UK Open Government Licence v3.0.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.breathe_london")

BASE_URL = "https://breathe-london-7x54d7qf.ew.gateway.dev"
SENSORS_URL = f"{BASE_URL}/ListSensors"
DATA_URL = f"{BASE_URL}/SensorData"

# Species we ingest (NO2 and PM2.5 in µg/m³)
SPECIES_OF_INTEREST = {"NO2", "PM25"}


class BreatheLondonIngestor(BaseIngestor):
    source_name = "breathe_london"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("BREATHE_LONDON_API_KEY", "")
        if not api_key:
            self.log.warning("BREATHE_LONDON_API_KEY not set — skipping")
            return None

        headers = {"X-API-KEY": api_key}

        # Fetch latest hour of data for all sensors (default when no time params)
        data = await self.fetch(DATA_URL, headers=headers)
        if data is None:
            return None

        # Also fetch sensor metadata for lat/lon and borough info
        sensors = await self.fetch(SENSORS_URL, headers=headers)
        return {"readings": data, "sensors": sensors}

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected response type: %s", type(data))
            return

        readings = data.get("readings")
        sensors_raw = data.get("sensors")

        if not readings or not isinstance(readings, list):
            self.log.info("Breathe London: no readings returned")
            return

        # Build sensor lookup: SiteCode -> {lat, lon, name, borough}
        sensor_info: dict[str, dict] = {}
        if sensors_raw and isinstance(sensors_raw, list):
            for s in sensors_raw:
                code = s.get("SiteCode")
                if not code:
                    continue
                try:
                    lat = float(s["Latitude"])
                    lon = float(s["Longitude"])
                except (KeyError, ValueError, TypeError):
                    continue
                sensor_info[code] = {
                    "lat": lat,
                    "lon": lon,
                    "name": s.get("SiteName", code),
                    "borough": s.get("Borough", ""),
                }

        processed = 0
        skipped = 0

        for reading in readings:
            species = reading.get("Species", "")
            if species not in SPECIES_OF_INTEREST:
                skipped += 1
                continue

            site_code = reading.get("SiteCode", "")
            value_raw = reading.get("ScaledValue")

            if value_raw is None:
                skipped += 1
                continue

            try:
                value = float(value_raw)
            except (ValueError, TypeError):
                skipped += 1
                continue

            # Sanity: discard negative or implausibly high readings
            if value < 0 or value > 1000:
                skipped += 1
                continue

            info = sensor_info.get(site_code, {})
            lat = info.get("lat")
            lon = info.get("lon")
            name = info.get("name", site_code)
            borough = info.get("borough", "")

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            unit = "µg/m³"
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "site_code": site_code,
                    "site_name": name,
                    "species": species,
                    "unit": unit,
                    "borough": borough,
                    "ratification": reading.get("RatificationStatus", ""),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Breathe London [{name}] {species}={value:.1f} {unit}"
                    + (f" ({borough})" if borough else "")
                ),
                data={
                    "site_code": site_code,
                    "site_name": name,
                    "species": species,
                    "value": value,
                    "unit": unit,
                    "borough": borough,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Breathe London: processed=%d skipped=%d total_readings=%d sensors=%d",
            processed, skipped, len(readings), len(sensor_info),
        )


async def ingest_breathe_london(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Breathe London fetch cycle."""
    ingestor = BreatheLondonIngestor(board, graph, scheduler)
    await ingestor.run()
