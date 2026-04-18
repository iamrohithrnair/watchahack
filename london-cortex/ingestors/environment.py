"""Environment Agency ingestor — Thames river level monitoring stations."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.environment")

EA_BASE_URL = "https://environment.data.gov.uk/flood-monitoring"
THAMES_STATIONS_URL = (
    f"{EA_BASE_URL}/id/stations"
    "?parameter=level&_limit=50&lat=51.5&long=-0.12&dist=30"
)
# Maximum concurrent station reading fetches
_MAX_CONCURRENT_READS = 5


class EnvironmentIngestor(BaseIngestor):
    source_name = "environment_agency"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        stations_data = await self.fetch(THAMES_STATIONS_URL)
        return stations_data

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected EA stations response: %s", type(data))
            return

        items = data.get("items", [])
        if not items:
            self.log.info("No Thames stations returned")
            return

        self.log.info("Fetching readings for %d Thames stations", len(items))

        # Fetch readings concurrently with a semaphore to avoid hammering the API
        semaphore = asyncio.Semaphore(_MAX_CONCURRENT_READS)

        async def fetch_station(station: dict[str, Any]) -> tuple[dict, Any]:
            station_id = station.get("stationReference", "") or station.get("@id", "")
            # EA API: readings endpoint per station
            readings_url = f"{EA_BASE_URL}/id/stations/{station_id}/readings?latest"
            async with semaphore:
                readings = await self.fetch(readings_url)
            return station, readings

        tasks = [fetch_station(s) for s in items[:50]]  # cap at 50 stations
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = 0
        skipped = 0

        for result in results:
            if isinstance(result, Exception):
                self.log.warning("Error fetching station reading: %s", result)
                skipped += 1
                continue

            station, readings = result

            station_ref = station.get("stationReference", "unknown")
            station_label = station.get("label", station_ref)
            lat_raw = station.get("lat") or station.get("latitude")
            lon_raw = station.get("long") or station.get("longitude")

            try:
                lat = float(lat_raw) if lat_raw is not None else None
                lon = float(lon_raw) if lon_raw is not None else None
            except (ValueError, TypeError):
                lat = lon = None

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            if not isinstance(readings, dict):
                skipped += 1
                continue

            reading_items = readings.get("items", [])
            if isinstance(reading_items, dict):
                reading_items = [reading_items]

            for reading in reading_items[:1]:  # latest reading only
                level_val = reading.get("value")
                measure_url = reading.get("measure", "")
                unit = "mAOD"  # metres Above Ordnance Datum — standard EA unit

                try:
                    level = float(level_val) if level_val is not None else None
                except (ValueError, TypeError):
                    level = None

                if level is None:
                    skipped += 1
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=level,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "station_ref": station_ref,
                        "station_label": station_label,
                        "unit": unit,
                        "measure": measure_url,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Thames level [{station_label}] level={level:.3f} {unit}"
                    ),
                    data={
                        "station_ref": station_ref,
                        "station_label": station_label,
                        "level": level,
                        "unit": unit,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "Environment Agency: processed=%d skipped=%d", processed, skipped
        )


async def ingest_environment(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Environment Agency fetch cycle."""
    ingestor = EnvironmentIngestor(board, graph, scheduler)
    await ingestor.run()
