"""EA river flow ingestor — redundant river monitoring via flow-rate sensors.

Complements the level-based environment.py ingestor (which uses parameter=level).
Flow sensors are physically distinct from level gauges, so this provides genuine
redundancy when parts of the EA level sensor network go offline.

Brain suggestion origin: 'Integrate Gaugemap API for crowdsourced river data.'
Evaluation: Gaugemap has no public API — it's a Shoothill visualization of EA data.
Instead we use EA's own flow-rate parameter as a genuinely independent sensor set.
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.river_flow")

EA_BASE_URL = "https://environment.data.gov.uk/flood-monitoring"
# Flow stations within 30km of central London
FLOW_STATIONS_URL = (
    f"{EA_BASE_URL}/id/stations"
    "?parameter=flow&_limit=50&lat=51.5&long=-0.12&dist=30"
)
_MAX_CONCURRENT_READS = 5


class RiverFlowIngestor(BaseIngestor):
    source_name = "ea_river_flow"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(FLOW_STATIONS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected EA flow stations response: %s", type(data))
            return

        items = data.get("items", [])
        if not items:
            self.log.info("No flow stations returned")
            return

        self.log.info("Fetching readings for %d flow stations", len(items))

        semaphore = asyncio.Semaphore(_MAX_CONCURRENT_READS)

        async def fetch_station(station: dict[str, Any]) -> tuple[dict, Any]:
            station_id = station.get("stationReference", "") or station.get("@id", "")
            readings_url = f"{EA_BASE_URL}/id/stations/{station_id}/readings?latest"
            async with semaphore:
                readings = await self.fetch(readings_url)
            return station, readings

        tasks = [fetch_station(s) for s in items[:50]]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        processed = 0
        skipped = 0

        for result in results:
            if isinstance(result, Exception):
                self.log.warning("Error fetching flow reading: %s", result)
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

            for reading in reading_items[:1]:
                flow_val = reading.get("value")

                try:
                    flow = float(flow_val) if flow_val is not None else None
                except (ValueError, TypeError):
                    flow = None

                if flow is None:
                    skipped += 1
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=flow,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "station_ref": station_ref,
                        "station_label": station_label,
                        "unit": "m3/s",
                        "parameter": "flow",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"River flow [{station_label}] flow={flow:.3f} m3/s"
                    ),
                    data={
                        "station_ref": station_ref,
                        "station_label": station_label,
                        "flow": flow,
                        "unit": "m3/s",
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info("EA river flow: processed=%d skipped=%d", processed, skipped)


async def ingest_river_flow(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one EA river flow fetch cycle."""
    ingestor = RiverFlowIngestor(board, graph, scheduler)
    await ingestor.run()
