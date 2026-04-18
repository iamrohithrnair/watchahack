"""OpenSenseMap mobile sensor ingestor — hyper-local air quality from mobile citizen-science boxes.

Queries the OpenSenseMap API for sensor boxes with exposure=mobile inside the
Greater London bounding box.  Returns PM2.5, PM10, temperature, and humidity
readings from devices that move through the city (bicycle-mounted, vehicle-
mounted, or pedestrian-carried).  No API key required for reads.

API docs: https://docs.opensensemap.org/
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.opensensemap_mobile")

# London bounding box as comma-separated string: west,south,east,north
_BBOX = (
    f"{LONDON_BBOX['min_lon']},{LONDON_BBOX['min_lat']},"
    f"{LONDON_BBOX['max_lon']},{LONDON_BBOX['max_lat']}"
)

OPENSENSEMAP_BOXES_URL = "https://api.opensensemap.org/boxes"

# Phenomena we care about (case-insensitive match against sensor title)
_PHENOMENA = {
    "pm2.5": "PM2.5",
    "pm10": "PM10",
    "temperature": "temperature",
    "humidity": "humidity",
    "no2": "NO2",
}


class OpenSenseMapMobileIngestor(BaseIngestor):
    source_name = "opensensemap_mobile"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(
            OPENSENSEMAP_BOXES_URL,
            params={
                "bbox": _BBOX,
                "exposure": "mobile",
                "format": "json",
            },
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected OpenSenseMap response type: %s", type(data))
            return

        processed = 0
        skipped = 0

        for box in data:
            box_name = box.get("name", "unknown")
            # Current location — mobile boxes update their coordinates
            loc = box.get("currentLocation", {})
            coords = loc.get("coordinates", [])
            if len(coords) >= 2:
                lon, lat = float(coords[0]), float(coords[1])
            else:
                skipped += 1
                continue

            # Check coords are actually in London (API bbox filter is approximate)
            if not (
                LONDON_BBOX["min_lat"] <= lat <= LONDON_BBOX["max_lat"]
                and LONDON_BBOX["min_lon"] <= lon <= LONDON_BBOX["max_lon"]
            ):
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            sensors = box.get("sensors", [])

            for sensor in sensors:
                title_raw = sensor.get("title", "")
                title_lower = title_raw.lower()

                # Match against known phenomena
                matched_phenomenon = None
                for key, label in _PHENOMENA.items():
                    if key in title_lower:
                        matched_phenomenon = label
                        break

                if matched_phenomenon is None:
                    continue

                last_val = sensor.get("lastMeasurement", {})
                value_str = last_val.get("value") if last_val else None
                if value_str is None:
                    skipped += 1
                    continue

                try:
                    value = float(value_str)
                except (ValueError, TypeError):
                    skipped += 1
                    continue

                unit = sensor.get("unit", "")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=value,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "box": box_name,
                        "phenomenon": matched_phenomenon,
                        "unit": unit,
                        "sensor_title": title_raw,
                        "mobile": True,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Mobile sensor [{box_name}] {matched_phenomenon}={value}{unit}"
                    ),
                    data={
                        "box": box_name,
                        "phenomenon": matched_phenomenon,
                        "value": value,
                        "unit": unit,
                        "lat": lat,
                        "lon": lon,
                        "mobile": True,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "OpenSenseMap mobile: %d boxes returned, processed=%d skipped=%d",
            len(data), processed, skipped,
        )


async def ingest_opensensemap_mobile(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one OpenSenseMap mobile fetch cycle."""
    ingestor = OpenSenseMapMobileIngestor(board, graph, scheduler)
    await ingestor.run()
