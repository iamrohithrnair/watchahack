"""OpenAQ citizen-science air quality ingestor — low-cost sensor networks.

Uses the OpenAQ v3 API to fetch real-time air quality measurements from
citizen-science and low-cost sensor networks across Greater London. Provides
a resilient alternative to official LAQN monitors by pulling data from
independent sensor networks (e.g. PurpleAir sensors registered on OpenAQ,
Clarity, AirGradient, government reference monitors).

Complements existing LAQN, PurpleAir, and Sensor.Community ingestors by
aggregating data from multiple provider networks via a single API.

Requires OPENAQ_API_KEY (free: https://explore.openaq.org).
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

log = logging.getLogger("cortex.ingestors.openaq")

OPENAQ_API_URL = "https://api.openaq.org/v3/locations"

# Pollutant parameters we care about
PARAMETERS_OF_INTEREST = {"pm25", "pm10", "no2", "o3", "so2", "co"}


class OpenAQIngestor(BaseIngestor):
    source_name = "openaq"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("OPENAQ_API_KEY", "")
        if not api_key:
            self.log.warning("OPENAQ_API_KEY not set — skipping")
            return None

        # Bounding box: min_lon, min_lat, max_lon, max_lat
        bbox = (
            f"{LONDON_BBOX['min_lon']},{LONDON_BBOX['min_lat']},"
            f"{LONDON_BBOX['max_lon']},{LONDON_BBOX['max_lat']}"
        )
        params = {
            "bbox": bbox,
            "limit": 1000,
        }
        headers = {"X-API-Key": api_key}
        return await self.fetch(OPENAQ_API_URL, params=params, headers=headers)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected response type: %s", type(data))
            return

        results = data.get("results", [])
        if not results:
            self.log.info("OpenAQ: no locations returned for London bbox")
            return

        processed = 0
        skipped = 0

        for location in results:
            loc_name = location.get("name", "unknown")
            coords = location.get("coordinates", {})
            lat = coords.get("latitude")
            lon = coords.get("longitude")

            if lat is None or lon is None:
                skipped += 1
                continue

            try:
                lat = float(lat)
                lon = float(lon)
            except (ValueError, TypeError):
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            if cell_id is None:
                skipped += 1
                continue

            provider = location.get("provider", {}).get("name", "unknown")
            loc_id = location.get("id")

            sensors = location.get("sensors", [])
            for sensor in sensors:
                param = sensor.get("parameter", {})
                param_name = param.get("name", "").lower()

                if param_name not in PARAMETERS_OF_INTEREST:
                    continue

                # Use the latest value from the sensor summary if available
                summary = sensor.get("summary", {})
                latest = summary.get("last", {})
                value = latest.get("value") if latest else None

                if value is None:
                    # Try the datetimeLast field as fallback
                    value = sensor.get("latest", {}).get("value")

                if value is None:
                    skipped += 1
                    continue

                try:
                    value = float(value)
                except (ValueError, TypeError):
                    skipped += 1
                    continue

                # Sanity: discard negative or implausibly high readings
                if value < 0 or value > 2000:
                    skipped += 1
                    continue

                unit = param.get("units", "ug/m3")
                display_name = param.get("displayName", param_name.upper())

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=value,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "site": loc_name,
                        "species": display_name,
                        "unit": unit,
                        "provider": provider,
                        "openaq_location_id": loc_id,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"OpenAQ [{loc_name}] {display_name}={value:.1f} {unit} "
                        f"(provider: {provider})"
                    ),
                    data={
                        "site": loc_name,
                        "species": display_name,
                        "value": value,
                        "unit": unit,
                        "provider": provider,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "OpenAQ: processed=%d skipped=%d locations=%d",
            processed, skipped, len(results),
        )


async def ingest_openaq(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one OpenAQ fetch cycle."""
    ingestor = OpenAQIngestor(board, graph, scheduler)
    await ingestor.run()
