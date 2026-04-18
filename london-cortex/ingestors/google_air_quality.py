"""Google Air Quality API ingestor — hyperlocal AQI + pollutant concentrations.

Uses Google's Air Quality API which aggregates mobile sensor networks,
low-cost sensors, traffic data, satellite imagery, and meteorological models
to provide 500m-resolution air quality estimates across London.

Complements the LAQN fixed-station data (air_quality.py) with interpolated
grid-level estimates, directly addressing the hyper-local monitoring gap.

API docs: https://developers.google.com/maps/documentation/air-quality/current-conditions
Requires GOOGLE_AQ_API_KEY in .env (Google Maps Platform key with Air Quality API enabled).
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.google_air_quality")

API_URL = "https://airquality.googleapis.com/v1/currentConditions:lookup"

# Sample points spread across Greater London for hyperlocal coverage.
# Each tuple: (label, lat, lon) — chosen to fill gaps between LAQN stations.
SAMPLE_POINTS = [
    ("City of London", 51.5155, -0.0922),
    ("Canary Wharf", 51.5054, -0.0235),
    ("Heathrow Corridor", 51.4700, -0.4543),
    ("Brixton", 51.4613, -0.1156),
    ("Camden Town", 51.5392, -0.1426),
    ("Stratford", 51.5430, 0.0005),
    ("Greenwich", 51.4769, -0.0005),
    ("Croydon", 51.3762, -0.0986),
    ("Ealing", 51.5136, -0.3015),
    ("Lewisham", 51.4415, -0.0117),
    ("Harrow", 51.5836, -0.3340),
    ("Woolwich", 51.4906, 0.0693),
    ("Tottenham", 51.5882, -0.0720),
    ("Richmond", 51.4613, -0.3037),
    ("Barking", 51.5363, 0.0815),
]

# Pollutants we track (codes returned by the API)
POLLUTANT_CODES = {"no2", "pm25", "pm10", "o3", "so2", "co"}


class GoogleAirQualityIngestor(BaseIngestor):
    source_name = "google_aq"
    rate_limit_name = "default"
    required_env_vars = ["GOOGLE_AQ_API_KEY"]

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        super().__init__(board, graph, scheduler)
        self._api_key = os.environ.get("GOOGLE_AQ_API_KEY", "")

    async def fetch_data(self) -> Any:
        if not self._api_key:
            self.log.warning("GOOGLE_AQ_API_KEY not set — skipping")
            return None

        results = []
        url = f"{API_URL}?key={self._api_key}"
        timeout = aiohttp.ClientTimeout(total=30, connect=10)

        async with aiohttp.ClientSession(timeout=timeout) as session:
            for label, lat, lon in SAMPLE_POINTS:
                body = {
                    "location": {"latitude": lat, "longitude": lon},
                    "extraComputations": [
                        "POLLUTANT_CONCENTRATION",
                        "DOMINANT_POLLUTANT_CONCENTRATION",
                    ],
                    "languageCode": "en",
                }
                try:
                    await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
                    async with session.post(url, json=body) as resp:
                        if resp.status == 200:
                            data = await resp.json()
                            results.append((label, lat, lon, data))
                        elif resp.status == 403:
                            self.log.error(
                                "Google AQ API returned 403 — check API key and billing"
                            )
                            return None
                        elif resp.status == 429:
                            self.log.warning("Google AQ API rate-limited, stopping batch")
                            break
                        else:
                            self.log.warning(
                                "Google AQ API HTTP %d for %s", resp.status, label
                            )
                except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                    self.log.warning("Network error for %s: %s", label, exc)

        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0

        for label, lat, lon, response in data:
            cell_id = self.graph.latlon_to_cell(lat, lon)

            # Extract AQI index
            aqi_value = None
            aqi_category = ""
            dominant_pollutant = ""
            for idx in response.get("indexes", []):
                if idx.get("code") == "uaqi":
                    aqi_value = idx.get("aqi")
                    aqi_category = idx.get("category", "")
                    dominant_pollutant = idx.get("dominantPollutant", "")
                    break

            if aqi_value is not None:
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=float(aqi_value),
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "label": label,
                        "metric": "uaqi",
                        "category": aqi_category,
                        "dominant_pollutant": dominant_pollutant,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Google AQ [{label}] UAQI={aqi_value} ({aqi_category}) "
                        f"dominant={dominant_pollutant}"
                    ),
                    data={
                        "label": label,
                        "aqi": aqi_value,
                        "category": aqi_category,
                        "dominant_pollutant": dominant_pollutant,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            # Extract individual pollutant concentrations
            for pollutant in response.get("pollutants", []):
                code = pollutant.get("code", "").lower()
                if code not in POLLUTANT_CODES:
                    continue
                conc = pollutant.get("concentration", {})
                value = conc.get("value")
                units = conc.get("units", "")
                if value is None:
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=float(value),
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "label": label,
                        "metric": code,
                        "units": units,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Google AQ [{label}] {code.upper()}={value}{units}"
                    ),
                    data={
                        "label": label,
                        "pollutant": code,
                        "value": value,
                        "units": units,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "Google Air Quality: %d observations from %d locations",
            processed,
            len(data),
        )


async def ingest_google_air_quality(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Google AQ fetch cycle."""
    ingestor = GoogleAirQualityIngestor(board, graph, scheduler)
    await ingestor.run()
