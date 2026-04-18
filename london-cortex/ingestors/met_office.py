"""Met Office Weather DataHub ingestor — UKV-derived hourly forecasts.

Provides quantitative weather data from the Met Office UKV (2km) model via the
site-specific API: visibility (m), precipitation rate (mm) and probability (%),
temperature, wind speed/gusts, humidity, and pressure across 6 London stations.

Complements Open-Meteo with Met Office's operational NWP output — particularly
valuable for visibility and precipitation nowcasts to test weather-transport
and weather-social-activity hypotheses.

Free tier: 360 API calls/day. With 6 stations at 30-min intervals = ~288/day.
API key required: register at https://datahub.metoffice.gov.uk/
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

log = logging.getLogger("cortex.ingestors.met_office")

BASE_URL = "https://data.hub.api.metoffice.gov.uk/sitespecific/v0/point/hourly"

# Open-Meteo fallback (free, no key) — covers same weather variables
OPEN_METEO_URL = "https://api.open-meteo.com/v1/forecast"
OPEN_METEO_CURRENT_VARS = (
    "temperature_2m,relative_humidity_2m,apparent_temperature,"
    "precipitation,wind_speed_10m,wind_gusts_10m,wind_direction_10m,"
    "surface_pressure,visibility,uv_index,weather_code"
)
# Map Open-Meteo field names to our OBSERVATION_FIELDS labels + units
_OPEN_METEO_FIELD_MAP: dict[str, tuple[str, str]] = {
    "temperature_2m": ("temperature", "C"),
    "apparent_temperature": ("feels_like", "C"),
    "relative_humidity_2m": ("humidity", "%"),
    "surface_pressure": ("pressure", "hPa"),
    "visibility": ("visibility", "m"),
    "wind_speed_10m": ("wind_speed", "m/s"),
    "wind_gusts_10m": ("wind_gust", "m/s"),
    "wind_direction_10m": ("wind_direction", "deg"),
    "weather_code": ("weather_code", ""),
    "precipitation": ("precip_amount", "mm"),
    "uv_index": ("uv_index", ""),
}

# Representative London stations spread across Greater London
# (lat, lon, label) — chosen to cover different areas
LONDON_STATIONS = [
    (51.4780, -0.4614, "Heathrow"),       # West London
    (51.5054, -0.1057, "City of London"),  # Central
    (51.4500, -0.0942, "Kenley"),          # South London
    (51.5764, -0.0987, "Tottenham"),       # North London
    (51.5033, 0.0553, "Greenwich"),        # East London
    (51.4343, -0.3375, "Hampton"),         # South-West London
]

# Variables we extract and their units
OBSERVATION_FIELDS: dict[str, tuple[str, str]] = {
    "screenTemperature": ("temperature", "C"),
    "feelsLikeTemperature": ("feels_like", "C"),
    "screenRelativeHumidity": ("humidity", "%"),
    "mslp": ("pressure", "hPa"),
    "visibility": ("visibility", "m"),
    "windSpeed10m": ("wind_speed", "m/s"),
    "windGustSpeed10m": ("wind_gust", "m/s"),
    "windDirectionFrom10m": ("wind_direction", "deg"),
    "significantWeatherCode": ("weather_code", ""),
    "probOfPrecipitation": ("precip_probability", "%"),
    "totalPrecipAmount": ("precip_amount", "mm"),
    "uvIndex": ("uv_index", ""),
}


class MetOfficeIngestor(BaseIngestor):
    source_name = "met_office"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("MET_OFFICE_API_KEY", "")

    async def fetch_data(self) -> Any:
        if not self.api_key:
            self.log.info(
                "MET_OFFICE_API_KEY not set — using Open-Meteo free fallback"
            )
            return await self._fetch_open_meteo()

        results = []
        for lat, lon, label in LONDON_STATIONS:
            data = await self.fetch(
                BASE_URL,
                params={
                    "datasource": "BD1",
                    "latitude": str(lat),
                    "longitude": str(lon),
                    "includeLocationName": "true",
                    "excludeParameterMetadata": "true",
                },
                headers={"apikey": self.api_key},
            )
            if data is not None:
                results.append((lat, lon, label, data))
        return results if results else None

    async def _fetch_open_meteo(self) -> list | None:
        """Fetch equivalent weather data from Open-Meteo (free, no key)."""
        results = []
        for lat, lon, label in LONDON_STATIONS:
            data = await self.fetch(
                OPEN_METEO_URL,
                params={
                    "latitude": lat,
                    "longitude": lon,
                    "current": OPEN_METEO_CURRENT_VARS,
                    "wind_speed_unit": "ms",
                },
            )
            if data is not None:
                results.append((lat, lon, label, data))
        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0
        stations_ok = 0

        for lat, lon, label, station_data in data:
            cell_id = self.graph.latlon_to_cell(lat, lon)

            # Detect format: Open-Meteo has "current", Met Office has "features"
            if isinstance(station_data, dict) and "current" in station_data:
                result = self._process_open_meteo(station_data, label, lat, lon, cell_id)
            else:
                result = self._process_met_office(station_data, label, lat, lon, cell_id)

            if result is None:
                continue

            obs_list, summary_parts, msg_data, obs_time, via = result
            stations_ok += 1

            for obs in obs_list:
                await self.board.store_observation(obs)
                processed += 1

            summary = ", ".join(summary_parts) if summary_parts else "no data"
            source_tag = f" (via {via})" if via != "met_office" else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Met Office [{label}]{source_tag} {summary}",
                data=msg_data,
                location_id=cell_id,
            )
            await self.board.post(msg)

        self.log.info(
            "Met Office observations: %d metrics from %d/%d stations",
            processed, stations_ok, len(LONDON_STATIONS),
        )

    def _process_met_office(self, geojson, label, lat, lon, cell_id):
        """Process Met Office DataHub GeoJSON response."""
        features = geojson.get("features", []) if isinstance(geojson, dict) else []
        if not features:
            self.log.debug("No features for station %s", label)
            return None

        props = features[0].get("properties", {})
        time_series = props.get("timeSeries", [])
        if not time_series:
            return None

        latest = time_series[-1]
        obs_time = latest.get("time", "")
        obs_list = []

        for field_key, (metric_label, unit) in OBSERVATION_FIELDS.items():
            raw = latest.get(field_key)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (ValueError, TypeError):
                continue
            obs_list.append(Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=lat, lon=lon,
                metadata={"station": label, "metric": metric_label,
                          "unit": unit, "obs_time": obs_time, "via": "met_office"},
            ))

        parts = []
        vis = latest.get("visibility")
        if vis is not None:
            parts.append(f"vis={vis}m")
        temp = latest.get("screenTemperature")
        if temp is not None:
            parts.append(f"temp={temp}C")
        wind = latest.get("windSpeed10m")
        if wind is not None:
            parts.append(f"wind={wind}m/s")
        precip = latest.get("probOfPrecipitation")
        if precip is not None:
            parts.append(f"precip_prob={precip}%")
        precip_amt = latest.get("totalPrecipAmount")
        if precip_amt is not None:
            parts.append(f"precip={precip_amt}mm")

        msg_data = {
            "station": label, "lat": lat, "lon": lon, "obs_time": obs_time,
            "visibility": vis, "temperature": temp, "wind_speed": wind,
            "wind_gust": latest.get("windGustSpeed10m"),
            "precip_probability": precip, "precip_amount": precip_amt,
            "humidity": latest.get("screenRelativeHumidity"),
            "pressure": latest.get("mslp"), "via": "met_office",
        }
        return obs_list, parts, msg_data, obs_time, "met_office"

    def _process_open_meteo(self, data, label, lat, lon, cell_id):
        """Process Open-Meteo current weather response."""
        current = data.get("current", {})
        if not current:
            return None

        obs_time = current.get("time", "")
        obs_list = []

        for om_field, (metric_label, unit) in _OPEN_METEO_FIELD_MAP.items():
            raw = current.get(om_field)
            if raw is None:
                continue
            try:
                value = float(raw)
            except (ValueError, TypeError):
                continue
            obs_list.append(Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=lat, lon=lon,
                metadata={"station": label, "metric": metric_label,
                          "unit": unit, "obs_time": obs_time, "via": "open_meteo"},
            ))

        temp = current.get("temperature_2m")
        wind = current.get("wind_speed_10m")
        vis = current.get("visibility")
        precip = current.get("precipitation")

        parts = []
        if vis is not None:
            parts.append(f"vis={vis}m")
        if temp is not None:
            parts.append(f"temp={temp}C")
        if wind is not None:
            parts.append(f"wind={wind}m/s")
        if precip is not None:
            parts.append(f"precip={precip}mm")

        msg_data = {
            "station": label, "lat": lat, "lon": lon, "obs_time": obs_time,
            "visibility": vis, "temperature": temp, "wind_speed": wind,
            "wind_gust": current.get("wind_gusts_10m"),
            "precip_amount": precip,
            "humidity": current.get("relative_humidity_2m"),
            "pressure": current.get("surface_pressure"), "via": "open_meteo",
        }
        return obs_list, parts, msg_data, obs_time, "open_meteo"


async def ingest_met_office(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Met Office fetch cycle."""
    ingestor = MetOfficeIngestor(board, graph, scheduler)
    await ingestor.run()
