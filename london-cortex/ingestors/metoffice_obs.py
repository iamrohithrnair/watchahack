"""Met Office DataPoint weather observations ingestor.

Fetches hourly observations from Met Office fixed weather stations in/near
London. Each station has a known, fixed location — if a station from
Manchester reports as being in London, that's an unambiguous geo-tagging
failure signal (canary system).

Requires METOFFICE_API_KEY env var (free: https://www.metoffice.gov.uk/services/data/datapoint/api).
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

log = logging.getLogger("cortex.ingestors.metoffice_obs")

DATAPOINT_BASE = "http://datapoint.metoffice.gov.uk/public/data"
SITELIST_URL = f"{DATAPOINT_BASE}/val/wxobs/all/json/sitelist"
OBS_ALL_URL = f"{DATAPOINT_BASE}/val/wxobs/all/json/all"

# Greater London bounding box (matches config.LONDON_BBOX with margin)
LONDON_BBOX = {
    "min_lat": 51.25,
    "max_lat": 51.75,
    "min_lon": -0.55,
    "max_lon": 0.40,
}

# DataPoint observation field codes → (label, unit)
OBS_FIELDS: dict[str, tuple[str, str]] = {
    "T": ("temperature", "°C"),
    "S": ("wind_speed", "mph"),
    "D": ("wind_direction", ""),
    "H": ("humidity", "%"),
    "P": ("pressure", "hPa"),
    "V": ("visibility", "m"),
    "Dp": ("dew_point", "°C"),
    "G": ("wind_gust", "mph"),
}


def _in_london(lat: float, lon: float) -> bool:
    return (LONDON_BBOX["min_lat"] <= lat <= LONDON_BBOX["max_lat"]
            and LONDON_BBOX["min_lon"] <= lon <= LONDON_BBOX["max_lon"])


class MetOfficeObsIngestor(BaseIngestor):
    source_name = "metoffice_obs"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("METOFFICE_API_KEY", "")
        # Cache of station metadata: id → {name, lat, lon, region}
        self._stations: dict[str, dict] = {}

    async def _load_stations(self) -> bool:
        """Fetch station site list and cache London-area stations."""
        if not self.api_key:
            self.log.warning("METOFFICE_API_KEY not set — skipping")
            return False
        data = await self.fetch(SITELIST_URL, params={"key": self.api_key})
        if not isinstance(data, dict):
            return False
        locations = data.get("Locations", {}).get("Location", [])
        if not locations:
            self.log.warning("No stations in sitelist response")
            return False
        for loc in locations:
            try:
                lat = float(loc.get("latitude", 0))
                lon = float(loc.get("longitude", 0))
            except (ValueError, TypeError):
                continue
            self._stations[str(loc.get("id", ""))] = {
                "name": loc.get("name", "Unknown"),
                "lat": lat,
                "lon": lon,
                "region": loc.get("region", ""),
            }
        self.log.info("Loaded %d Met Office stations", len(self._stations))
        return True

    async def fetch_data(self) -> Any:
        if not self.api_key:
            self.log.warning("METOFFICE_API_KEY not set — skipping")
            return None
        if not self._stations:
            if not await self._load_stations():
                return None
        return await self.fetch(OBS_ALL_URL, params={"res": "hourly", "key": self.api_key})

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected DataPoint response type: %s", type(data))
            return

        locations = data.get("SiteRep", {}).get("DV", {}).get("Location", [])
        if not locations:
            self.log.warning("No location data in DataPoint response")
            return

        processed = 0
        geo_anomalies = []

        for loc in locations:
            station_id = str(loc.get("i", ""))
            reported_lat = _safe_float(loc.get("lat"))
            reported_lon = _safe_float(loc.get("lon"))
            station_name = loc.get("name", "Unknown")

            if reported_lat is None or reported_lon is None:
                continue

            # Look up known station metadata
            known = self._stations.get(station_id)

            # --- Canary check: geo-tagging integrity ---
            # If a station we know is NOT in London suddenly reports London
            # coordinates, or vice versa, flag it.
            if known:
                known_in_london = _in_london(known["lat"], known["lon"])
                reported_in_london = _in_london(reported_lat, reported_lon)

                if not known_in_london and reported_in_london:
                    geo_anomalies.append({
                        "station_id": station_id,
                        "station_name": known["name"],
                        "known_lat": known["lat"],
                        "known_lon": known["lon"],
                        "reported_lat": reported_lat,
                        "reported_lon": reported_lon,
                        "known_region": known.get("region", ""),
                        "type": "non_london_reporting_as_london",
                    })

                if known_in_london and not reported_in_london:
                    geo_anomalies.append({
                        "station_id": station_id,
                        "station_name": known["name"],
                        "known_lat": known["lat"],
                        "known_lon": known["lon"],
                        "reported_lat": reported_lat,
                        "reported_lon": reported_lon,
                        "known_region": known.get("region", ""),
                        "type": "london_reporting_outside_london",
                    })

            # Only ingest observations from stations in/near London
            if not _in_london(reported_lat, reported_lon):
                continue

            # Get latest period's observations
            periods = loc.get("Period", [])
            if isinstance(periods, dict):
                periods = [periods]
            if not periods:
                continue
            latest_period = periods[-1]
            reps = latest_period.get("Rep", [])
            if isinstance(reps, dict):
                reps = [reps]
            if not reps:
                continue
            latest = reps[-1]

            cell_id = self.graph.latlon_to_cell(reported_lat, reported_lon)

            for field_code, (label, unit) in OBS_FIELDS.items():
                raw_val = latest.get(field_code)
                if raw_val is None:
                    continue
                try:
                    value = float(raw_val)
                except (ValueError, TypeError):
                    continue

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=value,
                    location_id=cell_id,
                    lat=reported_lat,
                    lon=reported_lon,
                    metadata={
                        "variable": label,
                        "unit": unit,
                        "station_id": station_id,
                        "station_name": station_name,
                        "field_code": field_code,
                    },
                )
                await self.board.store_observation(obs)
                processed += 1

        # Post summary
        london_stations = [
            loc.get("name", "?") for loc in locations
            if _in_london(_safe_float(loc.get("lat"), 0), _safe_float(loc.get("lon"), 0))
        ]
        summary = (
            f"Met Office obs: {processed} readings from "
            f"{len(london_stations)} London stations"
        )
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=summary,
            data={
                "station_count": len(london_stations),
                "observation_count": processed,
                "stations": london_stations[:10],
            },
        )
        await self.board.post(msg)

        # Post geo-tagging anomalies if any detected
        if geo_anomalies:
            anomaly_msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"GEO-TAG CANARY ALERT: {len(geo_anomalies)} Met Office station(s) "
                    f"reporting from unexpected locations. "
                    f"Stations: {', '.join(a['station_name'] for a in geo_anomalies)}"
                ),
                data={
                    "geo_anomalies": geo_anomalies,
                    "canary_type": "metoffice_geotag_integrity",
                    "severity": "high",
                },
            )
            await self.board.post(anomaly_msg)
            self.log.warning(
                "GEO-TAG CANARY: %d station(s) with location mismatch: %s",
                len(geo_anomalies),
                [a["station_name"] for a in geo_anomalies],
            )

        self.log.info("%s | geo_anomalies=%d", summary, len(geo_anomalies))


def _safe_float(val: Any, default: float | None = None) -> float | None:
    if val is None:
        return default
    try:
        return float(val)
    except (ValueError, TypeError):
        return default


async def ingest_metoffice_obs(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Met Office observation cycle."""
    ingestor = MetOfficeObsIngestor(board, graph, scheduler)
    await ingestor.run()
