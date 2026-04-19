"""ADS-B flight tracking ingestor — real-time aircraft over Greater London via OpenSky Network."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.flight_tracker")

# Greater London bounding box (matches graph.py grid)
LAMIN = 51.28
LAMAX = 51.70
LOMIN = -0.51
LOMAX = 0.33

OPENSKY_URL = "https://opensky-network.org/api/states/all"

# State vector field indices (OpenSky API response)
ICAO24 = 0
CALLSIGN = 1
ORIGIN_COUNTRY = 2
LONGITUDE = 5
LATITUDE = 6
BARO_ALTITUDE = 7
ON_GROUND = 8
VELOCITY = 9
TRUE_TRACK = 10
VERTICAL_RATE = 11
GEO_ALTITUDE = 13

# Low-altitude threshold for "interesting" flights (meters)
LOW_ALTITUDE_THRESHOLD = 500


class FlightTrackerIngestor(BaseIngestor):
    source_name = "opensky_adsb"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(
            OPENSKY_URL,
            params={
                "lamin": LAMIN,
                "lamax": LAMAX,
                "lomin": LOMIN,
                "lomax": LOMAX,
            },
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected OpenSky response type: %s", type(data))
            return

        states = data.get("states")
        if not states:
            self.log.info("No aircraft currently over London")
            return

        processed = 0
        low_altitude = 0

        for sv in states:
            try:
                lat = sv[LATITUDE]
                lon = sv[LONGITUDE]
                if lat is None or lon is None:
                    continue

                icao24 = sv[ICAO24] or "unknown"
                callsign = (sv[CALLSIGN] or "").strip() or icao24
                altitude = sv[BARO_ALTITUDE]  # meters, can be None
                velocity = sv[VELOCITY]  # m/s, can be None
                on_ground = sv[ON_GROUND]
                vertical_rate = sv[VERTICAL_RATE]
                geo_alt = sv[GEO_ALTITUDE]

                cell_id = self.graph.latlon_to_cell(lat, lon)

                # Use geo altitude as fallback
                alt_value = altitude if altitude is not None else geo_alt
                is_low = (
                    alt_value is not None
                    and alt_value < LOW_ALTITUDE_THRESHOLD
                    and not on_ground
                )
                if is_low:
                    low_altitude += 1

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.GEO,
                    value=alt_value,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "icao24": icao24,
                        "callsign": callsign,
                        "altitude_m": alt_value,
                        "velocity_ms": velocity,
                        "on_ground": on_ground,
                        "vertical_rate": vertical_rate,
                        "low_altitude": is_low,
                    },
                )
                await self.board.store_observation(obs)

                content = (
                    f"Aircraft [{callsign}] alt={alt_value}m "
                    f"vel={velocity}m/s on_ground={on_ground}"
                )
                if is_low:
                    content = f"LOW-ALT {content}"

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=content,
                    data={
                        "icao24": icao24,
                        "callsign": callsign,
                        "altitude_m": alt_value,
                        "velocity_ms": velocity,
                        "on_ground": on_ground,
                        "vertical_rate": vertical_rate,
                        "lat": lat,
                        "lon": lon,
                        "low_altitude": is_low,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except (IndexError, TypeError) as exc:
                self.log.debug("Skipping malformed state vector: %s", exc)
                continue

        self.log.info(
            "OpenSky ADS-B: %d aircraft tracked, %d low-altitude",
            processed,
            low_altitude,
        )


async def ingest_flight_tracker(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one OpenSky ADS-B fetch cycle."""
    ingestor = FlightTrackerIngestor(board, graph, scheduler)
    await ingestor.run()
