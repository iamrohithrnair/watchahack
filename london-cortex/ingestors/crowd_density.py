"""Crowd density ingestor — venue-level busyness via BestTime.app API.

Polls live foot traffic for key London locations (transport hubs, shopping
districts, tourist landmarks) to provide hyperlocal crowd density signals.
This partially fills the gap identified for mobile-network crowd data —
BestTime aggregates anonymized location signals to produce 0-100 busyness
scores per venue, with live vs forecast deltas.
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

log = logging.getLogger("cortex.ingestors.crowd_density")

BESTTIME_LIVE_URL = "https://besttime.app/api/v1/forecasts/live"

# Key London venues to monitor — covering transport, retail, tourism, events.
# Each tuple: (venue_name, venue_address, lat, lon)
LONDON_VENUES = [
    ("King's Cross Station", "Euston Rd, London N1C 4QP", 51.5320, -0.1240),
    ("Liverpool Street Station", "Liverpool St, London EC2M 7QH", 51.5178, -0.0823),
    ("Waterloo Station", "Waterloo Rd, London SE1 8SW", 51.5031, -0.1132),
    ("Victoria Station", "Terminus Pl, London SW1V 1JU", 51.4952, -0.1441),
    ("Paddington Station", "Praed St, London W2 1HQ", 51.5154, -0.1755),
    ("Oxford Circus", "Oxford St, London W1C 1JN", 51.5152, -0.1415),
    ("Covent Garden", "Covent Garden, London WC2E 8RF", 51.5117, -0.1240),
    ("Trafalgar Square", "Trafalgar Square, London WC2N 5DN", 51.5080, -0.1281),
    ("Borough Market", "8 Southwark St, London SE1 1TL", 51.5055, -0.0910),
    ("Camden Market", "Camden Lock Pl, London NW1 8AF", 51.5414, -0.1463),
    ("Westfield Stratford", "Montfichet Rd, London E20 1EJ", 51.5435, -0.0069),
    ("The O2 Arena", "Peninsula Square, London SE10 0DX", 51.5030, 0.0032),
    ("Wembley Stadium", "Wembley, London HA9 0WS", 51.5560, -0.2795),
    ("Tower of London", "London EC3N 4AB", 51.5081, -0.0759),
    ("British Museum", "Great Russell St, London WC1B 3DG", 51.5194, -0.1270),
    ("Canary Wharf", "Canary Wharf, London E14 5AB", 51.5054, -0.0235),
    ("Shoreditch High Street", "Shoreditch High St, London E1 6JE", 51.5234, -0.0755),
    ("South Bank Centre", "Belvedere Rd, London SE1 8XX", 51.5068, -0.1162),
    ("Hyde Park Corner", "Hyde Park Corner, London SW1X 7TA", 51.5027, -0.1527),
    ("Greenwich Market", "5B Greenwich Market, London SE10 9HZ", 51.4816, -0.0090),
]


class CrowdDensityIngestor(BaseIngestor):
    source_name = "crowd_density"
    rate_limit_name = "default"
    required_env_vars = ["BESTTIME_API_KEY"]

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("BESTTIME_API_KEY", "").strip()
        if not api_key:
            self.log.warning("BESTTIME_API_KEY not set — skipping crowd density")
            return None

        results = []
        for venue_name, venue_address, lat, lon in LONDON_VENUES:
            data = await self.fetch(
                BESTTIME_LIVE_URL,
                params={
                    "api_key_private": api_key,
                    "venue_name": venue_name,
                    "venue_address": venue_address,
                },
                retries=1,
            )
            if data and isinstance(data, dict) and data.get("status") == "OK":
                results.append({
                    "venue_name": venue_name,
                    "venue_address": venue_address,
                    "lat": lat,
                    "lon": lon,
                    "data": data,
                })
            else:
                self.log.debug("No data for venue %s", venue_name)

        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0

        for item in data:
            venue_name = item["venue_name"]
            lat = item["lat"]
            lon = item["lon"]
            raw = item["data"]

            analysis = raw.get("analysis", {})
            venue_info = raw.get("venue_info", {})

            live_busyness = analysis.get("venue_live_busyness")
            forecast_busyness = analysis.get("venue_forecasted_busyness")
            delta = analysis.get("venue_live_forecasted_delta")
            live_available = analysis.get("venue_live_busyness_available", False)

            # Use live if available, otherwise forecasted
            busyness_value = live_busyness if live_available and live_busyness is not None else forecast_busyness
            if busyness_value is None:
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(busyness_value),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "venue": venue_name,
                    "live_busyness": live_busyness,
                    "forecast_busyness": forecast_busyness,
                    "delta": delta,
                    "live_available": live_available,
                    "venue_open": venue_info.get("venue_open"),
                    "local_time": venue_info.get("venue_current_localtime"),
                },
            )
            await self.board.store_observation(obs)

            # Describe the busyness level
            if busyness_value >= 80:
                level = "very busy"
            elif busyness_value >= 60:
                level = "busy"
            elif busyness_value >= 40:
                level = "moderate"
            elif busyness_value >= 20:
                level = "quiet"
            else:
                level = "very quiet"

            delta_str = ""
            if delta is not None and live_available:
                sign = "+" if delta > 0 else ""
                delta_str = f", {sign}{delta}% vs forecast"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Crowd density [{venue_name}] busyness={busyness_value}% "
                    f"({level}{delta_str})"
                ),
                data={
                    "venue": venue_name,
                    "busyness": busyness_value,
                    "level": level,
                    "live_available": live_available,
                    "delta": delta,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Crowd density: processed=%d venues of %d", processed, len(data))


async def ingest_crowd_density(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one crowd density fetch cycle."""
    ingestor = CrowdDensityIngestor(board, graph, scheduler)
    await ingestor.run()
