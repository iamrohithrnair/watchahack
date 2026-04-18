"""Commuter displacement proxy — TfL Journey Planner travel-time monitoring.

Queries TfL Journey Planner for key commuter corridors and tracks journey
duration changes. Spikes in travel time indicate commuter displacement due
to disruptions — passengers are forced onto longer/slower alternative routes.

The Brain suggested mobile network data (O2 Motion) but that requires a
commercial enterprise contract. This ingestor uses the free TfL Journey
Planner API as a proxy: when a Tube line goes down, alternative journey
times balloon, directly quantifying the displacement effect.

No API key required (TfL open data).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.commuter_displacement")

TFL_JOURNEY_URL = "https://api.tfl.gov.uk/Journey/JourneyResults/{origin}/to/{destination}"

# Key commuter corridors: (name, origin_coord, destination_coord)
# Coordinates as "lat,lon" strings for TfL API.
# Chosen to cross major rail/tube lines so disruptions cause measurable rerouting.
CORRIDORS = [
    ("Stratford → Bank", "51.5430,-0.0035", "51.5133,-0.0886"),
    ("Brixton → Kings Cross", "51.4613,-0.1146", "51.5308,-0.1238"),
    ("Ealing Broadway → Paddington", "51.5150,-0.3019", "51.5154,-0.1755"),
    ("Lewisham → London Bridge", "51.4657,-0.0142", "51.5055,-0.0860"),
    ("Finsbury Park → Moorgate", "51.5642,-0.1065", "51.5186,-0.0886"),
    ("Clapham Jn → Victoria", "51.4640,-0.1703", "51.4952,-0.1439"),
    ("Walthamstow → Liverpool St", "51.5830,-0.0200", "51.5178,-0.0823"),
    ("Wimbledon → Waterloo", "51.4214,-0.2064", "51.5032,-0.1132"),
    ("Barking → Fenchurch St", "51.5396,0.0809", "51.5115,-0.0794"),
    ("Richmond → Hammersmith", "51.4613,-0.3037", "51.4927,-0.2246"),
]


class CommuterDisplacementIngestor(BaseIngestor):
    source_name = "commuter_displacement"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        results = []
        now = datetime.now(timezone.utc)
        time_str = now.strftime("%H%M")

        for name, origin, destination in CORRIDORS:
            url = TFL_JOURNEY_URL.format(origin=origin, destination=destination)
            params = {
                "mode": "tube,dlr,overground,elizabeth-line,bus,walking",
                "time": time_str,
                "timeIs": "Departing",
                "journeyPreference": "LeastTime",
            }
            data = await self.fetch(url, params=params, retries=2)
            if data and isinstance(data, dict):
                results.append((name, origin, destination, data))

        return results if results else None

    async def process(self, data: Any) -> None:
        processed = 0

        for name, origin, destination, response in data:
            journeys = response.get("journeys", [])
            if not journeys:
                continue

            # Extract durations from all suggested journeys
            durations = []
            disrupted_modes = set()
            for journey in journeys:
                dur = journey.get("duration")
                if dur is not None:
                    durations.append(dur)
                # Check if any legs are disrupted
                for leg in journey.get("legs", []):
                    disruptions = leg.get("disruptions", [])
                    if disruptions:
                        mode = leg.get("mode", {})
                        mode_name = mode.get("name", "") if isinstance(mode, dict) else str(mode)
                        disrupted_modes.add(mode_name)

            if not durations:
                continue

            fastest_min = min(durations)
            avg_min = sum(durations) / len(durations)

            # Parse origin coords for spatial indexing (midpoint of corridor)
            try:
                olat, olon = [float(x) for x in origin.split(",")]
                dlat, dlon = [float(x) for x in destination.split(",")]
                mid_lat = (olat + dlat) / 2
                mid_lon = (olon + dlon) / 2
            except (ValueError, IndexError):
                mid_lat = mid_lon = None

            cell_id = self.graph.latlon_to_cell(mid_lat, mid_lon) if mid_lat else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=fastest_min,
                location_id=cell_id,
                lat=mid_lat,
                lon=mid_lon,
                metadata={
                    "corridor": name,
                    "fastest_min": fastest_min,
                    "avg_min": round(avg_min, 1),
                    "num_routes": len(durations),
                    "disrupted_modes": sorted(disrupted_modes),
                    "all_durations": durations,
                },
            )
            await self.board.store_observation(obs)

            # Flag corridors with active disruptions more prominently
            status = "DISRUPTED" if disrupted_modes else "NORMAL"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Commuter displacement [{name}] fastest={fastest_min}min "
                    f"avg={avg_min:.0f}min routes={len(durations)} ({status})"
                    + (f" disrupted={','.join(sorted(disrupted_modes))}" if disrupted_modes else "")
                ),
                data={
                    "corridor": name,
                    "fastest_min": fastest_min,
                    "avg_min": round(avg_min, 1),
                    "num_routes": len(durations),
                    "status": status,
                    "disrupted_modes": sorted(disrupted_modes),
                    "lat": mid_lat,
                    "lon": mid_lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Commuter displacement: processed=%d corridors", processed)


async def ingest_commuter_displacement(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one commuter displacement fetch cycle."""
    ingestor = CommuterDisplacementIngestor(board, graph, scheduler)
    await ingestor.run()
