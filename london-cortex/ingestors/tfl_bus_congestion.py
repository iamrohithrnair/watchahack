"""TfL Bus Congestion ingestor — derives road congestion from bus arrival predictions.

Uses the TfL Unified API to fetch real-time bus arrival predictions for key routes.
By analysing timeToStation values and bus bunching patterns, this acts as a proxy
for road congestion — especially useful when JamCam cameras are offline.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl_bus_congestion")

TFL_API_BASE = "https://api.tfl.gov.uk"

# Key bus routes covering major corridors across London.
# Selected for geographic spread and overlap with JamCam-monitored roads.
KEY_ROUTES = [
    "24",   # Hampstead Heath → Pimlico (via Camden, Tottenham Ct Rd, Westminster)
    "11",   # Fulham → Liverpool St (via Chelsea, Westminster, City)
    "73",   # Victoria → Stoke Newington (via Oxford St, Angel)
    "38",   # Victoria → Clapton (via Piccadilly, Angel, Hackney)
    "12",   # Dulwich → Oxford Circus (via Elephant & Castle, Westminster)
    "159",  # Streatham → Paddington (via Brixton, Lambeth Bridge, Westminster)
    "148",  # Camberwell → White City (via Vauxhall, Westminster, Notting Hill)
    "25",   # Ilford → Oxford Circus (via Stratford, Mile End, Bank)
]

# Maximum routes to fetch per cycle (to stay within TfL rate limits)
MAX_ROUTES_PER_CYCLE = 4
# Rotate which routes we fetch each cycle
_cycle_counter = 0


class TflBusCongestionIngestor(BaseIngestor):
    source_name = "tfl_bus_congestion"
    rate_limit_name = "tfl"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._stop_coords: dict[str, tuple[float, float]] = {}

    async def _ensure_stop_coords(self, line_id: str) -> None:
        """Fetch and cache stop coordinates for a line if not already cached."""
        url = f"{TFL_API_BASE}/Line/{line_id}/StopPoints"
        data = await self.fetch(url)
        if not data or not isinstance(data, list):
            return
        for stop in data:
            naptan = stop.get("naptanId")
            lat = stop.get("lat")
            lon = stop.get("lon")
            if naptan and lat is not None and lon is not None:
                self._stop_coords[naptan] = (float(lat), float(lon))

    async def fetch_data(self) -> Any:
        global _cycle_counter
        offset = (_cycle_counter * MAX_ROUTES_PER_CYCLE) % len(KEY_ROUTES)
        _cycle_counter += 1
        routes_this_cycle = KEY_ROUTES[offset:offset + MAX_ROUTES_PER_CYCLE]

        results = {}
        for line_id in routes_this_cycle:
            await self._ensure_stop_coords(line_id)
            url = f"{TFL_API_BASE}/Line/{line_id}/Arrivals"
            data = await self.fetch(url)
            if data and isinstance(data, list):
                results[line_id] = data
        return results

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        total_observations = 0

        for line_id, arrivals in data.items():
            # Group arrivals by stop
            by_stop: dict[str, list[dict]] = defaultdict(list)
            for pred in arrivals:
                naptan = pred.get("naptanId")
                if naptan:
                    by_stop[naptan].append(pred)

            for naptan, preds in by_stop.items():
                coords = self._stop_coords.get(naptan)
                if not coords:
                    continue
                lat, lon = coords
                cell_id = self.graph.latlon_to_cell(lat, lon)

                # Sort by timeToStation
                valid_preds = [
                    p for p in preds
                    if p.get("timeToStation") is not None
                ]
                if not valid_preds:
                    continue
                valid_preds.sort(key=lambda p: p["timeToStation"])

                times = [p["timeToStation"] for p in valid_preds]
                n_vehicles = len(times)
                avg_wait = sum(times) / n_vehicles
                min_wait = times[0]

                # Bunching: count vehicles arriving within 60s of each other
                bunched = 0
                for i in range(1, len(times)):
                    if times[i] - times[i - 1] < 60:
                        bunched += 1

                stop_name = valid_preds[0].get("stationName", naptan)
                direction = valid_preds[0].get("direction", "unknown")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=avg_wait,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "line": line_id,
                        "stop_naptan": naptan,
                        "stop_name": stop_name,
                        "direction": direction,
                        "n_vehicles": n_vehicles,
                        "avg_wait_s": round(avg_wait, 1),
                        "min_wait_s": min_wait,
                        "bunched_pairs": bunched,
                        "metric": "bus_congestion_proxy",
                    },
                )
                await self.board.store_observation(obs)

                bunching_note = f" bunched={bunched}" if bunched else ""
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Bus congestion [{stop_name}] line={line_id} "
                        f"vehicles={n_vehicles} avg_wait={avg_wait:.0f}s "
                        f"min_wait={min_wait}s{bunching_note}"
                    ),
                    data={
                        "line": line_id,
                        "stop_name": stop_name,
                        "stop_naptan": naptan,
                        "direction": direction,
                        "n_vehicles": n_vehicles,
                        "avg_wait_s": round(avg_wait, 1),
                        "min_wait_s": min_wait,
                        "bunched_pairs": bunched,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                total_observations += 1

        self.log.info(
            "TfL bus congestion: routes=%d observations=%d",
            len(data), total_observations,
        )


async def ingest_tfl_bus_congestion(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL bus congestion fetch cycle."""
    ingestor = TflBusCongestionIngestor(board, graph, scheduler)
    await ingestor.run()
