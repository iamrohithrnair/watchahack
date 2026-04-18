"""TfL Bus Crowding ingestor — measures passenger displacement onto bus routes
paralleling suspended tube lines.

Uses the TfL Unified API to monitor bus routes that serve as surface alternatives
to the Bakerloo, Northern, Jubilee, and other tube lines. Unlike the bus_congestion
ingestor (which uses bus wait times as a road congestion proxy on major corridors),
this ingestor specifically tracks displacement signals: headway irregularity,
vehicle bunching density, and arrival frequency on tube-parallel routes.

When a tube line is disrupted, passengers flood surface bus routes. This ingestor
quantifies that stress in real-time.
"""

from __future__ import annotations

import logging
import statistics
from collections import defaultdict
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.bus_crowding")

TFL_API_BASE = "https://api.tfl.gov.uk"

# Bus routes that parallel tube lines — these absorb displaced passengers
# during suspensions.  Mapping: tube line → parallel bus routes.
TUBE_PARALLEL_ROUTES: dict[str, list[str]] = {
    "bakerloo": [
        "36",   # Paddington → New Cross (parallels Bakerloo south section)
        "6",    # Willesden → Aldwych (parallels northern Bakerloo)
        "187",  # Central Middlesex → Finchley Road (Bakerloo + Met corridor)
        "18",   # Sudbury → Baker Street (Bakerloo NW section)
        "205",  # Paddington → Bow (via Marylebone, Baker St)
    ],
    "northern": [
        "24",   # Hampstead Heath → Pimlico (Camden, Charing Cross branch)
        "134",  # North Finchley → Warren Street (High Barnet branch)
        "Northern-63",  # alias: route 63 King's Cross → Honor Oak
    ],
    "jubilee": [
        "188",  # North Greenwich → Russell Square (Jubilee central)
        "13",   # North Finchley → Aldwych (Jubilee NW section)
    ],
}

# Flatten to unique route IDs, stripping any alias prefixes
ALL_PARALLEL_ROUTES: list[str] = []
_ROUTE_TO_TUBE: dict[str, str] = {}
for _tube, _routes in TUBE_PARALLEL_ROUTES.items():
    for _r in _routes:
        route_id = _r.split("-")[-1] if "-" in _r else _r
        if route_id not in ALL_PARALLEL_ROUTES:
            ALL_PARALLEL_ROUTES.append(route_id)
        _ROUTE_TO_TUBE[route_id] = _tube

# Fetch a subset each cycle to respect rate limits
MAX_ROUTES_PER_CYCLE = 4
_cycle_offset = 0


class BusCrowdingIngestor(BaseIngestor):
    source_name = "tfl_bus_crowding"
    rate_limit_name = "tfl"

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._stop_coords: dict[str, tuple[float, float]] = {}

    async def _ensure_stop_coords(self, line_id: str) -> None:
        """Fetch and cache stop coordinates for a line."""
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
        global _cycle_offset
        offset = (_cycle_offset * MAX_ROUTES_PER_CYCLE) % len(ALL_PARALLEL_ROUTES)
        _cycle_offset += 1
        routes_this_cycle = ALL_PARALLEL_ROUTES[offset:offset + MAX_ROUTES_PER_CYCLE]

        results: dict[str, list[dict]] = {}
        for line_id in routes_this_cycle:
            await self._ensure_stop_coords(line_id)
            url = f"{TFL_API_BASE}/Line/{line_id}/Arrivals"
            data = await self.fetch(url)
            if data and isinstance(data, list):
                results[line_id] = data
        return results

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict) or not data:
            return

        total = 0

        for line_id, arrivals in data.items():
            tube_line = _ROUTE_TO_TUBE.get(line_id, "unknown")

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

                valid = [p for p in preds if p.get("timeToStation") is not None]
                if len(valid) < 2:
                    continue
                valid.sort(key=lambda p: p["timeToStation"])

                times = [p["timeToStation"] for p in valid]
                n_vehicles = len(times)
                avg_wait = statistics.mean(times)

                # Headway gaps between consecutive arrivals
                gaps = [times[i] - times[i - 1] for i in range(1, len(times))]
                avg_gap = statistics.mean(gaps)
                gap_cv = (statistics.stdev(gaps) / avg_gap) if len(gaps) > 1 and avg_gap > 0 else 0.0

                # Bunching: vehicles within 60s of each other
                bunched = sum(1 for g in gaps if g < 60)

                # Crowding score: high vehicle count + tight gaps + bunching = displacement
                # Normalized 0-1 scale
                density_score = min(n_vehicles / 10.0, 1.0)
                bunching_score = min(bunched / max(len(gaps), 1), 1.0)
                crowding_score = round(0.5 * density_score + 0.3 * bunching_score + 0.2 * min(gap_cv, 1.0), 3)

                stop_name = valid[0].get("stationName", naptan)
                direction = valid[0].get("direction", "unknown")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=crowding_score,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "metric": "bus_crowding_displacement",
                        "bus_line": line_id,
                        "parallel_tube": tube_line,
                        "stop_naptan": naptan,
                        "stop_name": stop_name,
                        "direction": direction,
                        "n_vehicles": n_vehicles,
                        "avg_wait_s": round(avg_wait, 1),
                        "avg_gap_s": round(avg_gap, 1),
                        "gap_cv": round(gap_cv, 3),
                        "bunched_pairs": bunched,
                        "crowding_score": crowding_score,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Bus crowding [bus {line_id} ↔ {tube_line}] "
                        f"{stop_name}: score={crowding_score} "
                        f"vehicles={n_vehicles} gap_cv={gap_cv:.2f} "
                        f"bunched={bunched}"
                    ),
                    data={
                        "bus_line": line_id,
                        "parallel_tube": tube_line,
                        "stop_name": stop_name,
                        "stop_naptan": naptan,
                        "direction": direction,
                        "n_vehicles": n_vehicles,
                        "avg_wait_s": round(avg_wait, 1),
                        "avg_gap_s": round(avg_gap, 1),
                        "gap_cv": round(gap_cv, 3),
                        "bunched_pairs": bunched,
                        "crowding_score": crowding_score,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                total += 1

        self.log.info(
            "Bus crowding: routes=%d observations=%d", len(data), total,
        )


async def ingest_bus_crowding(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one bus crowding fetch cycle."""
    ingestor = BusCrowdingIngestor(board, graph, scheduler)
    await ingestor.run()
