"""TfL Tube train arrival predictions — headway and frequency monitoring.

Queries the TfL Line Arrivals API for major Tube lines at key interchange
stations to compute real-time headways (minutes between successive trains).
During disruptions, headways spike — this is the most direct real-time
signal of service degradation available via public API.

When connectors cross-reference headway spikes with passenger count baselines
(from tfl_passenger_counts) and crowding (from tfl_crowding), the system can
estimate displacement flows: passengers forced onto alternative routes.

Displacement estimate = baseline_passengers × (1 - expected_freq / actual_freq)

No API key required (TfL open data).  Uses the "tfl" rate limiter.
"""

from __future__ import annotations

import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tube_arrivals")

TFL_LINE_ARRIVALS_URL = "https://api.tfl.gov.uk/Line/{line_id}/Arrivals"

# Lines to monitor — major Tube lines plus Elizabeth line.
# Each carries enough passengers that frequency drops cause measurable displacement.
LINES = [
    "central",
    "victoria",
    "jubilee",
    "northern",
    "piccadilly",
    "district",
    "bakerloo",
    "metropolitan",
    "circle",
    "hammersmith-city",
    "elizabeth",
]

# Expected peak headways (minutes) per line — used to flag degraded service.
# Source: TfL standard timetable.
EXPECTED_PEAK_HEADWAY: dict[str, float] = {
    "central": 2.5,
    "victoria": 2.0,
    "jubilee": 2.5,
    "northern": 2.5,
    "piccadilly": 3.0,
    "district": 4.0,
    "bakerloo": 3.5,
    "metropolitan": 5.0,
    "circle": 8.0,
    "hammersmith-city": 6.0,
    "elizabeth": 5.0,
}

# Off-peak expected headways (roughly double peak)
EXPECTED_OFFPEAK_HEADWAY: dict[str, float] = {
    k: v * 2.0 for k, v in EXPECTED_PEAK_HEADWAY.items()
}


def _is_peak_hour() -> bool:
    """True if current UTC hour is within weekday peak bands (07-10 or 16-19)."""
    now = datetime.now(timezone.utc)
    if now.weekday() >= 5:  # weekend
        return False
    return 7 <= now.hour < 10 or 16 <= now.hour < 19


class TubeArrivalsIngestor(BaseIngestor):
    """Ingests real-time train arrival predictions to compute per-line headways."""

    source_name = "tube_arrivals"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        """Fetch arrival predictions for all monitored lines."""
        results = {}
        for line_id in LINES:
            url = TFL_LINE_ARRIVALS_URL.format(line_id=line_id)
            data = await self.fetch(url, retries=2)
            if data and isinstance(data, list):
                results[line_id] = data
        return results if results else None

    async def process(self, data: Any) -> None:
        peak = _is_peak_hour()
        expected_headways = EXPECTED_PEAK_HEADWAY if peak else EXPECTED_OFFPEAK_HEADWAY
        processed = 0

        for line_id, arrivals in data.items():
            # Group arrivals by station (naptanId)
            by_station: dict[str, list[dict]] = defaultdict(list)
            for arr in arrivals:
                naptan = arr.get("naptanId")
                if naptan:
                    by_station[naptan].append(arr)

            # Compute headway per station: sort by timeToStation, take gaps
            station_headways: list[float] = []
            station_details: list[dict] = []

            for naptan, station_arrivals in by_station.items():
                # Sort by time to arrival (seconds)
                tts_values = []
                for a in station_arrivals:
                    tts = a.get("timeToStation")
                    if tts is not None:
                        tts_values.append(tts)

                if len(tts_values) < 2:
                    continue

                tts_values.sort()
                # Headway = gaps between consecutive trains (in minutes)
                gaps = [
                    (tts_values[i + 1] - tts_values[i]) / 60.0
                    for i in range(len(tts_values) - 1)
                ]
                if gaps:
                    avg_gap = sum(gaps) / len(gaps)
                    station_headways.append(avg_gap)

                    # Get station info from first arrival
                    first = station_arrivals[0]
                    station_details.append({
                        "naptan": naptan,
                        "station_name": first.get("stationName", naptan),
                        "headway_min": round(avg_gap, 1),
                        "train_count": len(tts_values),
                    })

            if not station_headways:
                continue

            # Line-level summary
            line_avg_headway = sum(station_headways) / len(station_headways)
            expected = expected_headways.get(line_id, 5.0)
            headway_ratio = line_avg_headway / expected if expected > 0 else 1.0

            # Determine service status from headway ratio
            if headway_ratio <= 1.2:
                status = "NORMAL"
            elif headway_ratio <= 2.0:
                status = "MINOR_DELAYS"
            elif headway_ratio <= 4.0:
                status = "SEVERE_DELAYS"
            else:
                status = "SUSPENDED"

            # Find worst station (highest headway) for spatial indexing
            worst = max(station_details, key=lambda s: s["headway_min"])
            # Get lat/lon from a representative arrival
            rep_lat = rep_lon = None
            for arr in data[line_id]:
                if arr.get("naptanId") == worst["naptan"]:
                    # TfL arrivals don't always include lat/lon directly;
                    # fall back to a known central London point for the line
                    break

            # Use station_details for the line-level observation
            # Pick the midpoint of the line's monitored stations
            all_lats = []
            all_lons = []
            for arr in data[line_id]:
                # Some arrivals don't have coordinates
                pass

            # Default to central London for line-level observations
            cell_id = self.graph.latlon_to_cell(51.51, -0.12)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=round(line_avg_headway, 2),
                location_id=cell_id,
                lat=51.51,
                lon=-0.12,
                metadata={
                    "line": line_id,
                    "avg_headway_min": round(line_avg_headway, 1),
                    "expected_headway_min": expected,
                    "headway_ratio": round(headway_ratio, 2),
                    "status": status,
                    "peak": peak,
                    "stations_sampled": len(station_headways),
                    "worst_station": worst["station_name"],
                    "worst_headway_min": worst["headway_min"],
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Tube arrivals [{line_id}] headway={line_avg_headway:.1f}min "
                    f"(expected {expected:.0f}min, ratio={headway_ratio:.1f}x) "
                    f"{status} worst={worst['station_name']}({worst['headway_min']:.0f}min) "
                    f"stations={len(station_headways)}"
                ),
                data={
                    "line": line_id,
                    "avg_headway_min": round(line_avg_headway, 1),
                    "expected_headway_min": expected,
                    "headway_ratio": round(headway_ratio, 2),
                    "status": status,
                    "peak": peak,
                    "stations_sampled": len(station_headways),
                    "worst_station": worst["station_name"],
                    "worst_headway_min": worst["headway_min"],
                    "station_details": station_details[:10],  # top 10 to limit payload
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Tube arrivals: processed=%d lines, peak=%s", processed, peak
        )


async def ingest_tube_arrivals(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one tube arrivals fetch cycle."""
    ingestor = TubeArrivalsIngestor(board, graph, scheduler)
    await ingestor.run()
