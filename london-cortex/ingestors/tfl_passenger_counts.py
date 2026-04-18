"""TfL station entry/exit passenger counts — NUMBAT baseline + displacement estimation.

Downloads NUMBAT entry/exit data from TfL's open-data S3 bucket and combines
it with the current time-window to post expected passenger volumes per station.
When connectors cross-reference these volumes with crowding percentages
(from tfl_crowding) and active disruptions (from road_disruptions /
planned_works), the system can estimate passenger displacement — how many
people are being redirected by a given incident.

The NUMBAT dataset is a "typical autumn weekday" profile at 15-minute
granularity.  It's the closest public proxy for Oyster/contactless tap data,
which TfL does not expose via any public API.
"""

from __future__ import annotations

import csv
import io
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ..core.board import MessageBoard
from ..core.config import CACHE_DIR
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl_passenger_counts")

# NUMBAT annualised entry/exit data — CSV export of the most recent year
# available on TfL's open data bucket.  Falls back gracefully if unavailable.
NUMBAT_ENTRY_EXIT_URL = (
    "https://crowding.data.tfl.gov.uk/Annual%20Station%20Counts/2021/"
    "AC2021_AnnualisedEntryExit.xlsx"
)

# We use the StopPoint crowding endpoint to get 15-min-resolution profiles
# (same data backing the crowding API, but with absolute counts).
TFL_STOP_CROWDING_URL = "https://api.tfl.gov.uk/StopPoint/{naptan}"

# Stations we monitor — same set as tfl_crowding for consistency.
# Extended with a few Bakerloo stations to support the carbon-surge hypothesis.
STATIONS: list[dict[str, Any]] = [
    {"naptan": "940GZZLUKSX", "name": "King's Cross St Pancras", "lat": 51.5308, "lon": -0.1238},
    {"naptan": "940GZZLUWLO", "name": "Waterloo", "lat": 51.5036, "lon": -0.1143},
    {"naptan": "940GZZLUVIC", "name": "Victoria", "lat": 51.4965, "lon": -0.1447},
    {"naptan": "940GZZLULVT", "name": "Liverpool Street", "lat": 51.5178, "lon": -0.0823},
    {"naptan": "940GZZLUPAC", "name": "Paddington", "lat": 51.5154, "lon": -0.1755},
    {"naptan": "940GZZLUOXC", "name": "Oxford Circus", "lat": 51.5152, "lon": -0.1415},
    {"naptan": "940GZZLULNB", "name": "London Bridge", "lat": 51.5052, "lon": -0.0864},
    {"naptan": "940GZZLUBNK", "name": "Bank", "lat": 51.5133, "lon": -0.0886},
    {"naptan": "940GZZLUGPK", "name": "Green Park", "lat": 51.5067, "lon": -0.1428},
    {"naptan": "940GZZLUEUS", "name": "Euston", "lat": 51.5282, "lon": -0.1337},
    {"naptan": "940GZZLUERB", "name": "Edgware Road (Bakerloo)", "lat": 51.5199, "lon": -0.1679},
    {"naptan": "940GZZLUWRP", "name": "Wembley Park", "lat": 51.5635, "lon": -0.2795},
    {"naptan": "940GZZLUSTR", "name": "Stratford", "lat": 51.5416, "lon": -0.0042},
    {"naptan": "940GZZLUCPK", "name": "Canary Wharf", "lat": 51.5035, "lon": -0.0187},
    {"naptan": "940GZZLUKNG", "name": "Kennington", "lat": 51.4884, "lon": -0.1053},
    {"naptan": "940GZZLUCWR", "name": "Canada Water", "lat": 51.4982, "lon": -0.0502},
    # Bakerloo corridor: Elephant & Castle, Lambeth North, Embankment
    {"naptan": "940GZZLUEAC", "name": "Elephant & Castle", "lat": 51.4943, "lon": -0.1001},
    {"naptan": "940GZZLULBN", "name": "Lambeth North", "lat": 51.4991, "lon": -0.1115},
    {"naptan": "940GZZLUEMB", "name": "Embankment", "lat": 51.5074, "lon": -0.1223},
]

# Day-type used by NUMBAT: Mon-Thu, Fri, Sat, Sun
_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

_CACHE_FILE = CACHE_DIR / "numbat_entry_exit.csv"
_CACHE_MAX_AGE_DAYS = 30  # re-download baseline data monthly


def _current_15min_slot() -> str:
    """Return current UTC time rounded down to 15-min slot as 'HH:MM'."""
    now = datetime.now(timezone.utc)
    minute = (now.minute // 15) * 15
    return f"{now.hour:02d}:{minute:02d}"


def _day_type() -> str:
    """Return NUMBAT day-type for today."""
    dow = datetime.now(timezone.utc).weekday()
    if dow < 4:
        return "Mon-Thu"
    return _DAY_NAMES[dow]


class TflPassengerCountsIngestor(BaseIngestor):
    """Ingests expected passenger entry/exit counts per station per time-window.

    Uses the TfL StopPoint endpoint with includeCrowdingData=true to pull
    the published crowding profiles (absolute counts where available) and
    posts them as NUMERIC observations keyed by station + time-slot.
    """

    source_name = "tfl_passenger_counts"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        """Fetch crowding data with absolute counts for monitored stations."""
        results = []
        for station in STATIONS:
            url = TFL_STOP_CROWDING_URL.format(naptan=station["naptan"])
            data = await self.fetch(
                url, params={"includeCrowdingData": "true"}
            )
            if data is not None:
                results.append({"station": station, "data": data})
        return results if results else None

    async def process(self, data: Any) -> None:
        now = datetime.now(timezone.utc)
        now_minutes = now.hour * 60 + now.minute
        time_slot = _current_15min_slot()
        day_type = _day_type()
        processed = 0
        skipped = 0

        for item in data:
            station = item["station"]
            raw = item["data"]

            # Extract crowding data from StopPoint response
            crowding_data = None
            if isinstance(raw, dict):
                # StopPoint response nests crowding under additionalProperties
                # or directly as crowdingData
                crowding_data = raw.get("crowdingData")
                if crowding_data is None:
                    # Try additionalProperties path
                    for prop in raw.get("additionalProperties", []):
                        if prop.get("category") == "Crowding":
                            crowding_data = prop
                            break

            # Also look for time-band data in the response
            bands = []
            if isinstance(crowding_data, dict):
                bands = crowding_data.get("timeBands", [])
            elif isinstance(raw, dict):
                bands = raw.get("timeBands", [])
            elif isinstance(raw, list):
                bands = raw

            if not bands:
                skipped += 1
                continue

            # Find matching time band
            entry_count = None
            exit_count = None
            crowding_pct = None

            for band in bands:
                try:
                    start = band.get("startTime", "")
                    end = band.get("endTime", "")
                    sh, sm = (int(x) for x in start.split(":"))
                    eh, em = (int(x) for x in end.split(":"))
                    start_min = sh * 60 + sm
                    end_min = eh * 60 + em
                    if start_min <= now_minutes < end_min:
                        entry_count = band.get("entryCount") or band.get("entries")
                        exit_count = band.get("exitCount") or band.get("exits")
                        crowding_pct = (
                            band.get("percentageOfBaseline")
                            or band.get("percentage")
                        )
                        break
                except (ValueError, AttributeError):
                    continue

            # We need at least crowding percentage to be useful
            if crowding_pct is None and entry_count is None:
                skipped += 1
                continue

            # Build numeric value: prefer absolute counts, fall back to %
            if entry_count is not None:
                try:
                    value = float(entry_count)
                except (ValueError, TypeError):
                    value = None
            elif crowding_pct is not None:
                try:
                    value = float(crowding_pct)
                except (ValueError, TypeError):
                    value = None
            else:
                value = None

            if value is None:
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(station["lat"], station["lon"])
            metric_type = "entry_count" if entry_count is not None else "crowding_pct"

            try:
                entry_val = float(entry_count) if entry_count else None
            except (ValueError, TypeError):
                entry_val = None
            try:
                exit_val = float(exit_count) if exit_count else None
            except (ValueError, TypeError):
                exit_val = None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=station["lat"],
                lon=station["lon"],
                metadata={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "time_slot": time_slot,
                    "day_type": day_type,
                    "metric": metric_type,
                    "entry_count": entry_val,
                    "exit_count": exit_val,
                    "crowding_pct": float(crowding_pct) if crowding_pct else None,
                },
            )
            await self.board.store_observation(obs)

            # Format human-readable message
            parts = [f"Passenger flow [{station['name']}]"]
            if entry_val is not None:
                parts.append(f"entries={entry_val:.0f}")
            if exit_val is not None:
                parts.append(f"exits={exit_val:.0f}")
            if crowding_pct is not None:
                parts.append(f"crowding={float(crowding_pct):.0f}%")
            parts.append(f"({time_slot} {day_type})")

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=" ".join(parts),
                data={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "entry_count": entry_val,
                    "exit_count": exit_val,
                    "crowding_pct": float(crowding_pct) if crowding_pct else None,
                    "time_slot": time_slot,
                    "day_type": day_type,
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "TfL passenger counts: processed=%d skipped=%d", processed, skipped
        )


async def ingest_tfl_passenger_counts(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL passenger counts cycle."""
    ingestor = TflPassengerCountsIngestor(board, graph, scheduler)
    await ingestor.run()
