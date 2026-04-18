"""TfL station crowding ingestor — passenger density at major tube stations.

Uses TfL's crowding API to fetch time-band crowding percentages for key
interchange stations.  Maps the current time to the relevant band so the
system gets a near-real-time passenger-flow signal that, combined with
disruption data, supports modelling how incidents redirect movement.

Note: True real-time Oyster tap data is not publicly available.  This
endpoint provides TfL's published crowding profiles (percentages by
time-of-day), which is the closest public proxy.
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

log = logging.getLogger("cortex.ingestors.tfl_crowding")

TFL_CROWDING_URL = "https://api.tfl.gov.uk/crowding/{naptan}"

# Major interchange stations — enough to capture cross-network flow without
# hammering the API.  NaPTAN IDs from TfL open data.
STATIONS: list[dict[str, Any]] = [
    {"naptan": "940GZZLUKSX", "name": "King's Cross St Pancras", "lat": 51.5308, "lon": -0.1238},
    {"naptan": "940GZZLUWLO", "name": "Waterloo", "lat": 51.5036, "lon": -0.1143},
    {"naptan": "940GZZLUVIC", "name": "Victoria", "lat": 51.4965, "lon": -0.1447},
    {"naptan": "940GZZLULVT", "name": "Liverpool Street", "lat": 51.5178, "lon": -0.0823},
    {"naptan": "940GZZLUPAC", "name": "Paddington", "lat": 51.5154, "lon": -0.1755},
    {"naptan": "940GZZLUOXC", "name": "Oxford Circus", "lat": 51.5152, "lon": -0.1415},
    {"naptan": "940GZZLUBND", "name": "Bond Street", "lat": 51.5142, "lon": -0.1494},
    {"naptan": "940GZZLUBST", "name": "Baker Street", "lat": 51.5226, "lon": -0.1571},
    {"naptan": "940GZZLULNB", "name": "London Bridge", "lat": 51.5052, "lon": -0.0864},
    {"naptan": "940GZZLUBKF", "name": "Blackfriars", "lat": 51.5120, "lon": -0.1039},
    {"naptan": "940GZZLUGPK", "name": "Green Park", "lat": 51.5067, "lon": -0.1428},
    {"naptan": "940GZZLUBNK", "name": "Bank", "lat": 51.5133, "lon": -0.0886},
    {"naptan": "940GZZLUWSM", "name": "Westminster", "lat": 51.5010, "lon": -0.1254},
    {"naptan": "940GZZLUTCR", "name": "Tottenham Court Road", "lat": 51.5165, "lon": -0.1310},
    {"naptan": "940GZZLUCST", "name": "Cannon Street", "lat": 51.5113, "lon": -0.0904},
    {"naptan": "940GZZLUEUS", "name": "Euston", "lat": 51.5282, "lon": -0.1337},
    {"naptan": "940GZZLUFCO", "name": "Farringdon", "lat": 51.5203, "lon": -0.1053},
    {"naptan": "940GZZLUMGT", "name": "Moorgate", "lat": 51.5186, "lon": -0.0886},
    {"naptan": "940GZZLUERB", "name": "Edgware Road (Bakerloo)", "lat": 51.5199, "lon": -0.1679},
    {"naptan": "940GZZLUWRP", "name": "Wembley Park", "lat": 51.5635, "lon": -0.2795},
    {"naptan": "940GZZLUSTR", "name": "Stratford", "lat": 51.5416, "lon": -0.0042},
    {"naptan": "940GZZLUCPK", "name": "Canary Wharf", "lat": 51.5035, "lon": -0.0187},
    {"naptan": "940GZZLUBMY", "name": "Bermondsey", "lat": 51.4979, "lon": -0.0637},
    {"naptan": "940GZZLUKNG", "name": "Kennington", "lat": 51.4884, "lon": -0.1053},
    {"naptan": "940GZZLUCWR", "name": "Canada Water", "lat": 51.4982, "lon": -0.0502},
]

# Day-name mapping expected by the TfL crowding API
_DAY_NAMES = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _current_day_name() -> str:
    return _DAY_NAMES[datetime.now(timezone.utc).weekday()]


def _match_time_band(bands: list[dict], now_minutes: int) -> dict | None:
    """Find the band whose time range contains *now_minutes* (minutes since midnight)."""
    for band in bands:
        # TfL returns "timeBand" as e.g. "AM Peak" with "startTime"/"endTime" like "07:00"/"10:00"
        try:
            start = band.get("startTime", "")
            end = band.get("endTime", "")
            sh, sm = (int(x) for x in start.split(":"))
            eh, em = (int(x) for x in end.split(":"))
            start_min = sh * 60 + sm
            end_min = eh * 60 + em
            if start_min <= now_minutes < end_min:
                return band
        except (ValueError, AttributeError):
            continue
    return None


class TflCrowdingIngestor(BaseIngestor):
    source_name = "tfl_crowding"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        """Fetch crowding data for all monitored stations."""
        day = _current_day_name()
        results = []
        for station in STATIONS:
            url = TFL_CROWDING_URL.format(naptan=station["naptan"])
            data = await self.fetch(url, params={"dayOfTheWeek": day})
            if data is not None:
                results.append({"station": station, "data": data})
        return results if results else None

    async def process(self, data: Any) -> None:
        now = datetime.now(timezone.utc)
        now_minutes = now.hour * 60 + now.minute
        processed = 0
        skipped = 0

        for item in data:
            station = item["station"]
            raw = item["data"]

            # The TfL crowding response structure varies; adapt to both
            # list-of-bands and nested-object formats.
            bands: list[dict] = []
            if isinstance(raw, list):
                bands = raw
            elif isinstance(raw, dict):
                bands = raw.get("timeBands", raw.get("data", []))
                if isinstance(bands, dict):
                    bands = [bands]

            if not bands:
                skipped += 1
                continue

            band = _match_time_band(bands, now_minutes)
            if band is None:
                skipped += 1
                continue

            crowding_pct = band.get("percentageOfBaseline") or band.get("percentage")
            if crowding_pct is None:
                skipped += 1
                continue

            try:
                crowding_pct = float(crowding_pct)
            except (ValueError, TypeError):
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(station["lat"], station["lon"])

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=crowding_pct,
                location_id=cell_id,
                lat=station["lat"],
                lon=station["lon"],
                metadata={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "time_band": band.get("timeBand", ""),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Tube crowding [{station['name']}] "
                    f"{crowding_pct:.0f}% of baseline ({band.get('timeBand', '')})"
                ),
                data={
                    "station": station["name"],
                    "naptan": station["naptan"],
                    "crowding_pct": crowding_pct,
                    "time_band": band.get("timeBand", ""),
                    "lat": station["lat"],
                    "lon": station["lon"],
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("TfL crowding: processed=%d skipped=%d", processed, skipped)


async def ingest_tfl_crowding(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL crowding fetch cycle."""
    ingestor = TflCrowdingIngestor(board, graph, scheduler)
    await ingestor.run()
