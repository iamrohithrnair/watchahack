"""Police stop-and-search ingestor — data.police.uk (free, no API key).

Fetches stop-and-search data for sampled London locations, providing
a policing-intensity signal that correlates with public gatherings,
protests, and large events. Complements police_crimes.py by capturing
enforcement *actions* (not just recorded offences).

Stop-and-search spikes often coincide with major public events, marches,
and demonstrations — helping verify predictions about crowd activity.

Data is monthly (not real-time), sourced from the Metropolitan Police via
the open data.police.uk API.
"""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.police_stop_search")

# data.police.uk endpoints — free, no API key required
STOPS_STREET_URL = "https://data.police.uk/api/stops-street"
STOP_DATES_URL = "https://data.police.uk/api/crimes-street-dates"

# Object-of-search categories relevant to event/gathering detection
EVENT_RELEVANT_SEARCHES = {
    "Offensive weapons",
    "Controlled drugs",
    "Firearms",
    "Anything to threaten or harm anyone",
    "Articles for use in criminal damage",
}

# Sample points across Greater London — spread to capture diverse areas
SAMPLE_POINTS = [
    (51.5074, -0.1278),   # Central London / Westminster
    (51.5155, -0.0922),   # City of London
    (51.5033, -0.1195),   # Whitehall / Parliament (protest hotspot)
    (51.5313, -0.1040),   # Kings Cross / Euston
    (51.4613, -0.1156),   # Brixton
    (51.5430, -0.0553),   # Hackney / Dalston
    (51.5225, -0.1540),   # Paddington
    (51.5556, -0.1084),   # Finsbury Park
    (51.5136, 0.0890),    # Canning Town / Newham
    (51.4816, -0.0105),   # Greenwich
    (51.5014, -0.1796),   # Hyde Park (event venue)
    (51.5081, -0.0759),   # Tower Bridge / Southwark
]

# Rate-limit friendly: sample a few points per cycle
MAX_POINTS_PER_CYCLE = 4


class PoliceStopSearchIngestor(BaseIngestor):
    source_name = "police_stop_search"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Get latest available date
        dates_data = await self.fetch(STOP_DATES_URL)
        if not dates_data or not isinstance(dates_data, list):
            self.log.warning("Could not fetch available dates for stop-search")
            return None

        latest_date = dates_data[0].get("date") if dates_data else None
        if not latest_date:
            self.log.warning("No date found in stop-search dates response")
            return None

        # Sample points to stay within rate limits
        points = random.sample(
            SAMPLE_POINTS, min(MAX_POINTS_PER_CYCLE, len(SAMPLE_POINTS))
        )

        all_stops: list[dict] = []
        for lat, lng in points:
            data = await self.fetch(
                STOPS_STREET_URL,
                params={"lat": str(lat), "lng": str(lng), "date": latest_date},
            )
            if isinstance(data, list):
                all_stops.extend(data)

        return {"date": latest_date, "stops": all_stops}

    async def process(self, data: Any) -> None:
        stops = data.get("stops", [])
        date = data.get("date", "unknown")

        if not stops:
            self.log.info("No stop-search data returned for %s", date)
            return

        # Aggregate by (object_of_search, outcome, snap_location) for efficiency
        location_counts: dict[tuple[str, str, float, float], int] = {}

        for stop in stops:
            object_of_search = stop.get("object_of_search", "unknown") or "unknown"
            outcome = stop.get("outcome", "unknown") or "unknown"
            location = stop.get("location", {}) or {}
            lat_str = location.get("latitude")
            lon_str = location.get("longitude")

            try:
                lat = float(lat_str) if lat_str else None
                lon = float(lon_str) if lon_str else None
            except (ValueError, TypeError):
                continue

            if lat is None or lon is None:
                continue

            key = (object_of_search, outcome, lat, lon)
            location_counts[key] = location_counts.get(key, 0) + 1

        processed = 0
        for (search_type, outcome, lat, lon), count in location_counts.items():
            cell_id = self.graph.latlon_to_cell(lat, lon)
            is_event_relevant = search_type in EVENT_RELEVANT_SEARCHES

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(count),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "search_type": search_type,
                    "outcome": outcome,
                    "date": date,
                    "count": count,
                    "event_relevant": is_event_relevant,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Stop-search [{date}] {search_type}={count} ({outcome})"
                    + (" [EVENT-SIGNAL]" if is_event_relevant else "")
                ),
                data={
                    "search_type": search_type,
                    "outcome": outcome,
                    "lat": lat,
                    "lon": lon,
                    "count": count,
                    "date": date,
                    "event_relevant": is_event_relevant,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        # Summary stats
        event_total = sum(
            count
            for (st, _, _, _), count in location_counts.items()
            if st in EVENT_RELEVANT_SEARCHES
        )
        total = sum(location_counts.values())

        self.log.info(
            "Police stop-search [%s]: %d locations, %d total stops (%d event-relevant)",
            date, processed, total, event_total,
        )


async def ingest_police_stop_search(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one police stop-search fetch cycle."""
    ingestor = PoliceStopSearchIngestor(board, graph, scheduler)
    await ingestor.run()
