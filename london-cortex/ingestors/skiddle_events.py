"""Skiddle Events API ingestor — hyper-local UK cultural events in London.

Free tier: https://www.skiddle.com/api/join.php (rate-limited, key required).
Covers gigs, club nights, theatre, comedy, exhibitions, festivals, and more —
smaller-scale cultural events that complement Ticketmaster/PredictHQ and provide
ground-truth for localised crowd formation and social-media activity predictions.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.skiddle_events")

SEARCH_URL = "https://www.skiddle.com/api/v1/events/search/"

# Centre of London (Charing Cross) + 15-mile radius covers Greater London
LONDON_LAT = 51.5074
LONDON_LON = -0.1278
LONDON_RADIUS_MILES = 15

# Map Skiddle eventcode to readable category
_CODE_MAP = {
    "FEST": "festival",
    "LIVE": "live_music",
    "CLUB": "club_night",
    "DATE": "dating",
    "THEATRE": "theatre",
    "COMEDY": "comedy",
    "EXHIB": "exhibition",
    "KIDS": "kids",
    "BARPUB": "bar_pub",
    "LGB": "lgbtq",
    "SPORT": "sports",
    "ARTS": "arts",
}


class SkiddleEventsIngestor(BaseIngestor):
    source_name = "skiddle_events"
    rate_limit_name = "default"
    required_env_vars = ["SKIDDLE_API_KEY"]

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("SKIDDLE_API_KEY", "")
        if not api_key:
            self.log.warning("SKIDDLE_API_KEY not set, skipping skiddle ingest")
            return None

        now = datetime.now(timezone.utc)
        params = {
            "api_key": api_key,
            "latitude": LONDON_LAT,
            "longitude": LONDON_LON,
            "radius": LONDON_RADIUS_MILES,
            "minDate": now.strftime("%Y-%m-%d"),
            "limit": 100,
            "description": 1,  # include genre/artist info
            "ticketsavailable": 1,
        }
        return await self.fetch(SEARCH_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Skiddle response type: %s", type(data))
            return

        events = data.get("results", [])
        if not events:
            self.log.info("Skiddle: no events returned")
            return

        processed = 0
        for event in events:
            event_id = str(event.get("id", ""))
            title = event.get("eventname", "Unknown event")
            event_code = event.get("EventCode", "")
            category = _CODE_MAP.get(event_code, "other")

            # Venue info — may be nested object or flat fields
            venue = event.get("venue", {})
            if isinstance(venue, dict):
                venue_name = venue.get("name", "")
                lat = _safe_float(venue.get("latitude"))
                lon = _safe_float(venue.get("longitude"))
            else:
                venue_name = str(venue) if venue else ""
                lat = _safe_float(event.get("latitude"))
                lon = _safe_float(event.get("longitude"))

            # Fall back to top-level lat/lon if venue didn't have them
            if lat is None:
                lat = _safe_float(event.get("latitude"))
            if lon is None:
                lon = _safe_float(event.get("longitude"))

            start_date = event.get("date", event.get("startdate", ""))
            genre = event.get("genre", "")

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=1.0,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": title,
                    "event_id": event_id,
                    "category": category,
                    "event_code": event_code,
                    "genre": genre,
                    "venue": venue_name,
                    "start_date": start_date,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Skiddle event [{category}] \"{title}\" "
                    f"at {venue_name} ({start_date})"
                ),
                data={
                    "title": title,
                    "event_id": event_id,
                    "category": category,
                    "genre": genre,
                    "venue": venue_name,
                    "start_date": start_date,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Skiddle events: processed=%d", processed)


def _safe_float(val: Any) -> float | None:
    """Convert a value to float, returning None on failure."""
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def ingest_skiddle_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Skiddle fetch cycle."""
    ingestor = SkiddleEventsIngestor(board, graph, scheduler)
    await ingestor.run()
