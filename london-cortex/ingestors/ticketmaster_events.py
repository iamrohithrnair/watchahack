"""Ticketmaster Discovery API ingestor — concerts, sports, festivals in London.

Free tier: 5,000 requests/day, 5 req/sec.
Provides ground-truth event data (title, venue, date, genre) to test
hypotheses about mass entertainment effects on energy/mood/transport.
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

log = logging.getLogger("cortex.ingestors.ticketmaster_events")

DISCOVERY_URL = "https://app.ticketmaster.com/discovery/v2/events.json"

# Map TM segment names to simpler categories
_SEGMENT_MAP = {
    "Music": "concert",
    "Sports": "sports",
    "Arts & Theatre": "performing_arts",
    "Film": "film",
    "Miscellaneous": "other",
}


def _estimate_scale(venue_name: str) -> str:
    """Rough scale estimate from well-known London venue names."""
    large = [
        "wembley", "o2 arena", "tottenham hotspur", "emirates stadium",
        "london stadium", "twickenham", "stamford bridge", "selhurst park",
        "craven cottage", "brentford community", "the oval", "lord's",
    ]
    medium = [
        "eventim apollo", "brixton academy", "alexandra palace",
        "roundhouse", "royal albert hall", "sse arena", "earls court",
    ]
    name_lower = (venue_name or "").lower()
    for v in large:
        if v in name_lower:
            return "large"
    for v in medium:
        if v in name_lower:
            return "medium"
    return "small"


class TicketmasterEventsIngestor(BaseIngestor):
    source_name = "ticketmaster_events"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("TICKETMASTER_API_KEY", "")
        if not api_key:
            self.log.warning("TICKETMASTER_API_KEY not set, skipping ticketmaster ingest")
            return None

        now = datetime.now(timezone.utc)
        params = {
            "apikey": api_key,
            "countryCode": "GB",
            "city": "London",
            "startDateTime": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "sort": "date,asc",
            "size": 100,
        }
        return await self.fetch(DISCOVERY_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Ticketmaster response type: %s", type(data))
            return

        embedded = data.get("_embedded", {})
        events = embedded.get("events", [])
        processed = 0

        for event in events:
            title = event.get("name", "Unknown event")
            event_id = event.get("id", "")

            # Dates
            dates = event.get("dates", {})
            start_info = dates.get("start", {})
            start_date = start_info.get("localDate", "")
            start_time = start_info.get("localTime", "")

            # Classification (genre/segment)
            classifications = event.get("classifications", [])
            segment = ""
            genre = ""
            if classifications:
                c = classifications[0]
                segment = (c.get("segment") or {}).get("name", "")
                genre = (c.get("genre") or {}).get("name", "")
            category = _SEGMENT_MAP.get(segment, "other")

            # Venue + location
            venues = (event.get("_embedded") or {}).get("venues", [])
            venue_name = ""
            lat = lon = None
            if venues:
                v = venues[0]
                venue_name = v.get("name", "")
                loc = v.get("location", {})
                try:
                    lat = float(loc.get("latitude", ""))
                    lon = float(loc.get("longitude", ""))
                except (ValueError, TypeError):
                    lat = lon = None

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None
            scale = _estimate_scale(venue_name)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=1.0,  # presence indicator; scale in metadata
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": title,
                    "event_id": event_id,
                    "category": category,
                    "segment": segment,
                    "genre": genre,
                    "venue": venue_name,
                    "start_date": start_date,
                    "start_time": start_time,
                    "scale": scale,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Ticketmaster event [{category}] \"{title}\" "
                    f"at {venue_name} ({start_date} {start_time}, scale={scale})"
                ),
                data={
                    "title": title,
                    "event_id": event_id,
                    "category": category,
                    "genre": genre,
                    "venue": venue_name,
                    "start_date": start_date,
                    "start_time": start_time,
                    "scale": scale,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Ticketmaster events: processed=%d", processed)


async def ingest_ticketmaster_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Ticketmaster fetch cycle."""
    ingestor = TicketmasterEventsIngestor(board, graph, scheduler)
    await ingestor.run()
