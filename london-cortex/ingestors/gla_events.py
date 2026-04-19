"""GLA official events ingestor — RSS feed from london.gov.uk.

Free, no API key required. Provides ground-truth scheduled event data from
the Greater London Authority (City Hall, Trafalgar Square, etc.) to complement
commercial event sources (PredictHQ, Skiddle).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.gla_events")

# GLA upcoming events RSS feed (free, no key)
GLA_EVENTS_RSS = "https://www.london.gov.uk/rss-feeds/80117"

# Known GLA venue locations (lat, lon)
_VENUE_COORDS: dict[str, tuple[float, float]] = {
    "trafalgar square": (51.5080, -0.1281),
    "city hall": (51.4982, -0.0786),
    "parliament square": (51.5007, -0.1246),
    "the crystal": (51.5083, 0.0177),
    "excel london": (51.5075, 0.0286),
    "london stadium": (51.5386, -0.0166),
    "hyde park": (51.5073, -0.1657),
    "olympic park": (51.5432, -0.0134),
    "queen elizabeth olympic park": (51.5432, -0.0134),
    "southbank": (51.5064, -0.1163),
    "south bank": (51.5064, -0.1163),
    "potters fields park": (51.5025, -0.0785),
}

# Regex patterns to extract event dates from description text
_DATE_PATTERNS = [
    # "Sunday 15 March 2026" or "15 March 2026"
    re.compile(
        r"(?:Monday|Tuesday|Wednesday|Thursday|Friday|Saturday|Sunday)?\s*"
        r"(\d{1,2})\s+(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})",
        re.IGNORECASE,
    ),
    # "March 2026" (month only)
    re.compile(
        r"(January|February|March|April|May|June|July|August|"
        r"September|October|November|December)\s+(\d{4})",
        re.IGNORECASE,
    ),
]

_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4,
    "may": 5, "june": 6, "july": 7, "august": 8,
    "september": 9, "october": 10, "november": 11, "december": 12,
}


def _extract_event_date(text: str) -> str | None:
    """Try to extract an event date from description text."""
    m = _DATE_PATTERNS[0].search(text)
    if m:
        day, month_name, year = m.group(1), m.group(2), m.group(3)
        month = _MONTHS.get(month_name.lower(), 1)
        return f"{year}-{month:02d}-{int(day):02d}"
    m = _DATE_PATTERNS[1].search(text)
    if m:
        month_name, year = m.group(1), m.group(2)
        month = _MONTHS.get(month_name.lower(), 1)
        return f"{year}-{month:02d}-01"
    return None


def _guess_venue(text: str) -> tuple[str | None, float | None, float | None]:
    """Match known venue names in text, return (venue, lat, lon)."""
    lower = text.lower()
    for venue, (lat, lon) in _VENUE_COORDS.items():
        if venue in lower:
            return venue.title(), lat, lon
    return None, None, None


def _strip_html(text: str) -> str:
    """Remove HTML tags from description text."""
    return re.sub(r"<[^>]+>", "", text).strip()


class GLAEventsIngestor(BaseIngestor):
    source_name = "gla_events"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # fetch() returns text for non-JSON responses
        return await self.fetch(GLA_EVENTS_RSS)

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            self.log.warning("Unexpected GLA RSS response type: %s", type(data))
            return

        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            self.log.warning("Failed to parse GLA RSS XML: %s", exc)
            return

        items = root.findall(".//item")
        if not items:
            self.log.info("GLA events: no items in RSS feed")
            return

        processed = 0
        for item in items:
            title = (item.findtext("title") or "Unknown GLA event").strip()
            link = (item.findtext("link") or "").strip()
            description = _strip_html(item.findtext("description") or "")

            # Parse publication date
            pub_date_str = item.findtext("pubDate")
            pub_date = None
            if pub_date_str:
                try:
                    pub_date = parsedate_to_datetime(pub_date_str).isoformat()
                except (ValueError, TypeError):
                    pass

            # Extract event date from description/title
            event_date = _extract_event_date(description) or _extract_event_date(title)

            # Guess venue location from description/title
            combined_text = f"{title} {description}"
            venue, lat, lon = _guess_venue(combined_text)

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
                    "category": "gla_official",
                    "event_date": event_date,
                    "published": pub_date,
                    "venue": venue,
                    "link": link,
                    "description": description[:500],
                },
            )
            await self.board.store_observation(obs)

            venue_str = f" at {venue}" if venue else ""
            date_str = f" ({event_date})" if event_date else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"GLA event [official] \"{title}\"{venue_str}{date_str}"
                ),
                data={
                    "title": title,
                    "category": "gla_official",
                    "event_date": event_date,
                    "venue": venue,
                    "lat": lat,
                    "lon": lon,
                    "link": link,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("GLA events: processed=%d from RSS feed", processed)


async def ingest_gla_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GLA events RSS fetch cycle."""
    ingestor = GLAEventsIngestor(board, graph, scheduler)
    await ingestor.run()
