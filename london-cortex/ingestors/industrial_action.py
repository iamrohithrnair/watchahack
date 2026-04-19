"""Transport industrial action ingestor — strike announcements from RMT RSS + Strike Calendar.

Sources:
  - RMT union RSS feed (https://www.rmt.org.uk/news/feed.xml)
  - Strike Calendar iCal feed (https://www.strikecalendar.co.uk/ical) — aggregates RMT, ASLEF, etc.

Produces TEXT observations with upcoming/announced strike details so the system
can correlate transport disruptions with their root cause (industrial action).
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.industrial_action")

RMT_RSS_URL = "https://www.rmt.org.uk/news/feed.xml"
STRIKE_CALENDAR_ICAL_URL = "https://www.strikecalendar.co.uk/ical"

# Keywords to filter RMT RSS items for strike-relevant content
_STRIKE_KEYWORDS = re.compile(
    r"\b(strike|industrial action|ballot|walkout|picket|dispute|stoppage|work.to.rule)\b",
    re.IGNORECASE,
)

# Keywords to filter Strike Calendar events for London transport relevance
_TRANSPORT_KEYWORDS = re.compile(
    r"\b(rail|train|tube|underground|london|tfl|bus|overground|dlr|elizabeth line|"
    r"rmt|aslef|network rail|southeastern|southern|thameslink|gwr|avanti|"
    r"c2c|greater anglia|chiltern|lner|crossrail)\b",
    re.IGNORECASE,
)

# Central London approximate location for strike observations (no specific lat/lon)
_LONDON_CENTER_LAT = 51.5074
_LONDON_CENTER_LON = -0.1278


def _parse_ical_events(text: str) -> list[dict[str, str]]:
    """Minimal iCal parser — extracts VEVENT blocks into dicts."""
    events: list[dict[str, str]] = []
    current: dict[str, str] | None = None

    for line in text.splitlines():
        line = line.strip()
        if line == "BEGIN:VEVENT":
            current = {}
        elif line == "END:VEVENT":
            if current:
                events.append(current)
            current = None
        elif current is not None and ":" in line:
            key, _, val = line.partition(":")
            # Handle properties with params like DTSTART;VALUE=DATE:20260310
            key = key.split(";")[0]
            current[key] = val

    return events


def _parse_ical_date(val: str) -> datetime | None:
    """Parse iCal date/datetime strings."""
    val = val.strip().rstrip("Z")
    for fmt in ("%Y%m%dT%H%M%S", "%Y%m%d"):
        try:
            return datetime.strptime(val, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    return None


class IndustrialActionIngestor(BaseIngestor):
    source_name = "industrial_action"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        rmt_data = await self.fetch(RMT_RSS_URL)
        ical_data = await self.fetch(STRIKE_CALENDAR_ICAL_URL)
        return {"rmt_rss": rmt_data, "strike_calendar": ical_data}

    async def process(self, data: Any) -> None:
        rmt_count = await self._process_rmt_rss(data.get("rmt_rss"))
        ical_count = await self._process_strike_calendar(data.get("strike_calendar"))
        self.log.info(
            "Industrial action: rmt_items=%d, calendar_events=%d",
            rmt_count, ical_count,
        )

    async def _process_rmt_rss(self, raw: Any) -> int:
        """Parse RMT RSS XML and post strike-related items."""
        if not raw or not isinstance(raw, str):
            self.log.debug("No RMT RSS data (type=%s)", type(raw).__name__)
            return 0

        try:
            root = ET.fromstring(raw)
        except ET.ParseError as exc:
            self.log.warning("Failed to parse RMT RSS XML: %s", exc)
            return 0

        count = 0
        for item in root.iter("item"):
            title = (item.findtext("title") or "").strip()
            description = (item.findtext("description") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()

            combined_text = f"{title} {description}"
            if not _STRIKE_KEYWORDS.search(combined_text):
                continue

            cell_id = self.graph.latlon_to_cell(_LONDON_CENTER_LAT, _LONDON_CENTER_LON)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=title,
                location_id=cell_id,
                lat=_LONDON_CENTER_LAT,
                lon=_LONDON_CENTER_LON,
                metadata={
                    "feed": "rmt_rss",
                    "title": title,
                    "description": description[:500],
                    "link": link,
                    "pub_date": pub_date,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Industrial action [RMT]: {title}",
                data={
                    "feed": "rmt_rss",
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            count += 1

        return count

    async def _process_strike_calendar(self, raw: Any) -> int:
        """Parse Strike Calendar iCal and post London-transport-relevant events."""
        if not raw or not isinstance(raw, str):
            self.log.debug("No Strike Calendar data (type=%s)", type(raw).__name__ if raw else "None")
            return 0

        events = _parse_ical_events(raw)
        now = datetime.now(timezone.utc)
        count = 0

        for evt in events:
            summary = evt.get("SUMMARY", "")
            description = evt.get("DESCRIPTION", "")
            dtstart_str = evt.get("DTSTART", "")
            location = evt.get("LOCATION", "")
            url = evt.get("URL", "")

            combined_text = f"{summary} {description} {location}"
            if not _TRANSPORT_KEYWORDS.search(combined_text):
                continue

            dtstart = _parse_ical_date(dtstart_str)
            dtend = _parse_ical_date(evt.get("DTEND", ""))

            # Only include future or very recent events (last 7 days)
            if dtstart and (now - dtstart).days > 7:
                continue

            date_str = dtstart.strftime("%Y-%m-%d") if dtstart else "unknown"
            cell_id = self.graph.latlon_to_cell(_LONDON_CENTER_LAT, _LONDON_CENTER_LON)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"[{date_str}] {summary}",
                location_id=cell_id,
                lat=_LONDON_CENTER_LAT,
                lon=_LONDON_CENTER_LON,
                metadata={
                    "feed": "strike_calendar",
                    "summary": summary,
                    "description": description[:500],
                    "date": date_str,
                    "end_date": dtend.strftime("%Y-%m-%d") if dtend else None,
                    "location": location,
                    "url": url,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Industrial action [{date_str}]: {summary}",
                data={
                    "feed": "strike_calendar",
                    "summary": summary,
                    "date": date_str,
                    "url": url,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            count += 1

        return count


async def ingest_industrial_action(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one industrial action fetch cycle."""
    ingestor = IndustrialActionIngestor(board, graph, scheduler)
    await ingestor.run()
