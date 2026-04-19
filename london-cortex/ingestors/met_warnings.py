"""Met Office severe weather warnings ingestor — RSS feed for London & SE England.

Polls the free Met Office RSS feed for national severe weather warnings
covering the London & South East England region. No API key required.
When warnings are active, each item contains the warning type (rain, wind,
snow, ice, fog, thunderstorm, lightning, extreme heat), severity level
(yellow/amber/red), and affected time period.

Also polls the UK-wide feed to catch national-level warnings that affect London.
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.met_warnings")

# Free RSS feeds — no API key needed
_BASE = "https://www.metoffice.gov.uk/public/data/PWSCache/WarningsRSS/Region"
LONDON_SE_URL = f"{_BASE}/se"
UK_URL = f"{_BASE}/UK"

# Central London for location tagging
LONDON_LAT = 51.5074
LONDON_LON = -0.1278

# Severity ordering for numeric value
_SEVERITY = {"yellow": 1, "amber": 2, "red": 3}


def _parse_rss_items(xml_text: str) -> list[dict[str, str]]:
    """Parse RSS XML and return list of item dicts."""
    items: list[dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    for item_el in root.iter("item"):
        item: dict[str, str] = {}
        for child in item_el:
            # Strip namespace prefix if any
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text:
                item[tag] = child.text.strip()
        if item:
            items.append(item)
    return items


def _extract_severity(title: str) -> tuple[str, int]:
    """Extract severity level from warning title, e.g. 'Yellow warning of Rain'."""
    lower = title.lower()
    for level, score in _SEVERITY.items():
        if level in lower:
            return level, score
    return "unknown", 0


class MetWarningsIngestor(BaseIngestor):
    source_name = "met_warnings"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Fetch both London/SE and UK feeds
        se_data = await self.fetch(LONDON_SE_URL)
        uk_data = await self.fetch(UK_URL)
        return {"se": se_data, "uk": uk_data}

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected data type: %s", type(data))
            return

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        seen_titles: set[str] = set()
        processed = 0

        for feed_name, raw in data.items():
            if raw is None:
                continue
            # RSS comes back as text/xml, not JSON
            if not isinstance(raw, str):
                continue

            items = _parse_rss_items(raw)
            for item in items:
                title = item.get("title", "")
                if not title or title in seen_titles:
                    continue
                seen_titles.add(title)

                description = item.get("description", "")
                pub_date = item.get("pubDate", "")
                link = item.get("link", "")
                severity_label, severity_score = _extract_severity(title)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=severity_score,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "title": title,
                        "description": description,
                        "severity": severity_label,
                        "severity_score": severity_score,
                        "pub_date": pub_date,
                        "link": link,
                        "feed": feed_name,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Met Office warning [{severity_label.upper()}]: {title}. "
                        f"{description[:200]}"
                    ),
                    data={
                        "title": title,
                        "severity": severity_label,
                        "severity_score": severity_score,
                        "description": description,
                        "pub_date": pub_date,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        if processed == 0:
            # Post a "no warnings" observation so downstream agents know
            # the feed is being checked (absence of warnings is also data)
            self.log.info("Met Office warnings: no active warnings for London/SE")
        else:
            self.log.info("Met Office warnings: %d active warning(s) ingested", processed)


async def ingest_met_warnings(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Met Office warnings fetch cycle."""
    ingestor = MetWarningsIngestor(board, graph, scheduler)
    await ingestor.run()
