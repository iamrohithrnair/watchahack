"""Greater London Authority press release ingestor — Mayor & London Assembly RSS feeds.

Polls official GLA RSS feeds for press releases from:
  - Mayor of London / Mayor's Office
  - London Assembly

Provides ground-truth data on official institutional statements, policy
announcements, and government actions. Useful for verifying predictions
about institutional responses and for narrative grounding.

No API key required — standard RSS feeds from london.gov.uk.
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

log = logging.getLogger("cortex.ingestors.gla_press")

# GLA RSS feeds — free, no auth
GLA_FEEDS = {
    "mayor_press": "https://www.london.gov.uk/rss-feeds/80610",
    "assembly_press": "https://www.london.gov.uk/rss-feeds/80611",
}

# City Hall coordinates as fallback
CITY_HALL_LAT = 51.4982
CITY_HALL_LON = -0.0786

# Topic classification keywords
_TOPIC_KEYWORDS = {
    "policing": ["police", "crime", "met police", "mopac", "knife crime", "violence", "safety"],
    "transport": ["tfl", "tube", "bus", "cycling", "road", "transport", "crossrail", "elizabeth line"],
    "environment": ["pollution", "air quality", "ulez", "climate", "green", "carbon", "emissions"],
    "housing": ["housing", "affordable homes", "rent", "homelessness", "planning", "development"],
    "health": ["nhs", "hospital", "health", "mental health", "ambulance"],
    "economy": ["business", "jobs", "economy", "funding", "investment", "growth"],
    "culture": ["culture", "arts", "museum", "festival", "night-time", "creative"],
}

# London area names for geo-tagging press releases
_LONDON_AREAS: dict[str, tuple[float, float]] = {
    "westminster": (51.4975, -0.1357),
    "camden": (51.5517, -0.1588),
    "islington": (51.5362, -0.1033),
    "hackney": (51.5450, -0.0553),
    "tower hamlets": (51.5150, -0.0227),
    "southwark": (51.5035, -0.0804),
    "lambeth": (51.4571, -0.1231),
    "greenwich": (51.4834, 0.0088),
    "newham": (51.5255, 0.0352),
    "croydon": (51.3714, -0.0977),
    "barnet": (51.6252, -0.1517),
    "ealing": (51.5130, -0.3089),
    "hounslow": (51.4688, -0.3615),
    "brent": (51.5673, -0.2711),
    "lewisham": (51.4415, -0.0117),
    "wandsworth": (51.4571, -0.1818),
    "haringey": (51.5906, -0.1110),
    "enfield": (51.6538, -0.0799),
    "bromley": (51.4039, 0.0198),
    "city of london": (51.5155, -0.0922),
    "canary wharf": (51.5054, -0.0235),
    "stratford": (51.5416, -0.0033),
    "heathrow": (51.4700, -0.4543),
    "oxford street": (51.5152, -0.1418),
    "royal docks": (51.5081, 0.0530),
    "trafalgar square": (51.5080, -0.1281),
}


def _parse_rss_items(xml_text: str) -> list[dict[str, str]]:
    """Parse RSS 2.0 XML and return list of item dicts."""
    items: list[dict[str, str]] = []
    try:
        root = ET.fromstring(xml_text)
    except ET.ParseError:
        return items

    for item_el in root.iter("item"):
        item: dict[str, str] = {}
        for child in item_el:
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text:
                item[tag] = child.text.strip()
        if item:
            items.append(item)
    return items


def _extract_location(text: str) -> tuple[float, float] | None:
    """Try to geo-tag press release from London area names."""
    text_lower = text.lower()
    for area, coords in _LONDON_AREAS.items():
        if area in text_lower:
            return coords
    return None


def _classify_topic(text: str) -> str:
    """Classify press release into a topic based on keywords."""
    text_lower = text.lower()
    best_topic = "governance"
    best_count = 0
    for topic, keywords in _TOPIC_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw in text_lower)
        if count > best_count:
            best_count = count
            best_topic = topic
    return best_topic


class GlaPressIngestor(BaseIngestor):
    source_name = "gla_press"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}
        for feed_name, url in GLA_FEEDS.items():
            data = await self.fetch(url)
            results[feed_name] = data
        return results

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected data type: %s", type(data))
            return

        seen_titles: set[str] = set()
        processed = 0
        skipped = 0

        for feed_name, raw in data.items():
            if raw is None:
                skipped += 1
                continue
            if not isinstance(raw, str):
                skipped += 1
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
                creator = item.get("creator", "")

                full_text = f"{title} {description}"
                coords = _extract_location(full_text)
                lat, lon = coords if coords else (CITY_HALL_LAT, CITY_HALL_LON)
                cell_id = self.graph.latlon_to_cell(lat, lon)
                topic = _classify_topic(full_text)

                source_label = {
                    "mayor_press": "mayor",
                    "assembly_press": "assembly",
                }.get(feed_name, "gla")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=title,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "title": title,
                        "description": description[:500],
                        "pub_date": pub_date,
                        "link": link,
                        "feed": feed_name,
                        "source_label": source_label,
                        "topic": topic,
                        "creator": creator,
                        "has_specific_location": coords is not None,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"GLA press [{source_label}/{topic}] {title[:150]}. "
                        f"{description[:200]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "title": title,
                        "description": description[:500],
                        "feed": feed_name,
                        "source_label": source_label,
                        "topic": topic,
                        "pub_date": pub_date,
                        "link": link,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "GLA press releases: processed=%d articles, %d feed(s) unavailable",
            processed, skipped,
        )


async def ingest_gla_press(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GLA press release fetch cycle."""
    ingestor = GlaPressIngestor(board, graph, scheduler)
    await ingestor.run()
