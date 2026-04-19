"""BBC London & UK domestic news ingestor — RSS feeds for local narrative modelling.

Polls BBC RSS feeds for London-specific and UK domestic news to complement
the GDELT international news ingestor. Enables the NarrativeConnector and
Brain to distinguish local London narratives from international ones.

Feeds:
  - BBC London News (regional stories)
  - BBC England News (national domestic)
  - BBC UK News (UK-wide domestic)

No API key required — standard BBC RSS feeds.
"""

from __future__ import annotations

import logging
import re
import xml.etree.ElementTree as ET
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.bbc_london_news")

# BBC RSS feeds — free, no auth
BBC_FEEDS = {
    "bbc_london": "https://feeds.bbci.co.uk/news/england/london/rss.xml",
    "bbc_england": "https://feeds.bbci.co.uk/news/england/rss.xml",
    "bbc_uk": "https://feeds.bbci.co.uk/news/uk/rss.xml",
}

# Central London fallback for articles without geo info
LONDON_LAT = 51.5074
LONDON_LON = -0.1278

# London borough/area names for rough geo-tagging
_LONDON_AREAS: dict[str, tuple[float, float]] = {
    "westminster": (51.4975, -0.1357),
    "camden": (51.5517, -0.1588),
    "islington": (51.5362, -0.1033),
    "hackney": (51.5450, -0.0553),
    "tower hamlets": (51.5150, -0.0227),
    "southwark": (51.5035, -0.0804),
    "lambeth": (51.4571, -0.1231),
    "lewisham": (51.4415, -0.0117),
    "greenwich": (51.4834, 0.0088),
    "newham": (51.5255, 0.0352),
    "barking": (51.5362, 0.0798),
    "havering": (51.5779, 0.2121),
    "redbridge": (51.5590, 0.0741),
    "waltham forest": (51.5886, -0.0118),
    "haringey": (51.5906, -0.1110),
    "enfield": (51.6538, -0.0799),
    "barnet": (51.6252, -0.1517),
    "harrow": (51.5898, -0.3346),
    "brent": (51.5673, -0.2711),
    "ealing": (51.5130, -0.3089),
    "hounslow": (51.4688, -0.3615),
    "richmond": (51.4613, -0.3037),
    "kingston": (51.4085, -0.3064),
    "merton": (51.4098, -0.1949),
    "sutton": (51.3618, -0.1945),
    "croydon": (51.3714, -0.0977),
    "bromley": (51.4039, 0.0198),
    "bexley": (51.4549, 0.1505),
    "wandsworth": (51.4571, -0.1818),
    "hammersmith": (51.4927, -0.2339),
    "kensington": (51.5020, -0.1947),
    "chelsea": (51.4875, -0.1687),
    "city of london": (51.5155, -0.0922),
    "canary wharf": (51.5054, -0.0235),
    "stratford": (51.5416, -0.0033),
    "brixton": (51.4627, -0.1145),
    "shoreditch": (51.5264, -0.0780),
    "soho": (51.5137, -0.1337),
    "covent garden": (51.5117, -0.1240),
    "whitechapel": (51.5155, -0.0596),
    "heathrow": (51.4700, -0.4543),
    "gatwick": (51.1537, -0.1821),
}

# Categories for article classification
_CATEGORY_KEYWORDS = {
    "crime": ["stabbing", "murder", "robbery", "arrest", "police", "crime", "assault", "theft"],
    "transport": ["tube", "bus", "tfl", "train", "traffic", "road", "cycling", "transport"],
    "politics": ["mayor", "council", "election", "sadiq khan", "parliament", "government"],
    "environment": ["pollution", "air quality", "flood", "climate", "green", "recycling"],
    "health": ["nhs", "hospital", "covid", "flu", "health", "ambulance", "a&e"],
    "housing": ["housing", "rent", "property", "homeless", "eviction", "planning"],
    "economy": ["business", "jobs", "economy", "cost of living", "inflation", "wages"],
    "culture": ["theatre", "museum", "festival", "art", "concert", "exhibition"],
}


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
            tag = child.tag.split("}")[-1] if "}" in child.tag else child.tag
            if child.text:
                item[tag] = child.text.strip()
        if item:
            items.append(item)
    return items


def _extract_location(text: str) -> tuple[float, float] | None:
    """Try to geo-tag article from London area names in title/description."""
    text_lower = text.lower()
    for area, coords in _LONDON_AREAS.items():
        if area in text_lower:
            return coords
    return None


def _classify_category(text: str) -> str:
    """Classify article into a broad category based on keywords."""
    text_lower = text.lower()
    best_cat = "general"
    best_count = 0
    for cat, keywords in _CATEGORY_KEYWORDS.items():
        count = sum(1 for kw in keywords if kw in text_lower)
        if count > best_count:
            best_count = count
            best_cat = cat
    return best_cat


class BbcLondonNewsIngestor(BaseIngestor):
    source_name = "bbc_london_news"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}
        for feed_name, url in BBC_FEEDS.items():
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

                full_text = f"{title} {description}"
                coords = _extract_location(full_text)
                lat, lon = coords if coords else (LONDON_LAT, LONDON_LON)
                cell_id = self.graph.latlon_to_cell(lat, lon)
                category = _classify_category(full_text)

                # Determine scope: london-specific, england, or uk-wide
                scope = {
                    "bbc_london": "london",
                    "bbc_england": "england",
                    "bbc_uk": "uk",
                }.get(feed_name, "uk")

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
                        "scope": scope,
                        "category": category,
                        "has_specific_location": coords is not None,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"UK news [{scope}/{category}] {title[:150]}. "
                        f"{description[:200]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "title": title,
                        "description": description[:500],
                        "feed": feed_name,
                        "scope": scope,
                        "category": category,
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
            "BBC London/UK news: processed=%d articles, %d feed(s) unavailable",
            processed, skipped,
        )


async def ingest_bbc_london_news(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one BBC news fetch cycle."""
    ingestor = BbcLondonNewsIngestor(board, graph, scheduler)
    await ingestor.run()
