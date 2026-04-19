"""UK national headlines ingestor — top UK news stories for macro-context.

Fetches top UK headlines from a free cached NewsAPI endpoint (no key required).
Provides national news context that helps explain counterintuitive correlations
(e.g., carbon intensity vs social sentiment — a major positive national story on
a cold day could drive both high grid demand and positive sentiment).

Brain suggestion origin: high-frequency UK news headlines for explaining
cross-domain correlations that require national news context.
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.uk_headlines")

# Free cached NewsAPI endpoint — no key required, returns UK top headlines
UK_HEADLINES_URL = (
    "https://saurav.tech/NewsAPI/top-headlines/category/{category}/gb.json"
)

# Categories to rotate through for breadth
CATEGORIES = ["general", "business", "technology", "science", "health"]

# Simple sentiment keywords for headline tone scoring
_POSITIVE = {
    "win", "wins", "won", "success", "celebrate", "boost", "record",
    "breakthrough", "hero", "triumph", "growth", "surge", "soar",
    "deal", "agree", "peace", "saved", "recovery", "milestone",
}
_NEGATIVE = {
    "crisis", "crash", "death", "deaths", "killed", "attack", "war",
    "collapse", "fail", "fear", "threat", "scandal", "strike", "protest",
    "emergency", "disaster", "flood", "storm", "recession", "cuts",
}

# London centre for default location when no geo data available
_LONDON_CENTRE = (51.5074, -0.1278)


def _headline_sentiment(text: str) -> float:
    """Quick keyword sentiment: -1.0 to 1.0."""
    words = set(text.lower().split())
    pos = len(words & _POSITIVE)
    neg = len(words & _NEGATIVE)
    total = pos + neg
    if total == 0:
        return 0.0
    return (pos - neg) / total


def _dedup_key(title: str) -> str:
    """Short hash for deduplication within a cycle."""
    return hashlib.md5(title.strip().lower().encode()).hexdigest()[:12]


class UkHeadlinesIngestor(BaseIngestor):
    source_name = "uk_headlines"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self._cycle_index = 0

    async def fetch_data(self) -> Any:
        """Fetch one category per cycle, rotating through CATEGORIES."""
        category = CATEGORIES[self._cycle_index % len(CATEGORIES)]
        self._cycle_index += 1
        url = UK_HEADLINES_URL.format(category=category)
        data = await self.fetch(url)
        if isinstance(data, dict):
            data["_category"] = category
        return data

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected UK headlines response: %s", type(data))
            return

        articles = data.get("articles", [])
        category = data.get("_category", "general")
        if not articles:
            self.log.info("UK headlines [%s]: 0 articles", category)
            return

        seen: set[str] = set()
        processed = 0
        skipped = 0

        for article in articles:
            title = article.get("title", "")
            if not title or title == "[Removed]":
                skipped += 1
                continue

            # Deduplicate
            key = _dedup_key(title)
            if key in seen:
                skipped += 1
                continue
            seen.add(key)

            description = article.get("description", "") or ""
            source_name = article.get("source", {}).get("name", "unknown")
            published = article.get("publishedAt", "")
            url = article.get("url", "")

            sentiment = _headline_sentiment(f"{title} {description}")
            sentiment_label = (
                "negative" if sentiment < -0.3
                else "positive" if sentiment > 0.3
                else "neutral"
            )

            # UK national news — tag to London centre for spatial grouping
            lat, lon = _LONDON_CENTRE
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=sentiment,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": title[:300],
                    "description": description[:500],
                    "source_name": source_name,
                    "category": category,
                    "published_at": published,
                    "url": url,
                    "sentiment": sentiment,
                    "sentiment_label": sentiment_label,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"UK headline [{source_name}] ({category}, {sentiment_label}): "
                    f"{title[:150]}"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "title": title[:300],
                    "category": category,
                    "source_name": source_name,
                    "sentiment": sentiment,
                    "sentiment_label": sentiment_label,
                    "published_at": published,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "UK headlines [%s]: processed=%d skipped=%d",
            category, processed, skipped,
        )


async def ingest_uk_headlines(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one UK headlines fetch cycle."""
    ingestor = UkHeadlinesIngestor(board, graph, scheduler)
    await ingestor.run()
