"""Social media sentiment ingestor — Reddit posts from London transport subreddits.

Uses Reddit's public JSON API (no auth required) to monitor r/london, r/tfl,
and r/LondonUnderground for transport-related keywords like 'crowded', 'stuck',
'delayed', 'signal failure', etc. Provides a real-time proxy for passenger
experience and crowd formation near disruption hotspots.

Brain suggestion origin: geo-fenced social media sentiment analysis for
passenger experience near disruptions.
"""

from __future__ import annotations

import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.social_sentiment")

# Subreddits with active London transport discussion
SUBREDDITS = ["london", "tfl", "LondonUnderground"]

# Reddit public JSON search (no auth, rate limit ~60 req/min with User-Agent)
REDDIT_SEARCH_URL = "https://www.reddit.com/r/{subreddit}/search.json"

# Transport disruption keywords
SEARCH_QUERY = (
    "crowded OR stuck OR delayed OR delay OR signal failure OR suspended OR "
    "overcrowded OR cancellation OR shut OR closed OR evacuation OR "
    "no service OR part suspended OR severe delays OR TfL OR tube"
)

# Station/line names for geo-tagging
_STATION_COORDS: dict[str, tuple[float, float]] = {
    "king's cross": (51.5308, -0.1238),
    "kings cross": (51.5308, -0.1238),
    "victoria": (51.4952, -0.1441),
    "waterloo": (51.5014, -0.1131),
    "liverpool street": (51.5178, -0.0823),
    "paddington": (51.5154, -0.1755),
    "euston": (51.5282, -0.1337),
    "oxford circus": (51.5152, -0.1418),
    "bank": (51.5133, -0.0886),
    "london bridge": (51.5055, -0.0860),
    "stratford": (51.5416, -0.0033),
    "canary wharf": (51.5054, -0.0235),
    "clapham junction": (51.4643, -0.1704),
    "finsbury park": (51.5642, -0.1065),
    "brixton": (51.4627, -0.1145),
    "camden town": (51.5392, -0.1426),
    "angel": (51.5322, -0.1058),
    "bethnal green": (51.5270, -0.0549),
    "tottenham court road": (51.5165, -0.1310),
    "piccadilly circus": (51.5100, -0.1347),
    "green park": (51.5067, -0.1428),
    "baker street": (51.5226, -0.1571),
    "moorgate": (51.5186, -0.0886),
    "holborn": (51.5174, -0.1201),
    "elephant and castle": (51.4943, -0.1001),
}

# Sentiment keywords for scoring
_NEGATIVE_WORDS = {
    "stuck", "stranded", "nightmare", "chaos", "awful", "terrible", "worst",
    "overcrowded", "packed", "rammed", "dangerous", "unsafe", "angry",
    "furious", "unacceptable", "disgusting", "shambles", "joke",
}
_POSITIVE_WORDS = {
    "running well", "smooth", "on time", "no issues", "quiet", "empty",
    "good service", "working fine", "back to normal", "resumed",
}

# Reddit User-Agent (required by their API policy)
_HEADERS = {"User-Agent": "LondonCortex/1.0 (transport research)"}


def _extract_location(text: str) -> tuple[float, float] | None:
    """Try to extract lat/lon from station names mentioned in text."""
    text_lower = text.lower()
    for station, coords in _STATION_COORDS.items():
        if station in text_lower:
            return coords
    return None


def _score_sentiment(text: str) -> float:
    """Simple keyword sentiment score: -1.0 (very negative) to 1.0 (very positive)."""
    text_lower = text.lower()
    neg = sum(1 for w in _NEGATIVE_WORDS if w in text_lower)
    pos = sum(1 for w in _POSITIVE_WORDS if w in text_lower)
    total = neg + pos
    if total == 0:
        return 0.0
    return (pos - neg) / total


class SocialSentimentIngestor(BaseIngestor):
    source_name = "social_sentiment"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import asyncio as _asyncio
        results = []
        fetch_failures = 0
        for i, sub in enumerate(SUBREDDITS):
            if i > 0:
                await _asyncio.sleep(2)  # Rate-limit: 2s between subreddit fetches
            url = REDDIT_SEARCH_URL.format(subreddit=sub)
            params = {
                "q": SEARCH_QUERY,
                "sort": "new",
                "t": "day",
                "restrict_sr": "1",
                "limit": "25",
            }
            try:
                data = await self.fetch(url, params=params, headers=_HEADERS)
            except Exception as exc:
                if "429" in str(exc):
                    self.log.warning("Reddit 429 rate limit on r/%s — backing off 10s", sub)
                    await _asyncio.sleep(10)
                    fetch_failures += 1
                    continue
                raise
            if data and isinstance(data, dict):
                children = data.get("data", {}).get("children", [])
                for child in children:
                    post = child.get("data", {})
                    results.append({
                        "title": post.get("title", ""),
                        "selftext": post.get("selftext", ""),
                        "subreddit": post.get("subreddit", sub),
                        "score": post.get("score", 0),
                        "created_utc": post.get("created_utc", 0),
                        "num_comments": post.get("num_comments", 0),
                        "permalink": post.get("permalink", ""),
                    })
            else:
                fetch_failures += 1
        # If ALL subreddits failed, signal to circuit breaker
        if fetch_failures == len(SUBREDDITS):
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No social media posts found")
            return

        processed = 0
        for post in data:
            full_text = f"{post['title']} {post['selftext']}"
            sentiment = _score_sentiment(full_text)
            coords = _extract_location(full_text)
            lat, lon = coords if coords else (None, None)
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=sentiment,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": post["title"][:200],
                    "subreddit": post["subreddit"],
                    "reddit_score": post["score"],
                    "num_comments": post["num_comments"],
                    "sentiment": sentiment,
                    "has_location": coords is not None,
                },
            )
            await self.board.store_observation(obs)

            sentiment_label = (
                "negative" if sentiment < -0.3
                else "positive" if sentiment > 0.3
                else "neutral"
            )
            location_str = f" near {full_text}" if coords else ""
            # Trim location_str to just the station name if found
            if coords:
                for station in _STATION_COORDS:
                    if station in full_text.lower():
                        location_str = f" near {station.title()}"
                        break

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Social sentiment [r/{post['subreddit']}] "
                    f"{sentiment_label} (score={post['score']}, "
                    f"comments={post['num_comments']}): "
                    f"{post['title'][:120]}{location_str}"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "title": post["title"][:200],
                    "subreddit": post["subreddit"],
                    "sentiment": sentiment,
                    "sentiment_label": sentiment_label,
                    "reddit_score": post["score"],
                    "num_comments": post["num_comments"],
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Social sentiment: processed=%d posts from %d subreddits",
            processed, len(SUBREDDITS),
        )


async def ingest_social_sentiment(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Reddit sentiment fetch cycle."""
    ingestor = SocialSentimentIngestor(board, graph, scheduler)
    await ingestor.run()
