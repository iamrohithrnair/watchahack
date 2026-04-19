"""Social media topic extraction ingestor — NER + topic modeling via TextRazor.

Complements social_sentiment.py by extracting *what* people are discussing,
not just sentiment. Identifies specific entities (stations, areas, services)
and topics (heating, football, weather) from London subreddit posts, enabling
the system to link social discussion topics to grid-level demand signals.

Brain suggestion: 'Integrate a real-time Topic Modeling/NER API to process
the raw social media text streams... identify the specific subjects driving
the sentiment, turning the 6-hour lead time on grid demand into a truly
actionable forecast.'

Uses TextRazor free tier (500 requests/day). API key required: TEXTRAZOR_API_KEY.
"""

from __future__ import annotations

import asyncio
import logging
import os
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.social_topics")

# TextRazor API endpoint
TEXTRAZOR_URL = "https://api.textrazor.com/"

# Subreddits to monitor (same as social_sentiment for consistency)
SUBREDDITS = ["london", "tfl", "LondonUnderground", "CasualUK"]
REDDIT_SEARCH_URL = "https://www.reddit.com/r/{subreddit}/new.json"
_REDDIT_HEADERS = {"User-Agent": "LondonCortex/1.0 (topic research)"}

# Max posts to send to TextRazor per cycle (budget: 500/day, cycles ~every 30 min)
MAX_POSTS_PER_CYCLE = 15

# London-relevant entity types to keep (DBpedia types)
_RELEVANT_ENTITY_TYPES = {
    "Place", "PopulatedPlace", "Settlement", "City", "Station", "RailwayStation",
    "Building", "Organisation", "Company", "SportsTeam", "Event",
    "Person", "PoliticalParty", "GovernmentAgency",
}

# Station/area coordinates for geo-tagging extracted entities
_LOCATION_COORDS: dict[str, tuple[float, float]] = {
    "king's cross": (51.5308, -0.1238),
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
    "brixton": (51.4627, -0.1145),
    "camden": (51.5392, -0.1426),
    "shoreditch": (51.5236, -0.0746),
    "soho": (51.5137, -0.1350),
    "westminster": (51.4995, -0.1248),
    "greenwich": (51.4769, -0.0005),
    "hackney": (51.5450, -0.0553),
    "islington": (51.5362, -0.1033),
    "croydon": (51.3762, -0.0982),
}


def _extract_location(text: str) -> tuple[float, float] | None:
    """Try to geo-tag text from known location names."""
    text_lower = text.lower()
    for name, coords in _LOCATION_COORDS.items():
        if name in text_lower:
            return coords
    return None


class SocialTopicsIngestor(BaseIngestor):
    source_name = "social_topics"
    rate_limit_name = "default"
    required_env_vars = ["TEXTRAZOR_API_KEY"]

    async def fetch_data(self) -> Any:
        """Fetch recent Reddit posts from London subreddits."""
        results: list[dict] = []
        for i, sub in enumerate(SUBREDDITS):
            if i > 0:
                await asyncio.sleep(2)
            url = REDDIT_SEARCH_URL.format(subreddit=sub)
            params = {"limit": "10", "raw_json": "1"}
            try:
                data = await self.fetch(url, params=params, headers=_REDDIT_HEADERS)
            except Exception as exc:
                if "429" in str(exc):
                    self.log.warning("Reddit 429 on r/%s — skipping", sub)
                    continue
                raise
            if data and isinstance(data, dict):
                children = data.get("data", {}).get("children", [])
                for child in children:
                    post = child.get("data", {})
                    title = post.get("title", "")
                    selftext = post.get("selftext", "")
                    full_text = f"{title}. {selftext}".strip()
                    # Skip very short posts (not enough for NER)
                    if len(full_text) < 30:
                        continue
                    results.append({
                        "text": full_text[:1000],  # TextRazor free limit
                        "subreddit": post.get("subreddit", sub),
                        "title": title[:200],
                        "score": post.get("score", 0),
                        "num_comments": post.get("num_comments", 0),
                        "created_utc": post.get("created_utc", 0),
                    })

        if not results:
            return None

        # Limit to top posts by engagement to stay within API budget
        results.sort(key=lambda p: p["score"] + p["num_comments"], reverse=True)
        return results[:MAX_POSTS_PER_CYCLE]

    async def _analyze_text(
        self, session: aiohttp.ClientSession, text: str
    ) -> dict[str, Any] | None:
        """Send text to TextRazor for NER + topic extraction."""
        api_key = os.environ.get("TEXTRAZOR_API_KEY", "")
        if not api_key:
            return None

        headers = {
            "X-TextRazor-Key": api_key,
            "Content-Type": "application/x-www-form-urlencoded",
        }
        payload = {
            "text": text,
            "extractors": "entities,topics",
        }

        try:
            async with session.post(
                TEXTRAZOR_URL, headers=headers, data=payload,
                timeout=aiohttp.ClientTimeout(total=15),
            ) as resp:
                if resp.status == 200:
                    return await resp.json(content_type=None)
                elif resp.status == 429:
                    self.log.warning("TextRazor rate limited")
                    return None
                else:
                    self.log.warning("TextRazor HTTP %d", resp.status)
                    return None
        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            self.log.warning("TextRazor request failed: %s", exc)
            return None

    async def process(self, data: Any) -> None:
        if not data:
            return

        processed = 0
        api_errors = 0

        async with aiohttp.ClientSession() as session:
            for post in data:
                # Rate limit TextRazor calls
                await asyncio.sleep(1)

                result = await self._analyze_text(session, post["text"])
                if result is None:
                    api_errors += 1
                    continue

                response = result.get("response", {})

                # Extract entities
                entities = []
                for ent in response.get("entities", []):
                    ent_id = ent.get("entityId", "")
                    confidence = ent.get("confidenceScore", 0)
                    ent_types = set(ent.get("type", []))
                    # Keep high-confidence or London-relevant entities
                    if confidence >= 0.5 or ent_types & _RELEVANT_ENTITY_TYPES:
                        entities.append({
                            "name": ent_id,
                            "confidence": round(confidence, 3),
                            "types": list(ent_types)[:5],
                            "relevance": round(ent.get("relevanceScore", 0), 3),
                        })

                # Extract topics
                topics = []
                for topic in response.get("topics", []):
                    score = topic.get("score", 0)
                    if score >= 0.3:
                        topics.append({
                            "label": topic.get("label", ""),
                            "score": round(score, 3),
                        })

                # Sort by relevance/score
                entities.sort(key=lambda e: e["relevance"], reverse=True)
                topics.sort(key=lambda t: t["score"], reverse=True)
                entities = entities[:10]
                topics = topics[:8]

                if not entities and not topics:
                    continue

                # Geo-tag from text content
                coords = _extract_location(post["text"])
                lat, lon = coords if coords else (None, None)
                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                # Build topic summary
                topic_labels = [t["label"] for t in topics[:5]]
                entity_names = [e["name"] for e in entities[:5]]

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=len(entities) + len(topics),  # richness score
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "title": post["title"],
                        "subreddit": post["subreddit"],
                        "entities": entities,
                        "topics": topics,
                        "entity_names": entity_names,
                        "topic_labels": topic_labels,
                        "reddit_score": post["score"],
                        "num_comments": post["num_comments"],
                    },
                )
                await self.board.store_observation(obs)

                topic_str = ", ".join(topic_labels[:3]) if topic_labels else "none"
                entity_str = ", ".join(entity_names[:3]) if entity_names else "none"

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Social topics [r/{post['subreddit']}] "
                        f"entities=[{entity_str}] topics=[{topic_str}] "
                        f"(score={post['score']}, comments={post['num_comments']}): "
                        f"{post['title'][:100]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "entities": entity_names,
                        "topics": topic_labels,
                        "entity_count": len(entities),
                        "topic_count": len(topics),
                        "subreddit": post["subreddit"],
                        "reddit_score": post["score"],
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "Social topics: processed=%d posts, api_errors=%d",
            processed, api_errors,
        )


async def ingest_social_topics(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one topic extraction cycle."""
    ingestor = SocialTopicsIngestor(board, graph, scheduler)
    await ingestor.run()
