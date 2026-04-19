"""The Guardian news headline ingestor — UK-focused headlines with tone keywords.

Complements GDELT by providing UK-specific news from The Guardian's Open Platform API.
Captures headline text, section, and pillar metadata for downstream sentiment analysis
by the TextInterpreter, supporting the Brain's hypothesis about news-cycle mediation
of carbon-intensity/mood correlations.

API: https://open-platform.theguardian.com/
Free tier: 12 req/s with key, 1 req/s without. We use ~2 req/30min = negligible.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.guardian_news")

GUARDIAN_URL = "https://content.guardianapis.com/search"

# Positive/negative keyword lists for simple headline tone scoring.
# Lightweight — avoids adding an NLP dependency. The TextInterpreter
# does the real analysis via Gemini.
_POS = frozenset(
    [
        "celebrate",
        "boost",
        "success",
        "win",
        "surge",
        "improve",
        "record",
        "breakthrough",
        "hope",
        "joy",
        "growth",
        "recovery",
        "peace",
        "safe",
        "thrive",
    ]
)
_NEG = frozenset(
    [
        "crisis",
        "attack",
        "death",
        "kill",
        "crash",
        "collapse",
        "fear",
        "threat",
        "pollution",
        "strike",
        "protest",
        "emergency",
        "fail",
        "drop",
        "worst",
        "scandal",
        "toxic",
        "flood",
        "fire",
        "closure",
    ]
)

# London bounding box (rough) for filtering — the API doesn't support geo
# but we can tag observations to the London centroid when no finer location.
_LONDON_LAT = 51.509865
_LONDON_LON = -0.118092


def _headline_tone(title: str) -> tuple[float, str]:
    """Score a headline -1..+1 using keyword matching.

    Returns (score, label) where label is one of positive/negative/neutral.
    """
    words = set(title.lower().split())
    pos = len(words & _POS)
    neg = len(words & _NEG)
    total = pos + neg
    if total == 0:
        return 0.0, "neutral"
    score = (pos - neg) / total
    if score > 0.2:
        return score, "positive"
    if score < -0.2:
        return score, "negative"
    return score, "neutral"


class GuardianNewsIngestor(BaseIngestor):
    source_name = "guardian_headlines"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("GUARDIAN_API_KEY", "test")

    async def fetch_data(self) -> Any:
        params = {
            "api-key": self.api_key,
            "q": "london",
            "page-size": "50",
            "order-by": "newest",
            "show-fields": "headline,trailText",
        }
        return await self.fetch(GUARDIAN_URL, params=params)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Guardian response type: %s", type(data))
            return

        response = data.get("response", {})
        results = response.get("results", [])
        if not results:
            self.log.info("Guardian returned 0 results")
            return

        processed = 0
        skipped = 0
        tone_totals = {"positive": 0, "negative": 0, "neutral": 0}

        cell_id = self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON)

        for article in results:
            try:
                title = (article.get("fields") or {}).get("headline", "") or article.get("webTitle", "")
                trail = (article.get("fields") or {}).get("trailText", "")
                url = article.get("webUrl", "")
                section = article.get("sectionName", "")
                pillar = article.get("pillarName", "")
                pub_date = article.get("webPublicationDate", "")

                if not title:
                    skipped += 1
                    continue

                tone_score, tone_label = _headline_tone(title)
                tone_totals[tone_label] += 1

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=title,
                    location_id=cell_id,
                    lat=_LONDON_LAT,
                    lon=_LONDON_LON,
                    metadata={
                        "url": url,
                        "title": title,
                        "trail": trail[:200] if trail else "",
                        "section": section,
                        "pillar": pillar,
                        "pub_date": pub_date,
                        "tone_score": tone_score,
                        "tone_label": tone_label,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Guardian [{section}] ({tone_label} {tone_score:+.1f}) {title[:120]}",
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "url": url,
                        "title": title,
                        "section": section,
                        "pillar": pillar,
                        "pub_date": pub_date,
                        "tone_score": tone_score,
                        "tone_label": tone_label,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing Guardian article")
                skipped += 1

        self.log.info(
            "Guardian headlines: processed=%d skipped=%d tones=%s",
            processed, skipped, dict(tone_totals),
        )


async def ingest_guardian_news(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Guardian fetch cycle."""
    ingestor = GuardianNewsIngestor(board, graph, scheduler)
    await ingestor.run()
