"""Metropolitan Police news ingestor — official press releases from news.met.police.uk.

Scrapes the Met Police Mynewsdesk newsroom and extracts JSON-LD structured data
(Schema.org NewsArticle objects). No API key required — the structured data is
embedded in the public HTML page.

Provides a secondary, independent channel for confirming changes in police
operational posture (counter-terrorism ops, public order events, major crime
investigations) that may correlate with anomalies from other sources.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.met_police_news")

# The Met Police newsroom — public HTML with embedded JSON-LD
NEWS_URL = "https://news.met.police.uk/latest_news"

# Keywords signalling operational posture changes (for tagging)
OPERATIONAL_KEYWORDS = {
    "counter terrorism", "counter-terrorism", "terrorism", "terrorist",
    "public order", "protest", "demonstration", "march",
    "major incident", "stabbing", "shooting", "murder", "homicide",
    "manhunt", "appeal", "wanted", "missing",
    "operation", "raid", "arrest", "charged",
    "security", "threat level", "cordon", "evacuate", "evacuation",
}

# Default location: New Scotland Yard (Met Police HQ)
MET_HQ_LAT = 51.4988
MET_HQ_LON = -0.1343


class MetPoliceNewsIngestor(BaseIngestor):
    source_name = "met_police_news"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        raw = await self.fetch(NEWS_URL)
        if raw is None:
            return None

        # The response is HTML — extract JSON-LD from <script> tags
        if not isinstance(raw, str):
            self.log.warning("Expected HTML string, got %s", type(raw))
            return None

        return self._extract_articles(raw)

    def _extract_articles(self, html: str) -> list[dict] | None:
        """Extract NewsArticle objects from JSON-LD script tags."""
        # Find all JSON-LD blocks
        pattern = r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>'
        matches = re.findall(pattern, html, re.DOTALL | re.IGNORECASE)

        articles = []
        for block in matches:
            try:
                data = json.loads(block)
            except json.JSONDecodeError:
                continue

            if isinstance(data, dict):
                # Could be a CollectionPage with mainEntity array
                if data.get("@type") == "CollectionPage":
                    entities = data.get("mainEntity", [])
                    if isinstance(entities, list):
                        articles.extend(
                            e for e in entities
                            if isinstance(e, dict) and e.get("@type") == "NewsArticle"
                        )
                elif data.get("@type") == "NewsArticle":
                    articles.append(data)
            elif isinstance(data, list):
                articles.extend(
                    item for item in data
                    if isinstance(item, dict) and item.get("@type") == "NewsArticle"
                )

        if not articles:
            self.log.warning("No NewsArticle JSON-LD found in Met Police page")
            return None

        return articles

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            return

        processed = 0
        for article in data:
            headline = article.get("headline", "").strip()
            description = article.get("description", "").strip()
            url = article.get("url", "")
            date_published = article.get("datePublished", "")
            keywords = article.get("keywords", "")

            if not headline:
                continue

            # Check for operational keywords
            text_lower = f"{headline} {description} {keywords}".lower()
            matched_ops = [kw for kw in OPERATIONAL_KEYWORDS if kw in text_lower]
            is_operational = len(matched_ops) > 0

            # Use Met HQ as default location (articles rarely have geo)
            cell_id = self.graph.latlon_to_cell(MET_HQ_LAT, MET_HQ_LON)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=headline,
                location_id=cell_id,
                lat=MET_HQ_LAT,
                lon=MET_HQ_LON,
                metadata={
                    "headline": headline,
                    "description": description,
                    "url": url,
                    "date_published": date_published,
                    "keywords": keywords,
                    "operational_keywords": matched_ops,
                    "is_operational": is_operational,
                },
            )
            await self.board.store_observation(obs)

            ops_tag = " [OPERATIONAL]" if is_operational else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Met Police News{ops_tag}: \"{headline}\" "
                    f"({date_published[:10] if date_published else 'undated'})"
                ),
                data={
                    "headline": headline,
                    "description": description,
                    "url": url,
                    "date_published": date_published,
                    "keywords": keywords,
                    "operational_keywords": matched_ops,
                    "is_operational": is_operational,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Met Police news: processed=%d articles (%d operational)",
            processed,
            sum(1 for a in data if isinstance(a, dict)),
        )


async def ingest_met_police_news(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Met Police news fetch cycle."""
    ingestor = MetPoliceNewsIngestor(board, graph, scheduler)
    await ingestor.run()
