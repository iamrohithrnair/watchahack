"""GDELT news ingestor — geocoded London news articles."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.news")

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_PARAMS = {
    "query": "london",
    "mode": "artlist",
    "maxrecords": "50",
    "format": "json",
}


class GdeltNewsIngestor(BaseIngestor):
    source_name = "gdelt"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(GDELT_DOC_URL, params=GDELT_PARAMS)

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected GDELT response type: %s", type(data))
            return

        articles = data.get("articles", [])
        if not articles:
            self.log.info("GDELT returned 0 articles")
            return

        processed = 0
        skipped = 0

        for article in articles:
            try:
                url = article.get("url", "")
                title = article.get("title", "")
                seendate = article.get("seendate", "")
                domain = article.get("domain", "")

                # GDELT may include geolocation fields
                lat = article.get("geolocation", {}).get("lat") if isinstance(
                    article.get("geolocation"), dict
                ) else None
                lon = article.get("geolocation", {}).get("lon") if isinstance(
                    article.get("geolocation"), dict
                ) else None

                # Try top-level lat/lon as fallback
                if lat is None:
                    lat = article.get("latitude") or article.get("lat")
                if lon is None:
                    lon = article.get("longitude") or article.get("lon")

                try:
                    lat = float(lat) if lat is not None else None
                    lon = float(lon) if lon is not None else None
                except (ValueError, TypeError):
                    lat = lon = None

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                text_value = title if title else url

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=text_value,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "url": url,
                        "title": title,
                        "domain": domain,
                        "seendate": seendate,
                    },
                )
                await self.board.store_observation(obs)

                content_snippet = title[:120] if title else url[:120]
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"News [{domain}] {content_snippet}",
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "url": url,
                        "title": title,
                        "domain": domain,
                        "seendate": seendate,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing GDELT article: %s", article.get("url"))
                skipped += 1

        self.log.info("GDELT news: processed=%d skipped=%d", processed, skipped)


async def ingest_news(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GDELT fetch cycle."""
    ingestor = GdeltNewsIngestor(board, graph, scheduler)
    await ingestor.run()
