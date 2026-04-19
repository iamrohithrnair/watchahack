"""London Assembly ingestor — committee meetings, agendas, minutes, and decisions.

Polls the ModernGov RSS feed for the Greater London Authority, which publishes
notifications when new agendas, minutes, and committee reports are made available
across all London Assembly committees (Transport, Police & Crime, Environment,
Housing, Budget, etc.).

No API key required. Public RSS feed at gla.moderngov.co.uk.
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

log = logging.getLogger("cortex.ingestors.london_assembly")

# ModernGov RSS feed — all London Assembly committees in one feed
_RSS_URL = "https://gla.moderngov.co.uk/mgRss.aspx?XXR=0"

# City Hall coordinates (Kamal Chunchie Way, Royal Docks)
_CITY_HALL_LAT = 51.5083
_CITY_HALL_LON = 0.0175

# Document type keywords for classification
_DOC_TYPES = {
    "minutes": "minutes",
    "agenda": "agenda",
    "decision": "decision",
    "report": "report",
    "transcript": "transcript",
    "motion": "motion",
    "question": "question",
    "amendment": "amendment",
}


def _classify_item(title: str) -> str:
    """Classify an RSS item by document type based on title keywords."""
    lower = title.lower()
    for keyword, doc_type in _DOC_TYPES.items():
        if keyword in lower:
            return doc_type
    return "publication"


class LondonAssemblyIngestor(BaseIngestor):
    source_name = "london_assembly"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # RSS returns XML text, not JSON
        text = await self.fetch(_RSS_URL)
        if text is None:
            return None
        # fetch() may return parsed JSON (if content-type matches) or raw text
        if isinstance(text, str):
            return text
        # Shouldn't happen for RSS, but handle gracefully
        self.log.warning("Unexpected response type from RSS: %s", type(text))
        return None

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            self.log.warning("Expected XML string, got %s", type(data))
            return

        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            self.log.error("Failed to parse RSS XML: %s", exc)
            return

        cell_id = self.graph.latlon_to_cell(_CITY_HALL_LAT, _CITY_HALL_LON)
        channel = root.find("channel")
        if channel is None:
            self.log.warning("No <channel> element in RSS feed")
            return

        items = channel.findall("item")
        processed = 0

        for item in items:
            title = (item.findtext("title") or "").strip()
            link = (item.findtext("link") or "").strip()
            pub_date = (item.findtext("pubDate") or "").strip()
            description = (item.findtext("description") or "").strip()

            if not title:
                continue

            doc_type = _classify_item(title)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"London Assembly: {title}",
                location_id=cell_id,
                lat=_CITY_HALL_LAT,
                lon=_CITY_HALL_LON,
                metadata={
                    "doc_type": doc_type,
                    "pub_date": pub_date,
                    "link": link,
                    "description": description[:500] if description else "",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"London Assembly [{doc_type}]: {title}"
                    + (f" (published {pub_date})" if pub_date else "")
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "doc_type": doc_type,
                    "title": title,
                    "link": link,
                    "pub_date": pub_date,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "London Assembly: processed=%d items from RSS feed", processed
        )


async def ingest_london_assembly(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = LondonAssemblyIngestor(board, graph, scheduler)
    await ingestor.run()
