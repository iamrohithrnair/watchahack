"""Hansard ingestor — UK Parliamentary debate transcripts from the official record.

Fetches recent debate sections and spoken contributions from the Hansard API.
This complements uk_parliament.py (which covers bills and votes) by providing
the actual substance of parliamentary discourse — who said what, and when.

API docs: https://hansard-api.parliament.uk/swagger/docs/v1
No API key required. Public, free access.
"""

from __future__ import annotations

import logging
import re
from datetime import date, timedelta
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.hansard")

_BASE = "https://hansard-api.parliament.uk"
_SECTIONS_URL = f"{_BASE}/overview/sectionsforday.json"
_CONTRIBUTIONS_URL = f"{_BASE}/search/contributions/Spoken.json"
_LAST_SITTING_URL = f"{_BASE}/overview/lastsittingdate.json"

# Westminster coordinates
_WESTMINSTER_LAT = 51.4995
_WESTMINSTER_LON = -0.1248

# Cap per cycle to avoid flooding the board
_MAX_CONTRIBUTIONS = 30


class HansardIngestor(BaseIngestor):
    source_name = "hansard"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        # Find the most recent sitting date (Parliament may not sit every day)
        last_commons = await self.fetch(
            _LAST_SITTING_URL, params={"house": "Commons"}
        )
        sitting_date = None
        if isinstance(last_commons, str):
            # API returns a bare date string like "2026-03-05T00:00:00"
            sitting_date = last_commons[:10]
        elif isinstance(last_commons, dict):
            sitting_date = (last_commons.get("date") or "")[:10]

        if not sitting_date or len(sitting_date) < 10:
            # Fallback: try yesterday
            sitting_date = (date.today() - timedelta(days=1)).isoformat()

        # Get debate sections for that day
        sections = await self.fetch(
            _SECTIONS_URL,
            params={"date": sitting_date, "house": "Commons"},
        )

        # Get recent spoken contributions
        contributions = await self.fetch(
            _CONTRIBUTIONS_URL,
            params={
                "house": "Commons",
                "startDate": sitting_date,
                "endDate": sitting_date,
                "take": str(_MAX_CONTRIBUTIONS),
                "orderBy": "SittingDateDesc",
            },
        )

        return {
            "sitting_date": sitting_date,
            "sections": sections,
            "contributions": contributions,
        }

    async def process(self, data: dict[str, Any]) -> None:
        cell_id = self.graph.latlon_to_cell(_WESTMINSTER_LAT, _WESTMINSTER_LON)
        sitting_date = data.get("sitting_date", "unknown")

        await self._process_sections(data.get("sections"), cell_id, sitting_date)
        await self._process_contributions(
            data.get("contributions"), cell_id, sitting_date
        )

    async def _process_sections(
        self, data: Any, cell_id: str | None, sitting_date: str
    ) -> None:
        if not isinstance(data, list):
            self.log.info("No debate sections data: %s", type(data))
            return

        processed = 0
        for section in data:
            try:
                title = section.get("Title") or section.get("title") or ""
                if not title:
                    continue

                section_id = section.get("ExternalId") or section.get("Id") or ""

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"Hansard debate: {title}",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "section_id": section_id,
                        "title": title,
                        "sitting_date": sitting_date,
                        "data_type": "debate_section",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Hansard debate topic ({sitting_date}): {title}",
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "section_id": section_id,
                        "title": title,
                        "sitting_date": sitting_date,
                        "data_type": "debate_section",
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing debate section")

        self.log.info("Hansard debate sections: processed=%d", processed)

    async def _process_contributions(
        self, data: Any, cell_id: str | None, sitting_date: str
    ) -> None:
        # API may return {"TotalContributions": N, "Contributions": [...]}
        items: list = []
        if isinstance(data, dict):
            items = data.get("Contributions") or data.get("Results") or []
        elif isinstance(data, list):
            items = data

        if not items:
            self.log.info("No Hansard contributions for %s", sitting_date)
            return

        processed = 0
        for contrib in items[:_MAX_CONTRIBUTIONS]:
            try:
                member_name = (
                    contrib.get("MemberName")
                    or contrib.get("AttributedTo")
                    or "Unknown MP"
                )
                text = contrib.get("ContributionText") or contrib.get("Body") or ""
                debate_title = (
                    contrib.get("DebateSection") or contrib.get("Section") or ""
                )
                contrib_id = contrib.get("ContributionExtId") or ""

                # Strip HTML tags from contribution text
                clean_text = re.sub(r"<[^>]+>", "", text).strip()

                # Truncate for the observation value
                summary = clean_text[:300] + ("..." if len(clean_text) > 300 else "")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"{member_name}: {summary}",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "contribution_id": contrib_id,
                        "member": member_name,
                        "debate_section": debate_title,
                        "sitting_date": sitting_date,
                        "full_text_length": len(clean_text),
                        "data_type": "spoken_contribution",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Hansard ({sitting_date}) {member_name} in "
                        f"'{debate_title}': {summary}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "contribution_id": contrib_id,
                        "member": member_name,
                        "debate_section": debate_title,
                        "sitting_date": sitting_date,
                        "data_type": "spoken_contribution",
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing contribution")

        self.log.info(
            "Hansard contributions (%s): processed=%d", sitting_date, processed
        )


async def ingest_hansard(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Hansard fetch cycle."""
    ingestor = HansardIngestor(board, graph, scheduler)
    await ingestor.run()
