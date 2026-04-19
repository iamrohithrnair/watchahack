"""UK Parliament Petitions ingestor — public petitions with signature counts.

Tracks open petitions, signature velocity, and debate/response thresholds.
Provides a structured signal on public mobilization around legislation
*before* it translates into physical protests.

API: https://petition.parliament.uk/petitions.json (no key required)
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.parliament_petitions")

_PETITIONS_URL = "https://petition.parliament.uk/petitions.json"

# Thresholds defined by Parliament
_RESPONSE_THRESHOLD = 10_000   # Government must respond
_DEBATE_THRESHOLD = 100_000    # Considered for debate

# Westminster coordinates
_WESTMINSTER_LAT = 51.4995
_WESTMINSTER_LON = -0.1248


class ParliamentPetitionsIngestor(BaseIngestor):
    source_name = "parliament_petitions"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Fetch open petitions (most active first)
        return await self.fetch(
            _PETITIONS_URL,
            params={"state": "open"},
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected petitions response: %s", type(data))
            return

        items = data.get("data", [])
        cell_id = self.graph.latlon_to_cell(_WESTMINSTER_LAT, _WESTMINSTER_LON)
        processed = 0

        for item in items:
            try:
                attrs = item.get("attributes", {})
                petition_id = item.get("id", "")
                action = attrs.get("action", "")
                state = attrs.get("state", "open")
                signatures = attrs.get("signature_count", 0)
                created = attrs.get("created_at", "")
                opened = attrs.get("opened_at", "")
                background = attrs.get("background", "")
                departments = attrs.get("departments", [])
                topics = attrs.get("topics", [])
                gov_response = attrs.get("government_response")
                debate = attrs.get("debate")
                debate_threshold_reached = attrs.get(
                    "debate_threshold_reached_at"
                )
                response_threshold_reached = attrs.get(
                    "response_threshold_reached_at"
                )

                # Classify momentum
                if signatures >= _DEBATE_THRESHOLD:
                    momentum = "debate_threshold"
                elif signatures >= _RESPONSE_THRESHOLD:
                    momentum = "response_threshold"
                elif signatures >= 1000:
                    momentum = "gaining_traction"
                else:
                    momentum = "early"

                dept_names = []
                if isinstance(departments, list):
                    for d in departments:
                        if isinstance(d, dict):
                            dept_names.append(d.get("name", ""))

                topic_names = []
                if isinstance(topics, list):
                    for t in topics:
                        if isinstance(t, dict):
                            topic_names.append(t.get("name", ""))

                obs = Observation(
                    source="parliament_petitions",
                    obs_type=ObservationType.NUMERIC,
                    value=float(signatures),
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "petition_id": petition_id,
                        "action": action,
                        "state": state,
                        "momentum": momentum,
                        "background": background[:300],
                        "departments": dept_names,
                        "topics": topic_names,
                        "has_gov_response": gov_response is not None,
                        "has_debate": debate is not None,
                        "debate_threshold_reached": debate_threshold_reached,
                        "response_threshold_reached": response_threshold_reached,
                        "opened_at": opened,
                        "created_at": created,
                    },
                )
                await self.board.store_observation(obs)

                # Build descriptive message
                status_parts = [f"{signatures:,} signatures"]
                if debate_threshold_reached:
                    status_parts.append("DEBATE THRESHOLD reached")
                elif response_threshold_reached:
                    status_parts.append("response threshold reached")
                if gov_response:
                    status_parts.append("gov responded")
                if debate:
                    status_parts.append("debated")

                msg = AgentMessage(
                    from_agent="parliament_petitions",
                    channel="#raw",
                    content=(
                        f"Petition: {action} — {', '.join(status_parts)}"
                    ),
                    data={
                        "source": "parliament_petitions",
                        "obs_type": "numeric",
                        "petition_id": petition_id,
                        "action": action,
                        "signatures": signatures,
                        "momentum": momentum,
                        "departments": dept_names,
                        "topics": topic_names,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception(
                    "Error processing petition: %s", item.get("id")
                )

        self.log.info(
            "Parliament petitions: processed=%d total_in_response=%d",
            processed,
            len(items),
        )


async def ingest_parliament_petitions(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one petitions fetch cycle."""
    ingestor = ParliamentPetitionsIngestor(board, graph, scheduler)
    await ingestor.run()
