"""
DiscoveryEngine -- finds connections between data streams via open-ended exploration.

Three strategies, randomly weighted each cycle:
1. Cross-domain scan: pick random sources, show to LLM, ask "what's interesting?"
2. Chain extension: build adjacency from connections, find 2-hop paths, ask about implications
3. Implication mining: take recent discoveries, ask "where does this lead?"
"""

from __future__ import annotations

import json
import logging
import random
import uuid
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import Connection, AgentMessage
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.discovery")

# All source names the system knows about
_ALL_SOURCES = [
    "laqn", "open_meteo", "carbon_intensity", "environment_agency",
    "tfl_jamcam", "financial_stocks", "financial_crypto", "polymarket",
    "nature_inaturalist", "gdelt",
    "social_sentiment", "bluesky_transport", "tfl_cycle_hire",
    "police_crimes", "tfl_crowding", "tfl_bus_crowding",
    "sensor_community", "tomtom_traffic", "grid_generation",
    "grid_demand", "retail_spending",
    "vam_museum", "land_registry", "uk_parliament",
    "uk_petitions", "ons_demographics", "open_plaques",
]

_CROSS_DOMAIN_PROMPT = """\
You are exploring London's real-time data streams for interesting patterns and connections.
Current time: {current_time}

Here is what's happening RIGHT NOW across these data streams:

{observations}

What do you notice? What's interesting? Follow any thread that seems compelling.
Look for:
- Unexpected co-occurrences across different domains
- Values that seem to relate to each other (even if obviously)
- Patterns that a human analyst might find noteworthy

Respond with JSON:
{{
  "observations": [
    {{
      "description": "What you noticed",
      "sources_involved": ["source_a", "source_b"],
      "relationship_type": "hypothesis",
      "confidence": 0.1-0.9,
      "interesting_because": "Why this matters"
    }}
  ],
  "most_compelling": "The single most interesting thing across all these streams"
}}
"""

_CHAIN_EXTENSION_PROMPT = """\
Cortex has found these connections:

Path: {source_a} -> {source_b} -> {source_c}
Connection A->B: {desc_ab}
Connection B->C: {desc_bc}

Given that {source_a} connects to {source_b} and {source_b} connects to {source_c}:
1. What does the full A->C chain imply?
2. What should we check next to validate or extend this chain?
3. What practical implications does this chain have?

Respond with JSON:
{{
  "chain_implication": "What the A->C chain means",
  "validation_check": "What to look for next",
  "practical_implications": "Why this matters for London",
  "confidence": 0.1-0.9,
  "next_source_to_check": "which data source to examine"
}}
"""

_IMPLICATION_PROMPT = """\
Cortex recently discovered this:

{discovery}

Think freely about what this is interesting for. What could this mean?
Where does this lead? Don't limit yourself to predefined categories --
follow whatever threads you find compelling.

Respond with JSON:
{{
  "implications": [
    {{
      "domain": "what area this affects",
      "implication": "what it means",
      "testable_prediction": "how we could verify this"
    }}
  ],
  "most_surprising": "The most unexpected implication",
  "follow_up_source": "which data source to check next"
}}
"""


class DiscoveryEngine(BaseAgent):
    """
    Open-ended discovery agent. Explores connections between data streams
    without requiring anomalies. Runs every 5 minutes.
    """

    name = "discovery_engine"

    # Strategy weights: cross_domain=3, chain_extension=2, implication=1
    _STRATEGIES = [
        ("cross_domain", 3),
        ("chain_extension", 2),
        ("implication_mining", 1),
    ]

    async def run(self) -> None:
        # Weighted random strategy selection
        strategies, weights = zip(*self._STRATEGIES)
        strategy = random.choices(strategies, weights=weights, k=1)[0]

        log.info("[DiscoveryEngine] Running strategy: %s", strategy)

        if strategy == "cross_domain":
            await self._cross_domain_scan()
        elif strategy == "chain_extension":
            await self._chain_extension()
        elif strategy == "implication_mining":
            await self._implication_mining()

    async def _cross_domain_scan(self) -> None:
        """Pick 3-5 random sources, fetch all recent observations, explore."""
        sources = random.sample(_ALL_SOURCES, min(5, len(_ALL_SOURCES)))

        # Fetch observations from #observations channel
        obs_messages = await self.read_channel("#observations", since_hours=2.0, limit=200)

        # Filter to selected sources
        relevant = [
            m for m in obs_messages
            if m.get("data", {}).get("source") in sources
        ]

        # Also grab some raw data
        if len(relevant) < 5:
            raw_messages = await self.read_channel("#raw", since_hours=1.0, limit=100)
            for m in raw_messages:
                if m.get("data", {}).get("source") in sources:
                    relevant.append(m)

        if len(relevant) < 3:
            log.debug("[DiscoveryEngine] Not enough observations for cross-domain scan.")
            return

        obs_text = self._summarise_messages(relevant, max_chars=3000)
        now = datetime.now(timezone.utc)

        prompt = _CROSS_DOMAIN_PROMPT.format(
            current_time=now.strftime("%Y-%m-%d %H:%M UTC"),
            observations=obs_text,
        )

        response = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            trigger="discovery:cross_domain",
        )

        if response.startswith("ERROR:"):
            log.warning("[DiscoveryEngine] LLM error: %s", response)
            return

        parsed = parse_json_response(response)
        if not parsed or not isinstance(parsed, dict):
            return

        observations = parsed.get("observations", [])
        most_compelling = parsed.get("most_compelling", "")

        chain_id = uuid.uuid4().hex[:12]

        for obs in observations[:5]:
            sources_involved = obs.get("sources_involved", [])
            if len(sources_involved) < 2:
                continue

            conn = Connection(
                source_a=sources_involved[0],
                source_b=sources_involved[1],
                description=obs.get("description", ""),
                confidence=float(obs.get("confidence", 0.3)),
                relationship_type=obs.get("relationship_type", "hypothesis"),
                chain_id=chain_id,
                metadata={
                    "interesting_because": obs.get("interesting_because", ""),
                    "strategy": "cross_domain",
                },
            )
            await self.board.store_connection(conn)

        if most_compelling:
            await self.post(
                channel="#discoveries",
                content=f"[DISCOVERY] {most_compelling}",
                data={
                    "type": "cross_domain_discovery",
                    "chain_id": chain_id,
                    "sources_scanned": sources,
                    "observations_count": len(observations),
                },
                priority=2,
                ttl_hours=6,
            )
            log.info("[DiscoveryEngine] Cross-domain: %s", most_compelling[:100])

    async def _chain_extension(self) -> None:
        """Find 2-hop paths in connection graph and ask about implications."""
        connections = await self.board.get_recent_connections(since_hours=24.0, limit=100)

        if len(connections) < 2:
            log.debug("[DiscoveryEngine] Not enough connections for chain extension.")
            return

        # Build adjacency: source -> [(other_source, description, connection)]
        adjacency: dict[str, list[tuple[str, str, dict]]] = defaultdict(list)
        for conn in connections:
            a = conn.get("source_a", "")
            b = conn.get("source_b", "")
            desc = conn.get("description", "")
            if a and b:
                adjacency[a].append((b, desc, conn))
                adjacency[b].append((a, desc, conn))

        # Find 2-hop paths: A->B->C where A!=C
        two_hop_paths = []
        for a, neighbors_a in adjacency.items():
            for b, desc_ab, _ in neighbors_a:
                for c, desc_bc, _ in adjacency.get(b, []):
                    if c != a:
                        two_hop_paths.append((a, b, c, desc_ab, desc_bc))

        if not two_hop_paths:
            log.debug("[DiscoveryEngine] No 2-hop paths found.")
            return

        # Pick a random path
        path = random.choice(two_hop_paths)
        a, b, c, desc_ab, desc_bc = path

        prompt = _CHAIN_EXTENSION_PROMPT.format(
            source_a=a, source_b=b, source_c=c,
            desc_ab=desc_ab[:200], desc_bc=desc_bc[:200],
        )

        response = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            trigger="discovery:chain_extension",
        )

        if response.startswith("ERROR:"):
            return

        parsed = parse_json_response(response)
        if not parsed or not isinstance(parsed, dict):
            return

        chain_impl = parsed.get("chain_implication", "")
        confidence = float(parsed.get("confidence", 0.3))

        if chain_impl:
            chain_id = uuid.uuid4().hex[:12]
            conn = Connection(
                source_a=a,
                source_b=c,
                description=f"[CHAIN] {a}->{b}->{c}: {chain_impl}",
                confidence=confidence,
                relationship_type="chain",
                chain_id=chain_id,
                metadata={
                    "intermediate": b,
                    "desc_ab": desc_ab[:200],
                    "desc_bc": desc_bc[:200],
                    "validation_check": parsed.get("validation_check", ""),
                    "practical_implications": parsed.get("practical_implications", ""),
                    "strategy": "chain_extension",
                },
            )
            await self.board.store_connection(conn)

            await self.post(
                channel="#discoveries",
                content=(
                    f"[CHAIN] {a} -> {b} -> {c}: {chain_impl}\n"
                    f"Implications: {parsed.get('practical_implications', '')}"
                ),
                data={
                    "type": "chain_discovery",
                    "chain_id": chain_id,
                    "path": [a, b, c],
                    "confidence": confidence,
                },
                priority=2,
                ttl_hours=6,
            )
            log.info("[DiscoveryEngine] Chain: %s->%s->%s: %s", a, b, c, chain_impl[:80])

    async def _implication_mining(self) -> None:
        """Take recent discoveries and explore their implications freely."""
        discoveries = await self.read_channel("#discoveries", since_hours=6.0, limit=20)

        if not discoveries:
            log.debug("[DiscoveryEngine] No recent discoveries for implication mining.")
            return

        # Pick a random recent discovery
        discovery = random.choice(discoveries)
        disc_content = discovery.get("content", "")

        if not disc_content:
            return

        prompt = _IMPLICATION_PROMPT.format(discovery=disc_content[:500])

        response = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            trigger="discovery:implication",
        )

        if response.startswith("ERROR:"):
            return

        parsed = parse_json_response(response)
        if not parsed or not isinstance(parsed, dict):
            return

        implications = parsed.get("implications", [])
        most_surprising = parsed.get("most_surprising", "")

        if most_surprising:
            await self.post(
                channel="#discoveries",
                content=f"[IMPLICATION] {most_surprising}",
                data={
                    "type": "implication_discovery",
                    "source_discovery": discovery.get("id", ""),
                    "implications_count": len(implications),
                    "follow_up_source": parsed.get("follow_up_source", ""),
                },
                priority=2,
                ttl_hours=6,
                references=[discovery.get("id", "")],
            )
            log.info("[DiscoveryEngine] Implication: %s", most_surprising[:100])


# -- Async entry point ----------------------------------------------------------

async def run_discovery_engine(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager,
) -> None:
    agent = DiscoveryEngine(board, graph, memory)
    await agent.run()
