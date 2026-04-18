"""
CuriosityAgent -- generative investigation engine.

No fixed topic lists. Discovers what's worth investigating by examining
random data sources, grid cells, and investigation threads, then using
the LLM to find patterns worth exploring.

Runs every ~10 minutes.
"""

from __future__ import annotations

import json
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.curiosity")

_AGENT_TARGETS = [
    "vision_interpreter", "spatial_connector", "narrative_connector",
    "web_searcher", "statistical_connector",
]


class CuriosityAgent(BaseAgent):
    """Truly generative curiosity engine -- discovers topics by examining random data."""

    name = "curiosity_engine"

    async def run(self) -> None:
        # First, handle any pending investigations assigned to me
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                await self._handle_investigation(inv)
        except Exception as exc:
            log.debug("[CuriosityEngine] Investigation check failed: %s", exc)

        strategy = random.choice(["juxtapose", "temporal", "spatial", "follow_thread"])
        log.info("[CuriosityEngine] Strategy: %s", strategy)

        try:
            if strategy == "juxtapose":
                await self._juxtapose()
            elif strategy == "temporal":
                await self._temporal()
            elif strategy == "spatial":
                await self._spatial()
            elif strategy == "follow_thread":
                await self._follow_thread()
        except Exception as exc:
            log.error("[CuriosityEngine] Error in %s strategy: %s", strategy, exc)

    async def _juxtapose(self) -> None:
        """Pick 2-3 random data sources, fetch recent observations, ask LLM what's interesting."""
        all_sources = await self._get_active_sources()
        if len(all_sources) < 2:
            log.debug("[CuriosityEngine] Not enough sources for juxtaposition")
            return

        chosen = random.sample(all_sources, min(3, len(all_sources)))
        obs_data: dict[str, list] = {}
        for src in chosen:
            obs = await self.board.get_observations(src, since_hours=6.0, limit=5)
            if obs:
                obs_data[src] = [
                    {"value": o.get("value"), "timestamp": o.get("timestamp", "")[:16],
                     "location_id": o.get("location_id", "")}
                    for o in obs[:5]
                ]

        if len(obs_data) < 2:
            return

        prompt = f"""You are a curious investigator looking at London sensor data.
Here are observations from {len(obs_data)} randomly selected sources:
{json.dumps(obs_data, default=str)[:3000]}

Find something genuinely interesting, surprising, or worth investigating.
Think creatively -- what questions does this data raise?
If nothing is interesting, say so.

Respond with JSON:
{{"interesting": true/false, "observation": "...", "question": "...",
  "investigate_with": "agent name", "reasoning": "..."}}"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger="curiosity:juxtapose")
        parsed = parse_json_response(result)
        if parsed and parsed.get("interesting"):
            target = self._resolve_agent(parsed.get("investigate_with", ""))
            observation = parsed.get("observation", "Curiosity juxtaposition")
            question = parsed.get("question", "")
            reasoning = parsed.get("reasoning", "")
            await self.start_investigation(
                trigger=observation,
                question=question,
                target_agent=target,
                data={"strategy": "juxtapose", "sources": chosen, "reasoning": reasoning},
            )
            # Post the finding to a visible channel
            await self.post(
                channel="#hypotheses",
                content=f"[CURIOSITY] Juxtaposing {', '.join(chosen)}: {observation}. {reasoning}",
                data={"type": "curiosity_finding", "strategy": "juxtapose", "sources": chosen, "question": question},
                priority=2, ttl_hours=4,
            )
            log.info("[CuriosityEngine] Started investigation: %s", question[:80])

    async def _temporal(self) -> None:
        """Pick a random source, compare recent vs historical."""
        all_sources = await self._get_active_sources()
        if not all_sources:
            return

        source = random.choice(all_sources)
        recent = await self.board.get_observations(source, since_hours=1.0, limit=10)
        historical = await self.board.get_observations(source, since_hours=24.0, limit=20)

        if not recent or not historical:
            return

        prompt = f"""You are analyzing temporal patterns in {source} data.

Recent (last 1h): {json.dumps([{"value": o.get("value"), "ts": o.get("timestamp", "")[:16]} for o in recent[:5]], default=str)[:1000]}

Historical (last 24h): {json.dumps([{"value": o.get("value"), "ts": o.get("timestamp", "")[:16]} for o in historical[:10]], default=str)[:1000]}

Is there anything unusual about the recent data compared to the historical pattern?
If so, what? If nothing interesting, say so.

Respond with JSON:
{{"interesting": true/false, "observation": "...", "question": "...",
  "investigate_with": "agent name", "reasoning": "..."}}"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger=f"curiosity:temporal:{source}")
        parsed = parse_json_response(result)
        if parsed and parsed.get("interesting"):
            target = self._resolve_agent(parsed.get("investigate_with", ""))
            observation = parsed.get("observation", f"Temporal anomaly in {source}")
            question = parsed.get("question", "")
            await self.start_investigation(
                trigger=observation,
                question=question,
                target_agent=target,
                data={"strategy": "temporal", "source": source},
            )
            await self.post(
                channel="#hypotheses",
                content=f"[CURIOSITY] Temporal pattern in {source}: {observation}. Investigating: {question}",
                data={"type": "curiosity_finding", "strategy": "temporal", "source": source},
                priority=2, ttl_hours=4,
            )

    async def _spatial(self) -> None:
        """Pick a random grid cell, gather everything about it."""
        cells = list(self.graph.cells.keys())
        if not cells:
            return

        cell = random.choice(cells)
        messages = await self.board.read_location(cell, since_hours=6.0, limit=20)
        anomalies = await self.board.get_active_anomalies(location_id=cell, since_hours=6.0)

        if not messages and not anomalies:
            return

        cell_info = self.graph.cells.get(cell)
        borough = cell_info.borough if cell_info else "unknown"

        prompt = f"""You are investigating grid cell {cell} in {borough}, London.

Recent messages: {self._summarise_messages(messages, max_chars=1500)}

Active anomalies: {json.dumps([{"source": a.get("source"), "desc": a.get("description", "")[:100]} for a in anomalies[:5]], default=str)[:800]}

What's the story of this place right now? Is anything interesting happening?

Respond with JSON:
{{"interesting": true/false, "observation": "...", "question": "...",
  "investigate_with": "agent name", "reasoning": "..."}}"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger=f"curiosity:spatial:{cell[:8]}")
        parsed = parse_json_response(result)
        if parsed and parsed.get("interesting"):
            target = self._resolve_agent(parsed.get("investigate_with", ""))
            observation = parsed.get("observation", f"Spatial pattern at {cell}")
            question = parsed.get("question", "")
            await self.start_investigation(
                trigger=observation,
                question=question,
                target_agent=target,
                data={"strategy": "spatial", "cell": cell, "borough": borough},
            )
            await self.post(
                channel="#hypotheses",
                content=f"[CURIOSITY] Spatial: {borough or cell} -- {observation}. Investigating: {question}",
                data={"type": "curiosity_finding", "strategy": "spatial", "cell": cell, "borough": borough},
                priority=2, ttl_hours=4,
            )

    async def _follow_thread(self) -> None:
        """Find an unanswered question from any investigation thread or #requests."""
        # Check for requests without responses
        requests = await self.read_channel("#requests", since_hours=6.0, limit=20)
        unanswered = [
            r for r in requests
            if r.get("data", {}).get("request_type") in ("brain_question", "explorer_follow_up")
        ]

        if not unanswered:
            return

        req = random.choice(unanswered)
        question = req.get("data", {}).get("question", req.get("content", ""))

        if question:
            target = self._resolve_agent("web_searcher")
            await self.start_investigation(
                trigger=f"Following up on unanswered question",
                question=question[:300],
                target_agent=target,
                data={"strategy": "follow_thread", "original_msg_id": req.get("id", "")},
            )
            log.info("[CuriosityEngine] Following up: %s", question[:80])

    async def _handle_investigation(self, inv_thread: list[dict]) -> None:
        """Handle an investigation assigned to the curiosity engine."""
        if not inv_thread:
            return
        last_step = inv_thread[-1]
        question = last_step.get("next_question", "")
        thread_id = last_step.get("thread_id", "")
        if not question:
            return

        # Use LLM to reason about it and route to the best agent
        prompt = f"""An investigation thread has been routed to you. The question is:
{question}

Previous steps:
{json.dumps([{"agent": s.get("agent_name"), "finding": s.get("finding", "")[:100]} for s in inv_thread], default=str)[:1000]}

Think about who should investigate this next. Available agents: {', '.join(_AGENT_TARGETS)}

Respond with JSON:
{{"finding": "your analysis", "next_question": "refined question or null", "next_agent": "agent name or null"}}"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger=f"curiosity:inv:{thread_id[:8]}")
        parsed = parse_json_response(result)
        if parsed:
            finding = parsed.get("finding", "No finding")
            next_q = parsed.get("next_question")
            await self.continue_investigation(
                thread_id=thread_id,
                action=f"Curiosity analysis: {question[:60]}",
                finding=finding,
                next_question=next_q,
                next_agent=self._resolve_agent(parsed.get("next_agent", "")) if next_q else None,
            )
            # Share finding in channels
            target = parsed.get("next_agent", "")
            await self.think(
                f"Investigation step: {finding[:150]}"
                + (f" -> Routing to {target}: {next_q}" if next_q else " -- Investigation complete."),
                channel="#requests" if next_q else "#meta",
            )

    async def _get_active_sources(self) -> list[str]:
        """Query distinct sources that have recent data."""
        try:
            assert self.board._db
            since = (datetime.now(timezone.utc) - timedelta(hours=6)).isoformat()
            cursor = await self.board._db.execute(
                "SELECT DISTINCT source FROM observations WHERE timestamp > ? LIMIT 50",
                (since,),
            )
            rows = await cursor.fetchall()
            return [r[0] for r in rows]
        except Exception:
            return []

    def _resolve_agent(self, name: str) -> str:
        """Resolve a suggested agent name to a valid agent name."""
        if not name:
            return "web_searcher"
        name_lower = name.lower().replace(" ", "_")
        for target in _AGENT_TARGETS:
            if target in name_lower or name_lower in target:
                return target
        if "web" in name_lower or "search" in name_lower:
            return "web_searcher"
        if "camera" in name_lower or "vision" in name_lower or "image" in name_lower:
            return "vision_interpreter"
        if "spatial" in name_lower or "location" in name_lower or "nearby" in name_lower:
            return "spatial_connector"
        return "web_searcher"


# -- Async entry point ----------------------------------------------------------

async def run_curiosity_engine(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager,
) -> None:
    agent = CuriosityAgent(board, graph, memory)
    await agent.run()
