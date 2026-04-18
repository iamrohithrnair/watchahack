"""
Chronicler Agent -- long-term narrative tracking across days/weeks.

Runs hourly. Maintains evolving narratives in data/memory/evolving_narratives.json.
Reads discoveries + completed investigation threads, detects multi-day patterns,
and produces weekly reflective summaries.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_PRO_MODEL, LLM_FLASH_MODEL
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.chronicler")

_CHRONICLER_SYSTEM = """\
You are the long-term memory and narrative tracker for London Cortex.
You maintain evolving stories about what's happening in London over days and weeks.
You notice when patterns emerge, escalate, resolve, or go dormant.
You think like a journalist maintaining beat coverage of the city.
"""


class ChroniclerAgent(BaseAgent):
    """Maintains evolving narratives and detects multi-day patterns."""

    name = "chronicler"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._last_weekly: datetime | None = None

    async def run(self) -> None:
        # Check pending investigations
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                last = inv[-1] if inv else {}
                thread_id = last.get("thread_id", "")
                await self.continue_investigation(
                    thread_id=thread_id,
                    action="Chronicler noted investigation",
                    finding=f"Recorded for narrative tracking: {last.get('next_question', '')}",
                )
        except Exception:
            pass

        now = datetime.now(timezone.utc)

        # Load existing narratives
        narratives = self.memory.load_json("evolving_narratives.json")
        if not isinstance(narratives, list):
            narratives = []

        # -- Hard TTL: auto-expire narratives not updated in 12h --
        # This prevents stale narratives from persisting indefinitely
        # regardless of what the LLM decides.
        ttl_cutoff = (now - timedelta(hours=12)).isoformat()
        expired = [n for n in narratives if n.get("last_updated", "") <= ttl_cutoff]
        narratives = [n for n in narratives if n.get("last_updated", "") > ttl_cutoff]
        if expired:
            log.info(
                "[Chronicler] Auto-expired %d stale narratives (>12h without evidence): %s",
                len(expired),
                [n.get("theme", "?")[:30] for n in expired],
            )
            # Persist the pruned list immediately
            self.memory.save_json("evolving_narratives.json", narratives)

        # Gather recent discoveries (last 6h)
        discoveries = await self.read_channel("#discoveries", since_hours=6.0, limit=30)

        # Gather completed investigation threads
        try:
            active_invs = await self.board.get_active_investigations(limit=20)
        except Exception:
            active_invs = []

        # Build context
        disc_text = self._summarise_messages(discoveries, max_chars=2000) if discoveries else "None"
        inv_text = json.dumps(active_invs, default=str)[:1000] if active_invs else "None"
        existing_text = json.dumps(narratives[-5:], default=str)[:1500] if narratives else "None"

        prompt = f"""Current time: {now.strftime("%Y-%m-%d %H:%M UTC")}

=== EXISTING EVOLVING NARRATIVES ===
{existing_text}

=== RECENT DISCOVERIES (last 6h) ===
{disc_text}

=== ACTIVE INVESTIGATIONS ===
{inv_text}

Update the evolving narratives. For each narrative, decide:
- Is it PROGRESSING (new evidence supports it)?
- Is it ESCALATING (getting more serious)?
- Is it RESOLVING (returning to normal)?
- Is it DEAD (no new evidence, should be archived)?
- Any NEW narratives emerging?

Respond with JSON:
{{
  "updated_narratives": [
    {{
      "theme": "short title",
      "status": "progressing|escalating|resolving|dead|new",
      "summary": "current state in 1-2 sentences",
      "evidence_count": <int>,
      "first_seen": "ISO timestamp or existing",
      "last_updated": "{now.isoformat()}"
    }}
  ],
  "meta_observation": "One higher-level observation about London patterns"
}}

Keep max 10 active narratives. Archive dead ones."""

        response = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            trigger="chronicler:update",
            system_instruction=_CHRONICLER_SYSTEM,
        )

        if response.startswith("ERROR:"):
            log.warning("[Chronicler] LLM error: %s", response)
            return

        parsed = parse_json_response(response)
        if not parsed or not isinstance(parsed, dict):
            return

        updated = parsed.get("updated_narratives", [])
        meta = parsed.get("meta_observation", "")

        if updated:
            # Keep only active narratives (not dead)
            active = [n for n in updated if n.get("status") != "dead"][:10]
            self.memory.save_json("evolving_narratives.json", active)
            log.info(
                "[Chronicler] Updated %d narratives (%d active)",
                len(updated), len(active),
            )

        if meta:
            await self.post(
                channel="#meta",
                content=f"[CHRONICLER] {meta}",
                data={"type": "chronicler_observation", "meta": meta, "narrative_count": len(updated)},
                priority=1,
                ttl_hours=24,
            )

        # Weekly reflection (every 7 days)
        if self._last_weekly is None or (now - self._last_weekly).total_seconds() > 7 * 86400:
            await self._weekly_reflection(now)
            self._last_weekly = now

    async def _weekly_reflection(self, now: datetime) -> None:
        """Produce a weekly summary of what the system has learned."""
        narratives = self.memory.load_json("evolving_narratives.json")
        improvements = self.memory.load_json("self_improvement_log.json")
        correlations = self.memory.load_json("learned_correlations.json")

        prompt = f"""Produce a weekly reflection on what London Cortex has learned.

Evolving narratives: {json.dumps(narratives, default=str)[:1500]}
Self-improvement observations: {json.dumps(improvements[-5:] if isinstance(improvements, list) else [], default=str)[:800]}
Discovered correlations: {json.dumps(correlations[-10:] if isinstance(correlations, list) else [], default=str)[:800]}

Respond with JSON:
{{
  "weekly_summary": "2-3 paragraph reflection on London this week",
  "key_learnings": ["what the system has confirmed about how London works"],
  "open_questions": ["questions that remain unanswered"],
  "prediction_quality": "assessment of how well predictions have done"
}}"""

        response = await self.call_llm(
            prompt=prompt,
            model=LLM_PRO_MODEL,
            trigger="chronicler:weekly",
            system_instruction=_CHRONICLER_SYSTEM,
        )
        self.record_pro_call()

        if response.startswith("ERROR:"):
            return

        parsed = parse_json_response(response)
        if not parsed:
            return

        # Save weekly report
        weekly_reports = self.memory.load_json("weekly_reports.json")
        if not isinstance(weekly_reports, list):
            weekly_reports = []
        weekly_reports.append({
            "timestamp": now.isoformat(),
            **parsed,
        })
        weekly_reports = weekly_reports[-52:]  # keep 1 year
        self.memory.save_json("weekly_reports.json", weekly_reports)

        summary = parsed.get("weekly_summary", "")
        if summary:
            await self.post(
                channel="#discoveries",
                content=f"[CHRONICLER WEEKLY] {summary[:500]}",
                data={"type": "weekly_reflection", **parsed},
                priority=3,
                ttl_hours=168,  # keep for a week
            )

        log.info("[Chronicler] Weekly reflection complete")


# -- Async entry point ----------------------------------------------------------

async def run_chronicler(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager,
) -> None:
    agent = ChroniclerAgent(board, graph, memory)
    await agent.run()
