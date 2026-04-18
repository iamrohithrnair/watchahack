"""
Explorer agents -- autonomous hypothesis investigators.

ExplorerSpawner  -- watches #hypotheses for medium-confidence claims (0.3-0.8),
                   spawns Explorer tasks to investigate them.
Explorer         -- reads a hypothesis, plans investigation, fetches supporting
                   data, posts evidence back to #hypotheses, self-terminates.
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL, LLM_PRO_MODEL, ENDPOINTS
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import Connection
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.explorers")

_SPAWNER_CONFIDENCE_MIN = 0.3
_SPAWNER_CONFIDENCE_MAX = 0.8
_MAX_CONCURRENT_EXPLORERS = 5
_EXPLORER_TIMEOUT_SECONDS = 180

_EXPLORER_PLANNING_SYSTEM = """\
You are an autonomous field investigator for a city-wide sensor network. You receive a hypothesis
about what might be happening and plan a targeted investigation.
You are resourceful, methodical, and know which data APIs to query to find evidence.
Think like a skilled investigative journalist with access to real-time sensor data.
"""

_EXPLORER_PLAN_PROMPT = """\
Hypothesis to investigate:
{hypothesis}

Confidence so far: {confidence:.0%}
Related evidence so far:
{related_evidence}

Your task is to plan HOW to investigate this hypothesis using available data sources.
Available APIs:
- TfL JamCam (traffic cameras): {tfl_url}
- Air Quality (LAQN): {laqn_url}
- Carbon Intensity: {carbon_url}
- Environment Agency (rivers/floods): {env_url}
- Open-Meteo (weather): https://api.open-meteo.com/v1/forecast?latitude=51.5&longitude=-0.12&current=temperature_2m,rain,wind_speed_10m

- Polymarket: {polymarket_url}

Plan 2-4 specific API calls that would help confirm or refute this hypothesis.

Respond with JSON:
{{
  "investigation_rationale": "Why these specific data sources will help",
  "queries": [
    {{
      "description": "What this query will reveal",
      "url": "full URL to fetch",
      "expected_if_true": "what we expect to see if hypothesis is true",
      "expected_if_false": "what we expect if hypothesis is false"
    }}
  ]
}}
"""

_EXPLORER_ANALYSIS_PROMPT = """\
You investigated the following hypothesis:
{hypothesis}

You executed these queries and got these results:
{query_results}

Original confidence: {confidence:.0%}
Original evidence: {related_evidence}

Now synthesize: does this new evidence SUPPORT or REFUTE the hypothesis?

Respond with JSON:
{{
  "conclusion": "SUPPORTED" | "REFUTED" | "INCONCLUSIVE",
  "updated_confidence": <0.0-1.0>,
  "key_finding": "The single most important thing you found",
  "supporting_evidence": ["list of specific evidence points that support the hypothesis"],
  "contradicting_evidence": ["list of specific evidence points that contradict it"],
  "narrative": "2-3 sentence story of what you found",
  "follow_up_needed": true | false,
  "follow_up_question": "If follow-up needed, what specific question remains?"
}}
"""


class ExplorerSpawner(BaseAgent):
    """
    Watches #hypotheses for medium-confidence claims and spawns Explorer tasks.
    Maintains a set of already-explored hypothesis IDs to avoid re-work.
    """

    name = "explorer_spawner"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._explored: set[str] = set()
        self._active_tasks: list[asyncio.Task] = []

    async def run(self) -> None:
        # Clean up finished tasks
        self._active_tasks = [t for t in self._active_tasks if not t.done()]

        if len(self._active_tasks) >= _MAX_CONCURRENT_EXPLORERS:
            log.debug("[ExplorerSpawner] Max concurrent explorers reached (%d).", _MAX_CONCURRENT_EXPLORERS)
            return

        hypotheses = await self.read_channel("#hypotheses", since_hours=2.0, limit=50)

        # Find medium-confidence hypotheses that haven't been explored yet
        candidates: list[dict[str, Any]] = []
        for h in hypotheses:
            data = h.get("data", {})
            if isinstance(data, str):
                try:
                    data = json.loads(data)
                except (json.JSONDecodeError, TypeError):
                    data = {}
            if not isinstance(data, dict):
                data = {}
            hyp_id = h.get("id", "")
            confidence = float(data.get("confidence", data.get("confidence_score", 0.5)))
            hyp_type = data.get("type", "")

            # Skip already explored, spatial (too low-level), or statistical (already handled)
            if hyp_id in self._explored:
                continue
            if hyp_type == "spatial_colocation":
                continue  # spatial connector handles these
            if confidence < _SPAWNER_CONFIDENCE_MIN or confidence > _SPAWNER_CONFIDENCE_MAX:
                continue

            candidates.append(h)

        if not candidates:
            log.debug("[ExplorerSpawner] No explorable hypotheses found.")
            return

        # Sort by priority then confidence
        def _sort_key(h: dict[str, Any]) -> tuple[int, float]:
            d = h.get("data", {})
            if isinstance(d, str):
                try:
                    d = json.loads(d)
                except (json.JSONDecodeError, TypeError):
                    d = {}
            if not isinstance(d, dict):
                d = {}
            return (h.get("priority", 1), float(d.get("confidence", d.get("confidence_score", 0.5))))

        candidates.sort(key=_sort_key, reverse=True)

        slots = _MAX_CONCURRENT_EXPLORERS - len(self._active_tasks)
        for hypothesis_msg in candidates[:slots]:
            hyp_id = hypothesis_msg.get("id", "")
            self._explored.add(hyp_id)
            if len(self._explored) > 5000:
                self._explored = set(list(self._explored)[-2500:])

            # Announce spawning
            await self.post(
                channel="#meta",
                content=(
                    f"[SPAWNER] Launching Explorer for hypothesis: "
                    f"{hypothesis_msg.get('content', '')[:120]}"
                ),
                data={
                    "type": "explorer_spawned",
                    "hypothesis_id": hyp_id,
                    "confidence": hypothesis_msg.get("data", {}).get("confidence", 0.5),
                },
                priority=1,
                ttl_hours=2,
            )

            # Spawn async task
            task = asyncio.create_task(
                _run_explorer_task(self.board, self.graph, self.memory, hypothesis_msg),
                name=f"explorer:{hyp_id[:8]}",
            )
            self._active_tasks.append(task)

            log.info(
                "[ExplorerSpawner] Spawned Explorer for hypothesis %s (conf=%.2f): %s",
                hyp_id[:8],
                hypothesis_msg.get("data", {}).get("confidence", 0.5),
                hypothesis_msg.get("content", "")[:80],
            )


async def _run_explorer_task(
    board: MessageBoard,
    graph: CortexGraph,
    memory: MemoryManager,
    hypothesis_msg: dict[str, Any],
) -> None:
    """Top-level coroutine for an Explorer. Self-terminates on completion or timeout."""
    try:
        async with asyncio.timeout(_EXPLORER_TIMEOUT_SECONDS):
            explorer = Explorer(board, graph, memory, hypothesis_msg)
            await explorer.investigate()
    except asyncio.TimeoutError:
        hyp_id = hypothesis_msg.get("id", "")[:8]
        log.warning("[Explorer:%s] Timed out after %ds.", hyp_id, _EXPLORER_TIMEOUT_SECONDS)
        # Post timeout notice
        explorer_name = f"explorer:{hyp_id}"
        msg_content = f"[EXPLORER] Investigation timed out for: {hypothesis_msg.get('content', '')[:100]}"
        try:
            from ..core.models import AgentMessage
            msg = AgentMessage(
                from_agent=explorer_name,
                channel="#meta",
                content=msg_content,
                data={"type": "explorer_timeout", "hypothesis_id": hypothesis_msg.get("id", "")},
                priority=1,
                ttl_hours=2,
            )
            await board.post(msg)
        except Exception:
            pass
    except Exception as exc:
        log.error("[Explorer] Unexpected error: %s", exc, exc_info=True)


class Explorer(BaseAgent):
    """
    Individual explorer. Investigates ONE hypothesis:
    1. Plans which APIs to query (LLM)
    2. Fetches data from those APIs
    3. Analyses results (LLM)
    4. Posts findings back to #hypotheses
    5. Self-terminates
    """

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        memory: MemoryManager,
        hypothesis_msg: dict[str, Any],
    ) -> None:
        super().__init__(board, graph, memory)
        self._hypothesis_msg = hypothesis_msg
        hyp_id = hypothesis_msg.get("id", "")[:8]
        self.name = f"explorer:{hyp_id}"

    async def investigate(self) -> None:
        hypothesis_content = self._hypothesis_msg.get("content", "")
        data = self._hypothesis_msg.get("data", {})
        confidence = float(data.get("confidence", 0.5))
        hyp_id = self._hypothesis_msg.get("id", "")
        location_id = self._hypothesis_msg.get("location_id")

        log.info("[%s] Starting investigation: %s", self.name, hypothesis_content[:100])

        # Step 1: Gather related evidence from the board
        related_anomalies = await self.read_channel("#anomalies", since_hours=3.0, limit=30)
        evidence_ids = data.get("anomaly_ids", data.get("evidence", []))

        # Filter for relevant anomalies
        related: list[dict[str, Any]] = []
        for a in related_anomalies:
            if a.get("id") in evidence_ids or a.get("data", {}).get("anomaly_id") in evidence_ids:
                related.append(a)
        if location_id:
            for a in related_anomalies:
                if a.get("location_id") == location_id and a not in related:
                    related.append(a)
        related_evidence_text = self._summarise_messages(related[:10], max_chars=800) or "No specific evidence available."

        # Step 2: Plan the investigation using LLM
        plan_prompt = _EXPLORER_PLAN_PROMPT.format(
            hypothesis=hypothesis_content,
            confidence=confidence,
            related_evidence=related_evidence_text,
            tfl_url=ENDPOINTS["tfl_jamcams"],
            laqn_url=ENDPOINTS["laqn"],
            carbon_url=ENDPOINTS["carbon_intensity"],
            env_url=ENDPOINTS["environment_agency"],

            polymarket_url=ENDPOINTS["polymarket_gamma"],
        )

        plan_response = await self.call_llm(
            prompt=plan_prompt,
            model=LLM_PRO_MODEL,
            trigger=f"explore:plan:{hyp_id[:8]}",
            system_instruction=_EXPLORER_PLANNING_SYSTEM,
        )
        self.record_pro_call()

        if plan_response.startswith("ERROR:"):
            log.warning("[%s] Planning failed: %s", self.name, plan_response)
            return

        plan = parse_json_response(plan_response)
        if not plan or not isinstance(plan, dict):
            log.debug("[%s] Could not parse plan.", self.name)
            return

        queries: list[dict[str, Any]] = plan.get("queries", [])[:4]  # max 4 queries
        rationale = plan.get("investigation_rationale", "")

        log.info("[%s] Plan: %s queries. Rationale: %s", self.name, len(queries), rationale[:100])

        # Step 3: Execute the planned queries
        query_results: list[dict[str, Any]] = []
        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=25, connect=10),
            headers={"User-Agent": "Cortex/1.0"},
        ) as session:
            for q in queries:
                url = q.get("url", "")
                description = q.get("description", "")
                if not url:
                    continue
                result = await self._fetch_url(session, url, description)
                query_results.append({
                    "description": description,
                    "url": url,
                    "expected_if_true": q.get("expected_if_true", ""),
                    "expected_if_false": q.get("expected_if_false", ""),
                    "result": result,
                })
                # Small delay between requests
                await asyncio.sleep(0.5)

        if not query_results:
            log.debug("[%s] No query results obtained.", self.name)
            await self._post_null_finding(hyp_id, hypothesis_content, location_id)
            return

        # Step 4: Analyse results with LLM
        results_text = json.dumps(
            [{
                "description": r["description"],
                "expected_if_true": r["expected_if_true"],
                "result_preview": str(r["result"])[:400],
            } for r in query_results],
            indent=2,
        )[:3000]

        analysis_prompt = _EXPLORER_ANALYSIS_PROMPT.format(
            hypothesis=hypothesis_content,
            query_results=results_text,
            confidence=confidence,
            related_evidence=related_evidence_text,
        )

        analysis_response = await self.call_llm(
            prompt=analysis_prompt,
            model=LLM_FLASH_MODEL,
            trigger=f"explore:analyse:{hyp_id[:8]}",
            system_instruction=_EXPLORER_PLANNING_SYSTEM,
        )

        if analysis_response.startswith("ERROR:"):
            log.warning("[%s] Analysis failed: %s", self.name, analysis_response)
            return

        analysis = parse_json_response(analysis_response)
        if not analysis or not isinstance(analysis, dict):
            log.debug("[%s] Could not parse analysis.", self.name)
            return

        # Step 5: Post evidence back to #hypotheses
        conclusion = analysis.get("conclusion", "INCONCLUSIVE")
        updated_confidence = float(analysis.get("updated_confidence", confidence))

        # Escalate inconclusive high-confidence investigations to Claude
        if conclusion == "INCONCLUSIVE" and updated_confidence > 0.5:
            try:
                await self.request_claude(
                    question=f"Investigate inconclusive hypothesis: {hypothesis_content[:200]}",
                    context=f"Key finding: {analysis.get('key_finding', '')}. Follow-up: {analysis.get('follow_up_question', '')}",
                )
                log.info("[%s] Escalated to Claude for deeper investigation", self.name)
            except Exception as exc:
                log.debug("[%s] Claude escalation failed: %s", self.name, exc)
        key_finding = analysis.get("key_finding", "")
        narrative = analysis.get("narrative", "")
        supporting = analysis.get("supporting_evidence", [])
        contradicting = analysis.get("contradicting_evidence", [])
        follow_up = analysis.get("follow_up_needed", False)
        follow_up_q = analysis.get("follow_up_question", "")

        content = (
            f"[EXPLORER {conclusion}] {key_finding} | "
            f"Confidence: {confidence:.0%} -> {updated_confidence:.0%}. "
            f"{narrative}"
        )

        evidence_summary = {
            "supporting": supporting[:5],
            "contradicting": contradicting[:5],
            "queries_run": len(query_results),
            "rationale": rationale,
        }

        # Update connection if confidence changed significantly
        conn_id = data.get("connection_id")
        if conn_id and abs(updated_confidence - confidence) > 0.1:
            conn = Connection(
                source_a=data.get("source_a", "explorer"),
                source_b=data.get("source_b", "evidence"),
                description=f"[EXPLORER UPDATE] {key_finding}",
                confidence=updated_confidence,
                evidence=[hyp_id] + [r.get("url", "") for r in query_results[:3]],
                metadata={
                    "original_confidence": confidence,
                    "conclusion": conclusion,
                    "narrative": narrative,
                    "supporting_evidence": supporting,
                    "contradicting_evidence": contradicting,
                    "parent_hypothesis_id": hyp_id,
                },
            )
            await self.board.store_connection(conn)

        await self.post(
            channel="#hypotheses",
            content=content,
            data={
                "type": "explorer_finding",
                "conclusion": conclusion,
                "updated_confidence": updated_confidence,
                "original_confidence": confidence,
                "key_finding": key_finding,
                "narrative": narrative,
                "evidence": evidence_summary,
                "parent_hypothesis_id": hyp_id,
                "follow_up_needed": follow_up,
                "follow_up_question": follow_up_q,
                "queries_run": len(query_results),
            },
            priority=3 if conclusion == "SUPPORTED" else 2,
            location_id=location_id,
            references=[hyp_id],
        )

        log.info(
            "[%s] Finding: %s (conf %.0f%% -> %.0f%%): %s",
            self.name, conclusion, confidence * 100, updated_confidence * 100, key_finding[:80],
        )

        # If follow-up needed, post a request
        if follow_up and follow_up_q:
            await self.post(
                channel="#requests",
                content=f"[EXPLORER FOLLOW-UP] {follow_up_q}",
                data={
                    "request_type": "explorer_follow_up",
                    "parent_hypothesis_id": hyp_id,
                    "question": follow_up_q,
                    "current_confidence": updated_confidence,
                },
                priority=2,
                ttl_hours=3,
            )

        # Post to #meta
        await self.post(
            channel="#meta",
            content=f"[EXPLORER DONE] {self.name}: {conclusion} after {len(query_results)} queries.",
            data={
                "type": "explorer_complete",
                "explorer": self.name,
                "conclusion": conclusion,
                "updated_confidence": updated_confidence,
            },
            priority=1,
            ttl_hours=2,
        )

    async def _fetch_url(
        self,
        session: aiohttp.ClientSession,
        url: str,
        description: str,
    ) -> dict[str, Any]:
        """Fetch a URL and return a structured result dict."""
        try:
            async with session.get(url) as resp:
                status = resp.status
                if status == 200:
                    ct = resp.content_type or ""
                    if "json" in ct:
                        try:
                            raw = await resp.json(content_type=None)
                            # Truncate large responses
                            raw_str = json.dumps(raw)
                            if len(raw_str) > 2000:
                                # Try to extract most useful sub-structure
                                raw = _truncate_json(raw)
                            return {"status": status, "data": raw, "error": None}
                        except Exception:
                            text = await resp.text(errors="replace")
                            return {"status": status, "data": text[:1000], "error": None}
                    else:
                        # Skip binary content types
                        if any(t in ct for t in ("image", "octet-stream", "video", "audio", "pdf")):
                            return {"status": status, "data": f"[binary: {ct}]", "error": None}
                        try:
                            text = await resp.text(errors="replace")
                        except Exception:
                            raw_bytes = await resp.read()
                            text = raw_bytes[:1000].decode("utf-8", errors="replace")
                        return {"status": status, "data": text[:1000], "error": None}
                else:
                    return {"status": status, "data": None, "error": f"HTTP {status}"}
        except aiohttp.ClientError as exc:
            log.debug("[%s] Fetch error %s: %s", self.name, url[:80], exc)
            return {"status": 0, "data": None, "error": str(exc)[:200]}
        except asyncio.TimeoutError:
            return {"status": 0, "data": None, "error": "timeout"}

    async def _post_null_finding(
        self, hyp_id: str, hypothesis_content: str, location_id: str | None
    ) -> None:
        """Post when no evidence could be gathered."""
        await self.post(
            channel="#hypotheses",
            content=(
                f"[EXPLORER NULL] Could not gather evidence for: "
                f"{hypothesis_content[:120]}. Insufficient data available."
            ),
            data={
                "type": "explorer_finding",
                "conclusion": "INCONCLUSIVE",
                "key_finding": "No data could be retrieved from planned queries.",
                "parent_hypothesis_id": hyp_id,
                "queries_run": 0,
            },
            priority=1,
            location_id=location_id,
            references=[hyp_id],
        )


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _truncate_json(data: Any, max_items: int = 5) -> Any:
    """Recursively truncate lists/dicts to avoid massive payloads."""
    if isinstance(data, dict):
        return {k: _truncate_json(v, max_items) for k, v in list(data.items())[:20]}
    elif isinstance(data, list):
        return [_truncate_json(item, max_items) for item in data[:max_items]]
    elif isinstance(data, str) and len(data) > 500:
        return data[:500] + "..."
    return data


# -- Async entry points ---------------------------------------------------------

async def run_explorer_spawner(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    # Note: ExplorerSpawner is stateful (tracks active tasks and explored hypotheses).
    # The scheduler should keep a single instance alive rather than creating a new one each cycle.
    # If called per-cycle, it won't have persistent task tracking.
    # The main.py / runner should create ONE spawner instance and call .run() each cycle.
    # For compatibility with the scheduler's simple loop model, we use a module-level singleton.
    spawner = _get_spawner_singleton(board, graph, memory)
    await spawner.run()


_spawner_singleton: ExplorerSpawner | None = None


def _get_spawner_singleton(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> ExplorerSpawner:
    global _spawner_singleton
    if _spawner_singleton is None:
        _spawner_singleton = ExplorerSpawner(board, graph, memory)
    return _spawner_singleton
