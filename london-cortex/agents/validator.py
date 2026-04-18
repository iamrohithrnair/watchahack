"""
Validator agent -- checks predictions against actual outcomes.

Runs every 30 minutes. Fetches all predictions past their deadline from the board,
attempts to determine their outcomes by examining recent observations, updates
the connection registry, and posts accuracy metrics to #meta.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL, ENDPOINTS
from ..core.epistemics import SourceTracker
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import AgentConversation
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.validator")

_VALIDATOR_SYSTEM = """\
You are a rigorous fact-checker for an urban sensor network's predictions about London.
Given a prediction and available evidence, determine whether the prediction was correct, incorrect, or inconclusive.
Be strict: mark as TRUE only if there is clear supporting evidence, FALSE if clearly wrong, INCONCLUSIVE if insufficient data.
"""

_OUTCOME_PROMPT = """\
Prediction to evaluate:
"{prediction}"

This prediction was made at {made_at} with a deadline of {deadline}.
It was made in the context of: {context}

Evidence gathered from London sensor data since the prediction was made:
{evidence}

Was this prediction correct?

Respond with JSON:
{{
  "outcome": "TRUE" | "FALSE" | "INCONCLUSIVE",
  "confidence": <0.0-1.0>,
  "explanation": "Why you determined this outcome",
  "key_evidence": "The most relevant evidence for your conclusion"
}}
"""


class ValidatorAgent(BaseAgent):
    """
    Checks pending predictions. Uses sensor data + LLM to determine outcomes.
    Updates connection registry and posts accuracy metrics to #meta.
    """

    name = "validator"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._source_tracker = SourceTracker()

    async def run(self) -> None:
        pending = await self.board.get_pending_predictions()

        if not pending:
            log.debug("[Validator] No pending predictions to check.")
            await self._post_accuracy_summary()
            return

        log.info("[Validator] Checking %d pending predictions.", len(pending))

        checked = 0
        for conn in pending:
            try:
                outcome = await self._evaluate_prediction(conn)
                if outcome is not None:
                    await self._record_outcome(conn, outcome)
                    checked += 1
            except Exception as exc:
                log.error("[Validator] Error checking prediction %s: %s", conn.get("id"), exc)

        log.info("[Validator] Evaluated %d / %d predictions.", checked, len(pending))
        await self._post_accuracy_summary()

    async def _evaluate_prediction(
        self, conn: dict[str, Any]
    ) -> dict[str, Any] | None:
        """Gather evidence and use LLM to evaluate a prediction."""
        prediction = conn.get("prediction", "")
        if not prediction:
            return None

        conn_id = conn.get("id", "")
        made_at = conn.get("timestamp", "")
        deadline = conn.get("prediction_deadline", "")
        source_a = conn.get("source_a", "")
        source_b = conn.get("source_b", "")
        description = conn.get("description", "")

        # Parse deadline for time-window queries
        deadline_dt = _parse_ts(deadline)
        if not deadline_dt:
            return None

        made_at_dt = _parse_ts(made_at)
        since_hours = 3.0
        if made_at_dt:
            elapsed = (deadline_dt - made_at_dt).total_seconds() / 3600
            since_hours = max(elapsed + 1.0, 2.0)

        # Gather evidence: anomalies, discoveries, and observations from relevant sources
        evidence_parts: list[str] = []

        # Check #anomalies for relevant recent data
        anomalies = await self.read_channel("#anomalies", since_hours=min(since_hours, 6.0), limit=100)
        relevant_anomalies = _filter_relevant(anomalies, source_a, source_b, prediction)
        if relevant_anomalies:
            evidence_parts.append("ANOMALIES:\n" + "\n".join(
                f"- {a.get('content', '')[:150]}" for a in relevant_anomalies[:10]
            ))

        # Check #discoveries
        discoveries = await self.read_channel("#discoveries", since_hours=min(since_hours, 6.0), limit=30)
        if discoveries:
            evidence_parts.append("DISCOVERIES:\n" + "\n".join(
                f"- {d.get('content', '')[:150]}" for d in discoveries[:5]
            ))

        # Try to fetch fresh data from relevant APIs for key prediction types
        fresh_evidence = await self._fetch_fresh_evidence(prediction, source_a, source_b)
        if fresh_evidence:
            evidence_parts.append(f"LIVE DATA:\n{fresh_evidence}")

        if not evidence_parts:
            return {"outcome": "INCONCLUSIVE", "confidence": 0.0, "explanation": "No evidence gathered.", "key_evidence": ""}

        evidence_text = "\n\n".join(evidence_parts)[:3000]

        prompt = _OUTCOME_PROMPT.format(
            prediction=prediction,
            made_at=made_at,
            deadline=deadline,
            context=description[:300],
            evidence=evidence_text,
        )

        response_text = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            trigger=f"validate:{conn_id[:8]}",
            system_instruction=_VALIDATOR_SYSTEM,
        )

        if response_text.startswith("ERROR:"):
            log.warning("[Validator] LLM error for %s: %s", conn_id, response_text)
            return None

        parsed = parse_json_response(response_text)
        if not parsed or not isinstance(parsed, dict):
            return None

        return parsed

    async def _fetch_fresh_evidence(
        self, prediction: str, source_a: str, source_b: str
    ) -> str:
        """Try to fetch live data relevant to the prediction type."""
        pred_lower = prediction.lower()
        evidence_lines: list[str] = []

        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10)) as session:
                # If prediction involves air quality
                if any(kw in pred_lower for kw in ["air quality", "pollution", "no2", "pm2"]):
                    try:
                        url = f"{ENDPOINTS['laqn']}/Data/Wide/Site/SiteCode=MY1/StartDate={_today()}/EndDate={_today()}/Json"
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                evidence_lines.append(f"Current LAQN data: {json.dumps(data)[:500]}")
                    except Exception as e:
                        log.debug("[Validator] LAQN fetch error: %s", e)

                # If prediction involves weather/flooding
                if any(kw in pred_lower for kw in ["flood", "rain", "weather", "river"]):
                    try:
                        url = f"{ENDPOINTS['environment_agency']}/id/3-day-flood-forecasts"
                        async with session.get(url, headers={"Accept": "application/json"}) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                items = data.get("items", [])[:3]
                                evidence_lines.append(f"Flood forecasts: {json.dumps(items)[:400]}")
                    except Exception as e:
                        log.debug("[Validator] Environment Agency fetch error: %s", e)

                # If prediction involves energy / carbon
                if any(kw in pred_lower for kw in ["energy", "carbon", "electricity", "grid"]):
                    try:
                        url = f"{ENDPOINTS['carbon_intensity']}/v2/intensity"
                        async with session.get(url) as resp:
                            if resp.status == 200:
                                data = await resp.json()
                                evidence_lines.append(f"Current carbon intensity: {json.dumps(data)[:400]}")
                    except Exception as e:
                        log.debug("[Validator] Carbon intensity fetch error: %s", e)

        except Exception as exc:
            log.debug("[Validator] Fresh evidence fetch failed: %s", exc)

        return "\n".join(evidence_lines) if evidence_lines else ""

    async def _record_outcome(
        self, conn: dict[str, Any], outcome: dict[str, Any]
    ) -> None:
        """Record the outcome in DB and memory, update registry."""
        conn_id = conn.get("id", "")
        source_a = conn.get("source_a", "")
        source_b = conn.get("source_b", "")
        outcome_str = outcome.get("outcome", "INCONCLUSIVE")
        confidence = float(outcome.get("confidence", 0.0))
        explanation = outcome.get("explanation", "")
        key_evidence = outcome.get("key_evidence", "")

        # Determine boolean outcome
        if outcome_str == "TRUE":
            hit = True
        elif outcome_str == "FALSE":
            hit = False
        else:
            # INCONCLUSIVE -- don't update registry
            log.debug("[Validator] Prediction %s inconclusive.", conn_id)
            self.memory.log_backtesting_result({
                "connection_id": conn_id,
                "prediction": conn.get("prediction", ""),
                "outcome": None,
                "confidence": confidence,
                "explanation": explanation,
            })
            return

        # Update connection registry and source reliability
        self.memory.update_connection_registry(source_a, source_b, hit)
        self._source_tracker.update(source_a, hit)
        self._source_tracker.update(source_b, hit)

        # Log backtesting result
        self.memory.log_backtesting_result({
            "connection_id": conn_id,
            "prediction": conn.get("prediction", ""),
            "outcome": hit,
            "outcome_label": outcome_str,
            "confidence": confidence,
            "explanation": explanation,
            "key_evidence": key_evidence,
            "source_a": source_a,
            "source_b": source_b,
        })

        # Update the connections table -- mark prediction outcome
        if self.board._db:
            try:
                outcome_int = 1 if hit else 0
                await self.board._db.execute(
                    "UPDATE connections SET prediction_outcome = ? WHERE id = ?",
                    (outcome_int, conn_id),
                )
                await self.board._db.commit()
            except Exception as exc:
                log.warning("[Validator] Could not update DB outcome for %s: %s", conn_id, exc)

        # Post outcome to #meta
        emoji = "CORRECT" if hit else "WRONG"
        await self.post(
            channel="#meta",
            content=(
                f"[VALIDATOR] Prediction {emoji}: '{conn.get('prediction', '')[:100]}' "
                f"(confidence={confidence:.0%}). {explanation[:150]}"
            ),
            data={
                "type": "prediction_outcome",
                "connection_id": conn_id,
                "outcome": outcome_str,
                "hit": hit,
                "confidence": confidence,
                "source_a": source_a,
                "source_b": source_b,
                "explanation": explanation,
                "key_evidence": key_evidence,
            },
            priority=2,
            ttl_hours=24,
        )
        log.info(
            "[Validator] Prediction %s: %s (conf=%.2f) -- %s -> %s",
            conn_id[:8], outcome_str, confidence, source_a, source_b,
        )

    async def _post_accuracy_summary(self) -> None:
        """Post a summary of system-wide prediction accuracy."""
        registry = self.memory.load_json("connection_registry.json")
        if not isinstance(registry, dict) or not registry:
            return

        total_hits = sum(v.get("hits", 0) for v in registry.values())
        total_misses = sum(v.get("misses", 0) for v in registry.values())
        total = total_hits + total_misses

        if total == 0:
            return

        accuracy = total_hits / total

        # Find best and worst source pairs
        pairs_with_rate = []
        for key, stats in registry.items():
            t = stats.get("total", 0)
            if t >= 3:
                rate = stats.get("hits", 0) / t
                pairs_with_rate.append((key, rate, t))

        pairs_with_rate.sort(key=lambda x: x[1], reverse=True)
        best = pairs_with_rate[:3] if pairs_with_rate else []
        worst = pairs_with_rate[-3:] if len(pairs_with_rate) >= 3 else []

        # Load backtesting for recent accuracy
        backtesting = self.memory.load_json("backtesting_results.json")
        if isinstance(backtesting, list):
            recent = [r for r in backtesting[-50:] if r.get("outcome") is not None]
            recent_accuracy = sum(1 for r in recent if r.get("outcome")) / len(recent) if recent else 0.0
        else:
            recent_accuracy = 0.0

        content = (
            f"[VALIDATOR METRICS] Overall accuracy: {accuracy:.1%} ({total_hits}/{total} predictions). "
            f"Recent 50: {recent_accuracy:.1%}. "
            f"Best pairs: {[f'{p[0]} ({p[1]:.0%})' for p in best[:2]]}. "
            f"Worst: {[f'{p[0]} ({p[1]:.0%})' for p in worst[:2]]}."
        )

        await self.post(
            channel="#meta",
            content=content,
            data={
                "type": "accuracy_metrics",
                "overall_accuracy": accuracy,
                "recent_accuracy": recent_accuracy,
                "total_predictions": total,
                "total_hits": total_hits,
                "best_pairs": [{"pair": p[0], "accuracy": p[1], "count": p[2]} for p in best],
                "worst_pairs": [{"pair": p[0], "accuracy": p[1], "count": p[2]} for p in worst],
            },
            priority=1,
            ttl_hours=6,
        )
        log.info("[Validator] Accuracy summary: %.1f%% over %d predictions.", accuracy * 100, total)


# -------------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------------

def _parse_ts(ts_str: str | None) -> datetime | None:
    if not ts_str:
        return None
    try:
        dt = datetime.fromisoformat(ts_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except (ValueError, TypeError):
        return None


def _today() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


def _filter_relevant(
    messages: list[dict[str, Any]],
    source_a: str,
    source_b: str,
    prediction: str,
) -> list[dict[str, Any]]:
    """Filter messages for relevance to a prediction."""
    relevant: list[dict[str, Any]] = []
    pred_words = set(prediction.lower().split())
    for m in messages:
        content = m.get("content", "").lower()
        source = m.get("data", {}).get("source", "")
        source_match = source_a in source or source_b in source
        # Keyword overlap
        content_words = set(content.split())
        overlap = len(pred_words & content_words)
        if source_match or overlap >= 3:
            relevant.append(m)
    return relevant


# -- Async entry point ----------------------------------------------------------

async def run_validator(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = ValidatorAgent(board, graph, memory)
    await agent.run()
