"""
Connector agents -- find relationships between anomalies.

SpatialConnector     -- co-located anomalies from different sources -> hypothesis
NarrativeConnector   -- LLM reasoning over anomaly clusters
StatisticalConnector -- full cross-correlation analysis (hourly)
CausalChainConnector -- forward-chaining causal template matching
"""

from __future__ import annotations

import itertools
import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL, LLM_PRO_MODEL
from ..core.epistemics import ConfidenceCalculator
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import Connection, AgentMessage
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.connectors")


# -------------------------------------------------------------------------
# SpatialConnector
# -------------------------------------------------------------------------

class SpatialConnector(BaseAgent):
    """
    Watches #anomalies. When 2+ anomalies from DIFFERENT sources appear in the
    same or adjacent grid cells within 2 hours, posts a spatial co-location
    hypothesis to #hypotheses.
    No LLM needed -- pure spatial + temporal logic.
    """

    name = "spatial_connector"
    _COLOCATION_WINDOW_HOURS = 2.0
    _MIN_DIFFERENT_SOURCES = 2

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._posted_pairs: set[frozenset[str]] = set()  # avoid re-posting same pairs

    async def run(self) -> None:
        # Check for pending investigations assigned to me
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                await self._handle_spatial_investigation(inv)
        except Exception as exc:
            log.debug("[SpatialConnector] Investigation check failed: %s", exc)

        # Prune expired anomalies from spatial index
        pruned = self.graph.prune_expired_anomalies()
        if pruned:
            log.debug("[SpatialConnector] Pruned %d expired anomalies from spatial index.", pruned)

        # Fetch active anomalies from DB
        active = await self.board.get_active_anomalies(since_hours=self._COLOCATION_WINDOW_HOURS)

        # Also read #observations for co-located normal readings
        obs_messages = await self.read_channel("#observations", since_hours=2.0, limit=200)
        for obs in obs_messages:
            data = obs.get("data", {})
            loc = obs.get("location_id") or data.get("location_id")
            if loc and data.get("source"):
                active.append({
                    "id": obs.get("id", ""),
                    "source": data["source"],
                    "description": obs.get("content", "")[:200],
                    "location_id": loc,
                    "timestamp": obs.get("timestamp", ""),
                    "metadata": data,
                    "_is_observation": True,
                })

        if len(active) < 2:
            return

        # Deduplicate: keep only the most recent item per (source, location_id)
        # to avoid combinatorial explosion when many observations share a cell
        best: dict[tuple[str, str], dict[str, Any]] = {}
        for a in active:
            key = (a.get("source", ""), a.get("location_id", ""))
            existing = best.get(key)
            if existing is None:
                best[key] = a
            else:
                # Keep the one with the later timestamp
                t_new = _parse_ts(a.get("timestamp", ""))
                t_old = _parse_ts(existing.get("timestamp", ""))
                if t_new and (not t_old or t_new > t_old):
                    best[key] = a
        active = list(best.values())

        # Group by location_id
        by_location: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for a in active:
            loc = a.get("location_id")
            if loc:
                by_location[loc].append(a)

        # Flag cells with 3+ active sources as "rich data points" for DiscoveryEngine
        for cell_id, cell_items in by_location.items():
            unique_sources = {item.get("source", "") for item in cell_items}
            if len(unique_sources) >= 3:
                await self.post(
                    channel="#observations",
                    content=f"[RICH] Cell {cell_id} has {len(unique_sources)} active sources: {', '.join(sorted(unique_sources)[:5])}",
                    data={
                        "source": "spatial_connector",
                        "obs_type": "rich_cell",
                        "cell_id": cell_id,
                        "source_count": len(unique_sources),
                        "sources": sorted(unique_sources),
                    },
                    priority=1,
                    location_id=cell_id,
                    ttl_hours=2,
                )

        # For each cell, collect anomalies from cell + neighbours
        checked_sets: set[frozenset[str]] = set()

        for cell_id, cell_anomalies in by_location.items():
            # Get neighbouring cells too
            neighbour_ids = self.graph.get_neighbors(cell_id)
            extended_anomalies = list(cell_anomalies)
            for nid in neighbour_ids:
                extended_anomalies.extend(by_location.get(nid, []))

            if len(extended_anomalies) < 2:
                continue

            # Find pairs from different sources
            for a1, a2 in itertools.combinations(extended_anomalies, 2):
                src1 = a1.get("source", "")
                src2 = a2.get("source", "")
                if src1 == src2:
                    continue

                # Check time window
                t1 = _parse_ts(a1.get("timestamp", ""))
                t2 = _parse_ts(a2.get("timestamp", ""))
                if t1 and t2:
                    if abs((t1 - t2).total_seconds()) > self._COLOCATION_WINDOW_HOURS * 3600:
                        continue

                pair_key = frozenset([a1.get("id", ""), a2.get("id", "")])
                if pair_key in checked_sets or pair_key in self._posted_pairs:
                    continue
                checked_sets.add(pair_key)

                # Build hypothesis
                desc1 = a1.get("description", "anomaly")[:120]
                desc2 = a2.get("description", "anomaly")[:120]
                loc_label = cell_id
                borough = ""
                if cell_id in self.graph.cells:
                    borough = self.graph.cells[cell_id].borough or ""

                hypothesis = (
                    f"[SPATIAL] Co-located anomalies in {loc_label}"
                    f"{' (' + borough + ')' if borough else ''}: "
                    f"({src1}) {desc1}  +  ({src2}) {desc2}"
                )

                # If both anomalies come from very similar sources, reduce confidence
                same_domain = src1.split("_")[0] == src2.split("_")[0]
                base_confidence = 0.15 if same_domain else 0.4

                conn = Connection(
                    source_a=src1,
                    source_b=src2,
                    description=hypothesis,
                    confidence=base_confidence,
                    evidence=[a1.get("id", ""), a2.get("id", "")],
                    location_id=cell_id,
                    metadata={
                        "anomaly_1": a1.get("id"),
                        "anomaly_2": a2.get("id"),
                        "desc_1": desc1,
                        "desc_2": desc2,
                        "cell_id": cell_id,
                        "borough": borough,
                    },
                )
                await self.board.store_connection(conn)

                await self.post(
                    channel="#hypotheses",
                    content=hypothesis,
                    data={
                        "connection_id": conn.id,
                        "type": "spatial_colocation",
                        "source_a": src1,
                        "source_b": src2,
                        "confidence": conn.confidence,
                        "anomaly_ids": [a1.get("id"), a2.get("id")],
                    },
                    priority=2,
                    location_id=cell_id,
                    references=[a1.get("id", ""), a2.get("id", "")],
                )
                self._posted_pairs.add(pair_key)
                # Bound memory
                if len(self._posted_pairs) > 2000:
                    self._posted_pairs = set(list(self._posted_pairs)[-1000:])

                log.info(
                    "[SpatialConnector] Hypothesis: %s + %s at %s",
                    src1, src2, cell_id,
                )

    async def _handle_spatial_investigation(self, inv_thread: list[dict]) -> None:
        """Handle an investigation thread assigned to spatial_connector."""
        if not inv_thread:
            return
        last_step = inv_thread[-1]
        thread_id = last_step.get("thread_id", "")
        question = last_step.get("next_question", "")
        if not question:
            return

        # Look for nearby cameras/data based on location context
        data = last_step.get("data", {})
        if isinstance(data, str):
            import json as _json
            try:
                data = _json.loads(data)
            except Exception:
                data = {}

        location_id = data.get("location_id", "")
        if not location_id:
            # Try to extract from the question/thread
            for step in inv_thread:
                step_data = step.get("data", {})
                if isinstance(step_data, str):
                    try:
                        step_data = json.loads(step_data)
                    except Exception:
                        step_data = {}
                if step_data.get("location_id"):
                    location_id = step_data["location_id"]
                    break

        if location_id:
            neighbor_ids = self.graph.get_neighbors(location_id)
            all_cells = [location_id] + list(neighbor_ids)
            nearby_anomalies = []
            for cell in all_cells[:5]:
                msgs = await self.board.read_location(cell, since_hours=6.0, limit=10)
                nearby_anomalies.extend(msgs)

            finding = f"Found {len(nearby_anomalies)} messages in {len(all_cells)} cells near {location_id}"
            cameras = [m for m in nearby_anomalies if "camera" in m.get("content", "").lower() or "jamcam" in m.get("from_agent", "").lower()]
            if cameras:
                finding += f" including {len(cameras)} camera observations"

            await self.continue_investigation(
                thread_id=thread_id,
                action=f"Spatial search near {location_id}",
                finding=finding,
                next_question=f"Analyze nearby camera images to corroborate: {question}" if cameras else None,
                next_agent="vision_interpreter" if cameras else None,
                data={"cells_checked": all_cells[:5], "anomalies_found": len(nearby_anomalies)},
            )
        else:
            await self.continue_investigation(
                thread_id=thread_id,
                action="Spatial search (no location available)",
                finding="Could not determine location for spatial analysis",
                next_question=None,
                next_agent=None,
            )

        log.info("[SpatialConnector] Handled investigation thread %s", thread_id[:8])


# -------------------------------------------------------------------------
# NarrativeConnector
# -------------------------------------------------------------------------

_NARRATIVE_SYSTEM = """\
You are a sophisticated urban intelligence analyst for Cortex, a real-time city monitoring system.
Your job is to look at clusters of anomalies from different sensors and construct the most
plausible narrative that explains them together. You think like a detective, combining
street-level knowledge of the city with analytical rigor.
"""

_NARRATIVE_PROMPT = """\
Cortex's sensor network has detected the following co-located anomalies. Your task is to:
1. Determine what single underlying event or chain of events could explain ALL of these observations together
2. Rate your confidence (0.0-1.0) that they are genuinely connected
3. Make ONE specific, testable prediction about what will be observed in the NEXT 2-6 hours if this hypothesis is correct

Anomaly cluster:
{anomaly_summary}

Recent related board context:
{context_summary}

Respond with JSON:
{{
  "narrative": "The full story connecting these anomalies in 2-3 sentences",
  "root_cause": "The most likely single cause or event",
  "confidence": <0.0-1.0>,
  "supporting_factors": ["list of specific evidence points"],
  "alternative_explanations": ["other possible explanations"],
  "prediction": "Specific, falsifiable prediction about what will happen next",
  "prediction_horizon_hours": <2-24>,
  "severity_assessment": <1-5>,
  "recommended_actions": ["what to monitor next"]
}}
"""


class NarrativeConnector(BaseAgent):
    """
    Reads #hypotheses (spatial clusters) + #anomalies.
    Uses LLM to construct causal narratives and make predictions.
    Budget: max 10 Pro calls per hour.
    """

    name = "narrative_connector"
    _MAX_PRO_PER_HOUR = 10

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._processed_hypotheses: set[str] = set()

    async def run(self) -> None:
        # Check for pending investigations assigned to me
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                await self._handle_investigation(inv)
        except Exception as exc:
            log.debug("[NarrativeConnector] Investigation check failed: %s", exc)

        remaining = self.pro_calls_remaining_this_hour(self._MAX_PRO_PER_HOUR)

        # Get spatial hypotheses posted recently
        hypotheses = await self.read_channel("#hypotheses", since_hours=0.5, limit=30)
        spatial = [
            h for h in hypotheses
            if isinstance(h.get("data"), dict)
            and h["data"].get("type") == "spatial_colocation"
            and h.get("id", "") not in self._processed_hypotheses
        ]

        if not spatial:
            return

        # Process highest-priority first, up to budget
        spatial.sort(key=lambda h: h.get("priority", 1), reverse=True)

        for hypothesis_msg in spatial[:remaining]:
            hyp_id = hypothesis_msg.get("id", "")
            data = hypothesis_msg.get("data", {})
            anomaly_ids: list[str] = data.get("anomaly_ids", [])
            location_id = hypothesis_msg.get("location_id")

            # Fetch the actual anomaly descriptions from DB
            anomaly_details: list[str] = []
            for aid in anomaly_ids:
                # Try to get from board by looking in recent anomaly messages
                recent_anom = await self.read_channel("#anomalies", since_hours=3.0, limit=200)
                for am in recent_anom:
                    am_data = am.get("data") if isinstance(am.get("data"), dict) else {}
                    if am_data.get("anomaly_id") == aid or am.get("id") == aid:
                        anomaly_details.append(
                            f"- [{am_data.get('source', '?')}] {am.get('content', '')[:200]}"
                        )
                        break

            if not anomaly_details:
                anomaly_details = [hypothesis_msg.get("content", "Unknown anomaly cluster")]

            # Get broader context from #anomalies
            recent_anomalies = await self.read_channel("#anomalies", since_hours=2.0, limit=50)
            context_lines: list[str] = []
            if location_id:
                for am in recent_anomalies:
                    if am.get("location_id") == location_id:
                        context_lines.append(f"- {am.get('content', '')[:150]}")
            context_summary = "\n".join(context_lines[:10]) or "No additional context."

            anomaly_summary = "\n".join(anomaly_details) or hypothesis_msg.get("content", "")

            prompt = _NARRATIVE_PROMPT.format(
                anomaly_summary=anomaly_summary,
                context_summary=context_summary,
            )

            response_text = await self.call_llm(
                prompt=prompt,
                model=LLM_PRO_MODEL,
                trigger=f"narrative:{hyp_id[:8]}",
                system_instruction=_NARRATIVE_SYSTEM,
            )
            self.record_pro_call()
            self._processed_hypotheses.add(hyp_id)

            if response_text.startswith("ERROR:"):
                log.warning("[NarrativeConnector] LLM call error: %s", response_text)
                continue

            parsed = parse_json_response(response_text)
            if not parsed or not isinstance(parsed, dict):
                log.debug("[NarrativeConnector] Could not parse response.")
                continue

            narrative = parsed.get("narrative", "")
            root_cause = parsed.get("root_cause", "")
            raw_confidence = float(parsed.get("confidence", 0.5))
            prediction = parsed.get("prediction", "")
            horizon_h = int(parsed.get("prediction_horizon_hours", 6))
            severity = int(parsed.get("severity_assessment", 2))

            # Apply narrated-layer decay to LLM-returned confidence
            _calc = ConfidenceCalculator()
            confidence = _calc.compute_confidence(
                raw_confidence, layer="narrated", n_sources=2,
            )

            if not narrative:
                continue

            # Store connection with prediction
            conn = Connection(
                source_a=data.get("source_a", "multi"),
                source_b=data.get("source_b", "multi"),
                description=f"[NARRATIVE] {narrative}",
                confidence=confidence,
                evidence=anomaly_ids,
                prediction=prediction,
                prediction_deadline=(
                    datetime.now(timezone.utc) + timedelta(hours=horizon_h)
                    if prediction else None
                ),
                location_id=location_id,
                metadata={
                    "root_cause": root_cause,
                    "supporting_factors": parsed.get("supporting_factors", []),
                    "alternative_explanations": parsed.get("alternative_explanations", []),
                    "recommended_actions": parsed.get("recommended_actions", []),
                    "parent_hypothesis_id": hyp_id,
                    "severity_assessment": severity,
                },
            )
            await self.board.store_connection(conn)

            content = (
                f"[NARRATIVE conf={confidence:.2f}] {narrative} "
                f"Root cause: {root_cause}. "
                f"Prediction: {prediction}"
            )

            await self.post(
                channel="#hypotheses",
                content=content,
                data={
                    "connection_id": conn.id,
                    "type": "narrative_hypothesis",
                    "confidence": confidence,
                    "root_cause": root_cause,
                    "narrative": narrative,
                    "prediction": prediction,
                    "prediction_horizon_hours": horizon_h,
                    "severity": severity,
                    "supporting_factors": parsed.get("supporting_factors", []),
                    "recommended_actions": parsed.get("recommended_actions", []),
                },
                priority=min(severity, 5),
                location_id=location_id,
                references=anomaly_ids,
            )
            log.info(
                "[NarrativeConnector] Hypothesis conf=%.2f: %s",
                confidence, narrative[:100],
            )

    async def _handle_investigation(self, inv_thread: list[dict]) -> None:
        """Handle an investigation thread assigned to narrative_connector."""
        if not inv_thread:
            return
        last_step = inv_thread[-1]
        thread_id = last_step.get("thread_id", "")
        question = last_step.get("next_question", "")
        if not question:
            return

        # Gather context
        context = await self.read_channel("#anomalies", since_hours=2.0, limit=30)
        context_text = self._summarise_messages(context, max_chars=1000)

        prompt = (
            f"Investigation question: {question}\n\n"
            f"Previous findings in this thread:\n"
            + "\n".join(f"- {s.get('agent_name', '?')}: {s.get('finding', 'N/A')}" for s in inv_thread)
            + f"\n\nRecent anomalies:\n{context_text}\n\n"
            f"Provide your narrative analysis. Respond with JSON:\n"
            f'{{"finding": "...", "next_question": "..." or null, "next_agent": "..." or null}}'
        )

        response = await self.call_llm(
            prompt=prompt, model=LLM_PRO_MODEL,
            trigger=f"narrative:inv:{thread_id[:8]}",
            system_instruction=_NARRATIVE_SYSTEM,
        )
        self.record_pro_call()
        if response.startswith("ERROR:"):
            return

        parsed = parse_json_response(response)
        if not parsed:
            return

        await self.continue_investigation(
            thread_id=thread_id,
            action=f"Narrative analysis of: {question[:80]}",
            finding=parsed.get("finding", ""),
            next_question=parsed.get("next_question"),
            next_agent=parsed.get("next_agent"),
        )
        log.info("[NarrativeConnector] Investigation step added for thread %s", thread_id[:8])


# -------------------------------------------------------------------------
# StatisticalConnector
# -------------------------------------------------------------------------

try:
    from scipy import stats as scipy_stats
    _SCIPY_AVAILABLE = True
except ImportError:
    _SCIPY_AVAILABLE = False
    log.warning("scipy not available -- StatisticalConnector will use fallback correlation.")


def _pearsonr_fallback(x: list[float], y: list[float]) -> tuple[float, float]:
    """Pure-Python Pearson correlation when scipy is unavailable."""
    n = len(x)
    if n < 3:
        return 0.0, 1.0
    mx = sum(x) / n
    my = sum(y) / n
    num = sum((xi - mx) * (yi - my) for xi, yi in zip(x, y))
    dx = math.sqrt(sum((xi - mx) ** 2 for xi in x))
    dy = math.sqrt(sum((yi - my) ** 2 for yi in y))
    if dx == 0 or dy == 0:
        return 0.0, 1.0
    r = num / (dx * dy)
    r = max(-1.0, min(1.0, r))
    # Approximate p-value via t-distribution
    if abs(r) >= 1.0:
        return r, 0.0
    t = r * math.sqrt((n - 2) / (1 - r ** 2))
    # Very rough two-tailed p-value
    p = 2 * (1 - min(0.9999, abs(t) / (abs(t) + n - 2)))
    return r, p


def _compute_correlation(x: list[float], y: list[float]) -> tuple[float, float]:
    # Skip constant arrays -- correlation is undefined
    if len(set(x)) <= 1 or len(set(y)) <= 1:
        return 0.0, 1.0
    if _SCIPY_AVAILABLE and len(x) >= 5:
        try:
            r, p = scipy_stats.pearsonr(x, y)
            return float(r), float(p)
        except Exception:
            pass
    return _pearsonr_fallback(x, y)


class StatisticalConnector(BaseAgent):
    """
    Runs hourly. Pulls all recent numeric observations and cross-correlates them
    at multiple lags. Posts surprising correlations (after Bonferroni correction)
    to #hypotheses and requests NarrativeConnector to explain them.
    """

    name = "statistical_connector"
    _LAGS_HOURS = [0, 1, 6, 24]
    _MIN_OBSERVATIONS = 3
    _BONFERRONI_ALPHA = 0.05  # relaxed from 0.01 to catch more connections

    async def run(self) -> None:
        # Gather numeric data from all sources in the last 24 hours
        sources = [
            "laqn", "open_meteo", "carbon_intensity", "environment_agency",
            "financial_stocks", "financial_crypto", "polymarket",

            "social_sentiment", "police_crimes", "tfl_cycle_hire",
            "tfl_crowding", "sensor_community", "grid_generation",
            "grid_demand", "tomtom_traffic",
        ]

        # Build time-series: {source_metric: [(timestamp, value), ...]}
        series: dict[str, list[tuple[datetime, float]]] = defaultdict(list)

        for source in sources:
            try:
                obs_rows = await self.board.get_observations(source, since_hours=25.0, limit=500)
            except Exception as exc:
                log.debug("[StatisticalConnector] Could not fetch %s: %s", source, exc)
                continue

            for obs in obs_rows:
                ts = _parse_ts(obs.get("timestamp", ""))
                if not ts:
                    continue
                # Extract numeric values from the observation
                raw_val = obs.get("value")
                metric = obs.get("metadata", {}).get("metric", "value") if isinstance(obs.get("metadata"), dict) else "value"
                try:
                    if isinstance(raw_val, str):
                        raw_val = json.loads(raw_val)
                    if isinstance(raw_val, dict):
                        for k, v in raw_val.items():
                            try:
                                series[f"{source}.{k}"].append((ts, float(v)))
                            except (TypeError, ValueError):
                                pass
                    else:
                        series[f"{source}.{metric}"].append((ts, float(raw_val)))
                except (TypeError, ValueError, json.JSONDecodeError):
                    pass

        if len(series) < 2:
            log.debug("[StatisticalConnector] Not enough series to correlate.")
            return

        # Convert to hourly buckets for correlation
        series_hourly = _bucket_to_hourly(series)

        metric_keys = [k for k, v in series_hourly.items() if len(v) >= self._MIN_OBSERVATIONS]

        if len(metric_keys) < 2:
            return

        # Number of tests for Bonferroni correction
        n_pairs = len(metric_keys) * (len(metric_keys) - 1) // 2 * len(self._LAGS_HOURS)
        alpha_corrected = self._BONFERRONI_ALPHA / max(n_pairs, 1)

        discoveries: list[dict[str, Any]] = []

        for key_a, key_b in itertools.combinations(metric_keys, 2):
            for lag_h in self._LAGS_HOURS:
                x_vals, y_vals = _align_series_with_lag(
                    series_hourly[key_a], series_hourly[key_b], lag_h
                )
                if len(x_vals) < self._MIN_OBSERVATIONS:
                    continue

                r, p = _compute_correlation(x_vals, y_vals)

                if p < alpha_corrected and abs(r) >= 0.5:
                    discoveries.append({
                        "source_a": key_a,
                        "source_b": key_b,
                        "lag_hours": lag_h,
                        "correlation": r,
                        "p_value": p,
                        "n": len(x_vals),
                    })

        if not discoveries:
            log.debug("[StatisticalConnector] No significant correlations found this hour.")
            return

        # Sort by absolute correlation strength
        discoveries.sort(key=lambda d: abs(d["correlation"]), reverse=True)

        for disc in discoveries[:10]:  # top 10 to avoid spam
            key_a = disc["source_a"]
            key_b = disc["source_b"]
            lag_h = disc["lag_hours"]
            r = disc["correlation"]
            p = disc["p_value"]

            direction = "positive" if r > 0 else "negative"
            lag_str = f"at lag {lag_h}h" if lag_h > 0 else "simultaneously"
            description = (
                f"Significant {direction} correlation (r={r:.3f}, p={p:.4f}) "
                f"between {key_a} and {key_b} {lag_str} (n={disc['n']})"
            )

            conn = Connection(
                source_a=key_a.split(".")[0],
                source_b=key_b.split(".")[0],
                description=description,
                confidence=_r_to_confidence(r),
                evidence=[],
                metadata=disc,
            )
            await self.board.store_connection(conn)

            # Save to long-term memory
            self.memory.add_learned_correlation({
                "source_a": key_a.split(".")[0],
                "source_b": key_b.split(".")[0],
                "metric_a": key_a,
                "metric_b": key_b,
                "correlation": r,
                "lag_hours": lag_h,
                "p_value": p,
                "sample_size": disc["n"],
            })

            conn_msg_id = await self.post(
                channel="#hypotheses",
                content=description,
                data={
                    "connection_id": conn.id,
                    "type": "statistical_correlation",
                    "source_a": key_a,
                    "source_b": key_b,
                    "lag_hours": lag_h,
                    "correlation": r,
                    "p_value": p,
                    "confidence": conn.confidence,
                    "n": disc["n"],
                },
                priority=2,
            )

            # Request NarrativeConnector to explain this correlation
            await self.post(
                channel="#requests",
                content=(
                    f"Please explain this statistical correlation: {description}. "
                    f"What mechanism could cause {key_a} to be correlated with {key_b}?"
                ),
                data={
                    "request_type": "explain_correlation",
                    "correlation_data": disc,
                    "hypothesis_msg_id": conn_msg_id,
                },
                to_agent="narrative_connector",
                priority=2,
            )
            log.info("[StatisticalConnector] Correlation: %s", description[:100])


# -------------------------------------------------------------------------
# CausalChainConnector
# -------------------------------------------------------------------------

# Built-in causal templates
# Each template: name, ordered steps (source patterns to watch for)
# When step 1 is detected, watch for steps 2+
_CAUSAL_TEMPLATES = [
    {
        "name": "protest_cascade",
        "description": "Protest -> traffic disruption -> economic impact",
        "steps": [
            {"sources": ["gdelt", "news"], "keywords": ["protest", "demonstration", "march"]},
            {"sources": ["tfl_jamcam"], "keywords": ["congestion", "stationary", "blocked"]},
            {"sources": ["financial_stocks"], "keywords": ["retail", "transport"]},
        ],
        "typical_lag_hours": [0, 2, 24],
    },
    {
        "name": "weather_impact",
        "description": "Extreme weather -> traffic disruption -> crime drop / hospital spike",
        "steps": [
            {"sources": ["open_meteo"], "keywords": ["extreme", "storm", "flood", "snow", "heat"]},
            {"sources": ["tfl_jamcam", "laqn"], "keywords": ["unusual", "anomaly"]},
            {"sources": ["environment_agency"], "keywords": ["flood", "warning"]},
        ],
        "typical_lag_hours": [0, 1, 3],
    },
    {
        "name": "major_event",
        "description": "Large event (concert/match) -> crowd -> transport congestion -> pollution spike",
        "steps": [
            {"sources": ["gdelt"], "keywords": ["concert", "match", "final", "ceremony", "festival"]},
            {"sources": ["tfl_jamcam"], "keywords": ["crowd", "pedestrian", "unusual"]},
            {"sources": ["laqn", "carbon_intensity"], "keywords": ["high", "anomaly"]},
        ],
        "typical_lag_hours": [0, 1, 2],
    },
    {
        "name": "financial_shock",
        "description": "Market shock -> news spike -> sentiment drop",
        "steps": [
            {"sources": ["financial_stocks", "financial_crypto", "polymarket"], "keywords": ["crash", "spike", "unusual"]},
            {"sources": ["gdelt", "news"], "keywords": ["economy", "market", "financial"]},
            {"sources": ["gdelt", "social_sentiment"], "keywords": []},
        ],
        "typical_lag_hours": [0, 0.5, 2],
    },
    {
        "name": "air_quality_event",
        "description": "Air quality degradation -> health advisory -> transport behaviour change",
        "steps": [
            {"sources": ["laqn"], "keywords": ["high", "pollution", "NO2", "PM25"]},
            {"sources": ["gdelt", "news"], "keywords": ["air quality", "pollution", "health"]},
            {"sources": ["tfl_jamcam"], "keywords": ["unusual", "reduced"]},
        ],
        "typical_lag_hours": [0, 2, 6],
    },
]


class CausalChainConnector(BaseAgent):
    """
    Maintains causal templates. When step 1 of a known chain is detected in
    #anomalies, proactively posts to #requests to investigate steps 2 and 3.
    Also posts evidence of completed chain detections to #hypotheses.
    """

    name = "causal_chain_connector"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._active_chains: dict[str, dict[str, Any]] = {}  # chain_instance_id -> state
        # Load custom learned templates from memory
        self._templates = list(_CAUSAL_TEMPLATES)

    async def run(self) -> None:
        # Load any dynamically learned templates
        learned = self.memory.load_json("causal_templates.json")
        if isinstance(learned, list):
            # Merge without duplicates
            learned_names = {t["name"] for t in self._templates}
            for t in learned:
                if t.get("name") not in learned_names:
                    self._templates.append(t)

        anomalies = await self.read_channel("#anomalies", since_hours=1.0, limit=100)

        for anomaly_msg in anomalies:
            data = anomaly_msg.get("data", {})
            source = data.get("source", "")
            content = anomaly_msg.get("content", "").lower()

            for template in self._templates:
                steps = template.get("steps", [])
                if not steps:
                    continue

                step0 = steps[0]
                step0_sources = step0.get("sources", [])
                step0_keywords = step0.get("keywords", [])

                # Check if this anomaly matches step 1
                source_match = any(s in source for s in step0_sources)
                keyword_match = not step0_keywords or any(kw in content for kw in step0_keywords)

                if source_match and keyword_match:
                    # Trigger chain investigation
                    chain_id = f"{template['name']}:{anomaly_msg.get('id', '')}:{_now_ts()}"
                    self._active_chains[chain_id] = {
                        "template": template,
                        "trigger_anomaly": anomaly_msg,
                        "started_at": datetime.now(timezone.utc).isoformat(),
                        "step": 0,
                    }

                    await self.post(
                        channel="#requests",
                        content=(
                            f"[CAUSAL] Step 1 of '{template['name']}' chain detected: "
                            f"{template['description']}. "
                            f"Trigger: {anomaly_msg.get('content', '')[:150]}. "
                            f"Now watching for step 2: {steps[1].get('sources', [])} -- "
                            f"keywords: {steps[1].get('keywords', [])} "
                            f"(expected within {template['typical_lag_hours'][1]}h)"
                        ),
                        data={
                            "request_type": "causal_chain_monitor",
                            "template_name": template["name"],
                            "chain_id": chain_id,
                            "trigger_anomaly_id": anomaly_msg.get("id", ""),
                            "next_step": 1,
                            "expected_sources": steps[1].get("sources", []) if len(steps) > 1 else [],
                            "expected_keywords": steps[1].get("keywords", []) if len(steps) > 1 else [],
                            "expected_within_hours": template["typical_lag_hours"][1] if len(template["typical_lag_hours"]) > 1 else 2,
                        },
                        priority=3,
                        location_id=anomaly_msg.get("location_id"),
                        references=[anomaly_msg.get("id", "")],
                    )
                    log.info(
                        "[CausalChainConnector] Chain triggered: %s -- %s",
                        template["name"], template["description"],
                    )

        # Check active chains for step 2+ completion
        await self._check_chain_progressions(anomalies)

        # Clean up stale chains (older than 48h)
        cutoff = datetime.now(timezone.utc) - timedelta(hours=48)
        stale = [
            cid for cid, state in self._active_chains.items()
            if _parse_ts(state.get("started_at", "")) and
               (_parse_ts(state["started_at"]) or cutoff) < cutoff
        ]
        for cid in stale:
            del self._active_chains[cid]

    async def _check_chain_progressions(
        self, anomalies: list[dict[str, Any]]
    ) -> None:
        """For each active chain, check if subsequent steps have been observed."""
        for chain_id, state in list(self._active_chains.items()):
            template = state["template"]
            current_step = state.get("step", 0)
            next_step = current_step + 1
            steps = template.get("steps", [])

            if next_step >= len(steps):
                continue

            step_spec = steps[next_step]
            expected_sources = step_spec.get("sources", [])
            expected_keywords = step_spec.get("keywords", [])
            lags = template.get("typical_lag_hours", [])
            max_lag = lags[next_step] if next_step < len(lags) else 6
            start_time = _parse_ts(state.get("started_at", ""))

            for anomaly_msg in anomalies:
                a_time = _parse_ts(anomaly_msg.get("timestamp", ""))
                if start_time and a_time:
                    elapsed_h = (a_time - start_time).total_seconds() / 3600
                    if elapsed_h < 0 or elapsed_h > max_lag * 2:
                        continue

                source = anomaly_msg.get("data", {}).get("source", "")
                content = anomaly_msg.get("content", "").lower()
                source_match = any(s in source for s in expected_sources)
                keyword_match = not expected_keywords or any(kw in content for kw in expected_keywords)

                if source_match and keyword_match:
                    # Step progressed!
                    state["step"] = next_step
                    is_complete = next_step >= len(steps) - 1

                    trigger_id = state["trigger_anomaly"].get("id", "")
                    evidence_summary = (
                        f"Step {next_step + 1}/{len(steps)} of '{template['name']}' chain confirmed. "
                        f"Evidence: {anomaly_msg.get('content', '')[:150]}"
                    )

                    if is_complete:
                        # Full chain detected -- post to #hypotheses
                        conn = Connection(
                            source_a=steps[0].get("sources", ["unknown"])[0],
                            source_b=steps[-1].get("sources", ["unknown"])[0],
                            description=(
                                f"[CAUSAL CHAIN] Complete '{template['name']}': "
                                f"{template['description']}"
                            ),
                            confidence=0.75,
                            evidence=[trigger_id, anomaly_msg.get("id", "")],
                            metadata={
                                "template_name": template["name"],
                                "chain_id": chain_id,
                                "steps_detected": next_step + 1,
                            },
                        )
                        await self.board.store_connection(conn)
                        await self.post(
                            channel="#hypotheses",
                            content=(
                                f"[CAUSAL CHAIN COMPLETE] '{template['name']}': {template['description']}. "
                                f"All {len(steps)} steps confirmed."
                            ),
                            data={
                                "connection_id": conn.id,
                                "type": "causal_chain_complete",
                                "template_name": template["name"],
                                "confidence": 0.75,
                                "chain_id": chain_id,
                            },
                            priority=4,
                            references=[trigger_id, anomaly_msg.get("id", "")],
                        )
                        log.info("[CausalChainConnector] COMPLETE chain: %s", template["name"])
                    else:
                        await self.post(
                            channel="#requests",
                            content=f"[CAUSAL] {evidence_summary}. Watching for step {next_step + 2}.",
                            data={
                                "request_type": "causal_chain_progress",
                                "template_name": template["name"],
                                "chain_id": chain_id,
                                "step_completed": next_step,
                                "next_step": next_step + 1,
                                "expected_sources": steps[next_step + 1].get("sources", []) if next_step + 1 < len(steps) else [],
                            },
                            priority=3,
                        )
                    break


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


def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d%H%M%S")


def _r_to_confidence(r: float) -> float:
    """Map |r| to a 0-1 confidence score."""
    return min(1.0, abs(r) ** 0.7)


def _bucket_to_hourly(
    series: dict[str, list[tuple[datetime, float]]]
) -> dict[str, list[float]]:
    """Convert irregular time-series to hourly averages aligned to the same time grid."""
    if not series:
        return {}
    # Find global time range
    all_times = [t for vals in series.values() for t, _ in vals]
    if not all_times:
        return {}
    t_min = min(all_times)
    t_max = max(all_times)
    total_hours = int((t_max - t_min).total_seconds() / 3600) + 1
    if total_hours < 2:
        return {}

    result: dict[str, list[float]] = {}
    for key, vals in series.items():
        buckets: dict[int, list[float]] = defaultdict(list)
        for t, v in vals:
            hour_idx = int((t - t_min).total_seconds() / 3600)
            buckets[hour_idx].append(v)
        # Build dense array (use NaN-filled gaps, then interpolate naively)
        dense = []
        for h in range(total_hours):
            if h in buckets:
                dense.append(sum(buckets[h]) / len(buckets[h]))
            else:
                dense.append(float("nan"))
        # Forward-fill NaNs
        last = None
        filled = []
        for v in dense:
            if not math.isnan(v):
                last = v
                filled.append(v)
            elif last is not None:
                filled.append(last)
        if len(filled) >= 5:
            result[key] = filled
    return result


def _align_series_with_lag(
    x_series: list[float], y_series: list[float], lag_hours: int
) -> tuple[list[float], list[float]]:
    """Align two hourly series with a given lag (x leads y by lag_hours)."""
    if lag_hours == 0:
        n = min(len(x_series), len(y_series))
        return x_series[:n], y_series[:n]
    if lag_hours > 0:
        # x at time t, y at time t+lag
        x_aligned = x_series[: len(x_series) - lag_hours]
        y_aligned = y_series[lag_hours:]
    else:
        lag = abs(lag_hours)
        x_aligned = x_series[lag:]
        y_aligned = y_series[: len(y_series) - lag]
    n = min(len(x_aligned), len(y_aligned))
    return x_aligned[:n], y_aligned[:n]


# -- Async entry points --------------------------------------------------------

async def run_spatial_connector(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = SpatialConnector(board, graph, memory)
    await agent.run()


async def run_narrative_connector(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = NarrativeConnector(board, graph, memory)
    await agent.run()


async def run_statistical_connector(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = StatisticalConnector(board, graph, memory)
    await agent.run()


async def run_causal_chain_connector(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = CausalChainConnector(board, graph, memory)
    await agent.run()
