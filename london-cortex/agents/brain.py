"""
Brain agent — the London Cortex synthesis engine.

Runs every 5 minutes. Reads all channels. Uses the Pro-tier LLM to synthesize a
holistic picture of what London is experiencing right now, what patterns are
emerging, and what predictions can be made. Posts to #discoveries and #meta.
Also performs self-improvement observations every 6 hours.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_PRO_MODEL, LLM_FLASH_MODEL
from ..core.epistemics import AnomalyClusterer, ConfidenceCalculator, GroundingCheck, SourceTracker
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import Connection
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.brain")

_BRAIN_SYSTEM = """\
You are the analytical brain of London Cortex — a city-wide sensor network that
monitors everything from traffic cameras to air quality, financial markets, social media trends,
nature observations, and news. You synthesize real-time data streams into coherent narratives
about what is happening in London right now.

Your analysis should:
- Be genuinely insightful, not just a summary
- Find unexpected connections across domains — but ONLY when actually supported by multiple independent data sources
- Make specific, testable predictions
- Notice emerging trends before they become obvious
- Have the analytical depth of a seasoned urban analyst who knows London intimately

CRITICAL GROUNDING RULES — you MUST follow these:
- Camera outages are ROUTINE. TfL cameras go offline regularly for maintenance. NEVER describe camera outages as attacks, sabotage, or coordinated events unless corroborated by at least 2 other independent sources (police reports, news, etc.)
- Single-source anomalies are LOWER CONFIDENCE but NOT worthless. A genuinely unusual single-source event (crowd at 2am, weird spike) IS worth mentioning — just flag it as speculative and low-confidence.
- Do NOT manufacture drama. If the data is mundane, say so. A "boring" cycle is a GOOD cycle.
- Correlation is not causation. Two things happening at the same time in London is usually coincidence.
- If the GROUNDING FACTS section tells you something is routine, BELIEVE IT — it's based on historical data, not guesswork.

Do NOT repeat things you've already reported in previous cycles. Focus on what's NEW and SIGNIFICANT.
"""

_BRAIN_SYNTHESIS_PROMPT = """\
You are synthesizing London's sensor network data. Current time: {current_time}

=== RECENT ANOMALIES (last 15 min) ===
{recent_anomalies}

=== ACTIVE HYPOTHESES ===
{hypotheses}

=== RECENT DISCOVERIES (what you already reported — DO NOT REPEAT) ===
{recent_discoveries}

=== BROADER CONTEXT (anomalies last 2h) ===
{broader_context}

=== RECENT OBSERVATIONS (normal readings across data streams) ===
{observations_sample}

=== CURIOSITY ITEMS (single-source oddities — low confidence but potentially interesting) ===
{curiosity_items}

=== DISCOVERY CHAINS (multi-hop connections found) ===
{discovery_chains}

=== GRAPH STATS ===
{graph_stats}

=== EVOLVING NARRATIVES (multi-day patterns — treat as background context, do NOT just confirm these) ===
{evolving_narratives}

=== ACTIVE INVESTIGATIONS ===
{active_investigations}

=== UNANSWERED QUESTIONS ===
{unanswered_questions}

Synthesize this into a coherent picture. Answer:
1. What is the SINGLE most interesting thing London Cortex has detected right now?
2. What PATTERN is emerging across multiple sensors that humans might miss?
3. What is your PREDICTION for the next 30 minutes to 7 days, and how confident are you?
4. What QUESTION should the system investigate next? Name which agent should look into it.

Respond with JSON:
{{
  "headline": "One punchy sentence — the most important thing happening in London right now",
  "insight": "2-3 sentences of your key insight connecting multiple data streams",
  "pattern": "What pattern are you seeing emerge? Be specific about which sensors/sources.",
  "prediction": {{
    "text": "Specific prediction about what will happen",
    "confidence": <0.0-1.0>,
    "horizon_minutes": <15-120>,
    "falsifiable_test": "How would we know if this prediction is wrong?"
  }},
  "question": "What should the system investigate next?",
  "severity": <1-5>,
  "notable_locations": ["specific London places/areas that are significant"],
  "cross_domain_connection": "One surprising connection between two apparently unrelated data streams",
  "boring": false
}}

If nothing is genuinely interesting, set "boring": true and give a minimal response.
Don't manufacture drama where there is none.
"""

_SELF_IMPROVEMENT_PROMPT = """\
You are reflecting on how London Cortex is performing over the past 6 hours.

=== RECENT DISCOVERIES ===
{discoveries}

=== PREDICTION OUTCOMES ===
{prediction_outcomes}

=== STATISTICAL CORRELATIONS FOUND ===
{correlations}

=== SYSTEM METRICS ===
Anomalies detected: {anomaly_count}
Hypotheses generated: {hypothesis_count}
Predictions made: {prediction_count}
Predictions verified: {predictions_verified}

Based on this, answer:
1. Which data sources are proving most valuable?
2. Which anomaly types are being missed or over-detected?
3. What causal patterns have we confirmed vs. refuted?
4. What NEW data source or analysis approach would improve the system?
5. Are our z-score thresholds too sensitive or not sensitive enough?

Respond with JSON:
{{
  "valuable_sources": ["sources providing most signal"],
  "blind_spots": ["what we're likely missing"],
  "confirmed_patterns": ["patterns we've validated"],
  "refuted_patterns": ["hypotheses we can rule out"],
  "suggested_improvement": "One specific actionable improvement",
  "threshold_assessment": "too_sensitive | calibrated | not_sensitive_enough",
  "new_data_source_suggestion": "Specific API or data source to add",
  "meta_observation": "Higher-level observation about how London works"
}}
"""


class BrainAgent(BaseAgent):
    """
    The synthesis engine. Runs every 5 minutes, uses Pro model to generate
    holistic discoveries. Maintains narrative memory to avoid repetition.
    """

    name = "brain"
    _MAX_PRO_PER_HOUR = 15  # ~12 runs/hour but we throttle further
    _SELF_IMPROVE_INTERVAL_HOURS = 6.0

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._last_self_improvement: datetime | None = None
        # Epistemic engine components
        self._clusterer = AnomalyClusterer()
        self._source_tracker = SourceTracker()
        self._grounding = GroundingCheck(self._source_tracker)
        self._confidence_calc = ConfidenceCalculator()

    async def run(self) -> None:
        now = datetime.now(timezone.utc)

        # Gather data from all channels
        recent_anomalies = await self.read_channel("#anomalies", since_hours=0.25, limit=30)
        broader_anomalies = await self.read_channel("#anomalies", since_hours=2.0, limit=80)
        hypotheses = await self.read_channel("#hypotheses", since_hours=1.0, limit=20)
        recent_discoveries = await self.read_channel("#discoveries", since_hours=1.0, limit=10)

        # Also gather raw sensor data for context even without anomalies
        raw_messages = await self.read_channel("#raw", since_hours=0.5, limit=50)

        # Gather observations (normal readings) and discovery chains
        observations = await self.read_channel("#observations", since_hours=2.0, limit=50)
        discovery_msgs = await self.read_channel("#discoveries", since_hours=6.0, limit=20)
        discovery_chains = [
            d for d in discovery_msgs
            if d.get("data", {}).get("type") in ("chain_discovery", "cross_domain_discovery", "implication_discovery")
        ]

        # If there's absolutely nothing, skip
        if not recent_anomalies and not hypotheses and not raw_messages and not observations:
            log.debug("[Brain] No data at all — skipping cycle.")
            return

        # -- Epistemic grounding -------------------------------------------------
        # 1. Cluster anomalies
        clusters = self._clusterer.cluster(recent_anomalies)
        all_clusters = self._clusterer.cluster(broader_anomalies)

        # 2. Ground each cluster — separate into unsuppressed + curiosity items
        grounding_reports = []
        unsuppressed_clusters = []
        curiosity_clusters = []  # suppressed but potentially interesting
        for cluster in clusters:
            report = self._grounding.check(cluster, all_clusters=all_clusters)
            grounding_reports.append(report)
            if not report.should_suppress:
                unsuppressed_clusters.append((cluster, report))
            else:
                # Keep high-severity suppressed items as curiosity items
                if cluster.max_severity >= 2:
                    curiosity_clusters.append((cluster, report))
                log.info(
                    "[Brain] Suppressed cluster: %s (mundane=%s, base_rate=%.2f, kept_as_curiosity=%s)",
                    cluster.description[:60], report.mundane_explanation, report.base_rate,
                    cluster.max_severity >= 2,
                )

        # 3. If all suppressed AND no observations/chains, still post status
        if clusters and not unsuppressed_clusters and not hypotheses and not observations and not discovery_chains:
            log.info("[Brain] All %d anomaly clusters suppressed and no observations.", len(clusters))
            suppressed_summaries = [c.description[:80] for c, _ in clusters[:3] if hasattr(c, 'description')]
            await self.think(
                f"Reviewed {len(clusters)} anomaly clusters — all routine. "
                f"Suppressed: {'; '.join(suppressed_summaries) if suppressed_summaries else 'camera outages, sensor noise'}. "
                f"No cross-source patterns emerging. London is quiet.",
                channel="#meta",
                data={"type": "all_suppressed", "cluster_count": len(clusters)},
            )
            return

        # 4. Build grounding context for LLM
        grounding_context = ""
        if unsuppressed_clusters:
            grounding_parts = []
            for cluster, report in unsuppressed_clusters:
                grounding_parts.append(report.as_context_for_llm())
            grounding_context = "\n\n".join(grounding_parts)

        # 4b. Filter anomaly messages to ONLY those from unsuppressed clusters
        # This is critical — passing suppressed anomalies lets the LLM dramatize them
        unsuppressed_ids = set()
        for cluster, _ in unsuppressed_clusters:
            unsuppressed_ids.update(cluster.anomaly_ids)

        if unsuppressed_ids:
            filtered_anomalies = [
                a for a in recent_anomalies
                if a.get("data", {}).get("anomaly_id", "") in unsuppressed_ids
            ]
            filtered_broader = [
                a for a in broader_anomalies
                if a.get("data", {}).get("anomaly_id", "") in unsuppressed_ids
            ]
        else:
            filtered_anomalies = []
            filtered_broader = []

        suppressed_count = len(recent_anomalies) - len(filtered_anomalies)
        if suppressed_count > 0:
            log.info(
                "[Brain] Filtered out %d/%d anomalies as routine (camera offline, etc.)",
                suppressed_count, len(recent_anomalies),
            )

        # Build context strings — only unsuppressed anomalies reach the LLM
        anomalies_text = self._summarise_messages(filtered_anomalies, max_chars=2000)
        hypotheses_text = self._summarise_messages(hypotheses, max_chars=1000)
        discoveries_text = self._summarise_messages(recent_discoveries, max_chars=800)
        context_text = self._summarise_messages(filtered_broader, max_chars=1500)
        raw_text = self._summarise_messages(raw_messages, max_chars=2000)
        graph_stats = json.dumps(self.graph.stats())

        # Build suppression note
        suppression_note = ""
        if suppressed_count > 0:
            suppression_note = (
                f"\n\nNOTE: {suppressed_count} routine anomalies were filtered out "
                f"(camera offline, sensor maintenance, etc). These are NORMAL and should "
                f"NOT be interpreted as attacks, sabotage, or coordinated events.\n"
            )

        # Build curiosity items from suppressed-but-interesting anomalies
        curiosity_parts = []
        for cluster, report in curiosity_clusters[:5]:
            curiosity_parts.append(
                f"[LOW-CONFIDENCE] {cluster.description[:120]} "
                f"(single-source: {cluster.source}, severity={cluster.max_severity}, "
                f"mundane_explanation='{report.mundane_explanation or 'none'}')"
            )
        curiosity_text = "\n".join(curiosity_parts) if curiosity_parts else "None."

        # Build observations sample text
        observations_text = self._summarise_messages(observations, max_chars=1500) if observations else "No observations yet."
        chains_text = self._summarise_messages(discovery_chains, max_chars=800) if discovery_chains else "No chains yet."

        # Load evolving narratives — only show recent ones with fresh evidence
        evolving_narratives = self.memory.load_json("evolving_narratives.json")
        narratives_text = ""
        if isinstance(evolving_narratives, list) and evolving_narratives:
            # Hard TTL: only show narratives updated in the last 12h
            cutoff = (now - timedelta(hours=12)).isoformat()
            fresh_narratives = [
                n for n in evolving_narratives
                if n.get("last_updated", "") > cutoff
            ]
            # Show at most 3 fresh narratives to avoid bias
            narratives_text = json.dumps(fresh_narratives[-3:], default=str)[:1000] if fresh_narratives else ""

        # Load active investigation threads
        try:
            active_invs = await self.board.get_active_investigations(limit=10)
            inv_text = json.dumps(active_invs, default=str)[:1000] if active_invs else "None"
        except Exception:
            inv_text = "None"

        # Load unanswered questions from #requests
        unanswered = await self.read_channel("#requests", since_hours=6.0, limit=10)
        unanswered_text = self._summarise_messages(unanswered, max_chars=500) if unanswered else "None"

        prompt = _BRAIN_SYNTHESIS_PROMPT.format(
            current_time=now.strftime("%Y-%m-%d %H:%M UTC"),
            recent_anomalies=(anomalies_text + suppression_note) if anomalies_text else
                (f"No significant anomalies. ({suppressed_count} routine events filtered out.)" if suppressed_count > 0
                 else "No anomalies flagged yet (system warming up)."),
            hypotheses=hypotheses_text or "No active hypotheses.",
            recent_discoveries=discoveries_text or "No previous discoveries to avoid repeating.",
            broader_context=context_text or raw_text or "No broader context.",
            observations_sample=observations_text,
            curiosity_items=curiosity_text,
            discovery_chains=chains_text,
            graph_stats=graph_stats,
            evolving_narratives=narratives_text or "No long-term narratives yet.",
            active_investigations=inv_text,
            unanswered_questions=unanswered_text,
        )

        # 5. Inject grounding facts into prompt
        if grounding_context:
            prompt = grounding_context + "\n\n" + prompt

        response_text = await self.call_llm(
            prompt=prompt,
            model=LLM_PRO_MODEL,
            tier="pro",
            trigger="brain:synthesis",
            system_instruction=_BRAIN_SYSTEM,
        )
        self.record_pro_call()

        if response_text.startswith("ERROR:"):
            log.warning("[Brain] Pro-tier LLM error: %s", response_text)
            return

        parsed = parse_json_response(response_text)
        if not parsed or not isinstance(parsed, dict):
            log.debug("[Brain] Could not parse synthesis response.")
            return

        if parsed.get("boring", False):
            log.debug("[Brain] System reports boring cycle — no discovery posted.")
            # Still share what we looked at
            headline = parsed.get("headline", "nothing notable")
            insight = parsed.get("insight", "")
            await self.think(
                f"Synthesis complete — quiet period. Assessed: {headline}. "
                f"{('Observation: ' + insight) if insight else 'No significant patterns detected.'}",
                channel="#meta",
                data={"type": "quiet_period", "timestamp": now.isoformat()},
            )
            return

        headline = parsed.get("headline", "")
        insight = parsed.get("insight", "")
        pattern = parsed.get("pattern", "")
        prediction_data = parsed.get("prediction", {})
        question = parsed.get("question", "")
        raw_severity = int(parsed.get("severity", 2))
        locations = parsed.get("notable_locations", [])
        cross_domain = parsed.get("cross_domain_connection", "")

        # 6. Apply epistemic confidence and severity gating
        n_sources = max(c.n_sources for c, _ in unsuppressed_clusters) if unsuppressed_clusters else 1
        severity = self._confidence_calc.severity_gate(raw_severity, n_sources)

        if prediction_data and isinstance(prediction_data, dict):
            raw_conf = float(prediction_data.get("confidence", 0.5))
            prediction_data["confidence"] = self._confidence_calc.compute_confidence(
                raw_conf, layer="synthesized", n_sources=n_sources,
                n_observations=len(recent_anomalies),
            )

        if not headline:
            return

        # Check for duplicate (simple dedup against memory)
        narrative_memory = self.memory.load_json("brain_narrative_memory.json")
        if not isinstance(narrative_memory, list):
            narrative_memory = []

        # Check last 8 headlines for similarity (narrower window to avoid blocking new takes)
        recent_headlines = [entry.get("headline", "") for entry in narrative_memory[-8:]]
        if any(_headlines_similar(headline, h) for h in recent_headlines):
            log.debug("[Brain] Headline too similar to recent ones — skipping.")
            return

        # Save to narrative memory (keep 50 — enough for dedup, not so many it blocks)
        narrative_memory.append({
            "headline": headline,
            "timestamp": now.isoformat(),
            "severity": severity,
        })
        narrative_memory = narrative_memory[-50:]
        self.memory.save_json("brain_narrative_memory.json", narrative_memory)

        # Build discovery content
        content_parts = [f"[BRAIN] {headline}"]
        if insight:
            content_parts.append(f"Insight: {insight}")
        if pattern:
            content_parts.append(f"Pattern: {pattern}")
        if cross_domain:
            content_parts.append(f"Cross-domain: {cross_domain}")
        if prediction_data.get("text"):
            pred_text = prediction_data["text"]
            conf = prediction_data.get("confidence", 0.5)
            horizon = prediction_data.get("horizon_minutes", 60)
            content_parts.append(f"Prediction ({conf:.0%} conf, {horizon}min): {pred_text}")

        full_content = "\n".join(content_parts)

        # Store as connection if there's a prediction
        if prediction_data.get("text"):
            horizon_min = prediction_data.get("horizon_minutes", 60)
            conn = Connection(
                source_a="brain",
                source_b="synthesis",
                description=headline,
                confidence=float(prediction_data.get("confidence", 0.5)),
                evidence=[m.get("id", "") for m in recent_anomalies[:5]],
                prediction=prediction_data["text"],
                prediction_deadline=now + timedelta(minutes=horizon_min),
                metadata={
                    "insight": insight,
                    "pattern": pattern,
                    "question": question,
                    "locations": locations,
                    "cross_domain": cross_domain,
                    "falsifiable_test": prediction_data.get("falsifiable_test", ""),
                    "severity": severity,
                },
            )
            await self.board.store_connection(conn)

        # Post to #discoveries
        await self.post(
            channel="#discoveries",
            content=full_content,
            data={
                "type": "brain_synthesis",
                "headline": headline,
                "insight": insight,
                "pattern": pattern,
                "prediction": prediction_data,
                "question": question,
                "severity": severity,
                "locations": locations,
                "cross_domain_connection": cross_domain,
            },
            priority=min(severity, 5),
            ttl_hours=12,
        )

        # Route question via investigation thread
        if question:
            target = self._route_question(question)
            await self.start_investigation(
                trigger=headline,
                question=question,
                target_agent=target,
                data={"severity": severity, "locations": locations},
            )

        # Post meta summary
        await self.post(
            channel="#meta",
            content=(
                f"[BRAIN CYCLE] Severity={severity} | {headline[:80]} | "
                f"Locations: {', '.join(locations[:3]) or 'city-wide'}"
            ),
            data={
                "type": "brain_cycle",
                "severity": severity,
                "anomalies_seen": len(recent_anomalies),
                "hypotheses_seen": len(hypotheses),
            },
            priority=1,
            ttl_hours=2,
        )

        log.info("[Brain] Discovery severity=%d: %s", severity, headline)

        # Check pending investigations assigned to brain
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                steps = inv if isinstance(inv, list) else [inv]
                last_step = steps[-1] if steps else {}
                finding = last_step.get("finding", "")
                if finding:
                    log.info("[Brain] Incorporating investigation finding: %s", finding[:100])
        except Exception as exc:
            log.debug("[Brain] Could not check investigations: %s", exc)

        # Self-improvement check (every 6 hours)
        await self._maybe_self_improve(now)

    async def _maybe_self_improve(self, now: datetime) -> None:
        if (
            self._last_self_improvement is not None
            and (now - self._last_self_improvement).total_seconds() < self._SELF_IMPROVE_INTERVAL_HOURS * 3600
        ):
            return

        log.info("[Brain] Running self-improvement analysis.")
        self._last_self_improvement = now

        # Gather performance data
        discoveries_6h = await self.read_channel("#discoveries", since_hours=6.0, limit=50)
        hypotheses_6h = await self.read_channel("#hypotheses", since_hours=6.0, limit=100)
        anomalies_6h = await self.read_channel("#anomalies", since_hours=6.0, limit=200)

        # Load prediction outcomes
        backtesting = self.memory.load_json("backtesting_results.json")
        if not isinstance(backtesting, list):
            backtesting = []
        recent_results = backtesting[-20:]

        # Load correlations
        correlations = self.memory.load_json("learned_correlations.json")
        if not isinstance(correlations, list):
            correlations = []
        recent_corr = correlations[-10:]

        predictions_verified = sum(
            1 for r in recent_results if r.get("outcome") is not None
        )

        prompt = _SELF_IMPROVEMENT_PROMPT.format(
            discoveries=self._summarise_messages(discoveries_6h, max_chars=1500),
            prediction_outcomes=json.dumps(recent_results, default=str)[:800],
            correlations=json.dumps(recent_corr, default=str)[:800],
            anomaly_count=len(anomalies_6h),
            hypothesis_count=len(hypotheses_6h),
            prediction_count=len(recent_results),
            predictions_verified=predictions_verified,
        )

        response_text = await self.call_llm(
            prompt=prompt,
            model=LLM_PRO_MODEL,
            tier="pro",
            trigger="brain:self_improve",
            system_instruction=_BRAIN_SYSTEM,
        )
        self.record_pro_call()

        if response_text.startswith("ERROR:"):
            log.warning("[Brain] Self-improvement LLM error: %s", response_text)
            return

        parsed = parse_json_response(response_text)
        if not parsed or not isinstance(parsed, dict):
            return

        # Save improvement observations to memory
        improvements = self.memory.load_json("self_improvement_log.json")
        if not isinstance(improvements, list):
            improvements = []
        improvements.append({
            "timestamp": now.isoformat(),
            **parsed,
        })
        improvements = improvements[-50:]
        self.memory.save_json("self_improvement_log.json", improvements)

        meta_obs = parsed.get("meta_observation", "")
        suggestion = parsed.get("suggested_improvement", "")
        blind_spots = parsed.get("blind_spots", [])
        threshold = parsed.get("threshold_assessment", "calibrated")
        new_source = parsed.get("new_data_source_suggestion", "")

        content = (
            f"[BRAIN SELF-IMPROVEMENT] Meta: {meta_obs} | "
            f"Suggestion: {suggestion} | "
            f"Blind spots: {', '.join(blind_spots[:3])} | "
            f"Thresholds: {threshold} | "
            f"New data source: {new_source}"
        )

        await self.post(
            channel="#meta",
            content=content,
            data={
                "type": "self_improvement",
                **parsed,
            },
            priority=2,
            ttl_hours=24,
        )
        log.info("[Brain] Self-improvement: %s", suggestion[:100])


    def _route_question(self, question: str) -> str:
        """Route a brain question to the most appropriate agent."""
        q = question.lower()
        if any(w in q for w in ("camera", "image", "visual", "cctv", "traffic camera")):
            return "vision_interpreter"
        if any(w in q for w in ("spatial", "nearby", "adjacent", "location", "area")):
            return "spatial_connector"
        if any(w in q for w in ("news", "article", "report", "search", "web", "council", "planning")):
            return "web_searcher"
        if any(w in q for w in ("correlat", "connect", "chain", "implicat")):
            return "discovery_engine"
        if any(w in q for w in ("trend", "statistical", "data")):
            return "statistical_connector"
        if any(w in q for w in ("pattern", "narrative", "story", "explain")):
            return "narrative_connector"
        return "curiosity_engine"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _headlines_similar(a: str, b: str, threshold: float = 0.75) -> bool:
    """Very simple similarity check to avoid posting near-duplicate headlines."""
    if not a or not b:
        return False
    words_a = set(a.lower().split())
    words_b = set(b.lower().split())
    if not words_a or not words_b:
        return False
    overlap = len(words_a & words_b) / min(len(words_a), len(words_b))
    return overlap > threshold


# -- Async entry point --------------------------------------------------------

async def run_brain(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = BrainAgent(board, graph, memory)
    await agent.run()
