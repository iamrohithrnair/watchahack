"""Epistemic engine — structural grounding for London Cortex.

All code logic, no LLM calls. Gates what the LLM sees and how boldly it can speak.
"""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from .config import MEMORY_DIR
from .models import Anomaly, AnomalyCluster, GroundingReport

log = logging.getLogger("cortex.epistemics")

# ── Mundane explanation rules ────────────────────────────────────────────────
# Source pattern → likely mundane explanation (code rules, not LLM)
_MUNDANE_RULES: dict[str, dict[str, str]] = {
    "tfl_jamcam": {
        "offline": "Camera maintenance or connectivity issue",
        "unavailable": "Camera maintenance or connectivity issue",
        "no image": "Camera feed temporarily unavailable",
        "dark": "Night-time or camera exposure issue",
        "black": "Camera lens obstruction or night-time",
        "empty": "Low-traffic period (likely night/early morning)",
        "congestion": "Normal rush-hour congestion",
        "stationary": "Normal rush-hour congestion or traffic light phase",
    },
    "laqn": {
        "no data": "Sensor calibration or maintenance window",
        "high": "Normal diurnal pollution variation (rush hour)",
    },
    "open_meteo": {},
    "environment_agency": {
        "no data": "Sensor maintenance",
    },
}

# Source patterns that are high base-rate (happen frequently)
_HIGH_BASE_RATE_PATTERNS: dict[str, list[str]] = {
    "tfl_jamcam": ["offline", "unavailable", "congestion", "stationary"],
    "laqn": ["no data"],
    "environment_agency": ["no data"],
}


class AnomalyClusterer:
    """Groups anomalies by (source + nearby location + time window).

    50 'camera offline' messages become 1 cluster with count=50, n_sources=1.
    """

    def __init__(self, time_window_minutes: float = 30.0, location_radius: int = 2):
        self._time_window = timedelta(minutes=time_window_minutes)
        self._location_radius = location_radius

    def cluster(self, anomalies: list[dict[str, Any]]) -> list[AnomalyCluster]:
        """Cluster a list of anomaly dicts into AnomalyCluster objects."""
        if not anomalies:
            return []

        # Group by (source, location_prefix)
        groups: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
        for a in anomalies:
            source = a.get("source", a.get("data", {}).get("source", "unknown"))
            location = a.get("location_id") or "global"
            # Use location prefix for spatial grouping (nearby cells)
            loc_prefix = location[:8] if len(location) > 8 else location
            groups[(source, loc_prefix)].append(a)

        clusters: list[AnomalyCluster] = []
        for (source, loc_prefix), group_anomalies in groups.items():
            # Sub-cluster by time window
            sorted_anoms = sorted(group_anomalies, key=lambda a: a.get("timestamp", ""))
            time_buckets: list[list[dict[str, Any]]] = []
            current_bucket: list[dict[str, Any]] = []

            for a in sorted_anoms:
                ts_str = a.get("timestamp", "")
                ts = _parse_ts(ts_str)
                if not current_bucket:
                    current_bucket.append(a)
                    continue
                first_ts = _parse_ts(current_bucket[0].get("timestamp", ""))
                if ts and first_ts and (ts - first_ts) <= self._time_window:
                    current_bucket.append(a)
                else:
                    time_buckets.append(current_bucket)
                    current_bucket = [a]
            if current_bucket:
                time_buckets.append(current_bucket)

            for bucket in time_buckets:
                sources_seen = set()
                severities: list[int] = []
                anomaly_ids: list[str] = []
                descriptions: list[str] = []
                first_ts = None
                last_ts = None

                for a in bucket:
                    a_source = a.get("source", a.get("data", {}).get("source", "unknown"))
                    sources_seen.add(a_source)
                    sev = int(a.get("severity", a.get("data", {}).get("severity", 1)))
                    severities.append(sev)
                    aid = a.get("id", a.get("data", {}).get("anomaly_id", ""))
                    if aid:
                        anomaly_ids.append(aid)
                    desc = a.get("description", a.get("content", ""))[:100]
                    if desc and desc not in descriptions:
                        descriptions.append(desc)

                    ts = _parse_ts(a.get("timestamp", ""))
                    if ts:
                        if first_ts is None or ts < first_ts:
                            first_ts = ts
                        if last_ts is None or ts > last_ts:
                            last_ts = ts

                now = datetime.now(timezone.utc)
                cluster = AnomalyCluster(
                    source=source,
                    location_id=bucket[0].get("location_id"),
                    description=descriptions[0] if descriptions else f"{source} anomaly cluster",
                    anomaly_ids=anomaly_ids,
                    count=len(bucket),
                    n_sources=len(sources_seen),
                    mean_severity=sum(severities) / len(severities) if severities else 1.0,
                    max_severity=max(severities) if severities else 1,
                    first_seen=first_ts or now,
                    last_seen=last_ts or now,
                )
                clusters.append(cluster)

        return clusters


class ConfidenceCalculator:
    """Applies structural decay at each processing layer."""

    # Layer decay factors — tuned to let interesting single-source oddities through
    LAYER_DECAY: dict[str, float] = {
        "raw": 1.0,
        "interpreted": 0.90,
        "connected": 0.75,
        "narrated": 0.65,
        "synthesized": 0.60,
    }

    # Single-source discount — still penalised but not crushed
    SOURCE_MULTIPLIER: dict[int, float] = {
        1: 0.55,
        2: 0.75,
    }
    # 3+ sources = 1.0 (no discount)

    def __init__(self, system_start_time: datetime | None = None):
        self._system_start_time = system_start_time or datetime.now(timezone.utc)

    @property
    def system_maturity(self) -> float:
        """Ramps 0→1 over first 6h of runtime."""
        elapsed = (datetime.now(timezone.utc) - self._system_start_time).total_seconds()
        hours_6 = 6 * 3600
        return min(1.0, elapsed / hours_6)

    def compute_confidence(
        self,
        raw_confidence: float,
        layer: str,
        n_sources: int,
        n_observations: int = 1,
    ) -> float:
        """Compute grounded confidence with all decay factors applied."""
        # Layer decay
        layer_factor = self.LAYER_DECAY.get(layer, 0.5)

        # Source multiplier
        source_factor = self.SOURCE_MULTIPLIER.get(n_sources, 1.0)

        # Observation dedup: log2 scaling so 50 obs from 1 source != 50x
        obs_factor = 1.0
        if n_observations > 1:
            obs_factor = math.log2(n_observations + 1) / n_observations

        # System maturity gate
        maturity = self.system_maturity
        maturity_cap = max(0.5, maturity)

        confidence = raw_confidence * layer_factor * source_factor * obs_factor
        confidence = min(confidence, maturity_cap)
        return round(max(0.0, min(1.0, confidence)), 3)

    def severity_gate(self, raw_severity: int, n_sources: int) -> int:
        """Cap severity based on source count."""
        if n_sources <= 1:
            return min(raw_severity, 3)
        if n_sources == 2:
            return min(raw_severity, 4)
        return min(raw_severity, 5)


class SourceTracker:
    """EMA-based reliability scores per source, persisted to JSON."""

    def __init__(self, memory_dir: str | None = None):
        self._dir = Path(memory_dir or MEMORY_DIR)
        self._dir.mkdir(parents=True, exist_ok=True)
        self._path = self._dir / "source_reliability.json"
        self._scores: dict[str, float] = {}
        self._alpha = 0.1  # EMA smoothing
        self._load()

    def _load(self) -> None:
        if self._path.exists():
            try:
                self._scores = json.loads(self._path.read_text())
            except (json.JSONDecodeError, OSError):
                self._scores = {}

    def _save(self) -> None:
        try:
            self._path.write_text(json.dumps(self._scores, indent=2))
        except OSError as e:
            log.warning("Failed to save source reliability: %s", e)

    def get_reliability(self, source: str) -> float:
        """Get reliability score for a source (default 0.5)."""
        return self._scores.get(source, 0.5)

    def update(self, source: str, prediction_correct: bool) -> float:
        """Update source reliability based on prediction outcome."""
        current = self._scores.get(source, 0.5)
        target = 1.0 if prediction_correct else 0.0
        updated = self._alpha * target + (1 - self._alpha) * current
        self._scores[source] = round(updated, 4)
        self._save()
        return updated

    @property
    def all_scores(self) -> dict[str, float]:
        return dict(self._scores)


class GroundingCheck:
    """Pure code logic that produces a GroundingReport with hard facts."""

    def __init__(self, source_tracker: SourceTracker):
        self._source_tracker = source_tracker

    def check(
        self,
        cluster: AnomalyCluster,
        all_clusters: list[AnomalyCluster] | None = None,
        historical_counts: dict[str, int] | None = None,
    ) -> GroundingReport:
        """Produce a GroundingReport for an anomaly cluster."""
        now = datetime.now(timezone.utc)
        age_minutes = (now - cluster.first_seen).total_seconds() / 60.0

        # Check for mundane explanation
        mundane = self._find_mundane_explanation(cluster)

        # Calculate base rate
        base_rate = self._estimate_base_rate(cluster, historical_counts)

        # Count similar past events
        similar = 0
        if all_clusters:
            for other in all_clusters:
                if other.cluster_id == cluster.cluster_id:
                    continue
                if other.source == cluster.source and other.description[:30] == cluster.description[:30]:
                    similar += 1

        reliability = self._source_tracker.get_reliability(cluster.source)

        return GroundingReport(
            cluster_id=cluster.cluster_id,
            n_independent_sources=cluster.n_sources,
            is_single_source=cluster.n_sources <= 1,
            base_rate=base_rate,
            mundane_explanation=mundane,
            age_minutes=age_minutes,
            similar_past_events=similar,
            source_reliability=reliability,
        )

    def _find_mundane_explanation(self, cluster: AnomalyCluster) -> str:
        """Check if there's a mundane explanation based on code rules."""
        desc_lower = cluster.description.lower()
        source_rules = _MUNDANE_RULES.get(cluster.source, {})
        for pattern, explanation in source_rules.items():
            if pattern in desc_lower:
                return explanation
        return ""

    def _estimate_base_rate(
        self,
        cluster: AnomalyCluster,
        historical_counts: dict[str, int] | None = None,
    ) -> float:
        """Estimate how often this type of anomaly occurs."""
        desc_lower = cluster.description.lower()
        patterns = _HIGH_BASE_RATE_PATTERNS.get(cluster.source, [])
        for pattern in patterns:
            if pattern in desc_lower:
                return 0.5  # high base rate

        if historical_counts:
            key = f"{cluster.source}|{cluster.description[:30]}"
            count = historical_counts.get(key, 0)
            if count > 10:
                return 0.4
            if count > 5:
                return 0.2

        return 0.05  # default low base rate


# ── Helpers ──────────────────────────────────────────────────────────────────

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
