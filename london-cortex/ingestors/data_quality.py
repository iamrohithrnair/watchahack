"""Data quality ingestor — monitors validation failures, value anomalies, and
schema consistency across all ingestor sources.

Addresses the Brain's suggestion: detect data poisoning at the source by tracking
per-source validation metrics (null rates, value range violations, volume anomalies,
metadata schema drift).

Complements:
- sensor_health.py (external sensor hardware status)
- system_health.py (internal log errors and channel throughput)
- data_lineage.py (per-source freshness tracking)

Data sources: observations table (local, no external API).
"""

from __future__ import annotations

import logging
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.data_quality")

# Window for analysis (seconds)
_ANALYSIS_WINDOW = 1800  # 30 min

# Expected value ranges per source — observations outside these are flagged.
# (min_value, max_value). None means unbounded on that side.
_VALUE_RANGES: dict[str, tuple[float | None, float | None]] = {
    "laqn": (0, 10),                    # Air quality index 1-10
    "open_meteo": (-30, 50),            # Temperature in °C for London
    "carbon_intensity": (0, 600),       # gCO2/kWh
    "tfl_jamcam": (0, 1),              # image-derived scores typically 0-1
    "sensor_health": (0, 1),            # reporting rates 0-1
    "environment_agency": (0, 15),      # river levels (metres)
    "tfl_traffic": (0, 10),            # severity/status codes
}

# Minimum expected observations per source per window. Sudden drops below
# this fraction of the baseline count are flagged as volume anomalies.
_VOLUME_DROP_THRESHOLD = 0.3  # flag if current < 30% of historical average


class DataQualityIngestor(BaseIngestor):
    source_name = "data_quality"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Query observations table for quality metrics."""
        now = time.time()
        cutoff_iso = _ts_to_iso(now - _ANALYSIS_WINDOW)
        # Wider window for baseline comparison
        baseline_cutoff_iso = _ts_to_iso(now - _ANALYSIS_WINDOW * 4)

        try:
            # Recent observations: source, value, metadata length
            _cur = await self.board._db.execute(
                "SELECT source, value, metadata, timestamp "
                "FROM observations WHERE timestamp > ? "
                "ORDER BY source",
                (cutoff_iso,),
            )
            recent_rows = await _cur.fetchall()
            # Baseline counts (wider window) for volume comparison
            _cur = await self.board._db.execute(
                "SELECT source, COUNT(*) as cnt "
                "FROM observations WHERE timestamp > ? AND timestamp <= ? "
                "GROUP BY source",
                (baseline_cutoff_iso, cutoff_iso),
            )
            baseline_rows = await _cur.fetchall()
        except Exception as exc:
            self.log.warning("Failed to query observations for quality: %s", exc)
            return None

        return {
            "recent": recent_rows,
            "baseline_counts": {r[0]: r[1] for r in baseline_rows},
            "now": now,
        }

    async def process(self, data: Any) -> None:
        recent_rows: list = data["recent"]
        baseline_counts: dict[str, int] = data["baseline_counts"]

        if not recent_rows:
            self.log.info("Data quality: no recent observations to analyze")
            return

        # Aggregate per-source quality metrics
        source_metrics: dict[str, dict] = {}
        for source, value, metadata, ts in recent_rows:
            if source not in source_metrics:
                source_metrics[source] = {
                    "total": 0,
                    "null_values": 0,
                    "out_of_range": 0,
                    "empty_metadata": 0,
                }
            m = source_metrics[source]
            m["total"] += 1

            # Null/missing value check
            if value is None:
                m["null_values"] += 1

            # Range violation check
            if value is not None and source in _VALUE_RANGES:
                lo, hi = _VALUE_RANGES[source]
                try:
                    value_f = float(value)
                    if (lo is not None and value_f < lo) or (hi is not None and value_f > hi):
                        m["out_of_range"] += 1
                except (ValueError, TypeError):
                    pass  # non-numeric value, skip range check

            # Empty metadata check
            if not metadata or metadata in ("null", "{}", ""):
                m["empty_metadata"] += 1

        # Compute rates and detect issues
        issues: list[dict] = []
        obs_count = 0

        for source, m in sorted(source_metrics.items()):
            total = m["total"]
            null_rate = m["null_values"] / total if total else 0
            range_violation_rate = m["out_of_range"] / total if total else 0
            empty_meta_rate = m["empty_metadata"] / total if total else 0

            # Volume anomaly: compare against baseline
            # Baseline covers 3x the window, so normalize
            baseline_count = baseline_counts.get(source, 0)
            baseline_avg_per_window = baseline_count / 3.0 if baseline_count else 0
            volume_ratio = (
                total / baseline_avg_per_window
                if baseline_avg_per_window > 0
                else 1.0
            )
            volume_drop = volume_ratio < _VOLUME_DROP_THRESHOLD and baseline_avg_per_window >= 3
            volume_spike = volume_ratio > 3.0 and total > 10

            # Build quality score: 1.0 = perfect, lower = worse
            quality_score = 1.0 - (null_rate * 0.4 + range_violation_rate * 0.4 + empty_meta_rate * 0.2)
            quality_score = max(0.0, quality_score)

            has_issue = (
                null_rate > 0.1
                or range_violation_rate > 0.05
                or volume_drop
                or volume_spike
            )

            if has_issue:
                issue_types = []
                if null_rate > 0.1:
                    issue_types.append(f"null_rate={null_rate:.0%}")
                if range_violation_rate > 0.05:
                    issue_types.append(f"range_violations={range_violation_rate:.0%}")
                if volume_drop:
                    issue_types.append(f"volume_drop={volume_ratio:.1f}x")
                if volume_spike:
                    issue_types.append(f"volume_spike={volume_ratio:.1f}x")
                issues.append({"source": source, "types": issue_types})

            # Store per-source quality observation
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=quality_score,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "source_quality",
                    "monitored_source": source,
                    "total_observations": total,
                    "null_rate": round(null_rate, 3),
                    "range_violation_rate": round(range_violation_rate, 3),
                    "empty_metadata_rate": round(empty_meta_rate, 3),
                    "volume_ratio": round(volume_ratio, 2),
                    "volume_drop": volume_drop,
                    "volume_spike": volume_spike,
                    "has_issue": has_issue,
                },
            )
            await self.board.store_observation(obs)

            # Only post board messages for sources with issues (avoid flooding)
            if has_issue:
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Data quality issue [{source}]: "
                        + ", ".join(issues[-1]["types"])
                        + f" (quality={quality_score:.0%}, n={total})"
                    ),
                    data={
                        "metric": "source_quality",
                        "monitored_source": source,
                        "quality_score": round(quality_score, 3),
                        "null_rate": round(null_rate, 3),
                        "range_violation_rate": round(range_violation_rate, 3),
                        "volume_ratio": round(volume_ratio, 2),
                        "has_issue": True,
                        "observation_id": obs.id,
                    },
                ))
            obs_count += 1

        # Overall quality summary
        all_scores = [
            1.0 - (m["null_values"] / m["total"] * 0.4 + m["out_of_range"] / m["total"] * 0.4 + m["empty_metadata"] / m["total"] * 0.2)
            for m in source_metrics.values() if m["total"] > 0
        ]
        overall_quality = sum(all_scores) / len(all_scores) if all_scores else 1.0

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=max(0.0, overall_quality),
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "overall_quality",
                "sources_monitored": len(source_metrics),
                "sources_with_issues": len(issues),
                "issue_summary": issues[:10],
                "window_secs": _ANALYSIS_WINDOW,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Data quality: {len(source_metrics)} sources monitored, "
                f"{len(issues)} with issues, overall quality {overall_quality:.0%}"
            ),
            data={
                "metric": "overall_quality",
                "overall_quality": round(overall_quality, 3),
                "sources_monitored": len(source_metrics),
                "sources_with_issues": len(issues),
                "issue_sources": [i["source"] for i in issues],
                "observation_id": obs.id,
            },
        ))

        self.log.info(
            "Data quality: %d sources, %d issues, overall=%.0f%%",
            len(source_metrics), len(issues), overall_quality * 100,
        )


def _ts_to_iso(ts: float) -> str:
    import datetime as dt
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


async def ingest_data_quality(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one data quality check cycle."""
    ingestor = DataQualityIngestor(board, graph, scheduler)
    await ingestor.run()
