"""Data lineage health ingestor — monitors per-source data freshness and throughput.

Addresses the Brain's suggestion: track which data sources are actually delivering
fresh observations so the system can automatically down-weight stale or failing
pipelines. Queries the observations table directly — no external API needed.

Complements:
- sensor_health.py (external sensor hardware status)
- system_health.py (internal log errors and channel throughput)
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

log = logging.getLogger("cortex.ingestors.data_lineage")

# Expected sources and their approximate max delivery intervals (seconds).
# If a source hasn't delivered within this window, it's considered stale.
_EXPECTED_SOURCES: dict[str, float] = {
    "tfl_jamcam": 900,          # expect every ~5 min, stale after 15
    "laqn": 2700,               # expect every ~15 min, stale after 45
    "open_meteo": 5400,         # expect every ~30 min, stale after 90
    "gdelt": 2700,              # expect every ~15 min, stale after 45
    "financial_stocks": 2700,
    "financial_crypto": 5400,
    "carbon_intensity": 5400,
    "environment_agency": 900,
    "nature_inaturalist": 7200,

    "tfl_digital_health": 1800,
    "sentinel2": 7200,
    "public_events": 7200,
    "tfl_road_disruptions": 900,
    "tfl_traffic": 900,
    "waze_traffic": 600,
    "sensor_health": 2700,
    "system_health": 2700,
}

# Window for counting recent observations (seconds)
_THROUGHPUT_WINDOW = 1800  # 30 min


class DataLineageIngestor(BaseIngestor):
    source_name = "data_lineage"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Query the observations table for per-source freshness and counts."""
        now = time.time()
        cutoff_iso = _ts_to_iso(now - _THROUGHPUT_WINDOW)

        try:
            # Last observation timestamp per source
            _cur = await self.board._db.execute(
                "SELECT source, MAX(timestamp) as last_ts, COUNT(*) as total "
                "FROM observations GROUP BY source"
            )
            freshness_rows = await _cur.fetchall()
            # Recent observation counts per source
            _cur = await self.board._db.execute(
                "SELECT source, COUNT(*) as cnt "
                "FROM observations WHERE timestamp > ? GROUP BY source",
                (cutoff_iso,),
            )
            recent_rows = await _cur.fetchall()
        except Exception as exc:
            self.log.warning("Failed to query observation stats: %s", exc)
            return None

        return {
            "freshness": [(r[0], r[1], r[2]) for r in freshness_rows],
            "recent": {r[0]: r[1] for r in recent_rows},
            "now": now,
        }

    async def process(self, data: Any) -> None:
        freshness: list[tuple[str, str, int]] = data["freshness"]
        recent: dict[str, int] = data["recent"]
        now: float = data["now"]

        source_stats: dict[str, dict] = {}
        for source, last_ts, total in freshness:
            age_secs = _age_seconds(last_ts, now)
            expected_max = _EXPECTED_SOURCES.get(source, 7200)
            is_stale = age_secs > expected_max if age_secs is not None else True
            recent_count = recent.get(source, 0)

            source_stats[source] = {
                "last_observation_age_secs": round(age_secs) if age_secs is not None else None,
                "total_observations": total,
                "recent_count": recent_count,
                "expected_max_age_secs": expected_max,
                "is_stale": is_stale,
            }

        # Count healthy vs stale
        stale_sources = [s for s, v in source_stats.items() if v["is_stale"]]
        healthy_sources = [s for s, v in source_stats.items() if not v["is_stale"]]
        total_sources = len(source_stats)
        health_rate = len(healthy_sources) / total_sources if total_sources else 0.0

        # Overall pipeline health observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=health_rate,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "pipeline_health_rate",
                "total_sources": total_sources,
                "healthy_count": len(healthy_sources),
                "stale_count": len(stale_sources),
                "stale_sources": stale_sources[:20],
                "throughput_window_secs": _THROUGHPUT_WINDOW,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Data lineage: {len(healthy_sources)}/{total_sources} sources healthy "
                f"({health_rate:.0%}), {len(stale_sources)} stale"
            ),
            data={
                "metric": "pipeline_health_rate",
                "health_rate": round(health_rate, 3),
                "healthy_sources": healthy_sources,
                "stale_sources": stale_sources,
                "observation_id": obs.id,
            },
        ))

        # Per-source freshness observations (enables correlation with data anomalies)
        obs_count = 1
        for source, stats in sorted(
            source_stats.items(), key=lambda x: x[1].get("is_stale", False), reverse=True
        ):
            age = stats["last_observation_age_secs"]
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(age) if age is not None else -1.0,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "source_freshness",
                    "monitored_source": source,
                    "is_stale": stats["is_stale"],
                    "recent_count": stats["recent_count"],
                    "total_observations": stats["total_observations"],
                    "expected_max_age_secs": stats["expected_max_age_secs"],
                },
            )
            await self.board.store_observation(obs)

            # Only post board messages for stale sources (avoid flooding #raw)
            if stats["is_stale"]:
                age_str = _format_age(age) if age is not None else "never"
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Data lineage STALE: {source} last observation {age_str} ago "
                        f"(expected within {_format_age(stats['expected_max_age_secs'])})"
                    ),
                    data={
                        "metric": "source_freshness",
                        "monitored_source": source,
                        "is_stale": True,
                        "age_secs": age,
                        "recent_count": stats["recent_count"],
                        "observation_id": obs.id,
                    },
                ))
            obs_count += 1

        self.log.info(
            "Data lineage: %d sources tracked, %d healthy, %d stale",
            total_sources, len(healthy_sources), len(stale_sources),
        )


def _ts_to_iso(ts: float) -> str:
    """Convert epoch timestamp to ISO 8601 string."""
    import datetime as dt
    return dt.datetime.fromtimestamp(ts, tz=dt.timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%S"
    )


def _age_seconds(iso_ts: str, now: float) -> float | None:
    """Compute age in seconds from an ISO timestamp to now."""
    import datetime as dt
    try:
        parsed = dt.datetime.fromisoformat(iso_ts.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return now - parsed.timestamp()
    except (ValueError, AttributeError):
        return None


def _format_age(secs: float | None) -> str:
    if secs is None:
        return "unknown"
    if secs < 60:
        return f"{secs:.0f}s"
    if secs < 3600:
        return f"{secs / 60:.0f}min"
    return f"{secs / 3600:.1f}h"


async def ingest_data_lineage(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one data lineage check cycle."""
    ingestor = DataLineageIngestor(board, graph, scheduler)
    await ingestor.run()
