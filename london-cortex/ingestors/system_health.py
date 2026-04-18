"""System health ingestor — monitors the system's own logs and task health.

Addresses the Brain's suggestion: integrate internal monitoring data as a
first-class data source so the statistical connector can correlate data
anomalies with concrete system events (failed ingestors, error spikes, etc.).

Data sources (all local, no external API):
- london.log: error/warning rates per component over the last interval
- Coordinator task stats: consecutive errors, last-run staleness
- SQLite DB: message throughput per channel
"""

from __future__ import annotations

import json
import logging
import time
from collections import Counter
from pathlib import Path
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LOG_DIR
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor, get_circuit_states

log = logging.getLogger("cortex.ingestors.system_health")

LOG_FILE = LOG_DIR / "london.log"

# How far back to scan in the log (seconds)
_LOG_WINDOW_SECS = 900  # 15 min


class SystemHealthIngestor(BaseIngestor):
    source_name = "system_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Gather internal system metrics — no HTTP needed."""
        log_stats = _parse_recent_logs(LOG_FILE, _LOG_WINDOW_SECS)
        db_stats = await self._get_db_stats()
        return {"log_stats": log_stats, "db_stats": db_stats}

    async def _get_db_stats(self) -> dict:
        """Count recent messages per channel from the board DB."""
        try:
            import datetime as dt
            cutoff = dt.datetime.fromtimestamp(
                time.time() - _LOG_WINDOW_SECS, tz=dt.timezone.utc
            ).strftime("%Y-%m-%dT%H:%M:%S")
            _cur = await self.board._db.execute(
                "SELECT channel, COUNT(*) as cnt FROM messages "
                "WHERE timestamp > ? GROUP BY channel",
                (cutoff,),
            )
            rows = await _cur.fetchall()
            return {row[0]: row[1] for row in rows}
        except Exception as exc:
            self.log.warning("Failed to query DB stats: %s", exc)
            return {}

    async def process(self, data: Any) -> None:
        log_stats: dict = data.get("log_stats", {})
        db_stats: dict = data.get("db_stats", {})
        obs_count = 0

        # ── Log error/warning rates per component ─────────────────────────
        component_errors = log_stats.get("component_errors", {})
        component_warnings = log_stats.get("component_warnings", {})
        total_errors = log_stats.get("total_errors", 0)
        total_warnings = log_stats.get("total_warnings", 0)
        total_lines = log_stats.get("total_lines", 0)

        # Overall system error rate
        error_rate = total_errors / total_lines if total_lines else 0.0
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=error_rate,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "log_error_rate",
                "total_errors": total_errors,
                "total_warnings": total_warnings,
                "total_lines": total_lines,
                "window_secs": _LOG_WINDOW_SECS,
                "top_error_components": dict(
                    sorted(component_errors.items(), key=lambda x: -x[1])[:10]
                ),
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"System log health: {total_errors} errors, {total_warnings} warnings "
                f"in {total_lines} lines ({error_rate:.1%} error rate)"
            ),
            data={
                "metric": "log_error_rate",
                "error_rate": round(error_rate, 4),
                "total_errors": total_errors,
                "total_warnings": total_warnings,
                "total_lines": total_lines,
                "top_error_components": dict(
                    sorted(component_errors.items(), key=lambda x: -x[1])[:10]
                ),
                "observation_id": obs.id,
            },
        ))
        obs_count += 1

        # Per-component error observations (only for components with errors)
        for component, err_count in sorted(
            component_errors.items(), key=lambda x: -x[1]
        )[:10]:
            warn_count = component_warnings.get(component, 0)
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(err_count),
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "component_errors",
                    "component": component,
                    "error_count": err_count,
                    "warning_count": warn_count,
                    "window_secs": _LOG_WINDOW_SECS,
                },
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"System component errors: {component} had {err_count} errors, "
                    f"{warn_count} warnings"
                ),
                data={
                    "metric": "component_errors",
                    "component": component,
                    "error_count": err_count,
                    "warning_count": warn_count,
                    "observation_id": obs.id,
                },
            ))
            obs_count += 1

        # ── Channel throughput ────────────────────────────────────────────
        if db_stats:
            total_msgs = sum(db_stats.values())
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(total_msgs),
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "channel_throughput",
                    "channels": db_stats,
                    "window_secs": _LOG_WINDOW_SECS,
                },
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"System throughput: {total_msgs} messages across "
                    f"{len(db_stats)} channels in last {_LOG_WINDOW_SECS // 60}min"
                ),
                data={
                    "metric": "channel_throughput",
                    "total_messages": total_msgs,
                    "channels": db_stats,
                    "observation_id": obs.id,
                },
            ))
            obs_count += 1

        # ── Circuit breaker status ─────────────────────────────────────
        circuits = get_circuit_states()
        open_circuits = {
            name: {"failures": cs.failures, "total_trips": cs.total_trips, "cooldown_s": cs.cooldown}
            for name, cs in circuits.items() if cs.is_open
        }
        if open_circuits:
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=len(open_circuits),
                metadata={
                    "metric": "circuit_breakers_open",
                    "open_sources": open_circuits,
                },
            )
            await self.board.store_observation(obs)
            names = ", ".join(open_circuits.keys())
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#system",
                content=(
                    f"Circuit breakers: {len(open_circuits)} sources disabled — {names}"
                ),
                data={"open_circuits": open_circuits, "observation_id": obs.id},
            ))
            obs_count += 1

        self.log.info(
            "System health: %d observations (errors=%d warnings=%d lines=%d)",
            obs_count, total_errors, total_warnings, total_lines,
        )


def _parse_recent_logs(log_path: Path, window_secs: float) -> dict:
    """Parse the JSON-lines log file for recent errors/warnings.

    Returns counts by component and level within the time window.
    """
    result = {
        "total_lines": 0,
        "total_errors": 0,
        "total_warnings": 0,
        "component_errors": Counter(),
        "component_warnings": Counter(),
    }

    if not log_path.exists():
        return result

    cutoff_iso = time.strftime(
        "%Y-%m-%dT%H:%M:%S", time.gmtime(time.time() - window_secs)
    )

    try:
        # Read from the end of the file to avoid scanning the entire log
        # For a 15-min window, we should only need the tail
        with open(log_path, "r") as f:
            # Seek to near the end for large files
            f.seek(0, 2)
            file_size = f.tell()
            # Read last 500KB max — should cover 15 min easily
            read_start = max(0, file_size - 512_000)
            f.seek(read_start)
            if read_start > 0:
                f.readline()  # skip partial line
            lines = f.readlines()
    except OSError as exc:
        log.warning("Failed to read log file: %s", exc)
        return result

    for line in lines:
        line = line.strip()
        if not line:
            continue
        try:
            entry = json.loads(line)
        except json.JSONDecodeError:
            continue

        ts = entry.get("time", "")
        if ts < cutoff_iso:
            continue

        result["total_lines"] += 1
        level = entry.get("level", "").upper()
        component = entry.get("name", "unknown")

        if level == "ERROR":
            result["total_errors"] += 1
            result["component_errors"][component] += 1
        elif level == "WARNING":
            result["total_warnings"] += 1
            result["component_warnings"][component] += 1

    # Convert Counters to plain dicts for JSON serialization
    result["component_errors"] = dict(result["component_errors"])
    result["component_warnings"] = dict(result["component_warnings"])
    return result


async def ingest_system_health(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one system health check cycle."""
    ingestor = SystemHealthIngestor(board, graph, scheduler)
    await ingestor.run()
