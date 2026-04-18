"""Application Performance Monitoring ingestor — process-level resource metrics.

Provides leading indicators of component failure that system_health.py doesn't
cover: memory usage/growth, CPU usage, event loop latency, GC pressure, and
per-task timing from the coordinator. These metrics enable the statistical
connector to predict and correlate degradation events (memory leaks, latency
spikes) with data quality issues before they become outages.

Data sources (all local, no external API):
- os/resource: RSS memory, CPU time
- gc module: garbage collection stats
- asyncio: event loop responsiveness (measured via scheduled-vs-actual delay)
- Coordinator task registry: per-task error counts, last-run timestamps
"""

from __future__ import annotations

import asyncio
import gc
import logging
import os
import resource
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.apm")

# Thresholds for flagging in metadata (not for anomaly detection — that's
# handled by the numeric interpreter via z-scores on the baseline).
_RSS_WARNING_MB = 1024  # 1 GB
_LOOP_LAG_WARNING_MS = 100  # 100ms event loop lag is concerning


class APMIngestor(BaseIngestor):
    source_name = "apm"
    rate_limit_name = "default"

    _prev_cpu_user: float | None = None
    _prev_cpu_sys: float | None = None
    _prev_time: float | None = None

    async def fetch_data(self) -> Any:
        """Gather process-level metrics — no HTTP needed."""
        now = time.monotonic()

        # Memory via resource module (cross-platform enough for Linux/macOS)
        usage = resource.getrusage(resource.RUSAGE_SELF)
        # ru_maxrss is in KB on Linux, bytes on macOS
        rss_kb = usage.ru_maxrss
        if os.uname().sysname == "Darwin":
            rss_kb = rss_kb // 1024  # macOS reports bytes
        rss_mb = rss_kb / 1024.0

        # CPU utilization since last sample
        cpu_user = usage.ru_utime
        cpu_sys = usage.ru_stime
        cpu_pct = None
        if self._prev_time is not None:
            elapsed = now - self._prev_time
            if elapsed > 0:
                delta_cpu = (cpu_user - (self._prev_cpu_user or 0)) + (
                    cpu_sys - (self._prev_cpu_sys or 0)
                )
                cpu_pct = (delta_cpu / elapsed) * 100.0
        self._prev_cpu_user = cpu_user
        self._prev_cpu_sys = cpu_sys
        self._prev_time = now

        # GC stats
        gc_stats = gc.get_stats()  # list of dicts per generation
        gc_counts = gc.get_count()  # (gen0, gen1, gen2) uncollected counts

        # Event loop latency: schedule a callback and measure delay
        loop_lag_ms = await self._measure_loop_lag()

        # Open file descriptors (Linux/macOS)
        try:
            fd_count = len(os.listdir(f"/proc/{os.getpid()}/fd"))
        except (FileNotFoundError, PermissionError):
            # macOS fallback — count isn't easily available without lsof
            fd_count = None

        return {
            "rss_mb": round(rss_mb, 1),
            "cpu_user_s": round(cpu_user, 2),
            "cpu_sys_s": round(cpu_sys, 2),
            "cpu_pct": round(cpu_pct, 1) if cpu_pct is not None else None,
            "gc_counts": gc_counts,
            "gc_collections": [s.get("collections", 0) for s in gc_stats],
            "gc_collected": [s.get("collected", 0) for s in gc_stats],
            "gc_uncollectable": [s.get("uncollectable", 0) for s in gc_stats],
            "loop_lag_ms": round(loop_lag_ms, 2),
            "fd_count": fd_count,
            "thread_count": _get_thread_count(),
        }

    @staticmethod
    async def _measure_loop_lag() -> float:
        """Measure event loop responsiveness by timing a sleep(0)."""
        t0 = time.monotonic()
        await asyncio.sleep(0)
        return (time.monotonic() - t0) * 1000.0  # ms

    async def process(self, data: Any) -> None:
        obs_count = 0

        # ── RSS Memory ─────────────────────────────────────────────────────
        rss_mb = data["rss_mb"]
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=rss_mb,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "rss_memory_mb",
                "warning": rss_mb > _RSS_WARNING_MB,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"APM memory: {rss_mb:.0f} MB RSS",
            data={"metric": "rss_memory_mb", "value": rss_mb, "observation_id": obs.id},
        ))
        obs_count += 1

        # ── CPU utilization ────────────────────────────────────────────────
        cpu_pct = data.get("cpu_pct")
        if cpu_pct is not None:
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=cpu_pct,
                location_id=None,
                lat=None,
                lon=None,
                metadata={"metric": "cpu_percent"},
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"APM CPU: {cpu_pct:.1f}%",
                data={"metric": "cpu_percent", "value": cpu_pct, "observation_id": obs.id},
            ))
            obs_count += 1

        # ── Event loop latency ─────────────────────────────────────────────
        loop_lag = data["loop_lag_ms"]
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=loop_lag,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "event_loop_lag_ms",
                "warning": loop_lag > _LOOP_LAG_WARNING_MS,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"APM event loop lag: {loop_lag:.1f} ms",
            data={"metric": "event_loop_lag_ms", "value": loop_lag, "observation_id": obs.id},
        ))
        obs_count += 1

        # ── GC pressure ────────────────────────────────────────────────────
        gc_uncollectable = sum(data.get("gc_uncollectable", [0, 0, 0]))
        gc_total_collected = sum(data.get("gc_collected", [0, 0, 0]))
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=float(gc_uncollectable),
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "gc_uncollectable",
                "gc_counts": list(data.get("gc_counts", [])),
                "gc_collections": data.get("gc_collections", []),
                "gc_collected_total": gc_total_collected,
            },
        )
        await self.board.store_observation(obs)
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"APM GC: {gc_uncollectable} uncollectable, "
                f"{gc_total_collected} collected this cycle"
            ),
            data={
                "metric": "gc_uncollectable",
                "value": gc_uncollectable,
                "gc_collected_total": gc_total_collected,
                "observation_id": obs.id,
            },
        ))
        obs_count += 1

        # ── File descriptors / threads ─────────────────────────────────────
        fd_count = data.get("fd_count")
        thread_count = data.get("thread_count")
        if fd_count is not None or thread_count is not None:
            value = float(fd_count or thread_count or 0)
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "resource_handles",
                    "fd_count": fd_count,
                    "thread_count": thread_count,
                },
            )
            await self.board.store_observation(obs)
            parts = []
            if fd_count is not None:
                parts.append(f"{fd_count} FDs")
            if thread_count is not None:
                parts.append(f"{thread_count} threads")
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"APM resources: {', '.join(parts)}",
                data={
                    "metric": "resource_handles",
                    "fd_count": fd_count,
                    "thread_count": thread_count,
                    "observation_id": obs.id,
                },
            ))
            obs_count += 1

        self.log.info(
            "APM: %d observations (rss=%.0fMB cpu=%s loop_lag=%.1fms gc_uncollectable=%d)",
            obs_count,
            rss_mb,
            f"{cpu_pct:.1f}%" if cpu_pct is not None else "N/A",
            loop_lag,
            gc_uncollectable,
        )


def _get_thread_count() -> int | None:
    """Get active thread count."""
    try:
        import threading
        return threading.active_count()
    except Exception:
        return None


async def ingest_apm(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one APM metrics cycle."""
    ingestor = APMIngestor(board, graph, scheduler)
    await ingestor.run()
