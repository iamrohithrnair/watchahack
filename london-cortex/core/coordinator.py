"""Coordinator — adaptive scheduling with event-driven triggers.

Replaces fixed-interval scheduling with interval ranges, on-demand data
fetches, anomaly-triggered speedups, and error backoff.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import deque
from datetime import datetime, timezone
from typing import Any, Callable, Coroutine

from .models import CoordinatorEvent, TaskEntry
from .scheduler import RateLimiter

log = logging.getLogger("cortex.coordinator")

_PROBE_TIMEOUT = 20.0  # seconds per ingestor health check
_PROBE_BLACKLIST_COOLDOWN = 1800.0  # 30 min cooldown for failed probes


class Coordinator:
    """Wraps AsyncScheduler with adaptive scheduling and event-driven triggers."""

    def __init__(self):
        self.rate_limiter = RateLimiter()
        self._tasks: dict[str, TaskEntry] = {}
        self._async_tasks: dict[str, asyncio.Task] = {}
        self._event_queue: deque[CoordinatorEvent] = deque(maxlen=500)
        self._running = False
        # Map source names to task names for routing events
        self._source_to_task: dict[str, str] = {}

    def configure_rate_limit(self, name: str, rpm: int) -> None:
        self.rate_limiter.configure(name, rpm)

    def register(
        self,
        name: str,
        func: Callable[..., Coroutine],
        min_interval: float,
        max_interval: float,
        priority: int = 5,
        args: tuple = (),
        kwargs: dict[str, Any] | None = None,
        source_names: list[str] | None = None,
    ) -> None:
        """Register a task with interval range instead of fixed value."""
        entry = TaskEntry(
            name=name,
            func=func,
            min_interval=min_interval,
            max_interval=max_interval,
            current_interval=max_interval,  # start slow
            priority=priority,
            args=args,
            kwargs=kwargs or {},
            module_name=getattr(func, "__module__", "") or "",
            func_name=getattr(func, "__name__", "") or "",
        )
        self._tasks[name] = entry
        # Map source names to this task for event routing
        entry._source_names = source_names or []
        if source_names:
            for sn in source_names:
                self._source_to_task[sn] = name

    def get_tasks_for_module(self, module_name: str) -> list[TaskEntry]:
        """Return all tasks whose func came from the given module."""
        return [e for e in self._tasks.values() if e.module_name == module_name]

    def get_all_agent_modules(self) -> set[str]:
        """Return the set of all module names across registered tasks."""
        return {e.module_name for e in self._tasks.values() if e.module_name}

    def request_data(self, source: str, reason: str = "") -> None:
        """Agents can trigger on-demand data fetches."""
        event = CoordinatorEvent(
            event_type="data_request",
            source=source,
            reason=reason,
        )
        self._event_queue.append(event)
        log.info("Data request: source=%s reason=%s", source, reason)

    def notify_anomaly(self, source: str, severity: int = 1) -> None:
        """Anomaly detection speeds up related ingestors."""
        event = CoordinatorEvent(
            event_type="anomaly_detected",
            source=source,
            severity=severity,
        )
        self._event_queue.append(event)
        log.info("Anomaly notified: source=%s severity=%d", source, severity)

    async def run_startup_probes(self) -> dict[str, bool]:
        """Probe all ingestor tasks in parallel before starting the main loop.

        Tasks that fail the probe get their circuit breaker opened immediately
        with a 30-min cooldown so they don't waste resources on the first cycles.
        Returns {task_name: passed}.
        """
        from ..ingestors.base import get_circuit_states, _CircuitState

        # Only probe tasks that have source_names (i.e., ingestors)
        ingestor_tasks = {
            name: entry for name, entry in self._tasks.items()
            if getattr(entry, "_source_names", None)
        }
        if not ingestor_tasks:
            return {}

        log.info("Running startup probes for %d ingestors...", len(ingestor_tasks))
        results: dict[str, bool] = {}

        async def _probe_one(name: str, entry: TaskEntry) -> tuple[str, bool, str]:
            try:
                await asyncio.wait_for(
                    entry.func(*entry.args, **entry.kwargs),
                    timeout=_PROBE_TIMEOUT,
                )
                return name, True, ""
            except asyncio.TimeoutError:
                return name, False, "timeout"
            except Exception as exc:
                return name, False, str(exc)[:120]

        # Run all probes concurrently
        probe_tasks = [_probe_one(n, e) for n, e in ingestor_tasks.items()]
        probe_results = await asyncio.gather(*probe_tasks, return_exceptions=True)

        circuit_states = get_circuit_states()
        passed = 0
        failed = 0

        for result in probe_results:
            if isinstance(result, Exception):
                continue
            name, ok, detail = result
            results[name] = ok
            source_names = getattr(self._tasks[name], "_source_names", [])

            if ok:
                passed += 1
                log.info("  PASS: %s", name)
            else:
                failed += 1
                log.warning("  FAIL: %s (%s) — blacklisted for 30 min", name, detail)
                # Open circuit breaker for all source_names of this task
                for sn in source_names:
                    if sn not in circuit_states:
                        circuit_states[sn] = _CircuitState()
                    cb = circuit_states[sn]
                    cb.is_open = True
                    cb.open_since = __import__("time").monotonic()
                    cb.cooldown = _PROBE_BLACKLIST_COOLDOWN
                    cb.failures = 3  # mark as if threshold was hit
                    cb.total_trips += 1

        log.info(
            "Startup probes complete: %d passed, %d failed out of %d",
            passed, failed, len(ingestor_tasks),
        )
        return results

    async def start(self) -> None:
        """Start all registered tasks and the event processor."""
        self._running = True
        for name, entry in self._tasks.items():
            task = asyncio.create_task(
                self._run_loop(entry),
                name=name,
            )
            self._async_tasks[name] = task
        # Start event processor
        self._async_tasks["_event_processor"] = asyncio.create_task(
            self._process_events(),
            name="_event_processor",
        )
        log.info("Coordinator started %d tasks", len(self._tasks))

    async def stop(self) -> None:
        """Stop all tasks gracefully with a timeout."""
        self._running = False
        for name, task in self._async_tasks.items():
            task.cancel()
        try:
            await asyncio.wait_for(
                asyncio.gather(*self._async_tasks.values(), return_exceptions=True),
                timeout=3.0,
            )
        except asyncio.TimeoutError:
            log.warning("Some tasks did not cancel within 3s")
        log.info("Coordinator stopped all tasks")

    async def wait(self) -> None:
        await asyncio.gather(*self._async_tasks.values(), return_exceptions=True)

    def get_task_health(self) -> dict[str, dict[str, Any]]:
        """Get health info for all tasks (for daemon monitoring)."""
        health = {}
        for name, entry in self._tasks.items():
            health[name] = {
                "consecutive_errors": entry.consecutive_errors,
                "last_run": entry.last_run.isoformat() if entry.last_run else None,
                "last_success": entry.last_success.isoformat() if entry.last_success else None,
                "current_interval": entry.current_interval,
                "min_interval": entry.min_interval,
                "max_interval": entry.max_interval,
            }
        return health

    def restart_task(self, task_name: str, new_func: Any = None) -> bool:
        """Restart a specific task, optionally with a new function reference.

        After hot-reload, the old function object is stale — pass new_func
        to replace it so the restarted task runs the updated code.
        """
        if task_name not in self._tasks:
            return False
        entry = self._tasks[task_name]
        # Cancel existing
        if task_name in self._async_tasks:
            self._async_tasks[task_name].cancel()
        # Update function reference if provided (critical for hot-reload)
        if new_func is not None:
            entry.func = new_func
        # Reset error state
        entry.consecutive_errors = 0
        entry.current_interval = entry.max_interval
        # Restart
        task = asyncio.create_task(
            self._run_loop(entry),
            name=task_name,
        )
        self._async_tasks[task_name] = task
        log.info("Restarted task: %s", task_name)
        return True

    async def _run_loop(self, entry: TaskEntry) -> None:
        """Run a task in an adaptive loop."""
        log.info("Starting task: %s (interval=%.0f-%.0fs)", entry.name, entry.min_interval, entry.max_interval)
        while self._running:
            try:
                entry.last_run = datetime.now(timezone.utc)
                await entry.func(*entry.args, **entry.kwargs)
                entry.last_success = datetime.now(timezone.utc)
                entry.consecutive_errors = 0
                # Gradually return to max_interval during quiet periods
                entry.current_interval = min(
                    entry.current_interval * 1.1,
                    entry.max_interval,
                )
            except asyncio.CancelledError:
                log.info("Task %s cancelled", entry.name)
                return
            except Exception:
                entry.consecutive_errors += 1
                log.exception(
                    "Error in task %s (errors=%d, interval=%.0fs)",
                    entry.name, entry.consecutive_errors, entry.current_interval,
                )
                # Error backoff: increase interval
                backoff_factor = min(2 ** entry.consecutive_errors, 10)
                entry.current_interval = min(
                    entry.current_interval * backoff_factor,
                    entry.max_interval * 3,  # can exceed max during errors
                )
            await asyncio.sleep(entry.current_interval)

    async def _process_events(self) -> None:
        """Process event queue — data requests and anomaly speedups."""
        while self._running:
            try:
                while self._event_queue:
                    event = self._event_queue.popleft()
                    await self._handle_event(event)
            except asyncio.CancelledError:
                return
            except Exception:
                log.exception("Error processing events")
            await asyncio.sleep(2)  # check queue every 2s

    async def _handle_event(self, event: CoordinatorEvent) -> None:
        """Handle a single coordinator event."""
        if event.event_type == "data_request":
            # Find and immediately trigger the related task
            task_name = self._source_to_task.get(event.source)
            if task_name and task_name in self._tasks:
                entry = self._tasks[task_name]
                log.info(
                    "On-demand trigger: %s (requested by: %s)",
                    task_name, event.reason,
                )
                # Force immediate run by reducing interval
                entry.current_interval = entry.min_interval

        elif event.event_type == "anomaly_detected":
            # Speed up related ingestors
            task_name = self._source_to_task.get(event.source)
            if task_name and task_name in self._tasks:
                entry = self._tasks[task_name]
                # Speed up proportional to severity
                speedup = max(0.3, 1.0 - event.severity * 0.15)
                new_interval = max(entry.min_interval, entry.current_interval * speedup)
                log.info(
                    "Anomaly speedup: %s interval %.0f -> %.0f (severity=%d)",
                    task_name, entry.current_interval, new_interval, event.severity,
                )
                entry.current_interval = new_interval
