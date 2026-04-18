"""AsyncScheduler + RateLimiter for managing agent loops and API rate limits."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from typing import Any, Callable, Coroutine

log = logging.getLogger("cortex.scheduler")


class RateLimiter:
    """Token bucket rate limiter for API calls."""

    def __init__(self):
        self._limits: dict[str, int] = {}  # name → requests per minute
        self._tokens: dict[str, float] = {}
        self._last_refill: dict[str, float] = {}

    def configure(self, name: str, rpm: int) -> None:
        self._limits[name] = rpm
        self._tokens[name] = float(rpm)
        self._last_refill[name] = time.monotonic()

    async def acquire(self, name: str) -> None:
        if name not in self._limits:
            return
        while True:
            now = time.monotonic()
            elapsed = now - self._last_refill[name]
            refill = elapsed * (self._limits[name] / 60.0)
            self._tokens[name] = min(self._limits[name], self._tokens[name] + refill)
            self._last_refill[name] = now
            if self._tokens[name] >= 1.0:
                self._tokens[name] -= 1.0
                return
            wait = (1.0 - self._tokens[name]) / (self._limits[name] / 60.0)
            await asyncio.sleep(min(wait, 5.0))


class AsyncScheduler:
    """Manages async agent loops with error isolation and graceful shutdown."""

    def __init__(self):
        self.rate_limiter = RateLimiter()
        self._tasks: dict[str, asyncio.Task] = {}
        self._running = False

    def configure_rate_limit(self, name: str, rpm: int) -> None:
        self.rate_limiter.configure(name, rpm)

    async def run_loop(
        self,
        name: str,
        func: Callable[..., Coroutine],
        interval: float,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        """Run a function in a loop with error isolation."""
        log.info("Starting loop: %s (interval=%ds)", name, interval)
        backoff = 1.0
        while self._running:
            try:
                await func(*args, **kwargs)
                backoff = 1.0  # Reset on success
            except asyncio.CancelledError:
                log.info("Loop %s cancelled", name)
                return
            except Exception:
                log.exception("Error in loop %s (backoff=%.1fs)", name, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 300)
                continue
            await asyncio.sleep(interval)

    async def start(self, loops: list[tuple[str, Callable, float, tuple, dict]]) -> None:
        """Start all loops. Each entry: (name, func, interval, args, kwargs)."""
        self._running = True
        for name, func, interval, args, kwargs in loops:
            task = asyncio.create_task(
                self.run_loop(name, func, interval, *args, **kwargs),
                name=name,
            )
            self._tasks[name] = task
        log.info("Scheduler started %d loops", len(self._tasks))

    async def stop(self) -> None:
        self._running = False
        for name, task in self._tasks.items():
            task.cancel()
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        log.info("Scheduler stopped all loops")

    async def wait(self) -> None:
        await asyncio.gather(*self._tasks.values(), return_exceptions=True)
