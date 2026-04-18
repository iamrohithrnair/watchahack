"""BaseIngestor — shared async HTTP client, circuit breaker, and error handling."""

from __future__ import annotations

import asyncio
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage
from ..core.scheduler import AsyncScheduler

log = logging.getLogger("cortex.ingestors.base")

_DEFAULT_TIMEOUT = aiohttp.ClientTimeout(total=30, connect=10)
_MAX_RETRIES = 3
_BACKOFF_BASE = 2.0

_CB_FAILURE_THRESHOLD = 3
_CB_INITIAL_COOLDOWN = 300
_CB_MAX_COOLDOWN = 3600


class _CircuitState:
    __slots__ = ("failures", "cooldown", "open_since", "is_open", "total_trips")

    def __init__(self):
        self.failures: int = 0
        self.cooldown: float = _CB_INITIAL_COOLDOWN
        self.open_since: float = 0.0
        self.is_open: bool = False
        self.total_trips: int = 0


import sys as _sys

_CB_REGISTRY_KEY = "_cortex_circuit_states"
if not hasattr(_sys, _CB_REGISTRY_KEY):
    setattr(_sys, _CB_REGISTRY_KEY, {})
_circuit_states: dict[str, _CircuitState] = getattr(_sys, _CB_REGISTRY_KEY)


def get_circuit_states() -> dict[str, _CircuitState]:
    return _circuit_states


class BaseIngestor(ABC):
    """Abstract base for all Cortex ingestors.

    Subclasses must implement:
        async def fetch_data(self) -> Any
        async def process(self, data: Any) -> None
    """

    source_name: str = "base"
    rate_limit_name: str = "default"
    required_env_vars: list[str] = []

    @classmethod
    def check_keys(cls) -> tuple[bool, list[str]]:
        import os
        missing = [v for v in cls.required_env_vars if not os.environ.get(v, "").strip()]
        return (len(missing) == 0, missing)

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        self.board = board
        self.graph = graph
        self.scheduler = scheduler
        self.log = logging.getLogger(f"cortex.ingestors.{self.source_name}")
        if self.source_name not in _circuit_states:
            _circuit_states[self.source_name] = _CircuitState()

    async def fetch(
        self,
        url: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        timeout: aiohttp.ClientTimeout | None = None,
        retries: int = _MAX_RETRIES,
    ) -> Any:
        """GET a URL with exponential backoff retries."""
        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        _timeout = timeout or _DEFAULT_TIMEOUT
        backoff = _BACKOFF_BASE
        last_exc: Exception | None = None

        for attempt in range(1, retries + 1):
            try:
                async with aiohttp.ClientSession(timeout=_timeout) as session:
                    async with session.get(url, params=params, headers=headers) as resp:
                        if resp.status == 200:
                            try:
                                return await resp.json(content_type=None)
                            except Exception:
                                return await resp.text()
                        elif resp.status == 429:
                            self.log.warning("Rate-limited by %s (attempt %d/%d)", url, attempt, retries)
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120.0)
                        else:
                            self.log.warning("HTTP %d from %s (attempt %d/%d)", resp.status, url, attempt, retries)
                            last_exc = RuntimeError(f"HTTP {resp.status}")
                            await asyncio.sleep(backoff)
                            backoff = min(backoff * 2, 120.0)
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_exc = exc
                self.log.warning("Network error fetching %s (attempt %d/%d): %s", url, attempt, retries, exc)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 120.0)

        self.log.error("All %d retries exhausted for %s: %s", retries, url, last_exc)
        return None

    @abstractmethod
    async def fetch_data(self) -> Any:
        """Fetch raw data from external source."""

    @abstractmethod
    async def process(self, data: Any) -> None:
        """Process fetched data, store observations, post to board."""

    async def health_check(self) -> tuple[bool, str]:
        try:
            data = await asyncio.wait_for(self.fetch_data(), timeout=15.0)
            if data is None:
                return False, "fetch returned None"
            return True, "ok"
        except asyncio.TimeoutError:
            return False, "timeout (15s)"
        except Exception as exc:
            return False, str(exc)[:120]

    def _get_circuit(self) -> _CircuitState:
        return _circuit_states[self.source_name]

    async def _trip_circuit(self, cb: _CircuitState) -> None:
        cb.is_open = True
        cb.open_since = time.monotonic()
        cb.total_trips += 1
        self.log.warning(
            "CIRCUIT OPEN for %s after %d failures — skipping for %.0f min",
            self.source_name, cb.failures, cb.cooldown / 60,
        )
        await self.board.post(AgentMessage(
            from_agent="circuit_breaker", channel="#system",
            content=f"Circuit breaker OPEN for {self.source_name}: {cb.failures} failures",
            data={"source": self.source_name, "failures": cb.failures, "cooldown_seconds": cb.cooldown},
        ))

    async def _close_circuit(self, cb: _CircuitState) -> None:
        was_open = cb.is_open
        cb.failures = 0
        cb.is_open = False
        cb.cooldown = _CB_INITIAL_COOLDOWN
        if was_open:
            self.log.info("CIRCUIT CLOSED for %s — recovered", self.source_name)
            await self.board.post(AgentMessage(
                from_agent="circuit_breaker", channel="#system",
                content=f"Circuit breaker CLOSED for {self.source_name}: recovered",
                data={"source": self.source_name},
            ))

    async def run(self) -> None:
        """Execute one fetch-process cycle with circuit breaker protection."""
        cb = self._get_circuit()

        if cb.is_open:
            elapsed = time.monotonic() - cb.open_since
            if elapsed < cb.cooldown:
                return
            self.log.info("Circuit half-open for %s — probing...", self.source_name)

        try:
            data = await self.fetch_data()
            if data is None:
                cb.failures += 1
                if cb.failures >= _CB_FAILURE_THRESHOLD and not cb.is_open:
                    await self._trip_circuit(cb)
                elif cb.is_open:
                    cb.cooldown = min(cb.cooldown * 2, _CB_MAX_COOLDOWN)
                    cb.open_since = time.monotonic()
                return

            await self.process(data)
            await self._close_circuit(cb)
        except Exception:
            self.log.exception("Unhandled error in ingestor %s", self.source_name)
            cb.failures += 1
            if cb.failures >= _CB_FAILURE_THRESHOLD and not cb.is_open:
                await self._trip_circuit(cb)
