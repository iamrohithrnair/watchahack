"""BaseAgent — shared infrastructure for all London Cortex agents."""

from __future__ import annotations

import asyncio
import logging
import random
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL, LLM_PRO_MODEL
from ..core.graph import CortexGraph
from ..core.llm import LLMBackend, LLMRateLimiter, create_llm_backend, create_rate_limiter
from ..core.memory import MemoryManager
from ..core.models import AgentConversation, AgentMessage

log = logging.getLogger("cortex.agents.base")

# Module-level shared LLM backend and rate limiter (lazy-initialised).
_llm_backend: LLMBackend | None = None
_rate_limiter: LLMRateLimiter | None = None


def _get_backend() -> LLMBackend:
    global _llm_backend
    if _llm_backend is None:
        _llm_backend = create_llm_backend()
    return _llm_backend


def _get_rate_limiter() -> LLMRateLimiter:
    global _rate_limiter
    if _rate_limiter is None:
        _rate_limiter = create_rate_limiter()
    return _rate_limiter


class BaseAgent:
    """
    Base class for all London Cortex agents.

    Provides:
    - Pluggable LLM call infrastructure (call_llm) supporting Gemini and GLM backends
    - Rate-limited LLM calls with tiered buckets
    - AgentConversation logging to the message board
    - Helper to read recent channel messages
    - Centralised error handling
    """

    name: str = "base_agent"

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        memory: MemoryManager,
        llm: LLMBackend | None = None,
        rate_limiter: LLMRateLimiter | None = None,
        coordinator: Any | None = None,
    ) -> None:
        self.board = board
        self.graph = graph
        self.memory = memory
        self.llm = llm or _get_backend()
        self.rate_limiter = rate_limiter or _get_rate_limiter()
        self.coordinator = coordinator  # Optional Coordinator reference
        self._pro_calls_this_hour: list[datetime] = []

    # -- LLM ------------------------------------------------------------------

    async def call_llm(
        self,
        prompt: str,
        model: str = LLM_FLASH_MODEL,
        images: list[str | bytes] | None = None,
        trigger: str = "agent_call",
        system_instruction: str | None = None,
        tier: str | None = None,
    ) -> str:
        """
        Call the pluggable LLM backend and log the conversation to the board.

        Parameters
        ----------
        prompt:
            The text prompt.
        model:
            Model ID (default: Flash).
        images:
            Optional list of image URLs (str) or raw bytes.
        trigger:
            Short label describing what triggered this call (for logging).
        system_instruction:
            Optional system prompt.
        tier:
            "flash" or "pro".  If given, overrides model selection.

        Returns
        -------
        str
            The model's text response, or an error string beginning with
            "ERROR:" so callers can detect failures without exceptions.
        """
        # Determine tier from model if not explicitly given
        if tier is None:
            tier = "pro" if model == LLM_PRO_MODEL else "flash"

        # Acquire rate-limit token
        await self.rate_limiter.acquire(tier)

        # Build image bytes list
        image_bytes_list: list[bytes] = []
        if images:
            for img in images:
                if isinstance(img, bytes):
                    image_bytes_list.append(img)
                elif isinstance(img, str):
                    downloaded = await self._download_image(img)
                    if downloaded:
                        image_bytes_list.append(downloaded)

        # Retry with exponential backoff
        max_retries = 5
        base_delay = 2.0
        max_delay = 120.0
        text = ""
        tokens = 0

        for attempt in range(max_retries):
            try:
                system = system_instruction or ""
                if image_bytes_list:
                    # Use the first image for generate_with_image
                    text = await self.llm.generate_with_image(
                        prompt=prompt,
                        image_bytes=image_bytes_list[0],
                        system=system,
                        tier=tier,
                    )
                else:
                    text = await self.llm.generate(
                        prompt=prompt,
                        system=system,
                        tier=tier,
                    )
                break  # success

            except Exception as exc:
                exc_str = str(exc).lower()
                retryable = (
                    "429" in exc_str or "500" in exc_str or "503" in exc_str
                    or "quota" in exc_str or "rate" in exc_str or "resource" in exc_str
                )
                if retryable and attempt < max_retries - 1:
                    delay = min(base_delay * (2 ** attempt) + random.uniform(0, 1), max_delay)
                    log.warning(
                        "[%s] LLM call failed (attempt %d/%d, retrying in %.1fs): %s",
                        self.name, attempt + 1, max_retries, delay, exc,
                    )
                    await asyncio.sleep(delay)
                else:
                    log.warning("[%s] LLM call failed (%s): %s", self.name, model, exc)
                    text = f"ERROR: {exc}"
                    tokens = 0
                    break

        # Log conversation to board
        conv = AgentConversation(
            agent_name=self.name,
            trigger=trigger,
            model_used=model,
            prompt=prompt[:4000],  # truncate for storage
            response=text[:4000],
            tokens_used=tokens,
        )
        try:
            await self.board.log_conversation(conv)
        except Exception as log_exc:
            log.warning("[%s] Failed to log conversation: %s", self.name, log_exc)

        return text

    def pro_calls_remaining_this_hour(self, max_per_hour: int = 999999) -> int:
        """Track and return remaining Pro model budget for this hour."""
        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(hours=1)
        self._pro_calls_this_hour = [t for t in self._pro_calls_this_hour if t > cutoff]
        return max(0, max_per_hour - len(self._pro_calls_this_hour))

    def record_pro_call(self) -> None:
        self._pro_calls_this_hour.append(datetime.now(timezone.utc))

    # -- Image download -------------------------------------------------------

    async def _download_image(self, url: str, timeout_s: int = 10) -> bytes | None:
        """Download image bytes from a URL for passing to the LLM."""
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=timeout_s)
            ) as session:
                async with session.get(url) as resp:
                    if resp.status == 200:
                        return await resp.read()
                    log.debug("[%s] Image download HTTP %d: %s", self.name, resp.status, url)
        except Exception as exc:
            log.debug("[%s] Image download failed: %s %s", self.name, url, exc)
        return None

    # -- Board helpers --------------------------------------------------------

    async def read_channel(
        self,
        channel: str,
        since_hours: float = 1.0,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        """Read recent messages from a board channel (retries once on DB contention)."""
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        for attempt in range(2):
            try:
                return await self.board.read_channel(channel, since=since, limit=limit)
            except Exception as exc:
                if attempt == 0:
                    await asyncio.sleep(1)  # brief wait for DB contention
                    continue
                log.warning("[%s] Failed to read channel %s: %s", self.name, channel, exc)
                return []
        return []

    async def post(
        self,
        channel: str,
        content: str,
        data: dict[str, Any] | None = None,
        priority: int = 1,
        location_id: str | None = None,
        references: list[str] | None = None,
        to_agent: str | None = None,
        ttl_hours: int = 6,
    ) -> str:
        """Post a message to a channel."""
        msg = AgentMessage(
            from_agent=self.name,
            channel=channel,
            content=content,
            data=data or {},
            priority=priority,
            location_id=location_id,
            references=references or [],
            to_agent=to_agent,
            ttl_hours=ttl_hours,
        )
        try:
            return await self.board.post(msg)
        except Exception as exc:
            log.error("[%s] Failed to post to %s: %s", self.name, channel, exc)
            return ""

    # -- Investigation Threads -------------------------------------------------

    async def start_investigation(
        self, trigger: str, question: str, target_agent: str, data: dict | None = None,
    ) -> str:
        """Start a multi-step investigation thread."""
        return await self.board.start_investigation(self.name, trigger, question, data)

    async def continue_investigation(
        self, thread_id: str, action: str, finding: str,
        next_question: str | None = None, next_agent: str | None = None, data: dict | None = None,
    ):
        """Add your step to an investigation and optionally hand off to next agent."""
        await self.board.add_investigation_step(
            thread_id, self.name, action, finding, next_question, next_agent, data,
        )

    async def get_my_investigations(self) -> list:
        """Get investigation threads waiting for this agent."""
        return await self.board.get_pending_investigations(self.name)

    # -- Agent-to-Agent Messaging ---------------------------------------------

    async def ask_agent(self, target_agent: str, question: str, data: dict | None = None) -> str:
        """Send a directed one-off message to another agent."""
        return await self.post(
            channel="#requests",
            content=f"[FROM {self.name}] {question}",
            data={"request_type": "agent_question", "from_agent": self.name, **(data or {})},
            to_agent=target_agent,
            priority=2,
            ttl_hours=3,
        )

    async def respond_to(self, original_msg_id: str, response: str, data: dict | None = None) -> str:
        """Respond to a directed message from another agent."""
        return await self.post(
            channel="#requests",
            content=f"[RESPONSE from {self.name}] {response}",
            data={"request_type": "agent_response", "in_reply_to": original_msg_id, **(data or {})},
            priority=2,
            ttl_hours=3,
        )

    async def request_claude(self, question: str, context: str = "", data: dict | None = None) -> str:
        """Post a request for Claude Code daemon investigation."""
        return await self.post(
            channel="#requests",
            content=f"[CLAUDE REQUEST from {self.name}] {question}",
            data={
                "request_type": "claude_investigation",
                "question": question,
                "context": context,
                "from_agent": self.name,
                **(data or {}),
            },
            priority=3,
            ttl_hours=6,
        )

    # -- Activity / Reasoning Posts -------------------------------------------

    async def think(
        self,
        thought: str,
        channel: str = "#meta",
        data: dict[str, Any] | None = None,
    ) -> str:
        """Post a reasoning/activity update so other agents (and humans) can see what we're doing."""
        return await self.post(
            channel=channel,
            content=f"[{self.name.upper()}] {thought}",
            data={"type": "agent_reasoning", **(data or {})},
            priority=1,
            ttl_hours=2,
        )

    # -- Utility --------------------------------------------------------------

    def _summarise_messages(self, messages: list[dict[str, Any]], max_chars: int = 3000) -> str:
        """Build a compact text summary of board messages for LLM context."""
        lines: list[str] = []
        for m in messages:
            ts = m.get("timestamp", "")[:16]
            agent = m.get("from_agent", "?")
            content = m.get("content", "")
            loc = m.get("location_id", "")
            line = f"[{ts}] {agent}"
            if loc:
                line += f" @{loc}"
            line += f": {content}"
            lines.append(line)
        combined = "\n".join(lines)
        return combined[:max_chars]
