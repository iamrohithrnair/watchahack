"""Pluggable LLM backend — supports Gemini and Z.ai GLM 5.1."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Protocol, runtime_checkable

log = logging.getLogger("cortex.llm")


# ── Rate Limiter ──────────────────────────────────────────────────────────────

class LLMRateLimiter:
    """Token-bucket rate limiter shared across all agents."""

    def __init__(self):
        self._buckets: dict[str, dict[str, Any]] = {}

    def configure(self, tier: str, rpm: int, burst: int | None = None) -> None:
        self._buckets[tier] = {
            "tokens": float(burst or rpm),
            "max": float(burst or rpm),
            "refill_rate": rpm / 60.0,  # tokens per second
            "last_refill": time.monotonic(),
            "lock": asyncio.Lock(),
        }

    async def acquire(self, tier: str = "flash") -> None:
        bucket = self._buckets.get(tier)
        if not bucket:
            return
        async with bucket["lock"]:
            now = time.monotonic()
            elapsed = now - bucket["last_refill"]
            bucket["tokens"] = min(
                bucket["max"],
                bucket["tokens"] + elapsed * bucket["refill_rate"],
            )
            bucket["last_refill"] = now
            if bucket["tokens"] < 1.0:
                wait = (1.0 - bucket["tokens"]) / bucket["refill_rate"]
                log.debug("Rate limit: waiting %.1fs for %s tier", wait, tier)
                await asyncio.sleep(wait)
                bucket["tokens"] = 0.0
            else:
                bucket["tokens"] -= 1.0


# ── Backend Protocol ─────────────────────────────────────────────────────────

@runtime_checkable
class LLMBackend(Protocol):
    async def generate(
        self, prompt: str, *, system: str = "", temperature: float = 0.7,
        max_tokens: int = 4096, tier: str = "flash",
    ) -> str: ...

    async def generate_with_image(
        self, prompt: str, image_bytes: bytes, *, system: str = "",
        mime_type: str = "image/jpeg", temperature: float = 0.4,
        max_tokens: int = 2048, tier: str = "flash",
    ) -> str: ...


# ── Gemini Backend ────────────────────────────────────────────────────────────

class GeminiBackend:
    """Google Gemini via google-genai SDK."""

    def __init__(self, api_key: str, flash_model: str, pro_model: str):
        from google import genai
        self._client = genai.Client(api_key=api_key)
        self._flash_model = flash_model
        self._pro_model = pro_model

    def _model(self, tier: str) -> str:
        return self._pro_model if tier == "pro" else self._flash_model

    async def generate(
        self, prompt: str, *, system: str = "", temperature: float = 0.7,
        max_tokens: int = 4096, tier: str = "flash",
    ) -> str:
        from google.genai import types
        contents = []
        if system:
            contents.append(types.Content(role="user", parts=[types.Part.from_text(text=system)]))
            contents.append(types.Content(role="model", parts=[types.Part.from_text(text="Understood.")]))
        contents.append(types.Content(role="user", parts=[types.Part.from_text(text=prompt)]))
        config = types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
        )
        response = await asyncio.to_thread(
            self._client.models.generate_content,
            model=self._model(tier),
            contents=contents,
            config=config,
        )
        return response.text or ""

    async def generate_with_image(
        self, prompt: str, image_bytes: bytes, *, system: str = "",
        mime_type: str = "image/jpeg", temperature: float = 0.4,
        max_tokens: int = 2048, tier: str = "flash",
    ) -> str:
        from google.genai import types
        parts = [
            types.Part.from_bytes(data=image_bytes, mime_type=mime_type),
            types.Part.from_text(text=prompt),
        ]
        contents = []
        if system:
            contents.append(types.Content(role="user", parts=[types.Part.from_text(text=system)]))
            contents.append(types.Content(role="model", parts=[types.Part.from_text(text="Understood.")]))
        contents.append(types.Content(role="user", parts=parts))
        config = types.GenerateContentConfig(
            temperature=temperature,
            max_output_tokens=max_tokens,
        )
        response = await asyncio.to_thread(
            self._client.models.generate_content,
            model=self._model(tier),
            contents=contents,
            config=config,
        )
        return response.text or ""


# ── GLM Backend (Z.ai GLM 5.1 — OpenAI-compatible) ───────────────────────────

class GLMBackend:
    """Z.ai GLM 5.1 via OpenAI-compatible API."""

    def __init__(
        self, api_key: str, base_url: str = "https://open.bigmodel.cn/api/paas/v4",
        flash_model: str = "glm-4-flash", pro_model: str = "glm-4-plus",
    ):
        import httpx
        self._api_key = api_key
        self._base_url = base_url.rstrip("/")
        self._flash_model = flash_model
        self._pro_model = pro_model
        self._http = httpx.AsyncClient(
            base_url=self._base_url,
            headers={
                "Authorization": f"Bearer {self._api_key}",
                "Content-Type": "application/json",
            },
            timeout=120.0,
        )

    def _model(self, tier: str) -> str:
        return self._pro_model if tier == "pro" else self._flash_model

    async def generate(
        self, prompt: str, *, system: str = "", temperature: float = 0.7,
        max_tokens: int = 4096, tier: str = "flash",
    ) -> str:
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({"role": "user", "content": prompt})
        payload = {
            "model": self._model(tier),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        resp = await self._http.post("/chat/completions", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    async def generate_with_image(
        self, prompt: str, image_bytes: bytes, *, system: str = "",
        mime_type: str = "image/jpeg", temperature: float = 0.4,
        max_tokens: int = 2048, tier: str = "flash",
    ) -> str:
        import base64
        b64 = base64.b64encode(image_bytes).decode()
        image_content = {
            "type": "image_url",
            "image_url": {"url": f"data:{mime_type};base64,{b64}"},
        }
        messages = []
        if system:
            messages.append({"role": "system", "content": system})
        messages.append({
            "role": "user",
            "content": [
                {"type": "text", "text": prompt},
                image_content,
            ],
        })
        payload = {
            "model": self._model(tier),
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }
        resp = await self._http.post("/chat/completions", json=payload)
        resp.raise_for_status()
        data = resp.json()
        return data["choices"][0]["message"]["content"]

    async def close(self) -> None:
        await self._http.aclose()


# ── Factory ───────────────────────────────────────────────────────────────────

def create_llm_backend(
    provider: str | None = None,
    flash_model: str | None = None,
    pro_model: str | None = None,
) -> LLMBackend:
    """Create an LLM backend based on configuration."""
    provider = provider or os.getenv("LLM_PROVIDER", "gemini")

    if provider == "glm":
        api_key = os.getenv("GLM_API_KEY", "")
        if not api_key:
            raise ValueError("GLM_API_KEY environment variable is required for GLM backend")
        base_url = os.getenv("GLM_BASE_URL", "https://open.bigmodel.cn/api/paas/v4")
        flash = flash_model or os.getenv("LLM_FLASH_MODEL", "glm-4-flash")
        pro = pro_model or os.getenv("LLM_PRO_MODEL", "glm-4-plus")
        log.info("Using GLM backend (flash=%s, pro=%s)", flash, pro)
        return GLMBackend(api_key=api_key, base_url=base_url, flash_model=flash, pro_model=pro)

    # Default: Gemini
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise ValueError("GEMINI_API_KEY environment variable is required for Gemini backend")
    flash = flash_model or os.getenv("LLM_FLASH_MODEL", "gemini-2.0-flash")
    pro = pro_model or os.getenv("LLM_PRO_MODEL", "gemini-2.5-pro")
    log.info("Using Gemini backend (flash=%s, pro=%s)", flash, pro)
    return GeminiBackend(api_key=api_key, flash_model=flash, pro_model=pro)


def create_rate_limiter(
    flash_rpm: int = 60, pro_rpm: int = 30,
    flash_burst: int = 10, pro_burst: int = 5,
) -> LLMRateLimiter:
    """Create a configured rate limiter for LLM calls."""
    limiter = LLMRateLimiter()
    limiter.configure("flash", flash_rpm, flash_burst)
    limiter.configure("pro", pro_rpm, pro_burst)
    return limiter
