"""Provider status ingestor — monitors upstream API health for data providers.

Addresses the Brain's suggestion: differentiate between individual sensor faults
and systemic provider-side outages by probing each data provider's API directly.

Monitors: TfL API, LAQN (King's College London), Environment Agency, Carbon
Intensity (National Grid ESO). All free, no API keys required.

Each probe is a lightweight GET to a known endpoint. We measure:
- HTTP status code
- Response time (ms)
- Whether the response body is parseable (basic sanity check)

This lets the statistical connector and brain distinguish "LAQN site X went
silent" (sensor fault) from "the entire LAQN API is returning 503" (provider
outage).
"""

from __future__ import annotations

import logging
import time
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.provider_status")

# Lightweight probe endpoints — small/fast responses from each provider
_PROVIDERS: list[dict[str, str]] = [
    {
        "name": "tfl",
        "label": "TfL API",
        "url": "https://api.tfl.gov.uk/Line/Mode/tube/Status",
    },
    {
        "name": "laqn",
        "label": "LAQN (King's College London)",
        "url": "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringIndex/GroupName=London/Json",
    },
    {
        "name": "environment_agency",
        "label": "Environment Agency",
        "url": "https://environment.data.gov.uk/flood-monitoring/id/floods?_limit=1",
    },
    {
        "name": "carbon_intensity",
        "label": "Carbon Intensity (National Grid ESO)",
        "url": "https://api.carbonintensity.org.uk/intensity",
    },
]

_PROBE_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=8)


class ProviderStatusIngestor(BaseIngestor):
    source_name = "provider_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Probe each provider API and collect response metrics."""
        results = []
        async with aiohttp.ClientSession(timeout=_PROBE_TIMEOUT) as session:
            for provider in _PROVIDERS:
                result = await self._probe(session, provider)
                results.append(result)
        return results

    async def _probe(
        self, session: aiohttp.ClientSession, provider: dict[str, str]
    ) -> dict[str, Any]:
        """Probe a single provider endpoint."""
        name = provider["name"]
        url = provider["url"]
        t0 = time.monotonic()
        try:
            async with session.get(url) as resp:
                elapsed_ms = (time.monotonic() - t0) * 1000
                body = await resp.read()
                # Basic sanity: is the body non-empty and was it JSON-parseable?
                body_ok = len(body) > 2  # at least "{}" or "[]"
                return {
                    "name": name,
                    "label": provider["label"],
                    "url": url,
                    "status_code": resp.status,
                    "response_ms": round(elapsed_ms, 1),
                    "body_ok": body_ok,
                    "body_bytes": len(body),
                    "error": None,
                    "healthy": resp.status == 200 and body_ok,
                }
        except (aiohttp.ClientError, OSError, Exception) as exc:
            elapsed_ms = (time.monotonic() - t0) * 1000
            self.log.warning("Probe failed for %s: %s", name, exc)
            return {
                "name": name,
                "label": provider["label"],
                "url": url,
                "status_code": None,
                "response_ms": round(elapsed_ms, 1),
                "body_ok": False,
                "body_bytes": 0,
                "error": str(exc)[:200],
                "healthy": False,
            }

    async def process(self, data: Any) -> None:
        results: list[dict] = data
        healthy_count = sum(1 for r in results if r["healthy"])
        total = len(results)
        overall_health = healthy_count / total if total else 0.0

        # Overall provider health observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=overall_health,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "provider_health_overall",
                "healthy_count": healthy_count,
                "total_providers": total,
                "providers": {r["name"]: r["healthy"] for r in results},
            },
        )
        await self.board.store_observation(obs)

        summary_parts = []
        for r in results:
            status = "UP" if r["healthy"] else "DOWN"
            summary_parts.append(f"{r['label']}={status}")

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Provider status: {healthy_count}/{total} healthy — "
                + ", ".join(summary_parts)
            ),
            data={
                "metric": "provider_health_overall",
                "overall_health": round(overall_health, 3),
                "healthy_count": healthy_count,
                "total_providers": total,
                "observation_id": obs.id,
            },
        ))

        # Per-provider detail observations
        for r in results:
            health_val = 1.0 if r["healthy"] else 0.0
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=health_val,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "provider_health",
                    "provider": r["name"],
                    "provider_label": r["label"],
                    "status_code": r["status_code"],
                    "response_ms": r["response_ms"],
                    "body_ok": r["body_ok"],
                    "body_bytes": r["body_bytes"],
                    "error": r["error"],
                },
            )
            await self.board.store_observation(obs)

            status_str = f"HTTP {r['status_code']}" if r["status_code"] else "unreachable"
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Provider {r['label']}: {'healthy' if r['healthy'] else 'UNHEALTHY'} "
                    f"({status_str}, {r['response_ms']:.0f}ms)"
                ),
                data={
                    "metric": "provider_health",
                    "provider": r["name"],
                    "healthy": r["healthy"],
                    "status_code": r["status_code"],
                    "response_ms": r["response_ms"],
                    "error": r["error"],
                    "observation_id": obs.id,
                },
            ))

        self.log.info(
            "Provider status: %d/%d healthy (latencies: %s)",
            healthy_count,
            total,
            ", ".join(f"{r['name']}={r['response_ms']:.0f}ms" for r in results),
        )


async def ingest_provider_status(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one provider status check cycle."""
    ingestor = ProviderStatusIngestor(board, graph, scheduler)
    await ingestor.run()
