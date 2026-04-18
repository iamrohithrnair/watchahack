"""TfL Digital Health ingestor — monitors TfL API and JamCam feed health.

The Brain identified that JamCam outages are digital failures with physical symptoms.
TfL does not expose internal IT monitoring APIs, so we probe the observable surface:
  - TfL Unified API response time and HTTP status
  - JamCam feed URL reachability (sample of cameras)
  - Feed staleness detection (image URLs that return errors or stale content)

This moves capability from reactive observation to proactive prediction of
infrastructure failures that affect the JamCam and other TfL data feeds.
"""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.tfl_digital_health")

TFL_API_BASE = "https://api.tfl.gov.uk"
TFL_JAMCAM_URL = f"{TFL_API_BASE}/Place/Type/JamCam"

# Probe these endpoints for API health (cheap, fast endpoints)
HEALTH_PROBE_ENDPOINTS = [
    (f"{TFL_API_BASE}/Line/Mode/tube/Status", "tube_status"),
    (TFL_JAMCAM_URL, "jamcam_list"),
]

# How many JamCam image URLs to probe per cycle (avoid hammering)
JAMCAM_SAMPLE_SIZE = 10

# Timeouts for health probes (tighter than normal data fetches)
_PROBE_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=5)


class TflDigitalHealthIngestor(BaseIngestor):
    source_name = "tfl_digital_health"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        """Probe TfL API endpoints and a sample of JamCam feed URLs."""
        results: dict[str, Any] = {"api_probes": [], "feed_probes": []}

        # 1. Probe API endpoints for response time and status
        for url, label in HEALTH_PROBE_ENDPOINTS:
            await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
            probe = await self._probe_url(url, label)
            results["api_probes"].append(probe)

        # 2. Get JamCam list and probe a sample of image URLs
        jamcam_probe = next(
            (p for p in results["api_probes"] if p["label"] == "jamcam_list"),
            None,
        )
        if jamcam_probe and jamcam_probe.get("data") and isinstance(jamcam_probe["data"], list):
            cameras = jamcam_probe["data"]
            # Sample evenly across the list
            step = max(1, len(cameras) // JAMCAM_SAMPLE_SIZE)
            sample = cameras[::step][:JAMCAM_SAMPLE_SIZE]

            for cam in sample:
                image_url = None
                for prop in cam.get("additionalProperties", []):
                    if prop.get("key") in ("imageUrl", "ImageUrl", "image"):
                        image_url = prop.get("value")
                        break
                if image_url:
                    probe = await self._probe_url(
                        image_url,
                        f"jamcam_feed:{cam.get('id', 'unknown')}",
                        head_only=True,
                    )
                    probe["camera_id"] = cam.get("id", "unknown")
                    probe["camera_name"] = cam.get("commonName", "unknown")
                    probe["lat"] = cam.get("lat") or cam.get("Lat")
                    probe["lon"] = cam.get("lon") or cam.get("Lon")
                    results["feed_probes"].append(probe)

        return results

    async def _probe_url(
        self, url: str, label: str, head_only: bool = False
    ) -> dict[str, Any]:
        """Probe a single URL. Returns timing and status info."""
        result: dict[str, Any] = {
            "url": url,
            "label": label,
            "status": None,
            "response_time_ms": None,
            "error": None,
            "data": None,
        }
        start = time.monotonic()
        try:
            async with aiohttp.ClientSession(timeout=_PROBE_TIMEOUT) as session:
                method = session.head if head_only else session.get
                async with method(url) as resp:
                    elapsed_ms = (time.monotonic() - start) * 1000
                    result["status"] = resp.status
                    result["response_time_ms"] = round(elapsed_ms, 1)
                    # For GET requests on JSON endpoints, capture the data
                    if not head_only and resp.status == 200:
                        try:
                            result["data"] = await resp.json(content_type=None)
                        except Exception:
                            pass
        except asyncio.TimeoutError:
            elapsed_ms = (time.monotonic() - start) * 1000
            result["response_time_ms"] = round(elapsed_ms, 1)
            result["error"] = "timeout"
        except aiohttp.ClientError as exc:
            elapsed_ms = (time.monotonic() - start) * 1000
            result["response_time_ms"] = round(elapsed_ms, 1)
            result["error"] = str(exc)

        return result

    async def process(self, data: Any) -> None:
        api_probes = data.get("api_probes", [])
        feed_probes = data.get("feed_probes", [])

        # --- API endpoint health ---
        for probe in api_probes:
            label = probe["label"]
            status = probe.get("status")
            latency = probe.get("response_time_ms")
            error = probe.get("error")

            healthy = status == 200 and error is None
            health_score = 1.0 if healthy else 0.0

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=latency or -1,
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "probe_type": "api_endpoint",
                    "label": label,
                    "http_status": status,
                    "response_time_ms": latency,
                    "error": error,
                    "healthy": healthy,
                },
            )
            await self.board.store_observation(obs)

            status_str = f"HTTP {status}" if status else f"ERROR: {error}"
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"TfL API health [{label}] {status_str} "
                    f"latency={latency:.0f}ms healthy={healthy}"
                ),
                data={
                    "probe_type": "api_endpoint",
                    "label": label,
                    "http_status": status,
                    "response_time_ms": latency,
                    "healthy": healthy,
                    "observation_id": obs.id,
                },
            ))

        # --- JamCam feed health ---
        total_feeds = len(feed_probes)
        failed_feeds = []
        slow_feeds = []
        total_latency = 0.0

        for probe in feed_probes:
            camera_id = probe.get("camera_id", "unknown")
            camera_name = probe.get("camera_name", "unknown")
            status = probe.get("status")
            latency = probe.get("response_time_ms", 0)
            error = probe.get("error")
            lat = probe.get("lat")
            lon = probe.get("lon")

            total_latency += latency or 0
            feed_ok = status in (200, 301, 302) and error is None

            if not feed_ok:
                failed_feeds.append(camera_id)
            elif latency and latency > 5000:
                slow_feeds.append(camera_id)

            cell_id = None
            if lat and lon:
                try:
                    cell_id = self.graph.latlon_to_cell(float(lat), float(lon))
                except (ValueError, TypeError):
                    pass

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=latency or -1,
                location_id=cell_id,
                lat=float(lat) if lat else None,
                lon=float(lon) if lon else None,
                metadata={
                    "probe_type": "jamcam_feed",
                    "camera_id": camera_id,
                    "camera_name": camera_name,
                    "http_status": status,
                    "response_time_ms": latency,
                    "error": error,
                    "feed_ok": feed_ok,
                },
            )
            await self.board.store_observation(obs)

        # Post a summary message for feed health
        avg_latency = total_latency / total_feeds if total_feeds else 0
        failure_rate = len(failed_feeds) / total_feeds if total_feeds else 0

        summary = (
            f"TfL JamCam feed health: {total_feeds} probed, "
            f"{len(failed_feeds)} failed ({failure_rate:.0%}), "
            f"{len(slow_feeds)} slow, avg_latency={avg_latency:.0f}ms"
        )

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=summary,
            data={
                "probe_type": "jamcam_feed_summary",
                "total_probed": total_feeds,
                "failed_count": len(failed_feeds),
                "slow_count": len(slow_feeds),
                "failure_rate": round(failure_rate, 3),
                "avg_latency_ms": round(avg_latency, 1),
                "failed_cameras": failed_feeds,
                "slow_cameras": slow_feeds,
            },
        ))

        self.log.info(
            "TfL digital health: api_probes=%d feed_probes=%d failed=%d slow=%d avg_latency=%.0fms",
            len(api_probes), total_feeds, len(failed_feeds), len(slow_feeds), avg_latency,
        )


async def ingest_tfl_digital_health(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL digital health check cycle."""
    ingestor = TflDigitalHealthIngestor(board, graph, scheduler)
    await ingestor.run()
