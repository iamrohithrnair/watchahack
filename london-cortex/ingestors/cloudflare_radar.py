"""Cloudflare Radar ingestor — internet quality metrics for London/UK.

Fetches Internet Quality Index (bandwidth, latency, DNS response time)
from the Cloudflare Radar API. Helps correlate data source degradation
with broader internet infrastructure issues affecting London.

Requires CLOUDFLARE_RADAR_API_TOKEN in .env (free: create a Custom API Token
with Account > Radar > Read permission at https://dash.cloudflare.com/profile/api-tokens).
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.cloudflare_radar")

# Cloudflare Radar API base
_BASE = "https://api.cloudflare.com/client/v4/radar"

# GB = United Kingdom; Radar supports country-level filtering
_LOCATION = "GB"

# Metrics we track from the Internet Quality Index
_IQI_METRICS = ("bandwidthDownload", "bandwidthUpload", "latencyIdle", "latencyLoaded", "dns")


class CloudflareRadarIngestor(BaseIngestor):
    source_name = "cloudflare_radar"
    rate_limit_name = "default"

    # Central London coordinates for observation location
    _LONDON_LAT = 51.5074
    _LONDON_LON = -0.1278

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self._token = os.getenv("CLOUDFLARE_RADAR_API_TOKEN", "")
        if not self._token:
            self.log.warning("CLOUDFLARE_RADAR_API_TOKEN not set — cloudflare_radar ingestor will be inactive")

    def _headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {self._token}"}

    async def fetch_data(self) -> Any:
        if not self._token:
            return None

        # Fetch IQI summary for GB (percentiles over recent period)
        url = f"{_BASE}/quality/iqi/summary"
        params = {"location": _LOCATION, "format": "json"}
        data = await self.fetch(url, params=params, headers=self._headers())
        if data is None:
            return None

        # Also fetch speed test summary for additional metrics
        speed_url = f"{_BASE}/quality/speed/summary"
        speed_data = await self.fetch(speed_url, params=params, headers=self._headers())

        return {"iqi": data, "speed": speed_data}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(self._LONDON_LAT, self._LONDON_LON)
        processed = 0

        # Process IQI data
        iqi = data.get("iqi")
        if iqi and isinstance(iqi, dict):
            summary = iqi.get("result", iqi)
            if isinstance(summary, dict):
                # The IQI response has metric summaries (p25, p50, p75)
                meta = summary.get("summary_0") or summary.get("meta", {})
                iqi_metrics = summary if not meta else meta

                for metric_name in _IQI_METRICS:
                    value = _extract_metric(iqi_metrics, metric_name)
                    if value is None:
                        continue

                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=value,
                        location_id=cell_id,
                        lat=self._LONDON_LAT,
                        lon=self._LONDON_LON,
                        metadata={"metric": metric_name, "location": _LOCATION, "api": "iqi"},
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=f"Cloudflare Radar [UK] {metric_name}={value:.2f}",
                        data={
                            "metric": metric_name,
                            "value": value,
                            "location": _LOCATION,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1

        # Process speed test data
        speed = data.get("speed")
        if speed and isinstance(speed, dict):
            result = speed.get("result", speed)
            if isinstance(result, dict):
                speed_summary = result.get("summary_0") or result.get("meta", {}) or result
                for key in ("bandwidthDownload", "bandwidthUpload", "latencyIdle", "latencyLoaded", "jitter", "packetLoss"):
                    value = _extract_metric(speed_summary, key)
                    if value is None:
                        continue

                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=value,
                        location_id=cell_id,
                        lat=self._LONDON_LAT,
                        lon=self._LONDON_LON,
                        metadata={"metric": f"speed_{key}", "location": _LOCATION, "api": "speed"},
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=f"Cloudflare Speed [UK] {key}={value:.2f}",
                        data={
                            "metric": f"speed_{key}",
                            "value": value,
                            "location": _LOCATION,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1

        self.log.info("Cloudflare Radar: processed=%d metrics", processed)


def _extract_metric(data: dict, key: str) -> float | None:
    """Try to extract a numeric metric value from nested API response."""
    if not isinstance(data, dict):
        return None
    # Direct key
    val = data.get(key)
    if val is not None:
        try:
            return float(val)
        except (ValueError, TypeError):
            pass
    # Nested under p50 (median percentile)
    p50 = data.get("p50", {})
    if isinstance(p50, dict):
        val = p50.get(key)
        if val is not None:
            try:
                return float(val)
            except (ValueError, TypeError):
                pass
    return None


async def ingest_cloudflare_radar(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Cloudflare Radar fetch cycle."""
    ingestor = CloudflareRadarIngestor(board, graph, scheduler)
    await ingestor.run()
