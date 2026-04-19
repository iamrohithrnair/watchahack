"""NESO Data Portal feed health ingestor — API liveness and data freshness checks.

Addresses the Brain's suggestion: definitively determine whether the National Grid
ESO (NESO) upstream feeds are live or operating in fallback/cached mode. This is
distinct from provider_status.py (which probes Carbon Intensity, a separate product)
and grid_status.py (which processes operational data without staleness detection).

Two checks per cycle:
1. CKAN API liveness — hit the NESO Data Portal status_show endpoint to confirm
   the API itself is responding and healthy.
2. Data freshness — fetch the latest SOP record's timestamp and compare against
   wall-clock time. If the most recent record is older than STALE_THRESHOLD_MIN,
   the feed is likely serving cached/fallback data rather than live control-room data.

Free API, no key required.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.neso_feed_health")

NESO_STATUS_URL = "https://api.neso.energy/api/3/action/status_show"
NESO_DATASTORE_URL = "https://api.neso.energy/api/3/action/datastore_search"

# SOP resource — same as grid_status.py; we only fetch 1 record to check freshness
SOP_RESOURCE_ID = "e51f2721-00ab-4182-9cae-3c973e854aa8"

# If the most recent SOP record is older than this, flag as stale/cached
STALE_THRESHOLD_MIN = 60

_PROBE_TIMEOUT = aiohttp.ClientTimeout(total=15, connect=8)

LONDON_LAT = 51.5
LONDON_LON = -0.12


class NesoFeedHealthIngestor(BaseIngestor):
    source_name = "neso_feed_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        t0 = time.monotonic()
        api_healthy = False
        api_status_code: int | None = None
        api_response_ms: float = 0.0
        api_error: str | None = None

        try:
            async with aiohttp.ClientSession(timeout=_PROBE_TIMEOUT) as session:
                async with session.get(NESO_STATUS_URL) as resp:
                    api_response_ms = (time.monotonic() - t0) * 1000
                    api_status_code = resp.status
                    body = await resp.json(content_type=None)
                    api_healthy = resp.status == 200 and body.get("success") is True
        except Exception as exc:
            api_response_ms = (time.monotonic() - t0) * 1000
            api_error = str(exc)[:200]
            self.log.warning("NESO status_show probe failed: %s", exc)

        # Freshness check — fetch one SOP record
        sop_record = await self.fetch(
            NESO_DATASTORE_URL,
            params={
                "resource_id": SOP_RESOURCE_ID,
                "sort": "sop_datetime desc",
                "limit": "1",
            },
        )

        return {
            "api_healthy": api_healthy,
            "api_status_code": api_status_code,
            "api_response_ms": round(api_response_ms, 1),
            "api_error": api_error,
            "sop_record": sop_record,
        }

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        api_healthy: bool = data["api_healthy"]
        api_response_ms: float = data["api_response_ms"]
        api_status_code: int | None = data["api_status_code"]
        api_error: str | None = data["api_error"]
        sop_raw: Any = data["sop_record"]

        # --- API liveness observation ---
        obs_api = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=1.0 if api_healthy else 0.0,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "neso_api_liveness",
                "status_code": api_status_code,
                "response_ms": api_response_ms,
                "error": api_error,
            },
        )
        await self.board.store_observation(obs_api)

        # --- Data freshness check ---
        feed_mode = "unknown"
        sop_age_min: float | None = None
        sop_time_str: str = ""

        if isinstance(sop_raw, dict) and sop_raw.get("success"):
            records = sop_raw.get("result", {}).get("records", [])
            if records:
                sop_time_str = records[0].get("sop_datetime", "")
                if sop_time_str:
                    try:
                        # NESO timestamps: "2024-01-15T14:30:00" (UTC)
                        dt = datetime.fromisoformat(sop_time_str.replace("Z", "+00:00"))
                        if dt.tzinfo is None:
                            dt = dt.replace(tzinfo=timezone.utc)
                        age_s = (datetime.now(timezone.utc) - dt).total_seconds()
                        sop_age_min = age_s / 60.0
                        feed_mode = "stale/cached" if sop_age_min > STALE_THRESHOLD_MIN else "live"
                    except (ValueError, TypeError) as exc:
                        self.log.warning("Could not parse sop_datetime %r: %s", sop_time_str, exc)

        obs_freshness = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=sop_age_min if sop_age_min is not None else -1.0,
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "neso_sop_age_minutes",
                "sop_time": sop_time_str,
                "feed_mode": feed_mode,
                "stale_threshold_min": STALE_THRESHOLD_MIN,
            },
        )
        await self.board.store_observation(obs_freshness)

        # --- Summary message ---
        api_str = f"API={'UP' if api_healthy else 'DOWN'} ({api_response_ms:.0f}ms)"
        if sop_age_min is not None:
            freshness_str = f"SOP_age={sop_age_min:.1f}min mode={feed_mode}"
        else:
            freshness_str = "SOP_age=unknown"

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"NESO feed health: {api_str}, {freshness_str}",
            data={
                "api_healthy": api_healthy,
                "api_status_code": api_status_code,
                "api_response_ms": api_response_ms,
                "api_error": api_error,
                "sop_time": sop_time_str,
                "sop_age_minutes": sop_age_min,
                "feed_mode": feed_mode,
                "stale_threshold_min": STALE_THRESHOLD_MIN,
                "neso_api_liveness_obs": obs_api.id,
                "neso_freshness_obs": obs_freshness.id,
            },
            location_id=cell_id,
        ))

        self.log.info(
            "NESO feed health: api=%s response_ms=%.0f sop_age=%.1fmin mode=%s",
            "healthy" if api_healthy else "UNHEALTHY",
            api_response_ms,
            sop_age_min if sop_age_min is not None else -1.0,
            feed_mode,
        )


async def ingest_neso_feed_health(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one NESO feed health check cycle."""
    ingestor = NesoFeedHealthIngestor(board, graph, scheduler)
    await ingestor.run()
