"""DfT Street Manager ingestor — planned road and utility works across London.

Fetches active and upcoming street works (utility trenches, road closures,
scaffolding, skip permits) from the Department for Transport's Street Manager
API.  This complements TfL road_disruptions.py by providing the *utility
company* perspective — Thames Water pipe works, UKPN cable works, BT Openreach
fibre installations, SGN gas mains, etc. — often filed days or weeks before
work begins.

Knowing about planned works lets downstream agents:
  - Preemptively explain traffic disruptions (cause vs symptom)
  - Differentiate planned events from genuine incidents
  - Correlate utility works with air-quality spikes, bus delays, etc.

Requires: STREET_MANAGER_EMAIL + STREET_MANAGER_PASSWORD in .env
Register at https://www.gov.uk/guidance/find-and-use-roadworks-data
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.street_manager")

# Street Manager API base
_API_BASE = "https://api.manage-roadworks.service.gov.uk"
_AUTH_URL = f"{_API_BASE}/authenticate"
_WORKS_URL = f"{_API_BASE}/works"

# London bounding box for spatial filtering
_LONDON_BBOX_PARAMS = {
    "minLat": str(LONDON_BBOX["min_lat"]),
    "maxLat": str(LONDON_BBOX["max_lat"]),
    "minLon": str(LONDON_BBOX["min_lon"]),
    "maxLon": str(LONDON_BBOX["max_lon"]),
}

# Work status values that indicate active or upcoming works
_ACTIVE_STATUSES = {
    "planned",
    "in_progress",
    "permit_granted",
    "permit_submitted",
}

# Map work categories to severity (1-5)
_CATEGORY_SEVERITY = {
    "immediate_urgent": 4,
    "immediate_emergency": 5,
    "urgent": 3,
    "standard": 2,
    "minor": 1,
    "highway_works": 2,
    "planned": 2,
}

# Cache JWT token across ingestor cycles (module-level, survives hot-reload)
import sys as _sys
_TOKEN_KEY = "_cortex_street_manager_token"
if not hasattr(_sys, _TOKEN_KEY):
    setattr(_sys, _TOKEN_KEY, {"id_token": None, "expires_at": 0.0})
_token_cache: dict = getattr(_sys, _TOKEN_KEY)


class StreetManagerIngestor(BaseIngestor):
    source_name = "street_manager"
    rate_limit_name = "default"

    async def _authenticate(self, session: aiohttp.ClientSession) -> str | None:
        """Obtain or reuse a JWT id_token from Street Manager."""
        now = time.monotonic()
        # Reuse cached token if it has >2 min left
        if _token_cache["id_token"] and now < _token_cache["expires_at"] - 120:
            return _token_cache["id_token"]

        email = os.environ.get("STREET_MANAGER_EMAIL", "")
        password = os.environ.get("STREET_MANAGER_PASSWORD", "")
        if not email or not password:
            self.log.warning(
                "STREET_MANAGER_EMAIL / STREET_MANAGER_PASSWORD not set — skipping"
            )
            return None

        payload = {"email": email, "password": password}
        try:
            async with session.post(_AUTH_URL, json=payload) as resp:
                if resp.status != 200:
                    body = await resp.text()
                    self.log.warning(
                        "Street Manager auth failed HTTP %d: %s", resp.status, body[:200]
                    )
                    return None
                data = await resp.json(content_type=None)
                id_token = data.get("idToken") or data.get("id_token")
                if not id_token:
                    self.log.warning("No idToken in auth response: %s", list(data.keys()))
                    return None
                # JWT tokens expire after 1 hour; cache for 50 min to be safe
                _token_cache["id_token"] = id_token
                _token_cache["expires_at"] = now + 3000
                return id_token
        except Exception as exc:
            self.log.warning("Street Manager auth error: %s", exc)
            return None

    async def fetch_data(self) -> Any:
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            token = await self._authenticate(session)
            if not token:
                return None

            headers = {"token": token, "Accept": "application/json"}
            params = {
                **_LONDON_BBOX_PARAMS,
                "status": "in_progress,planned",
            }

            await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
            try:
                async with session.get(_WORKS_URL, params=params, headers=headers) as resp:
                    if resp.status == 401:
                        # Token expired — clear cache and retry once
                        _token_cache["id_token"] = None
                        _token_cache["expires_at"] = 0.0
                        token = await self._authenticate(session)
                        if not token:
                            return None
                        headers["token"] = token
                        async with session.get(_WORKS_URL, params=params, headers=headers) as r2:
                            if r2.status != 200:
                                self.log.warning("Street Manager HTTP %d after re-auth", r2.status)
                                return None
                            return await r2.json(content_type=None)
                    elif resp.status != 200:
                        self.log.warning("Street Manager HTTP %d", resp.status)
                        return None
                    return await resp.json(content_type=None)
            except (aiohttp.ClientError, Exception) as exc:
                self.log.warning("Street Manager fetch error: %s", exc)
                return None

    async def process(self, data: Any) -> None:
        # The API returns a list of work records (or a paginated wrapper)
        works = data if isinstance(data, list) else data.get("works", data.get("rows", []))
        if not isinstance(works, list):
            self.log.warning("Unexpected Street Manager data structure: %s", type(data))
            return

        processed = 0
        for work in works:
            try:
                await self._process_work(work)
                processed += 1
            except Exception:
                self.log.debug("Error processing work record", exc_info=True)

        self.log.info("Street Manager: processed=%d works in London", processed)

    async def _process_work(self, work: dict) -> None:
        work_ref = work.get("work_reference_number", work.get("permit_reference_number", "unknown"))
        status = work.get("work_status", work.get("permit_status", "unknown"))
        category = work.get("work_category", work.get("permit_category", "")).lower()
        promoter = work.get("promoter_organisation", work.get("promoter_name", ""))
        ha = work.get("highway_authority", "")
        street = work.get("street_name", work.get("street", ""))
        description = work.get("description_of_work", work.get("works_description", ""))
        work_type = work.get("activity_type", work.get("work_type", ""))

        # Dates
        start_date = work.get("proposed_start_date", work.get("actual_start_date", ""))
        end_date = work.get("proposed_end_date", work.get("actual_end_date", ""))

        # Location
        lat = _safe_float(work.get("latitude", work.get("lat")))
        lon = _safe_float(work.get("longitude", work.get("lon")))

        # Fall back to work_coordinates if present
        if lat is None or lon is None:
            coords = work.get("work_coordinates", {})
            if isinstance(coords, dict):
                lat = _safe_float(coords.get("latitude"))
                lon = _safe_float(coords.get("longitude"))

        cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

        # Traffic management
        traffic_mgmt = work.get("traffic_management_type", "")

        severity = _CATEGORY_SEVERITY.get(category, 2)
        # Bump severity if it involves road closure
        if "road_closure" in traffic_mgmt.lower() if traffic_mgmt else False:
            severity = min(severity + 1, 5)

        is_planned = status.lower() in ("planned", "permit_granted", "permit_submitted")

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.TEXT,
            value=severity,
            location_id=cell_id,
            lat=lat,
            lon=lon,
            metadata={
                "work_ref": work_ref,
                "status": status,
                "category": category,
                "promoter": promoter,
                "highway_authority": ha,
                "street": street,
                "description": (description or "")[:500],
                "work_type": work_type,
                "traffic_management": traffic_mgmt,
                "start_date": start_date,
                "end_date": end_date,
                "is_planned": is_planned,
            },
        )
        await self.board.store_observation(obs)

        tag = " [PLANNED]" if is_planned else " [ACTIVE]"
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Street works{tag} [{street or 'unknown road'}] "
                f"{promoter} — {category} severity={severity}"
                + (f" — {description[:150]}" if description else "")
            ),
            data={
                "work_ref": work_ref,
                "status": status,
                "category": category,
                "promoter": promoter,
                "street": street,
                "severity_score": severity,
                "lat": lat,
                "lon": lon,
                "observation_id": obs.id,
                "is_planned": is_planned,
                "start_date": start_date,
                "end_date": end_date,
                "traffic_management": traffic_mgmt,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


async def ingest_street_manager(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Street Manager fetch cycle."""
    ingestor = StreetManagerIngestor(board, graph, scheduler)
    await ingestor.run()
