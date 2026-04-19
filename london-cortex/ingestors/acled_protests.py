"""ACLED protest/demonstration ingestor — geo-coded public order events in London.

Uses the ACLED (Armed Conflict Location & Event Data) API to fetch recent
protest, demonstration, and riot events in the Greater London area. This
provides crucial context for crowd and transport anomalies, allowing the
system to differentiate between spontaneous events and pre-planned
demonstrations.

ACLED is free with registration: https://acleddata.com/register/
Data is media-derived (not forward-looking), typically 1-2 days behind.
"""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.acled_protests")

# ACLED API v3 — free with registered API key + email
ACLED_API_URL = "https://api.acleddata.com/acled/read"

# Greater London bounding box
LONDON_BBOX = {
    "lat_min": 51.28,
    "lat_max": 51.70,
    "lon_min": -0.51,
    "lon_max": 0.33,
}

# ACLED event types relevant to public order
# 1 = Battles, 2 = Protests, 3 = Riots, 6 = Violence against civilians
PROTEST_EVENT_TYPES = {"Protests", "Riots"}

# Sub-event types for finer classification
PLANNED_SUBTYPES = {
    "Peaceful protest",
    "Protest with intervention",
    "Excessive force against protesters",
}
DISORDER_SUBTYPES = {
    "Violent demonstration",
    "Mob violence",
}


class AcledProtestsIngestor(BaseIngestor):
    source_name = "acled_protests"
    rate_limit_name = "default"

    def _get_credentials(self) -> tuple[str, str] | None:
        key = os.environ.get("ACLED_API_KEY", "").strip()
        email = os.environ.get("ACLED_EMAIL", "").strip()
        if not key or not email:
            self.log.warning(
                "ACLED_API_KEY and ACLED_EMAIL required — skipping"
            )
            return None
        return key, email

    async def fetch_data(self) -> Any:
        creds = self._get_credentials()
        if creds is None:
            return None

        key, email = creds

        # Fetch events from the last 30 days (ACLED data has ~1-2 day lag)
        since = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")

        params = {
            "key": key,
            "email": email,
            "event_date": since,
            "event_date_where": ">=",
            "region": 1,  # Europe
            "country": "United Kingdom",
            "latitude1": str(LONDON_BBOX["lat_min"]),
            "latitude2": str(LONDON_BBOX["lat_max"]),
            "longitude1": str(LONDON_BBOX["lon_min"]),
            "longitude2": str(LONDON_BBOX["lon_max"]),
            "limit": "500",
        }

        return await self.fetch(ACLED_API_URL, params=params)

    async def process(self, data: Any) -> None:
        if isinstance(data, str):
            self.log.warning("ACLED returned non-JSON response")
            return

        # ACLED response: {"status": 200, "success": true, "data": [...]}
        if isinstance(data, dict):
            if not data.get("success", False):
                self.log.warning("ACLED API error: %s", data.get("error", "unknown"))
                return
            events = data.get("data", [])
        elif isinstance(data, list):
            events = data
        else:
            self.log.warning("Unexpected ACLED data type: %s", type(data))
            return

        processed = 0
        for event in events:
            event_type = event.get("event_type", "")
            if event_type not in PROTEST_EVENT_TYPES:
                continue

            sub_event_type = event.get("sub_event_type", "")
            event_date = event.get("event_date", "")
            location_name = event.get("location", "unknown")
            notes = event.get("notes", "")
            fatalities = int(event.get("fatalities", 0) or 0)
            actor1 = event.get("actor1", "")

            try:
                lat = float(event.get("latitude", 0))
                lon = float(event.get("longitude", 0))
            except (ValueError, TypeError):
                continue

            if not (LONDON_BBOX["lat_min"] <= lat <= LONDON_BBOX["lat_max"]):
                continue
            if not (LONDON_BBOX["lon_min"] <= lon <= LONDON_BBOX["lon_max"]):
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)

            # Classify as planned vs disorder
            is_planned = sub_event_type in PLANNED_SUBTYPES
            is_disorder = sub_event_type in DISORDER_SUBTYPES

            # Severity: 1 for peaceful, 2 for intervention, 3+ if fatalities
            severity = 1
            if is_disorder:
                severity = 2
            if fatalities > 0:
                severity = min(3 + fatalities, 5)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=float(severity),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "event_type": event_type,
                    "sub_event_type": sub_event_type,
                    "event_date": event_date,
                    "location_name": location_name,
                    "actor": actor1,
                    "fatalities": fatalities,
                    "is_planned": is_planned,
                    "is_disorder": is_disorder,
                    "notes": notes[:500] if notes else "",
                },
            )
            await self.board.store_observation(obs)

            label = "PLANNED" if is_planned else ("DISORDER" if is_disorder else "PROTEST")
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Public order event [{label}] {event_date} at {location_name}: "
                    f"{sub_event_type} — {actor1}"
                    + (f" ({fatalities} fatalities)" if fatalities else "")
                ),
                data={
                    "event_type": event_type,
                    "sub_event_type": sub_event_type,
                    "event_date": event_date,
                    "location_name": location_name,
                    "actor": actor1,
                    "lat": lat,
                    "lon": lon,
                    "severity": severity,
                    "is_planned": is_planned,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "ACLED protests: %d events processed from %d total records",
            processed, len(events),
        )


async def ingest_acled_protests(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one ACLED protest fetch cycle."""
    ingestor = AcledProtestsIngestor(board, graph, scheduler)
    await ingestor.run()
