"""UK Police street-level crime ingestor — data.police.uk (free, no API key).

Fetches recent crime data for sampled London locations, providing crime-type
context (violent crime, arson, robbery, etc.) to help the system distinguish
between accidents, malice, and routine failures.

Data is monthly (not real-time), but gives spatial crime-pattern awareness.
"""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.police_crimes")

# data.police.uk endpoints — free, no API key required
CRIMES_STREET_URL = "https://data.police.uk/api/crimes-street/all-crime"
CRIME_DATES_URL = "https://data.police.uk/api/crimes-street-dates"

# Categories most relevant to the system's "intent" detection
INTENT_CATEGORIES = {
    "violent-crime",
    "criminal-damage-arson",
    "robbery",
    "possession-of-weapons",
    "public-order",
    "anti-social-behaviour",
}

# Sample points across Greater London (spread to avoid API poly limits)
# Each point captures crimes within ~1 mile radius
SAMPLE_POINTS = [
    (51.5074, -0.1278),   # Central London / Westminster
    (51.5155, -0.0922),   # City of London
    (51.5033, -0.1195),   # Whitehall / Parliament
    (51.5313, -0.1040),   # Kings Cross / Euston
    (51.4657, -0.0170),   # Lewisham
    (51.4613, -0.1156),   # Brixton
    (51.5430, -0.0553),   # Hackney / Dalston
    (51.5225, -0.1540),   # Paddington
    (51.4975, -0.1357),   # Victoria
    (51.5556, -0.1084),   # Finsbury Park
    (51.4720, -0.4887),   # Heathrow area
    (51.5136, 0.0890),    # Canning Town / Newham
]

# data.police.uk limits: ~500 results per call, and rate-limited
MAX_POINTS_PER_CYCLE = 4


class PoliceCrimesIngestor(BaseIngestor):
    source_name = "police_crimes"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # First get the latest available date
        dates_data = await self.fetch(CRIME_DATES_URL)
        if not dates_data or not isinstance(dates_data, list):
            self.log.warning("Could not fetch available crime dates")
            return None

        latest_date = dates_data[0].get("date") if dates_data else None
        if not latest_date:
            self.log.warning("No date found in crime dates response")
            return None

        # Sample a subset of points each cycle to stay within rate limits
        points = random.sample(SAMPLE_POINTS, min(MAX_POINTS_PER_CYCLE, len(SAMPLE_POINTS)))

        all_crimes: list[dict] = []
        for lat, lng in points:
            data = await self.fetch(
                CRIMES_STREET_URL,
                params={"lat": str(lat), "lng": str(lng), "date": latest_date},
            )
            if isinstance(data, list):
                all_crimes.extend(data)

        return {"date": latest_date, "crimes": all_crimes}

    async def process(self, data: Any) -> None:
        crimes = data.get("crimes", [])
        date = data.get("date", "unknown")

        if not crimes:
            self.log.info("No crime data returned for %s", date)
            return

        # Aggregate by category and location for efficient observation posting
        # Instead of one obs per crime, aggregate counts per (category, snap_location)
        location_counts: dict[tuple[str, str, float, float], int] = {}

        for crime in crimes:
            category = crime.get("category", "other-crime")
            location = crime.get("location", {})
            lat_str = location.get("latitude")
            lon_str = location.get("longitude")
            street_name = location.get("street", {}).get("name", "unknown")

            try:
                lat = float(lat_str) if lat_str else None
                lon = float(lon_str) if lon_str else None
            except (ValueError, TypeError):
                continue

            if lat is None or lon is None:
                continue

            key = (category, street_name, lat, lon)
            location_counts[key] = location_counts.get(key, 0) + 1

        processed = 0
        for (category, street, lat, lon), count in location_counts.items():
            cell_id = self.graph.latlon_to_cell(lat, lon)

            is_intent_relevant = category in INTENT_CATEGORIES

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(count),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "category": category,
                    "street": street,
                    "date": date,
                    "count": count,
                    "intent_relevant": is_intent_relevant,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Police crimes [{date}] {street}: {category}={count}"
                    + (" [INTENT-RELEVANT]" if is_intent_relevant else "")
                ),
                data={
                    "category": category,
                    "street": street,
                    "lat": lat,
                    "lon": lon,
                    "count": count,
                    "date": date,
                    "intent_relevant": is_intent_relevant,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        # Post a summary message with intent-relevant totals
        intent_total = sum(
            count for (cat, _, _, _), count in location_counts.items()
            if cat in INTENT_CATEGORIES
        )
        total = sum(location_counts.values())

        self.log.info(
            "Police crimes [%s]: %d locations, %d total crimes (%d intent-relevant)",
            date, processed, total, intent_total,
        )


async def ingest_police_crimes(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one police crime fetch cycle."""
    ingestor = PoliceCrimesIngestor(board, graph, scheduler)
    await ingestor.run()
