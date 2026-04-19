"""Planning applications ingestor — London borough planning data from planning.data.gov.uk.

Fetches recent planning applications across London boroughs using the open
Planning Data API (no API key required). This captures infrastructure changes,
development proposals, and local planning conflicts that can serve as leading
indicators of community tension (e.g., LTN disputes, housing controversies).
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.planning")

API_BASE = "https://www.planning.data.gov.uk"
ENTITY_URL = f"{API_BASE}/entity.json"

# Planning-related datasets to monitor
DATASETS = [
    "planning-application",
]

# How far back to look on each fetch (hours)
LOOKBACK_HOURS = 48


class PlanningIngestor(BaseIngestor):
    source_name = "planning_data"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=LOOKBACK_HOURS)
        cutoff_str = cutoff.strftime("%Y-%m-%d")

        all_entities: list[dict] = []
        for dataset in DATASETS:
            params = {
                "dataset": dataset,
                "entry_date_day": cutoff_str,
                "entry_date_match": ">=",
                "limit": 100,
            }
            data = await self.fetch(ENTITY_URL, params=params)
            if data and isinstance(data, dict):
                entities = data.get("entities", [])
                if isinstance(entities, list):
                    all_entities.extend(entities)
            elif data and isinstance(data, list):
                all_entities.extend(data)

        return all_entities if all_entities else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected planning data type: %s", type(data))
            return

        processed = 0
        skipped = 0

        for entity in data:
            if not isinstance(entity, dict):
                skipped += 1
                continue

            # Extract location
            lat = entity.get("latitude") or entity.get("point", {}).get("lat") if isinstance(entity.get("point"), dict) else entity.get("latitude")
            lon = entity.get("longitude") or entity.get("point", {}).get("lon") if isinstance(entity.get("point"), dict) else entity.get("longitude")

            try:
                lat = float(lat) if lat is not None else None
                lon = float(lon) if lon is not None else None
            except (ValueError, TypeError):
                lat = lon = None

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            # Extract key fields
            entity_id = str(entity.get("entity", ""))
            name = entity.get("name", "") or entity.get("reference", "") or entity_id
            description = entity.get("description", "") or ""
            dataset = entity.get("dataset", "")
            organisation = entity.get("organisation-entity", "")
            entry_date = entity.get("entry-date", "")
            start_date = entity.get("start-date", "")
            end_date = entity.get("end-date", "")
            categories = entity.get("categories", "")

            # Build a human-readable summary
            summary_parts = [f"Planning [{dataset}]"]
            if name:
                summary_parts.append(name[:120])
            if description:
                summary_parts.append(f"— {description[:200]}")

            summary = " ".join(summary_parts)

            metadata = {
                "entity_id": entity_id,
                "name": name,
                "dataset": dataset,
                "organisation": str(organisation),
                "entry_date": entry_date,
                "start_date": start_date,
                "end_date": end_date,
            }
            if description:
                metadata["description"] = description[:500]
            if categories:
                metadata["categories"] = categories

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=summary,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata=metadata,
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=summary,
                data={
                    **metadata,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Planning data: processed=%d skipped=%d total_fetched=%d",
            processed, skipped, len(data),
        )


async def ingest_planning(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one planning data fetch cycle."""
    ingestor = PlanningIngestor(board, graph, scheduler)
    await ingestor.run()
