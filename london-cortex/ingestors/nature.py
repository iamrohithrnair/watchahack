"""Wildlife ingestor — iNaturalist observations for London."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.nature")

INATURALIST_URL = "https://api.inaturalist.org/v1/observations"

INATURALIST_PARAMS = {
    "place_id": "32339",   # London
    "per_page": "30",
    "order": "desc",
    "order_by": "created_at",
}


class NatureIngestor(BaseIngestor):
    source_name = "nature"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any] | None:
        inaturalist_data = await self.fetch(INATURALIST_URL, params=INATURALIST_PARAMS)
        if inaturalist_data is None:
            return None
        return {"inaturalist": inaturalist_data}

    async def process(self, data: dict[str, Any]) -> None:
        await self._process_inaturalist(data.get("inaturalist"))

    async def _process_inaturalist(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.info("No iNaturalist data or unexpected format: %s", type(data))
            return

        results = data.get("results", [])
        if not results:
            self.log.info("iNaturalist returned 0 observations")
            return

        processed = 0
        skipped = 0

        for obs_item in results:
            try:
                obs_id_remote = obs_item.get("id")
                taxon = obs_item.get("taxon") or {}
                common_name = taxon.get("preferred_common_name") or taxon.get("name", "")
                sci_name = taxon.get("name", "")
                taxon_rank = taxon.get("rank", "")

                # Location
                geo = obs_item.get("geojson") or {}
                coords = geo.get("coordinates", []) if geo else []
                lat: float | None = None
                lon: float | None = None
                if len(coords) == 2:
                    try:
                        lon = float(coords[0])
                        lat = float(coords[1])
                    except (ValueError, TypeError):
                        pass

                place_guess = obs_item.get("place_guess", "")
                observed_on = obs_item.get("observed_on", "")
                quality_grade = obs_item.get("quality_grade", "")
                obs_url = obs_item.get("uri", "")

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                obs = Observation(
                    source="inaturalist",
                    obs_type=ObservationType.TEXT,
                    value=f"{common_name} ({sci_name})" if common_name else sci_name,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "inaturalist_id": obs_id_remote,
                        "common_name": common_name,
                        "sci_name": sci_name,
                        "rank": taxon_rank,
                        "place": place_guess,
                        "observed_on": observed_on,
                        "quality_grade": quality_grade,
                        "url": obs_url,
                    },
                )
                await self.board.store_observation(obs)

                display_name = common_name if common_name else sci_name
                msg = AgentMessage(
                    from_agent="inaturalist",
                    channel="#raw",
                    content=(
                        f"Wildlife [{place_guess or 'London'}] {display_name} "
                        f"({taxon_rank}) quality={quality_grade}"
                    ),
                    data={
                        "inaturalist_id": obs_id_remote,
                        "common_name": common_name,
                        "sci_name": sci_name,
                        "place": place_guess,
                        "lat": lat,
                        "lon": lon,
                        "observed_on": observed_on,
                        "url": obs_url,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing iNaturalist obs: %s", obs_item.get("id"))
                skipped += 1

        self.log.info("iNaturalist: processed=%d skipped=%d", processed, skipped)


async def ingest_nature(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one nature fetch cycle."""
    ingestor = NatureIngestor(board, graph, scheduler)
    await ingestor.run()
