"""Misc ingestor — Food Standards Agency hygiene ratings."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.misc")

# Food Standards Agency — low-rated London establishments (local authority 197 = City of London)
FSA_URL = (
    "https://api.ratings.food.gov.uk/Establishments"
    "?localAuthorityId=197&pageSize=10"
    "&sortOptionKey=rating&ratingOperatorKey=LessThanOrEqual&ratingKey=2"
)
FSA_HEADERS = {"x-api-version": "2"}

LONDON_LAT = 51.5
LONDON_LON = -0.12


class MiscIngestor(BaseIngestor):
    source_name = "misc"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any] | None:
        fsa_data = await self.fetch(FSA_URL, headers=FSA_HEADERS)
        if fsa_data is None:
            return None
        return {"fsa": fsa_data}

    async def process(self, data: dict[str, Any]) -> None:
        await self._process_fsa(data.get("fsa"))

    async def _process_fsa(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.info("No FSA data or unexpected format: %s", type(data))
            return

        establishments = data.get("establishments", [])
        if not establishments:
            self.log.info("FSA returned 0 establishments")
            return

        processed = 0
        skipped = 0

        for est in establishments:
            try:
                name = est.get("BusinessName", "Unknown")
                business_type = est.get("BusinessType", "")
                rating_value = est.get("RatingValue", "")
                rating_date = est.get("RatingDate", "")
                address = est.get("AddressLine1", "") or ""
                postcode = est.get("PostCode", "")

                # Geocode if available
                geocode = est.get("Geocode") or {}
                lat_raw = geocode.get("Latitude")
                lon_raw = geocode.get("Longitude")

                try:
                    lat = float(lat_raw) if lat_raw is not None else None
                    lon = float(lon_raw) if lon_raw is not None else None
                except (ValueError, TypeError):
                    lat = lon = None

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else (
                    self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
                )

                try:
                    rating_num = float(str(rating_value).replace("AwaitingInspection", "0")
                                       .replace("Exempt", "-1")) if rating_value else None
                except (ValueError, TypeError):
                    rating_num = None

                obs = Observation(
                    source="food_hygiene",
                    obs_type=ObservationType.CATEGORICAL,
                    value=str(rating_value),
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "name": name,
                        "business_type": business_type,
                        "rating": rating_value,
                        "rating_numeric": rating_num,
                        "rating_date": rating_date,
                        "address": address,
                        "postcode": postcode,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent="food_hygiene",
                    channel="#raw",
                    content=(
                        f"Food hygiene [{name}] rating={rating_value} "
                        f"type={business_type} postcode={postcode}"
                    ),
                    data={
                        "name": name,
                        "business_type": business_type,
                        "rating": rating_value,
                        "rating_date": rating_date,
                        "postcode": postcode,
                        "lat": lat,
                        "lon": lon,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing FSA establishment: %s", est.get("BusinessName"))
                skipped += 1

        self.log.info("Food Standards Agency: processed=%d skipped=%d", processed, skipped)


async def ingest_misc(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one misc (Wikipedia + FSA) fetch cycle."""
    ingestor = MiscIngestor(board, graph, scheduler)
    await ingestor.run()
