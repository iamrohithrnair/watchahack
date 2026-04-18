"""Santander Cycles (TfL Cycle Hire) ingestor — bike availability at docking stations.

Monitors rate of change in bike availability, especially near tube stations,
as a leading indicator of mode-switching during disruptions.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.cycle_hire")

BIKEPOINT_URL = "https://api.tfl.gov.uk/BikePoint"


def _extract_prop(additional_properties: list[dict], key: str) -> str | None:
    """Extract a value from TfL's additionalProperties list."""
    for prop in additional_properties:
        if prop.get("key") == key:
            return prop.get("value")
    return None


class CycleHireIngestor(BaseIngestor):
    source_name = "tfl_cycle_hire"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(BIKEPOINT_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected BikePoint response type: %s", type(data))
            return

        processed = 0
        for station in data:
            props = station.get("additionalProperties", [])
            installed = _extract_prop(props, "Installed")
            if installed and installed.lower() != "true":
                continue

            name = station.get("commonName", "unknown")
            lat = station.get("lat")
            lon = station.get("lon")

            nb_bikes_str = _extract_prop(props, "NbBikes")
            nb_empty_str = _extract_prop(props, "NbEmptyDocks")
            nb_docks_str = _extract_prop(props, "NbDocks")

            try:
                nb_bikes = int(nb_bikes_str) if nb_bikes_str else None
                nb_empty = int(nb_empty_str) if nb_empty_str else None
                nb_docks = int(nb_docks_str) if nb_docks_str else None
            except (ValueError, TypeError):
                continue

            if nb_bikes is None or nb_docks is None or nb_docks == 0:
                continue

            occupancy = nb_bikes / nb_docks
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=occupancy,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "station": name,
                    "station_id": station.get("id", ""),
                    "nb_bikes": nb_bikes,
                    "nb_empty_docks": nb_empty,
                    "nb_docks": nb_docks,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Cycle hire [{name}] bikes={nb_bikes}/{nb_docks} "
                    f"({occupancy:.0%} full)"
                ),
                data={
                    "station": name,
                    "station_id": station.get("id", ""),
                    "nb_bikes": nb_bikes,
                    "nb_empty_docks": nb_empty,
                    "nb_docks": nb_docks,
                    "occupancy": round(occupancy, 3),
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Cycle hire: processed %d stations", processed)


async def ingest_cycle_hire(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one cycle hire fetch cycle."""
    ingestor = CycleHireIngestor(board, graph, scheduler)
    await ingestor.run()
