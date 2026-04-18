"""ONS demographics ingestor — census and economic data for London boroughs."""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.ons_demographics")

# ONS API
_DATASETS_URL = "https://api.beta.ons.gov.uk/v1/datasets"

# ONS dataset IDs relevant to London
_LONDON_DATASETS = [
    "regional-gdp-by-year",
    "wellbeing-local-authority",
    "suicides-in-the-uk",
    "life-expectancy-by-local-authority",
    "labour-market-by-region",
]

# London borough centers for geo-tagging
_BOROUGH_COORDS: dict[str, tuple[float, float]] = {
    "City of London": (51.5155, -0.0922),
    "Westminster": (51.4975, -0.1357),
    "Camden": (51.5290, -0.1255),
    "Hackney": (51.5450, -0.0553),
    "Tower Hamlets": (51.5099, -0.0059),
    "Islington": (51.5362, -0.1033),
    "Southwark": (51.5035, -0.0804),
    "Lambeth": (51.4571, -0.1231),
    "Wandsworth": (51.4571, -0.1918),
    "Greenwich": (51.4892, 0.0648),
    "Lewisham": (51.4415, -0.0117),
    "Newham": (51.5077, 0.0469),
    "Croydon": (51.3762, -0.0982),
    "Barnet": (51.6252, -0.1517),
    "Ealing": (51.5130, -0.3089),
    "Brent": (51.5588, -0.2817),
}


class ONSDemographicsIngestor(BaseIngestor):
    source_name = "ons_demographics"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        # Fetch the list of available datasets
        data = await self.fetch(_DATASETS_URL)
        return data

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.info("Unexpected ONS response: %s", type(data))
            return

        items = data.get("items", [])
        if not items:
            self.log.info("ONS returned 0 datasets")
            return

        # Central London as default location
        lat, lon = 51.5074, -0.1278
        cell_id = self.graph.latlon_to_cell(lat, lon)
        processed = 0

        for dataset in items[:20]:
            try:
                ds_id = dataset.get("id", "")
                title = dataset.get("title", "")
                description = dataset.get("description", "")
                state = dataset.get("state", "")
                next_release = dataset.get("next_release", "")
                keywords = dataset.get("keywords", [])
                contacts = dataset.get("contacts", [])
                contact_name = ""
                if contacts and isinstance(contacts[0], dict):
                    contact_name = contacts[0].get("name", "")

                # Get latest version info
                links = dataset.get("links", {})
                latest_version = links.get("latest_version", {}).get("href", "")

                obs = Observation(
                    source="ons_demographics",
                    obs_type=ObservationType.TEXT,
                    value=f"ONS Dataset: {title}",
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "dataset_id": ds_id,
                        "description": description[:300],
                        "state": state,
                        "next_release": next_release,
                        "keywords": keywords,
                        "latest_version_url": latest_version,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent="ons_demographics",
                    channel="#raw",
                    content=(
                        f"ONS Dataset: {title} — {description[:120]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "dataset_id": ds_id,
                        "title": title,
                        "state": state,
                        "next_release": next_release,
                        "keywords": keywords,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing ONS dataset: %s", dataset.get("id"))

        self.log.info("ONS Demographics: processed=%d datasets", processed)


# Keep function name matching run.py import
async def ingest_historic_england(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = ONSDemographicsIngestor(board, graph, scheduler)
    await ingestor.run()
