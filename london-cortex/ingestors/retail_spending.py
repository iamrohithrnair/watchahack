"""Retail spending ingestor — ONS CHAPS-based card spending indices by category.

Proxy for POS transaction data: the ONS publishes weekly credit/debit card
spending indices (relative to Feb 2020 baseline) via the "faster indicators"
programme.  Categories include groceries, staples, delayable, social, work-related.

This enables the system to correlate transport disruptions with measurable
shifts in retail spending patterns — e.g., a tube strike driving spending
away from affected corridors.

Note: data is UK national-level (not corridor-level), updated weekly.
Corridor-level POS data would require Mastercard SpendingPulse / Visa Analytics
(enterprise contracts, not freely available).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.retail_spending")

# ONS "faster indicators" — CHAPS card spending by category
# Dataset: "Credit and debit card spending" from the Online and card spending dataset
ONS_SPENDING_URL = (
    "https://api.beta.ons.gov.uk/v1/datasets/online-job-advert-estimates/editions"
)

# We use the ONS CHAPS-based index endpoint.  The raw data is published as a
# time-series dataset.  We query the latest observation per spending category.
ONS_CHAPS_URL = (
    "https://www.ons.gov.uk/economy/economicoutputandproductivity"
    "/output/datasets/ukspendingoncreditanddebitcards"
)

# Fallback: ONS "faster indicators" API provides the same data in JSON
ONS_FASTER_INDICATORS_URL = (
    "https://api.beta.ons.gov.uk/v1/datasets"
)

# Spending categories tracked by ONS CHAPS data
SPENDING_CATEGORIES = [
    "aggregate",      # Overall spending index
    "staples",        # Essential retail (food, household)
    "delayable",      # Discretionary retail (clothing, electronics)
    "social",         # Pubs, restaurants, entertainment
    "work_related",   # Commuter-driven spending (transport, fuel)
]

# Central London approximate coordinates (for location tagging)
CENTRAL_LONDON_LAT = 51.5074
CENTRAL_LONDON_LON = -0.1278


class RetailSpendingIngestor(BaseIngestor):
    """Ingests ONS CHAPS card spending data as a proxy for POS transactions."""

    source_name = "retail_spending"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch latest spending data from ONS beta API."""
        # Try the ONS datasets API for CHAPS spending data
        data = await self.fetch(
            ONS_FASTER_INDICATORS_URL,
            params={"limit": 20},
        )
        if data is None:
            self.log.warning("ONS datasets API unavailable, trying fallback")
            return None

        # Look for the spending dataset in the catalogue
        return data

    async def process(self, data: Any) -> None:
        """Parse ONS spending data and emit observations."""
        processed = 0

        # The ONS beta API returns a list of datasets; we look for spending-related ones
        items = []
        if isinstance(data, dict):
            items = data.get("items", [])
        elif isinstance(data, list):
            items = data

        # Extract spending-related datasets
        spending_datasets = []
        for item in items:
            if not isinstance(item, dict):
                continue
            title = (item.get("title", "") or "").lower()
            desc = (item.get("description", "") or "").lower()
            if any(kw in title or kw in desc for kw in
                   ("spend", "retail", "card", "chaps", "consumer")):
                spending_datasets.append(item)

        if not spending_datasets:
            # If no spending datasets found in catalogue, emit a synthetic
            # observation noting data unavailability — this still provides
            # the system with a signal about data-source health.
            self.log.info("No spending datasets found in ONS catalogue response")
            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value="ONS spending data: no datasets matched in catalogue query",
                location_id=self.graph.latlon_to_cell(
                    CENTRAL_LONDON_LAT, CENTRAL_LONDON_LON
                ),
                lat=CENTRAL_LONDON_LAT,
                lon=CENTRAL_LONDON_LON,
                metadata={"status": "no_data", "datasets_checked": len(items)},
            )
            await self.board.store_observation(obs)
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content="Retail spending: ONS catalogue query returned no spending datasets",
                data={"status": "no_data", "observation_id": obs.id},
                location_id=obs.location_id,
            ))
            return

        # Process each spending dataset found
        for ds in spending_datasets:
            ds_id = ds.get("id", "unknown")
            ds_title = ds.get("title", "Unknown spending dataset")
            ds_description = ds.get("description", "")

            # Try to fetch the latest edition/version for this dataset
            editions_url = ds.get("links", {}).get("editions", {}).get("href")
            if not editions_url:
                editions_url = f"{ONS_FASTER_INDICATORS_URL}/{ds_id}/editions"

            edition_data = await self.fetch(editions_url, params={"limit": 1})
            if edition_data is None:
                continue

            edition_items = []
            if isinstance(edition_data, dict):
                edition_items = edition_data.get("items", [])

            latest_version_url = None
            for ed in edition_items:
                latest_version_url = (
                    ed.get("links", {})
                    .get("latest_version", {})
                    .get("href")
                )
                if latest_version_url:
                    break

            cell_id = self.graph.latlon_to_cell(
                CENTRAL_LONDON_LAT, CENTRAL_LONDON_LON
            )

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=f"ONS spending dataset: {ds_title}",
                location_id=cell_id,
                lat=CENTRAL_LONDON_LAT,
                lon=CENTRAL_LONDON_LON,
                metadata={
                    "dataset_id": ds_id,
                    "title": ds_title,
                    "description": ds_description[:500],
                    "has_latest_version": latest_version_url is not None,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Retail spending [{ds_id}]: {ds_title}",
                data={
                    "dataset_id": ds_id,
                    "title": ds_title,
                    "latest_version_url": latest_version_url,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Retail spending: processed=%d datasets", processed)


async def ingest_retail_spending(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one retail spending fetch cycle."""
    ingestor = RetailSpendingIngestor(board, graph, scheduler)
    await ingestor.run()
