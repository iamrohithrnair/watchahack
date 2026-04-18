"""UKHSA syndromic surveillance ingestor — NHS 111 + GP + A&E respiratory data.

Fetches respiratory syndromic surveillance indicators from the UKHSA Data
Dashboard API (no auth required).  Data covers:
  - NHS 111 triaged calls for acute respiratory infections (ARI)
  - GP in-hours consultations for respiratory conditions
  - Emergency department attendances for ARI

These act as a city-wide 'human sensor network' to detect public health
impacts of pollution events, complementing the physical air-quality sensor
grid.

API docs: https://ukhsa-dashboard.data.gov.uk/access-our-data
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.nhs_syndromic")

# UKHSA Data Dashboard API — hierarchical REST endpoints, no auth required.
_API_BASE = "https://api.ukhsa-dashboard.data.gov.uk"

# Each metric is a (slug, friendly_label, system) tuple.
# The API path is: /themes/{theme}/sub_themes/{sub}/topics/{topic}/
#                  geography_types/{geo_type}/geographies/{geo}/metrics/{metric}
_METRICS: list[tuple[str, str, str]] = [
    # NHS 111 triaged calls
    (
        "syndromic_surveillance_nhs111_acute_respiratory_infections_countByDay",
        "NHS 111 ARI calls (daily)",
        "nhs_111",
    ),
    (
        "syndromic_surveillance_nhs111_acute_respiratory_infections_RollingMean",
        "NHS 111 ARI calls (7-day avg)",
        "nhs_111",
    ),
    # GP in-hours consultations
    (
        "syndromic_surveillance_gp_acute_respiratory_infections_countByDay",
        "GP ARI consultations (daily)",
        "gp_in_hours",
    ),
    (
        "syndromic_surveillance_gp_acute_respiratory_infections_RollingMean",
        "GP ARI consultations (7-day avg)",
        "gp_in_hours",
    ),
    # Emergency department attendances
    (
        "syndromic_surveillance_ed_acute_respiratory_infections_countByDay",
        "ED ARI attendances (daily)",
        "emergency_dept",
    ),
    (
        "syndromic_surveillance_ed_acute_respiratory_infections_RollingMean",
        "ED ARI attendances (7-day avg)",
        "emergency_dept",
    ),
]

# Central London lat/lon for location tagging (national-level data)
_LONDON_LAT = 51.509
_LONDON_LON = -0.118


class NHSSyndromicIngestor(BaseIngestor):
    source_name = "nhs_syndromic"
    rate_limit_name = "default"

    def _build_url(self, metric: str) -> str:
        return (
            f"{_API_BASE}/themes/infectious_disease"
            f"/sub_themes/respiratory"
            f"/topics/ARI"
            f"/geography_types/Nation"
            f"/geographies/England"
            f"/metrics/{metric}"
        )

    async def fetch_data(self) -> Any:
        """Fetch the latest data point for each syndromic metric."""
        results: list[dict] = []
        for metric_slug, label, system in _METRICS:
            url = self._build_url(metric_slug)
            # Only need the most recent data point; page_size=1
            data = await self.fetch(url, params={"page_size": "7"})
            if data and isinstance(data, dict):
                items = data.get("results", [])
                if items:
                    for item in items:
                        item["_label"] = label
                        item["_system"] = system
                        item["_metric_slug"] = metric_slug
                    results.extend(items)
            elif data and isinstance(data, list):
                for item in data:
                    item["_label"] = label
                    item["_system"] = system
                    item["_metric_slug"] = metric_slug
                results.extend(data)
        return results if results else None

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(_LONDON_LAT, _LONDON_LON)
        processed = 0

        for item in data:
            metric_value = item.get("metric_value") or item.get("value")
            if metric_value is None:
                continue
            try:
                metric_value = float(metric_value)
            except (ValueError, TypeError):
                continue

            label = item.get("_label", "unknown")
            system = item.get("_system", "unknown")
            metric_slug = item.get("_metric_slug", "unknown")
            date = item.get("date", item.get("period_end", ""))

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=metric_value,
                location_id=cell_id,
                lat=_LONDON_LAT,
                lon=_LONDON_LON,
                metadata={
                    "metric": metric_slug,
                    "label": label,
                    "system": system,
                    "date": date,
                    "geography": "England",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Syndromic [{system}] {label}: {metric_value:.0f} ({date})",
                data={
                    "metric": metric_slug,
                    "label": label,
                    "system": system,
                    "value": metric_value,
                    "date": date,
                    "lat": _LONDON_LAT,
                    "lon": _LONDON_LON,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("NHS syndromic surveillance: processed=%d metrics", processed)


async def ingest_nhs_syndromic(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one UKHSA syndromic fetch cycle."""
    ingestor = NHSSyndromicIngestor(board, graph, scheduler)
    await ingestor.run()
