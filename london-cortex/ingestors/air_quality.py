"""LAQN air quality ingestor — NO2, PM2.5, PM10, O3 per monitoring site."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.air_quality")

LAQN_URL = (
    "https://api.erg.ic.ac.uk/AirQuality/Hourly/MonitoringIndex/GroupName=London/Json"
)

# Species codes we care about
SPECIES_OF_INTEREST = {"NO2", "PM2.5", "PM10", "O3"}


class AirQualityIngestor(BaseIngestor):
    source_name = "laqn"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(LAQN_URL)

    async def process(self, data: Any) -> None:
        # LAQN returns {"HourlyAirQualityIndex": {"LocalAuthority": [...] | {...}}}
        try:
            hourly = data.get("HourlyAirQualityIndex", {})
            authorities = hourly.get("LocalAuthority", [])
            if isinstance(authorities, dict):
                authorities = [authorities]
        except AttributeError:
            self.log.warning("Unexpected LAQN data structure: %s", type(data))
            return

        processed = 0
        skipped = 0

        for authority in authorities:
            sites = authority.get("Site", [])
            if isinstance(sites, dict):
                sites = [sites]

            for site in sites:
                site_name = site.get("@SiteName", "unknown")
                lat_str = site.get("@Latitude")
                lon_str = site.get("@Longitude")

                try:
                    lat = float(lat_str) if lat_str else None
                    lon = float(lon_str) if lon_str else None
                except (ValueError, TypeError):
                    lat = lon = None

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                species_list = site.get("Species", [])
                if isinstance(species_list, dict):
                    species_list = [species_list]

                for species in species_list:
                    species_code = species.get("@SpeciesCode", "")
                    if species_code not in SPECIES_OF_INTEREST:
                        continue

                    index_str = species.get("@AirQualityIndex")
                    band = species.get("@AirQualityBand", "")

                    try:
                        index_val = float(index_str) if index_str else None
                    except (ValueError, TypeError):
                        index_val = None

                    if index_val is None:
                        skipped += 1
                        continue

                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=index_val,
                        location_id=cell_id,
                        lat=lat,
                        lon=lon,
                        metadata={
                            "site": site_name,
                            "species": species_code,
                            "band": band,
                        },
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"Air quality [{site_name}] {species_code}={index_val} ({band})"
                        ),
                        data={
                            "site": site_name,
                            "species": species_code,
                            "index": index_val,
                            "band": band,
                            "lat": lat,
                            "lon": lon,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1

        self.log.info("LAQN air quality: processed=%d skipped=%d", processed, skipped)


async def ingest_air_quality(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one LAQN fetch cycle."""
    ingestor = AirQualityIngestor(board, graph, scheduler)
    await ingestor.run()
