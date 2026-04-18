"""LAQN transport-interchange air quality — actual NO2/PM2.5 concentrations (µg/m³)
from roadside/kerbside monitoring sites near major transport interchanges and
river crossings.

Complements air_quality.py which ingests index values (1-10 bands) for all sites.
This ingestor provides granular concentration data enabling transport↔air-quality
anomaly correlation.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone, timedelta
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.laqn_transport")

# LAQN hourly data endpoint: returns µg/m³ concentrations per site/species
LAQN_DATA_URL = (
    "https://api.erg.ic.ac.uk/AirQuality/Data/SiteSpecies"
    "/SiteCode={site_code}/SpeciesCode={species}"
    "/StartDate={start}/EndDate={end}/Json"
)

# Roadside/kerbside sites near major transport interchanges and river crossings.
# Format: (SiteCode, display_name, lat, lon)
TRANSPORT_SITES = [
    ("MY1", "Marylebone Road", 51.5225, -0.1546),           # A501 corridor / Marylebone
    ("CD1", "Camden - Euston Road", 51.5293, -0.1288),      # Euston/King's Cross interchange
    ("TH4", "Tower Hamlets - Blackwall", 51.5087, -0.0055), # Blackwall Tunnel approach
    ("CT8", "City of London - Walbrook Wharf", 51.5107, -0.0912),  # near Southwark Bridge
    ("SK8", "Southwark - Old Kent Road", 51.4808, -0.0588), # A2 major route
    ("GR9", "Greenwich - Burrage Grove", 51.4854, 0.0107),  # near Woolwich crossing
    ("WM0", "Westminster - Horseferry Road", 51.4946, -0.1318),  # near Westminster/Vauxhall
    ("LB6", "Lambeth - Brixton Road", 51.4647, -0.1143),    # major A23 transport corridor
    ("IS2", "Islington - Holloway Road", 51.5571, -0.1165),  # near Highbury & Islington
    ("KC3", "Kensington - Cromwell Road", 51.4955, -0.1787), # near Earls Court interchange
]

SPECIES = ["NO2", "PM25"]

# LAQN uses "PM25" not "PM2.5" in API paths; map for display
SPECIES_DISPLAY = {"NO2": "NO2", "PM25": "PM2.5"}


class LAQNTransportIngestor(BaseIngestor):
    source_name = "laqn_transport"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        now = datetime.now(timezone.utc)
        # LAQN API 400s when StartDate == EndDate; always span at least 1 day
        start = (now - timedelta(days=1)).strftime("%Y-%m-%d")
        end = now.strftime("%Y-%m-%d")

        results = []
        fetch_failures = 0
        total_fetches = 0
        for site_code, site_name, lat, lon in TRANSPORT_SITES:
            for species in SPECIES:
                total_fetches += 1
                url = LAQN_DATA_URL.format(
                    site_code=site_code,
                    species=species,
                    start=start,
                    end=end,
                )
                data = await self.fetch(url)
                if data is not None:
                    results.append({
                        "site_code": site_code,
                        "site_name": site_name,
                        "lat": lat,
                        "lon": lon,
                        "species": species,
                        "raw": data,
                    })
                else:
                    fetch_failures += 1
        # If ALL fetches failed, signal to circuit breaker
        if fetch_failures == total_fetches:
            return None
        return results

    async def process(self, data: Any) -> None:
        processed = 0
        skipped = 0

        for entry in data:
            site_name = entry["site_name"]
            lat = entry["lat"]
            lon = entry["lon"]
            species = entry["species"]
            raw = entry["raw"]

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            # LAQN SiteSpecies response: {"RawAQData": {"Data": [{"@MeasurementDateGMT": ..., "@Value": ...}, ...]}}
            try:
                records = raw.get("RawAQData", {}).get("Data", [])
                if isinstance(records, dict):
                    records = [records]
            except AttributeError:
                self.log.warning("Unexpected structure for %s/%s", entry["site_code"], species)
                skipped += 1
                continue

            # Take the most recent non-empty reading
            latest_value = None
            latest_time = None
            for rec in reversed(records):
                val_str = rec.get("@Value", "")
                if val_str and val_str.strip():
                    try:
                        latest_value = float(val_str)
                        latest_time = rec.get("@MeasurementDateGMT", "")
                        break
                    except (ValueError, TypeError):
                        continue

            if latest_value is None:
                skipped += 1
                continue

            display_species = SPECIES_DISPLAY.get(species, species)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=latest_value,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "site": site_name,
                    "site_code": entry["site_code"],
                    "species": display_species,
                    "unit": "µg/m³",
                    "measurement_time": latest_time,
                    "site_type": "roadside",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Transport AQ [{site_name}] {display_species}={latest_value:.1f} µg/m³"
                    f" ({latest_time})"
                ),
                data={
                    "site": site_name,
                    "site_code": entry["site_code"],
                    "species": display_species,
                    "concentration": latest_value,
                    "unit": "µg/m³",
                    "measurement_time": latest_time,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "LAQN transport AQ: processed=%d skipped=%d", processed, skipped
        )


async def ingest_laqn_transport(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    ingestor = LAQNTransportIngestor(board, graph, scheduler)
    await ingestor.run()
