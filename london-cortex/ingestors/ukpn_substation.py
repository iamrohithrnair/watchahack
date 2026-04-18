"""UK Power Networks substation-level electricity demand & live faults ingestor.

Pulls two datasets from the UKPN Open Data Portal (Opendatasoft API v2.1):

1. **Peak Demand Scenarios** — substation-level demand forecasts for London Power
   Networks (LPN), covering grid and primary substations.  Half-hourly demand data
   would be ideal but requires higher-tier access; the DFES peak scenarios give
   per-substation demand in MW for current and future years.

2. **Live Faults** — near-real-time power cuts with geopoints, customer counts,
   and restoration times.  These correlate directly with transport disruption
   (traffic light outages, station closures) and work-from-home shifts.

API key: optional (improves rate limits + unlocks half-hourly transformer data).
Register free at https://ukpowernetworks.opendatasoft.com/signup/

Source: https://ukpowernetworks.opendatasoft.com/
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.ukpn_substation")

BASE_URL = "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1"

# Dataset IDs
LIVE_FAULTS_DS = "ukpn-live-faults"
PEAK_DEMAND_DS = "ukpn-dfes-peak-demand-scenarios"

# London bbox for geo filtering live faults
_LONDON_GEO_FILTER = (
    f"within_distance(geopoint, geom'POINT({(LONDON_BBOX['min_lon'] + LONDON_BBOX['max_lon']) / 2}"
    f" {(LONDON_BBOX['min_lat'] + LONDON_BBOX['max_lat']) / 2})', 30km)"
)


class UKPNSubstationIngestor(BaseIngestor):
    source_name = "ukpn_substation"
    rate_limit_name = "default"

    def _headers(self) -> dict[str, str] | None:
        key = os.environ.get("UKPN_API_KEY", "")
        if key:
            return {"Authorization": f"Apikey {key}"}
        return None

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}

        # 1. Live faults (near-real-time, has geopoints)
        faults_url = f"{BASE_URL}/catalog/datasets/{LIVE_FAULTS_DS}/records"
        faults_params = {
            "limit": 100,
            "where": _LONDON_GEO_FILTER,
        }
        faults = await self.fetch(faults_url, params=faults_params, headers=self._headers())
        if faults:
            results["faults"] = faults

        # 2. Peak demand scenarios — London Power Networks substations, latest year
        demand_url = f"{BASE_URL}/catalog/datasets/{PEAK_DEMAND_DS}/records"
        demand_params = {
            "limit": 100,
            "where": "dno='LPN'",
            "order_by": "year desc",
        }
        demand = await self.fetch(demand_url, params=demand_params, headers=self._headers())
        if demand:
            results["demand"] = demand

        return results if results else None

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            return

        fault_count = await self._process_faults(data.get("faults"))
        demand_count = await self._process_demand(data.get("demand"))

        self.log.info(
            "UKPN substation: %d live faults, %d demand records processed",
            fault_count, demand_count,
        )

    async def _process_faults(self, data: Any) -> int:
        if not data or not isinstance(data, dict):
            return 0

        records = data.get("results", [])
        count = 0

        for rec in records:
            fields = rec if "IncidentReference" in rec else rec.get("fields", rec)

            incident_ref = fields.get("IncidentReference", "unknown")
            power_cut_type = fields.get("PowerCutType", "")
            customers_affected = fields.get("NoCustomerAffected", 0)
            description = fields.get("IncidentDescription", "")
            postcodes = fields.get("PostCodesAffected", "")
            created = fields.get("CreationDateTime", "")
            est_restore = fields.get("EstimatedRestorationDate", "")

            # Extract geopoint
            geopoint = fields.get("geopoint")
            lat, lon = None, None
            if isinstance(geopoint, dict):
                lat = geopoint.get("lat")
                lon = geopoint.get("lon")
            elif isinstance(geopoint, (list, tuple)) and len(geopoint) == 2:
                lat, lon = geopoint[0], geopoint[1]

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            try:
                customers = int(customers_affected) if customers_affected else 0
            except (ValueError, TypeError):
                customers = 0

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.EVENT,
                value=customers,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "live_fault",
                    "incident_ref": incident_ref,
                    "power_cut_type": power_cut_type,
                    "customers_affected": customers,
                    "postcodes": postcodes,
                    "description": description,
                    "created": created,
                    "est_restore": est_restore,
                },
            )
            await self.board.store_observation(obs)

            severity_hint = ""
            if customers > 5000:
                severity_hint = " [MAJOR]"
            elif customers > 1000:
                severity_hint = " [significant]"

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"UKPN fault{severity_hint}: {power_cut_type} — "
                    f"{customers} customers affected near {postcodes[:60]}"
                    f"{f' | ETA restore: {est_restore}' if est_restore else ''}"
                ),
                data={
                    "type": "live_fault",
                    "incident_ref": incident_ref,
                    "power_cut_type": power_cut_type,
                    "customers_affected": customers,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            count += 1

        return count

    async def _process_demand(self, data: Any) -> int:
        if not data or not isinstance(data, dict):
            return 0

        records = data.get("results", [])
        count = 0

        for rec in records:
            fields = rec if "substation" in rec else rec.get("fields", rec)

            substation = fields.get("substation", "unknown")
            peak_mw = fields.get("peak_demand_mw")
            firm_capacity = fields.get("firm_capacity_mw")
            scenario = fields.get("scenario", "")
            season = fields.get("season", "")
            year = fields.get("year", "")
            gsp = fields.get("gsp", "")

            if peak_mw is None:
                continue

            try:
                peak_val = float(peak_mw)
            except (ValueError, TypeError):
                continue

            # No lat/lon in demand dataset — use central London as proxy
            lat, lon = 51.5, -0.12
            cell_id = self.graph.latlon_to_cell(lat, lon)

            # Calculate utilisation if capacity available
            utilisation = None
            if firm_capacity:
                try:
                    cap = float(firm_capacity)
                    if cap > 0:
                        utilisation = round(peak_val / cap * 100, 1)
                except (ValueError, TypeError):
                    pass

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=peak_val,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "peak_demand",
                    "substation": substation,
                    "gsp": gsp,
                    "scenario": scenario,
                    "season": season,
                    "year": year,
                    "firm_capacity_mw": firm_capacity,
                    "utilisation_pct": utilisation,
                },
            )
            await self.board.store_observation(obs)

            util_str = f" ({utilisation}% utilisation)" if utilisation else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"UKPN substation [{substation}] peak demand: {peak_val:.1f} MW"
                    f"{util_str} ({season} {year}, {scenario})"
                ),
                data={
                    "type": "peak_demand",
                    "substation": substation,
                    "peak_demand_mw": peak_val,
                    "firm_capacity_mw": firm_capacity,
                    "utilisation_pct": utilisation,
                    "scenario": scenario,
                    "season": season,
                    "year": year,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            count += 1

        return count


async def ingest_ukpn_substation(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one UKPN substation fetch cycle."""
    ingestor = UKPNSubstationIngestor(board, graph, scheduler)
    await ingestor.run()
