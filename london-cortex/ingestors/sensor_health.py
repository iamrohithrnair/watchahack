"""Sensor health ingestor — monitors LAQN and Environment Agency sensor status.

Addresses the Brain's suggestion: correlate data anomalies with hardware events
by tracking which sensors are reporting, which are silent, and station status.

Complements tfl_digital_health.py (which covers TfL/JamCam health).
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.sensor_health")

# LAQN: site species info shows which sensors *should* be active
LAQN_SITES_URL = (
    "https://api.erg.ic.ac.uk/AirQuality/Information/MonitoringSiteSpecies"
    "/GroupName=London/Json"
)
# LAQN: current hourly data — compare against site list to find silent sensors
LAQN_HOURLY_URL = (
    "https://api.erg.ic.ac.uk/AirQuality/Hourly/MonitoringIndex"
    "/GroupName=London/Json"
)

# Environment Agency: station list with status for London area
EA_STATIONS_URL = (
    "https://environment.data.gov.uk/flood-monitoring/id/stations"
    "?parameter=level&lat=51.5074&long=-0.1278&dist=30&_limit=200"
)


class SensorHealthIngestor(BaseIngestor):
    source_name = "sensor_health"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        laqn_sites = await self.fetch(LAQN_SITES_URL)
        laqn_hourly = await self.fetch(LAQN_HOURLY_URL)
        ea_stations = await self.fetch(EA_STATIONS_URL)
        return {
            "laqn_sites": laqn_sites,
            "laqn_hourly": laqn_hourly,
            "ea_stations": ea_stations,
        }

    async def process(self, data: Any) -> None:
        laqn_count = await self._process_laqn(
            data.get("laqn_sites"), data.get("laqn_hourly")
        )
        ea_count = await self._process_ea(data.get("ea_stations"))
        self.log.info(
            "Sensor health: laqn_observations=%d ea_observations=%d",
            laqn_count, ea_count,
        )

    # ── LAQN sensor health ────────────────────────────────────────────────────

    async def _process_laqn(self, sites_data: Any, hourly_data: Any) -> int:
        if not sites_data or not hourly_data:
            self.log.warning("Missing LAQN data for sensor health check")
            return 0

        # Build set of expected sites from site species list
        expected_sites: dict[str, dict] = {}
        try:
            site_list = sites_data.get("Sites", {}).get("Site", [])
            if isinstance(site_list, dict):
                site_list = [site_list]
            for site in site_list:
                code = site.get("@SiteCode", "")
                if code:
                    expected_sites[code] = {
                        "name": site.get("@SiteName", "unknown"),
                        "lat": site.get("@Latitude"),
                        "lon": site.get("@Longitude"),
                        "date_closed": site.get("@DateClosed", ""),
                    }
        except (AttributeError, TypeError):
            self.log.warning("Unexpected LAQN sites data structure")
            return 0

        # Build set of currently-reporting sites from hourly data
        reporting_sites: set[str] = set()
        try:
            hourly = hourly_data.get("HourlyAirQualityIndex", {})
            authorities = hourly.get("LocalAuthority", [])
            if isinstance(authorities, dict):
                authorities = [authorities]
            for auth in authorities:
                auth_sites = auth.get("Site", [])
                if isinstance(auth_sites, dict):
                    auth_sites = [auth_sites]
                for site in auth_sites:
                    code = site.get("@SiteCode", "")
                    if code:
                        reporting_sites.add(code)
        except (AttributeError, TypeError):
            self.log.warning("Unexpected LAQN hourly data structure")
            return 0

        # Filter to open sites only
        open_sites = {
            code: info for code, info in expected_sites.items()
            if not info.get("date_closed")
        }

        silent_sites = set(open_sites.keys()) - reporting_sites
        total_open = len(open_sites)
        reporting_count = len(reporting_sites & set(open_sites.keys()))
        silent_count = len(silent_sites)
        reporting_rate = reporting_count / total_open if total_open else 0

        # Store per-network health observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=reporting_rate,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "network": "laqn",
                "total_open_sites": total_open,
                "reporting_count": reporting_count,
                "silent_count": silent_count,
                "silent_sites": sorted(silent_sites)[:20],  # cap list size
            },
        )
        await self.board.store_observation(obs)

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"LAQN sensor health: {reporting_count}/{total_open} sites reporting "
                f"({reporting_rate:.0%}), {silent_count} silent"
            ),
            data={
                "network": "laqn",
                "reporting_rate": round(reporting_rate, 3),
                "total_open_sites": total_open,
                "reporting_count": reporting_count,
                "silent_count": silent_count,
                "silent_sites": sorted(silent_sites)[:20],
                "observation_id": obs.id,
            },
        ))

        # Post individual observations for silent sites (enables spatial correlation)
        count = 1
        for code in sorted(silent_sites)[:10]:  # limit to avoid flooding
            info = open_sites[code]
            lat = _safe_float(info.get("lat"))
            lon = _safe_float(info.get("lon"))
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=0.0,  # 0 = not reporting
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "network": "laqn",
                    "site_code": code,
                    "site_name": info["name"],
                    "status": "silent",
                },
            )
            await self.board.store_observation(obs)

            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"LAQN sensor silent: {info['name']} ({code})",
                data={
                    "network": "laqn",
                    "site_code": code,
                    "site_name": info["name"],
                    "status": "silent",
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            ))
            count += 1

        return count

    # ── Environment Agency station health ─────────────────────────────────────

    async def _process_ea(self, data: Any) -> int:
        if not data:
            self.log.warning("No Environment Agency station data")
            return 0

        items = data.get("items", [])
        if not items:
            return 0

        total = len(items)
        active = 0
        inactive = 0
        status_counts: dict[str, int] = {}

        for station in items:
            status = station.get("status", "")
            # Normalize: status can be a URI like ".../statusId/Active"
            if isinstance(status, str):
                status_label = status.rsplit("/", 1)[-1] if "/" in status else status
            else:
                status_label = "unknown"

            status_counts[status_label] = status_counts.get(status_label, 0) + 1

            if status_label.lower() in ("active", ""):
                active += 1
            else:
                inactive += 1

        active_rate = active / total if total else 0

        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=active_rate,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "network": "environment_agency",
                "total_stations": total,
                "active_count": active,
                "inactive_count": inactive,
                "status_breakdown": status_counts,
            },
        )
        await self.board.store_observation(obs)

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"EA station health: {active}/{total} active ({active_rate:.0%}), "
                f"{inactive} inactive"
            ),
            data={
                "network": "environment_agency",
                "active_rate": round(active_rate, 3),
                "total_stations": total,
                "active_count": active,
                "inactive_count": inactive,
                "status_breakdown": status_counts,
                "observation_id": obs.id,
            },
        ))

        return 1


def _safe_float(val: Any) -> float | None:
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


async def ingest_sensor_health(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one sensor health check cycle."""
    ingestor = SensorHealthIngestor(board, graph, scheduler)
    await ingestor.run()
