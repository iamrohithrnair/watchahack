"""Thames Water EDM ingestor — near-real-time storm overflow discharge monitoring.

Addresses the Brain's suggestion to integrate critical infrastructure maintenance/incident
data to differentiate between routine engineering work and genuine systemic failures in
London's water infrastructure.

Uses the Thames Water Open Data API:
- DischargeCurrentStatus: live status of all Event Duration Monitors (EDMs)
- Classifies each monitor as: offline, discharging, not discharging

This allows the system to detect:
- Mass discharge events (multiple CSOs discharging simultaneously = storm/sewage event)
- Monitor offline clusters (planned maintenance vs infrastructure failure)
- Normal baseline (most monitors not discharging)

Requires free API credentials from https://data.thameswater.co.uk/s/application-listing
Set THAMES_WATER_CLIENT_ID and THAMES_WATER_CLIENT_SECRET in .env.
"""

from __future__ import annotations

import logging
import math
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.thames_water")

_API_BASE = "https://prod-tw-opendata-app.uk-e1.cloudhub.io/data/STE/v1"
_CURRENT_STATUS_URL = f"{_API_BASE}/DischargeCurrentStatus"

# London-area permit prefixes (Thames Water permits covering Greater London)
# Thames Water covers London plus surrounding areas; we filter by lat/lon bounds
_LONDON_LAT_MIN = 51.28
_LONDON_LAT_MAX = 51.70
_LONDON_LON_MIN = -0.51
_LONDON_LON_MAX = 0.33


def _osgb36_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """Convert OSGB36 (British National Grid) to WGS84 lat/lon.

    Simplified Helmert transform — accurate to ~5m, sufficient for grid-cell
    assignment.
    """
    a, b = 6377563.396, 6356256.909
    e2 = 1 - (b * b) / (a * a)
    F0 = 0.9996012717
    phi0 = math.radians(49)
    lam0 = math.radians(-2)
    N0, E0 = -100000, 400000

    n = (a - b) / (a + b)
    n2, n3 = n * n, n * n * n

    phi = (northing - N0) / (a * F0) + phi0
    for _ in range(10):
        M = (
            b * F0 * (
                (1 + n + 1.25 * n2 + 1.25 * n3) * (phi - phi0)
                - (3 * n + 3 * n2 + 2.625 * n3)
                  * math.sin(phi - phi0) * math.cos(phi + phi0)
                + (1.875 * n2 + 1.875 * n3)
                  * math.sin(2 * (phi - phi0)) * math.cos(2 * (phi + phi0))
                - (35 / 24 * n3)
                  * math.sin(3 * (phi - phi0)) * math.cos(3 * (phi + phi0))
            )
        )
        if abs(northing - N0 - M) < 0.00001:
            break
        phi = (northing - N0 - M) / (a * F0) + phi

    sin_phi = math.sin(phi)
    cos_phi = math.cos(phi)
    tan_phi = math.tan(phi)
    nu = a * F0 / math.sqrt(1 - e2 * sin_phi * sin_phi)
    rho = a * F0 * (1 - e2) / ((1 - e2 * sin_phi * sin_phi) ** 1.5)
    eta2 = nu / rho - 1

    VII = tan_phi / (2 * rho * nu)
    VIII = tan_phi / (24 * rho * nu**3) * (5 + 3 * tan_phi**2 + eta2 - 9 * tan_phi**2 * eta2)
    IX = tan_phi / (720 * rho * nu**5) * (61 + 90 * tan_phi**2 + 45 * tan_phi**4)
    X_ = 1 / (cos_phi * nu)
    XI = 1 / (6 * cos_phi * nu**3) * (nu / rho + 2 * tan_phi**2)
    XII = 1 / (120 * cos_phi * nu**5) * (5 + 28 * tan_phi**2 + 24 * tan_phi**4)

    dE = easting - E0
    lat_rad = phi - VII * dE**2 + VIII * dE**4 - IX * dE**6
    lon_rad = lam0 + X_ * dE - XI * dE**3 + XII * dE**5

    return math.degrees(lat_rad), math.degrees(lon_rad)


class ThamesWaterIngestor(BaseIngestor):
    source_name = "thames_water_edm"
    rate_limit_name = "default"

    def _get_auth_headers(self) -> dict[str, str] | None:
        client_id = os.environ.get("THAMES_WATER_CLIENT_ID", "")
        client_secret = os.environ.get("THAMES_WATER_CLIENT_SECRET", "")
        if not client_id or not client_secret:
            return None
        return {"client_id": client_id, "client_secret": client_secret}

    async def fetch_data(self) -> Any:
        headers = self._get_auth_headers()
        if headers is None:
            # Try unauthenticated — the EDM endpoint serves public data
            self.log.info(
                "Thames Water credentials not set — attempting unauthenticated access"
            )
            result = await self.fetch(_CURRENT_STATUS_URL)
            if result is not None:
                self.log.info("Thames Water unauthenticated access succeeded")
                return result
            self.log.warning(
                "Thames Water unauthenticated access failed — "
                "set THAMES_WATER_CLIENT_ID and THAMES_WATER_CLIENT_SECRET"
            )
            return None
        return await self.fetch(_CURRENT_STATUS_URL, headers=headers)

    async def process(self, data: Any) -> None:
        # API returns a JSON object with an "items" array of EDM records
        if isinstance(data, dict):
            items = data.get("items", [])
        elif isinstance(data, list):
            items = data
        else:
            self.log.warning("Unexpected Thames Water response type: %s", type(data))
            return

        if not items:
            self.log.info("Thames Water EDM: no items returned")
            return

        # Classify monitors
        discharging = []
        not_discharging = []
        offline = []
        london_discharging = []
        total = 0

        for item in items:
            total += 1
            status = str(item.get("AlertStatus", "")).strip().lower()
            # API returns OSGB36 Eastings/Northings (X/Y), not lat/lon
            easting = _safe_float(item.get("X"))
            northing = _safe_float(item.get("Y"))
            if easting and northing and easting > 100 and northing > 100:
                lat, lon = _osgb36_to_wgs84(easting, northing)
            else:
                lat, lon = None, None
            site_name = item.get("LocationName", "") or item.get("SiteName", "unknown")
            permit = item.get("PermitNumber", "")
            watercourse = item.get("ReceivingWaterCourse", "")

            record = {
                "site": site_name,
                "permit": permit,
                "status": status,
                "watercourse": watercourse,
                "lat": lat,
                "lon": lon,
            }

            if "discharg" in status:
                discharging.append(record)
                if _in_london(lat, lon):
                    london_discharging.append(record)
            elif "offline" in status or "no data" in status:
                offline.append(record)
            else:
                not_discharging.append(record)

        # Severity based on London-area discharges
        london_count = len(london_discharging)
        if london_count > 20:
            severity = 3  # major storm/sewage event
        elif london_count > 10:
            severity = 2
        elif london_count > 0:
            severity = 1
        else:
            severity = 0

        # Offline ratio — high offline % might indicate maintenance or monitoring failure
        offline_ratio = len(offline) / total if total > 0 else 0.0

        # Store summary observation
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=float(london_count),
            location_id=None,
            lat=51.5074,
            lon=-0.1278,
            metadata={
                "metric": "water_discharge_count",
                "total_monitors": total,
                "discharging": len(discharging),
                "not_discharging": len(not_discharging),
                "offline": len(offline),
                "offline_ratio": round(offline_ratio, 3),
                "london_discharging": london_count,
                "severity": severity,
                "london_sites": [
                    {
                        "site": r["site"],
                        "watercourse": r["watercourse"],
                        "permit": r["permit"],
                    }
                    for r in london_discharging[:10]
                ],
            },
        )
        await self.board.store_observation(obs)

        # Post to #raw
        content = (
            f"Thames Water EDM: {london_count} London CSOs discharging "
            f"(of {len(discharging)} total), {len(offline)} monitors offline "
            f"({offline_ratio:.0%}), severity={severity}"
        )
        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=content,
            data={
                "metric": "water_discharge_count",
                "london_discharging": london_count,
                "total_discharging": len(discharging),
                "offline": len(offline),
                "offline_ratio": round(offline_ratio, 3),
                "severity": severity,
                "observation_id": obs.id,
            },
        ))

        # If many London CSOs discharging, post individual observations for spatial analysis
        if london_count > 0:
            for rec in london_discharging[:20]:
                lat, lon = rec["lat"], rec["lon"]
                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None
                site_obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=1.0,  # binary: discharging
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "metric": "cso_discharge",
                        "site": rec["site"],
                        "watercourse": rec["watercourse"],
                        "permit": rec["permit"],
                    },
                )
                await self.board.store_observation(site_obs)

        # High offline ratio may indicate planned maintenance window
        if offline_ratio > 0.3:
            await self.board.post(AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Thames Water: {len(offline)}/{total} monitors offline "
                    f"({offline_ratio:.0%}) — possible planned maintenance or monitoring failure"
                ),
                data={
                    "metric": "water_monitor_offline",
                    "offline": len(offline),
                    "total": total,
                    "offline_ratio": round(offline_ratio, 3),
                    "observation_id": obs.id,
                },
            ))

        self.log.info(
            "Thames Water EDM: total=%d discharging=%d (london=%d) offline=%d severity=%d",
            total, len(discharging), london_count, len(offline), severity,
        )


def _safe_float(val: Any) -> float | None:
    if val is None:
        return None
    try:
        return float(val)
    except (ValueError, TypeError):
        return None


def _in_london(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return _LONDON_LAT_MIN <= lat <= _LONDON_LAT_MAX and _LONDON_LON_MIN <= lon <= _LONDON_LON_MAX


async def ingest_thames_water(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Thames Water EDM fetch cycle."""
    ingestor = ThamesWaterIngestor(board, graph, scheduler)
    await ingestor.run()
