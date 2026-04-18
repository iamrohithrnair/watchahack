"""BODS Bus AVL (Automatic Vehicle Location) ingestor — real-time GPS positions for London buses.

Uses the Bus Open Data Service (BODS) SIRI-VM feed to get live vehicle positions,
IDs, bearings, and line references. This enables direct attribution of data ghosts
to specific vehicles rather than inferring from arrival predictions.

API docs: https://data.bus-data.dft.gov.uk/guidance/requirements/?section=api
Requires a free BODS API key: https://data.bus-data.dft.gov.uk/account/signup/
"""

from __future__ import annotations

import logging
import os
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.config import LONDON_BBOX
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.bus_avl")

BODS_SIRI_VM_URL = "https://data.bus-data.dft.gov.uk/api/v1/datafeed/"

# SIRI XML namespaces
NS = {"siri": "http://www.siri.org.uk/siri"}


class BusAvlIngestor(BaseIngestor):
    source_name = "bods_bus_avl"
    rate_limit_name = "default"
    required_env_vars = ["BODS_API_KEY"]

    async def fetch_data(self) -> Any:
        api_key = os.environ.get("BODS_API_KEY", "")
        if not api_key:
            self.log.warning("BODS_API_KEY not set, skipping bus AVL ingest")
            return None

        # Bounding box covering Greater London
        bbox = (
            f"{LONDON_BBOX['min_lon']},{LONDON_BBOX['min_lat']},"
            f"{LONDON_BBOX['max_lon']},{LONDON_BBOX['max_lat']}"
        )
        params = {
            "api_key": api_key,
            "boundingBox": bbox,
            "operatorRef": "TFLO",  # TfL operator reference in BODS
        }

        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(BODS_SIRI_VM_URL, params=params) as resp:
                    if resp.status == 200:
                        return await resp.text()
                    self.log.warning("BODS API returned HTTP %d", resp.status)
                    return None
        except (aiohttp.ClientError, Exception) as exc:
            self.log.warning("BODS fetch error: %s", exc)
            return None

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            return

        try:
            root = ET.fromstring(data)
        except ET.ParseError as exc:
            self.log.warning("Failed to parse SIRI-VM XML: %s", exc)
            return

        # Navigate: Siri > ServiceDelivery > VehicleMonitoringDelivery > VehicleActivity
        activities = (
            root.findall(".//siri:VehicleActivity", NS)
            or root.findall(".//{http://www.siri.org.uk/siri}VehicleActivity")
        )
        # Fallback: try without namespace (some feeds omit it)
        if not activities:
            activities = root.findall(".//VehicleActivity")

        processed = 0
        skipped = 0

        for activity in activities:
            try:
                vehicle = self._parse_activity(activity)
            except Exception:
                skipped += 1
                continue

            if vehicle is None:
                skipped += 1
                continue

            lat, lon = vehicle["lat"], vehicle["lon"]
            cell_id = self.graph.latlon_to_cell(lat, lon)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=vehicle.get("bearing", 0.0),
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "vehicle_ref": vehicle["vehicle_ref"],
                    "line_ref": vehicle.get("line_ref", ""),
                    "direction_ref": vehicle.get("direction_ref", ""),
                    "operator_ref": vehicle.get("operator_ref", ""),
                    "origin_ref": vehicle.get("origin_ref", ""),
                    "destination_ref": vehicle.get("destination_ref", ""),
                    "bearing": vehicle.get("bearing"),
                    "recorded_at": vehicle.get("recorded_at", ""),
                    "metric": "bus_avl_position",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Bus AVL [{vehicle['vehicle_ref']}] "
                    f"line={vehicle.get('line_ref', '?')} "
                    f"bearing={vehicle.get('bearing', '?')}° "
                    f"({lat:.5f},{lon:.5f})"
                ),
                data={
                    "vehicle_ref": vehicle["vehicle_ref"],
                    "line_ref": vehicle.get("line_ref", ""),
                    "direction_ref": vehicle.get("direction_ref", ""),
                    "bearing": vehicle.get("bearing"),
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "BODS bus AVL: vehicles=%d skipped=%d", processed, skipped
        )

    def _parse_activity(self, activity: ET.Element) -> dict[str, Any] | None:
        """Extract fields from a SIRI VehicleActivity element."""

        def _text(parent: ET.Element, tag: str) -> str | None:
            """Find text in element, trying with and without namespace."""
            el = parent.find(f"siri:{tag}", NS)
            if el is None:
                el = parent.find(f"{{http://www.siri.org.uk/siri}}{tag}")
            if el is None:
                el = parent.find(tag)
            return el.text if el is not None and el.text else None

        def _deep_text(parent: ET.Element, *tags: str) -> str | None:
            """Walk nested tags."""
            node = parent
            for tag in tags:
                found = node.find(f"siri:{tag}", NS)
                if found is None:
                    found = node.find(f"{{http://www.siri.org.uk/siri}}{tag}")
                if found is None:
                    found = node.find(tag)
                if found is None:
                    return None
                node = found
            return node.text if node.text else None

        recorded_at = _text(activity, "RecordedAtTime")

        # MonitoredVehicleJourney contains most fields
        mvj_tag = "MonitoredVehicleJourney"
        mvj = activity.find(f"siri:{mvj_tag}", NS)
        if mvj is None:
            mvj = activity.find(f"{{http://www.siri.org.uk/siri}}{mvj_tag}")
        if mvj is None:
            mvj = activity.find(mvj_tag)
        if mvj is None:
            return None

        vehicle_ref = _text(mvj, "VehicleRef")
        if not vehicle_ref:
            return None

        # Location is nested: VehicleLocation > Longitude / Latitude
        lat_str = _deep_text(mvj, "VehicleLocation", "Latitude")
        lon_str = _deep_text(mvj, "VehicleLocation", "Longitude")
        if not lat_str or not lon_str:
            return None

        try:
            lat = float(lat_str)
            lon = float(lon_str)
        except (ValueError, TypeError):
            return None

        # Skip vehicles outside Greater London bounds
        bbox = LONDON_BBOX
        if not (bbox["min_lat"] <= lat <= bbox["max_lat"] and
                bbox["min_lon"] <= lon <= bbox["max_lon"]):
            return None

        bearing_str = _text(mvj, "Bearing")
        bearing = None
        if bearing_str:
            try:
                bearing = float(bearing_str)
            except (ValueError, TypeError):
                pass

        return {
            "vehicle_ref": vehicle_ref,
            "lat": lat,
            "lon": lon,
            "bearing": bearing,
            "line_ref": _text(mvj, "LineRef") or "",
            "direction_ref": _text(mvj, "DirectionRef") or "",
            "operator_ref": _text(mvj, "OperatorRef") or "",
            "origin_ref": _text(mvj, "OriginRef") or "",
            "destination_ref": _text(mvj, "DestinationRef") or "",
            "recorded_at": recorded_at or "",
        }


async def ingest_bus_avl(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one BODS bus AVL fetch cycle."""
    ingestor = BusAvlIngestor(board, graph, scheduler)
    await ingestor.run()
