"""TomTom Traffic ingestor — real-time flow segments and incidents for London.

Uses the TomTom Traffic Flow Segment Data API and Traffic Incident Details API.
Free tier: 2,500 requests/day at https://developer.tomtom.com/
Set TOMTOM_API_KEY in .env.
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

log = logging.getLogger("cortex.ingestors.tomtom_traffic")

# TomTom API base
_BASE = "https://api.tomtom.com"

# Flow Segment Data — returns speed/freeflow for a point on the road network
_FLOW_URL = f"{_BASE}/traffic/services/4/flowSegmentData/relative0/10/json"

# Incident Details — bounding box query for London
_INCIDENTS_URL = f"{_BASE}/traffic/services/5/incidentDetails"

# Sample points on major London corridors for flow monitoring
# (lat, lon, label) — covers key arterials the Brain flagged (e.g. A41)
_FLOW_PROBE_POINTS = [
    (51.5557, -0.1760, "A41 Finchley Rd"),
    (51.5074, -0.1278, "A4 Westminster"),
    (51.5155, -0.0922, "A11 Whitechapel"),
    (51.4720, -0.1147, "A23 Brixton"),
    (51.5362, -0.1050, "A1 Islington"),
    (51.4875, -0.0146, "A2 New Cross"),
    (51.5010, -0.1932, "A4 Cromwell Rd"),
    (51.5225, -0.1550, "A41 Baker St"),
    (51.4613, -0.1150, "A205 S Circular Dulwich"),
    (51.5560, -0.0740, "A10 Stamford Hill"),
]

# Incident severity mapping (TomTom 1-4 → our 1-5)
_SEVERITY_MAP = {1: 1, 2: 2, 3: 3, 4: 5}


class TomTomTrafficIngestor(BaseIngestor):
    source_name = "tomtom_traffic"
    rate_limit_name = "default"
    required_env_vars = ["TOMTOM_API_KEY"]

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.api_key = os.environ.get("TOMTOM_API_KEY", "")

    async def fetch_data(self) -> Any:
        if not self.api_key:
            self.log.debug("TOMTOM_API_KEY not set, skipping")
            return None

        result: dict[str, Any] = {"flow": [], "incidents": []}

        # Fetch flow data for probe points
        for lat, lon, label in _FLOW_PROBE_POINTS:
            data = await self.fetch(
                _FLOW_URL,
                params={"key": self.api_key, "point": f"{lat},{lon}"},
            )
            if data and isinstance(data, dict):
                seg = data.get("flowSegmentData")
                if seg:
                    seg["_probe_label"] = label
                    seg["_probe_lat"] = lat
                    seg["_probe_lon"] = lon
                    result["flow"].append(seg)

        # Fetch incidents in London bounding box
        bbox = LONDON_BBOX
        bbox_str = (
            f"{bbox['min_lat']},{bbox['min_lon']},"
            f"{bbox['max_lat']},{bbox['max_lon']}"
        )
        incidents_data = await self.fetch(
            f"{_INCIDENTS_URL}",
            params={
                "key": self.api_key,
                "bbox": bbox_str,
                "fields": "{incidents{type,geometry{type,coordinates},properties{id,iconCategory,magnitudeOfDelay,events{description,code},startTime,endTime,from,to,length,delay,roadNumbers}}}",
                "language": "en-GB",
                "categoryFilter": "1,2,3,4,5,6,7,8,9,10,11,14",
            },
        )
        if incidents_data and isinstance(incidents_data, dict):
            result["incidents"] = incidents_data.get("incidents", [])

        return result

    async def _process_flow(self, segments: list[dict]) -> int:
        processed = 0
        for seg in segments:
            label = seg.get("_probe_label", "Unknown")
            lat = seg.get("_probe_lat")
            lon = seg.get("_probe_lon")
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            current_speed = seg.get("currentSpeed", 0)
            free_flow_speed = seg.get("freeFlowSpeed", 0)
            current_tt = seg.get("currentTravelTime", 0)
            free_flow_tt = seg.get("freeFlowTravelTime", 0)
            confidence = seg.get("confidence", 0)

            # Congestion ratio: 0 = free flow, 1 = standstill
            congestion = 0.0
            if free_flow_speed > 0:
                congestion = round(1.0 - (current_speed / free_flow_speed), 3)
                congestion = max(0.0, min(1.0, congestion))

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=congestion,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "flow",
                    "label": label,
                    "current_speed_kmh": current_speed,
                    "free_flow_speed_kmh": free_flow_speed,
                    "current_travel_time_sec": current_tt,
                    "free_flow_travel_time_sec": free_flow_tt,
                    "confidence": confidence,
                    "congestion_ratio": congestion,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"TomTom flow [{label}] speed={current_speed}km/h "
                    f"freeflow={free_flow_speed}km/h congestion={congestion:.1%}"
                ),
                data={
                    "type": "flow",
                    "label": label,
                    "current_speed_kmh": current_speed,
                    "free_flow_speed_kmh": free_flow_speed,
                    "congestion_ratio": congestion,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        return processed

    async def _process_incidents(self, incidents: list[dict]) -> int:
        processed = 0
        for inc in incidents:
            props = inc.get("properties", {})
            geom = inc.get("geometry", {})

            # Get location from first coordinate
            coords = geom.get("coordinates", [])
            if not coords:
                continue

            # Coordinates may be nested (LineString) or flat (Point)
            first = coords[0] if coords else None
            if isinstance(first, list):
                # LineString — use midpoint
                mid = coords[len(coords) // 2]
                lon, lat = mid[0], mid[1]
            elif isinstance(first, (int, float)):
                lon, lat = coords[0], coords[1]
            else:
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            icon_cat = props.get("iconCategory", 0)
            delay = props.get("delay", 0)  # seconds
            length_m = props.get("length", 0)
            from_road = props.get("from", "")
            to_road = props.get("to", "")
            road_numbers = props.get("roadNumbers", [])
            magnitude = props.get("magnitudeOfDelay", 0)  # 0-4
            events = props.get("events", [])
            description = events[0].get("description", "") if events else ""

            severity_score = _SEVERITY_MAP.get(magnitude, 2)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity_score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "incident",
                    "description": description,
                    "from": from_road,
                    "to": to_road,
                    "road_numbers": road_numbers,
                    "delay_sec": delay,
                    "length_m": length_m,
                    "magnitude": magnitude,
                    "icon_category": icon_cat,
                },
            )
            await self.board.store_observation(obs)

            road_str = ", ".join(road_numbers) if road_numbers else "unknown road"
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"TomTom incident [{road_str}] {description} "
                    f"delay={delay}s length={length_m}m magnitude={magnitude}"
                ),
                data={
                    "type": "incident",
                    "description": description,
                    "road_numbers": road_numbers,
                    "delay_sec": delay,
                    "length_m": length_m,
                    "magnitude": magnitude,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        return processed

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected TomTom data format: %s", type(data))
            return

        flow_count = await self._process_flow(data.get("flow", []))
        incident_count = await self._process_incidents(data.get("incidents", []))

        self.log.info(
            "TomTom traffic: flow_segments=%d incidents=%d",
            flow_count, incident_count,
        )


async def ingest_tomtom_traffic(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TomTom fetch cycle."""
    ingestor = TomTomTrafficIngestor(board, graph, scheduler)
    await ingestor.run()
