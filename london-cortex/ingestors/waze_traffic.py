"""Waze for Cities ingestor — crowd-sourced jams, incidents, and irregularities.

Requires a Waze for Cities partnership (free for public agencies).
Set WAZE_FEED_URL in .env to the GeoRSS feed URL provided by Waze.
Feed format: JSON with jams, alerts, and irregularities updated every ~2 min.
"""

from __future__ import annotations

import logging
import os
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.waze_traffic")

# Waze jam severity levels (0-5) mapped to our 1-5 scale
_JAM_LEVEL_MAP = {0: 1, 1: 1, 2: 2, 3: 3, 4: 4, 5: 5}

# Alert types we care about
_ALERT_TYPES = {
    "ACCIDENT": 4,
    "JAM": 2,
    "ROAD_CLOSED": 4,
    "WEATHERHAZARD": 3,
    "HAZARD": 3,
    "CONSTRUCTION": 2,
    "POLICE": 1,
    "MISC": 1,
}


class WazeTrafficIngestor(BaseIngestor):
    source_name = "waze_traffic"
    rate_limit_name = "default"
    required_env_vars = ["WAZE_FEED_URL"]

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self.feed_url = os.environ.get("WAZE_FEED_URL", "")

    async def fetch_data(self) -> Any:
        if not self.feed_url:
            self.log.debug("WAZE_FEED_URL not set, skipping")
            return None
        return await self.fetch(self.feed_url, params={"format": "JSON"})

    async def _ingest_jams(self, jams: list[dict]) -> int:
        processed = 0
        for jam in jams:
            line = jam.get("line", [])
            if not line:
                continue

            # Use midpoint of jam line for location
            mid = line[len(line) // 2]
            lat = mid.get("y")
            lon = mid.get("x")
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            street = jam.get("street", "Unknown road")
            speed_kmh = round(jam.get("speed", 0), 1)  # already km/h
            delay = jam.get("delay", 0)  # seconds
            length_m = jam.get("length", 0)
            level = jam.get("level", 0)  # 0-5
            severity_score = _JAM_LEVEL_MAP.get(level, 2)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=severity_score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "jam",
                    "street": street,
                    "speed_kmh": speed_kmh,
                    "delay_sec": delay,
                    "length_m": length_m,
                    "level": level,
                    "road_type": jam.get("roadType", -1),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Waze jam [{street}] level={level} speed={speed_kmh}km/h "
                    f"delay={delay}s length={length_m}m"
                ),
                data={
                    "type": "jam",
                    "street": street,
                    "speed_kmh": speed_kmh,
                    "delay_sec": delay,
                    "length_m": length_m,
                    "level": level,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        return processed

    async def _ingest_alerts(self, alerts: list[dict]) -> int:
        processed = 0
        for alert in alerts:
            lat = alert.get("location", {}).get("y")
            lon = alert.get("location", {}).get("x")
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            alert_type = alert.get("type", "MISC")
            subtype = alert.get("subtype", "")
            street = alert.get("street", "Unknown")
            severity_score = _ALERT_TYPES.get(alert_type, 2)
            reliability = alert.get("reliability", 0)  # 0-10
            confidence = alert.get("confidence", 0)  # 0-10

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=severity_score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "alert",
                    "alert_type": alert_type,
                    "subtype": subtype,
                    "street": street,
                    "reliability": reliability,
                    "confidence": confidence,
                    "report_by": alert.get("reportBy", ""),
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Waze alert [{street}] {alert_type}/{subtype} "
                    f"reliability={reliability} confidence={confidence}"
                ),
                data={
                    "type": "alert",
                    "alert_type": alert_type,
                    "subtype": subtype,
                    "street": street,
                    "severity_score": severity_score,
                    "reliability": reliability,
                    "confidence": confidence,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        return processed

    async def _ingest_irregularities(self, irregularities: list[dict]) -> int:
        processed = 0
        for irr in irregularities:
            line = irr.get("line", [])
            if not line:
                continue

            mid = line[len(line) // 2]
            lat = mid.get("y")
            lon = mid.get("x")
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            street = irr.get("street", "Unknown road")
            regular_speed = irr.get("regularSpeed", 0)
            delay_seconds = irr.get("delaySeconds", 0)
            length_m = irr.get("length", 0)
            severity = irr.get("severity", 0)
            trend = irr.get("trend", 0)  # -1 improving, 0 stable, 1 worsening
            jam_level = irr.get("jamLevel", 0)
            severity_score = _JAM_LEVEL_MAP.get(jam_level, 2)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=severity_score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "type": "irregularity",
                    "street": street,
                    "regular_speed": regular_speed,
                    "delay_sec": delay_seconds,
                    "length_m": length_m,
                    "severity": severity,
                    "trend": trend,
                    "jam_level": jam_level,
                },
            )
            await self.board.store_observation(obs)

            trend_str = {-1: "improving", 0: "stable", 1: "worsening"}.get(trend, "unknown")
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Waze irregularity [{street}] delay={delay_seconds}s "
                    f"trend={trend_str} jam_level={jam_level}"
                ),
                data={
                    "type": "irregularity",
                    "street": street,
                    "delay_sec": delay_seconds,
                    "length_m": length_m,
                    "trend": trend_str,
                    "jam_level": jam_level,
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
            self.log.warning("Unexpected Waze data format: %s", type(data))
            return

        jams_processed = await self._ingest_jams(data.get("jams", []))
        alerts_processed = await self._ingest_alerts(data.get("alerts", []))
        irregularities_processed = await self._ingest_irregularities(
            data.get("irregularities", [])
        )

        self.log.info(
            "Waze traffic: jams=%d alerts=%d irregularities=%d",
            jams_processed, alerts_processed, irregularities_processed,
        )


async def ingest_waze_traffic(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Waze feed cycle."""
    ingestor = WazeTrafficIngestor(board, graph, scheduler)
    await ingestor.run()
