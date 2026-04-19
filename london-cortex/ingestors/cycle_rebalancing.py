"""Santander Cycles rebalancing detector — infers van activity from dock-level deltas.

Polls the same TfL BikePoint API as cycle_hire.py but serves a different purpose:
instead of raw occupancy snapshots, this ingestor compares consecutive polls to
detect large step-changes in bike counts that indicate rebalancing van visits.

Key insight from the Brain: between 23:00 and 05:00 organic ridership is near zero,
so any significant dock-level change almost certainly reflects a redistribution van.
During daytime, we use a higher threshold to separate organic flow from van activity.
"""

from __future__ import annotations

import logging
import sys as _sys
import time
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.cycle_rebalancing")

BIKEPOINT_URL = "https://api.tfl.gov.uk/BikePoint"

# Nighttime window (UTC) — organic ridership near zero
_NIGHT_START_HOUR = 23
_NIGHT_END_HOUR = 5

# Lower threshold at night (any meaningful change is likely a van)
_NIGHT_THRESHOLD = 3
# Higher threshold during daytime (filters organic rider flow)
_DAY_THRESHOLD = 8

# Stash previous snapshot in sys to survive hot-reloads.
_SNAPSHOT_KEY = "_cortex_cycle_rebalancing_snapshot"
if not hasattr(_sys, _SNAPSHOT_KEY):
    setattr(_sys, _SNAPSHOT_KEY, {"data": {}, "ts": 0.0})


def _extract_prop(additional_properties: list[dict], key: str) -> str | None:
    for prop in additional_properties:
        if prop.get("key") == key:
            return prop.get("value")
    return None


class CycleRebalancingIngestor(BaseIngestor):
    source_name = "cycle_rebalancing"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(BIKEPOINT_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected BikePoint response type: %s", type(data))
            return

        snapshot_store = getattr(_sys, _SNAPSHOT_KEY)
        prev_snapshot: dict[str, dict] = snapshot_store["data"]
        prev_ts: float = snapshot_store["ts"]
        now = time.time()

        # Build current snapshot
        current_snapshot: dict[str, dict] = {}
        for station in data:
            props = station.get("additionalProperties", [])
            installed = _extract_prop(props, "Installed")
            if installed and installed.lower() != "true":
                continue

            station_id = station.get("id", "")
            if not station_id:
                continue

            nb_bikes_str = _extract_prop(props, "NbBikes")
            nb_docks_str = _extract_prop(props, "NbDocks")
            try:
                nb_bikes = int(nb_bikes_str) if nb_bikes_str else None
                nb_docks = int(nb_docks_str) if nb_docks_str else None
            except (ValueError, TypeError):
                continue

            if nb_bikes is None or nb_docks is None:
                continue

            current_snapshot[station_id] = {
                "name": station.get("commonName", "unknown"),
                "lat": station.get("lat"),
                "lon": station.get("lon"),
                "nb_bikes": nb_bikes,
                "nb_docks": nb_docks,
            }

        # First run — just store the snapshot, no deltas to compute.
        if not prev_snapshot or prev_ts == 0.0:
            snapshot_store["data"] = current_snapshot
            snapshot_store["ts"] = now
            self.log.info(
                "Rebalancing detector: baseline snapshot stored (%d stations)",
                len(current_snapshot),
            )
            return

        # Determine if we're in the nighttime window
        utc_hour = datetime.now(timezone.utc).hour
        nighttime = utc_hour >= _NIGHT_START_HOUR or utc_hour < _NIGHT_END_HOUR
        threshold = _NIGHT_THRESHOLD if nighttime else _DAY_THRESHOLD

        # Compute deltas
        events = 0
        interval_min = (now - prev_ts) / 60.0

        for station_id, cur in current_snapshot.items():
            prev = prev_snapshot.get(station_id)
            if prev is None:
                continue

            delta = cur["nb_bikes"] - prev["nb_bikes"]
            if abs(delta) < threshold:
                continue

            lat, lon = cur["lat"], cur["lon"]
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None
            name = cur["name"]
            event_type = "drop_off" if delta > 0 else "pick_up"
            rate_per_hour = (delta / interval_min) * 60 if interval_min > 0 else 0

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=rate_per_hour,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "station": name,
                    "station_id": station_id,
                    "event_type": event_type,
                    "bikes_before": prev["nb_bikes"],
                    "bikes_after": cur["nb_bikes"],
                    "delta": delta,
                    "nb_docks": cur["nb_docks"],
                    "interval_minutes": round(interval_min, 1),
                    "rate_per_hour": round(rate_per_hour, 1),
                    "nighttime": nighttime,
                },
            )
            await self.board.store_observation(obs)

            tag = " [NIGHT]" if nighttime else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Rebalancing{tag} {event_type} [{name}] "
                    f"delta={delta:+d} bikes "
                    f"({prev['nb_bikes']}->{cur['nb_bikes']}/{cur['nb_docks']} docks) "
                    f"over {interval_min:.0f}min "
                    f"(rate: {rate_per_hour:+.1f}/hr)"
                ),
                data={
                    "station": name,
                    "station_id": station_id,
                    "event_type": event_type,
                    "delta": delta,
                    "rate_per_hour": round(rate_per_hour, 1),
                    "bikes_before": prev["nb_bikes"],
                    "bikes_after": cur["nb_bikes"],
                    "nb_docks": cur["nb_docks"],
                    "interval_minutes": round(interval_min, 1),
                    "nighttime": nighttime,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            events += 1

        # Update stored snapshot
        snapshot_store["data"] = current_snapshot
        snapshot_store["ts"] = now

        self.log.info(
            "Rebalancing detector: %d events (%d stations, %.0fmin interval, night=%s, threshold=%d)",
            events, len(current_snapshot), interval_min, nighttime, threshold,
        )


async def ingest_cycle_rebalancing(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one rebalancing detection cycle."""
    ingestor = CycleRebalancingIngestor(board, graph, scheduler)
    await ingestor.run()
