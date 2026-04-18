"""The City's Retina — perceptual compression via attention zones.

Applies neuroscience-inspired foveal attention to the data grid:
- FOVEA cells: full resolution — every observation stored, all agents process
- PERIFOVEA cells: standard — observations stored, agents process at medium threshold
- PERIPHERY cells: compressed — observations dropped (but z-scores ALWAYS computed),
  anomalies ALWAYS stored, and anomalies trigger saccades that promote cells to fovea

Key guarantee: memory.update_baseline() and z-score detection run on EVERY observation
regardless of zone. Anomalies are always stored. Compression saves DB storage of raw
observations in quiet cells and LLM compute by skipping analysis on peripheral cells.
"""

from __future__ import annotations

import logging
import time
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from .graph import CortexGraph
    from .models import Anomaly

log = logging.getLogger("cortex.retina")


# ── Constants ────────────────────────────────────────────────────────────────

MAX_FOVEA_CELLS = 225          # ~2% of grid (50% increase)
MAX_PERIFOVEA_CELLS = 340      # (50% increase)
FOVEA_DECAY_SECONDS = 1800     # 30 min quiet → demote to perifovea
PERIFOVEA_DECAY_SECONDS = 3600 # 60 min quiet → demote to periphery
SACCADE_COOLDOWN_SECONDS = 300 # 5 min cooldown per cell
SACCADE_MIN_SEVERITY = 2
SACCADE_NEIGHBOR_RADIUS = 4    # larger spotlight radius
EMA_ALPHA = 0.2


# ── Data Structures ──────────────────────────────────────────────────────────

class AttentionZone(IntEnum):
    PERIPHERY = 0
    PERIFOVEA = 1
    FOVEA = 2


@dataclass
class CellAttention:
    zone: AttentionZone = AttentionZone.PERIPHERY
    promoted_at: float = 0.0        # monotonic timestamp of last promotion
    interest_score: float = 0.0     # EMA of anomaly activity
    obs_total: int = 0
    obs_stored: int = 0
    obs_dropped: int = 0
    anomaly_count: int = 0
    last_anomaly_at: float = 0.0
    last_saccade_at: float = 0.0    # for cooldown
    promote_reason: str = ""
    pinned: bool = False            # pinned cells never decay


@dataclass
class SaccadeEvent:
    cell_id: str
    trigger: str
    severity: int
    source: str
    from_zone: AttentionZone
    to_zone: AttentionZone
    timestamp: float = field(default_factory=time.monotonic)

    def to_dict(self) -> dict[str, Any]:
        return {
            "cell_id": self.cell_id,
            "trigger": self.trigger,
            "severity": self.severity,
            "source": self.source,
            "from_zone": self.from_zone.name,
            "to_zone": self.to_zone.name,
            "age_s": round(time.monotonic() - self.timestamp),
        }


@dataclass
class CompressionStats:
    total_obs: int = 0
    stored_obs: int = 0
    dropped_obs: int = 0
    saccade_count: int = 0
    peripheral_detections: int = 0   # anomalies caught in periphery

    @property
    def compression_ratio(self) -> float:
        if self.total_obs == 0:
            return 0.0
        return self.dropped_obs / self.total_obs


# ── AttentionManager ─────────────────────────────────────────────────────────

class AttentionManager:
    """Manages foveal attention across the grid cells."""

    def __init__(self, graph: CortexGraph) -> None:
        self._graph = graph
        self._cells: dict[str, CellAttention] = {}
        self._stats = CompressionStats()
        self._saccades: deque[SaccadeEvent] = deque(maxlen=50)
        # Track anomaly sources per cell for multi-source detection
        self._recent_sources: dict[str, set[str]] = {}  # cell_id -> {sources in last 10 min}
        self._recent_sources_ts: dict[str, float] = {}   # cell_id -> last reset time

        # Initialize all cells as periphery
        for cell_id in graph.cells:
            self._cells[cell_id] = CellAttention()

        log.info("Retina initialized: %d cells, all PERIPHERY", len(self._cells))

        # Pin Heathrow as permanent fovea (51.4700, -0.4543)
        self._pin_fovea(graph, 51.4700, -0.4543, "Heathrow Airport", radius=2)

    # ── Pinned Foci ────────────────────────────────────────────────────────

    def _pin_fovea(self, graph: CortexGraph, lat: float, lon: float, name: str, radius: int = 2) -> None:
        """Pin a lat/lon as permanent fovea (never decays)."""
        now = time.monotonic()
        center = graph.latlon_to_cell(lat, lon)
        if not center or center not in self._cells:
            log.warning("Retina: cannot pin %s — cell not found for (%.4f, %.4f)", name, lat, lon)
            return
        # Pin center as fovea
        cell = self._cells[center]
        cell.zone = AttentionZone.FOVEA
        cell.pinned = True
        cell.promoted_at = now
        cell.promote_reason = f"pinned:{name}"
        # Pin immediate neighbors as perifovea
        for nid in graph.get_nearby_cells(center, radius=radius):
            ncell = self._cells.get(nid)
            if ncell:
                ncell.zone = AttentionZone.PERIFOVEA
                ncell.pinned = True
                ncell.promoted_at = now
                ncell.promote_reason = f"pinned:{name} (vicinity)"
        log.info("Retina: pinned %s as permanent fovea at %s", name, center)

    # ── Zone Queries ─────────────────────────────────────────────────────────

    def get_zone(self, cell_id: str) -> AttentionZone:
        """O(1) zone lookup."""
        cell = self._cells.get(cell_id)
        return cell.zone if cell else AttentionZone.PERIPHERY

    # ── Observation Gate ─────────────────────────────────────────────────────

    def should_store_observation(self, obs: Any) -> bool:
        """Track observation stats per zone. Always stores (returns True).

        Stats still track what *would* be compressed so the dashboard can
        show compression savings when the retina filter toggle is active.
        """
        location_id = getattr(obs, "location_id", None)
        if not location_id:
            return True  # global data always stored

        cell = self._cells.get(location_id)
        if not cell:
            return True  # unknown cell — store to be safe

        cell.obs_total += 1
        self._stats.total_obs += 1

        if cell.zone >= AttentionZone.PERIFOVEA:
            cell.obs_stored += 1
            self._stats.stored_obs += 1
        else:
            cell.obs_dropped += 1
            self._stats.dropped_obs += 1
        return True  # always store — filtering is done at display time

    # ── Agent Processing Gate ────────────────────────────────────────────────

    def should_agent_process(self, cell_id: str, agent_type: str, severity: int = 0) -> bool:
        """Gate for LLM agents. Numeric interpreter always processes (rod cells).
        Vision/text return True based on zone thresholds.
        """
        # Numeric always runs — these are the rod cells
        if agent_type == "numeric":
            return True

        cell = self._cells.get(cell_id)
        if not cell:
            return True

        zone = cell.zone

        if zone == AttentionZone.FOVEA:
            return True
        elif zone == AttentionZone.PERIFOVEA:
            return severity >= 3
        else:  # PERIPHERY
            return severity >= 4

    # ── Anomaly Notification / Saccade ───────────────────────────────────────

    def notify_anomaly(self, anomaly: Any) -> SaccadeEvent | None:
        """Called after every anomaly is stored. May trigger a saccade."""
        cell_id = getattr(anomaly, "location_id", None)
        if not cell_id or cell_id not in self._cells:
            return None

        cell = self._cells[cell_id]
        now = time.monotonic()

        # Update interest score (EMA)
        cell.anomaly_count += 1
        cell.last_anomaly_at = now
        cell.interest_score = (EMA_ALPHA * 1.0) + ((1 - EMA_ALPHA) * cell.interest_score)

        severity = getattr(anomaly, "severity", 1)
        source = getattr(anomaly, "source", "unknown")

        # Track if this was a peripheral detection
        if cell.zone == AttentionZone.PERIPHERY:
            self._stats.peripheral_detections += 1

        # Multi-source tracking (reset every 10 min)
        if cell_id not in self._recent_sources or (now - self._recent_sources_ts.get(cell_id, 0)) > 600:
            self._recent_sources[cell_id] = set()
            self._recent_sources_ts[cell_id] = now
        self._recent_sources[cell_id].add(source)

        # Check saccade triggers
        multi_source = len(self._recent_sources.get(cell_id, set())) >= 2
        severity_trigger = severity >= SACCADE_MIN_SEVERITY

        if not (severity_trigger or multi_source):
            return None

        # Cooldown check
        if (now - cell.last_saccade_at) < SACCADE_COOLDOWN_SECONDS:
            return None

        trigger = f"multi_source_{len(self._recent_sources[cell_id])}" if multi_source else f"severity_{severity}_anomaly"
        return self._execute_saccade(cell_id, trigger, severity, source)

    def _execute_saccade(self, cell_id: str, trigger: str, severity: int, source: str) -> SaccadeEvent:
        """Promote cell to FOVEA and neighbors to PERIFOVEA."""
        now = time.monotonic()
        cell = self._cells[cell_id]
        from_zone = cell.zone

        # Budget enforcement — demote oldest non-pinned fovea cell if over cap
        fovea_cells = [(cid, c) for cid, c in self._cells.items() if c.zone == AttentionZone.FOVEA]
        if len(fovea_cells) >= MAX_FOVEA_CELLS:
            unpinned = [(cid, c) for cid, c in fovea_cells if not c.pinned]
            if unpinned:
                oldest = min(unpinned, key=lambda x: x[1].promoted_at)
                oldest[1].zone = AttentionZone.PERIFOVEA
                log.debug("Retina: budget demote %s FOVEA→PERIFOVEA", oldest[0])

        # Promote target cell
        cell.zone = AttentionZone.FOVEA
        cell.promoted_at = now
        cell.last_saccade_at = now
        cell.promote_reason = trigger

        # Promote neighbors to PERIFOVEA
        neighbors = self._graph.get_nearby_cells(cell_id, radius=SACCADE_NEIGHBOR_RADIUS)
        for nid in neighbors:
            ncell = self._cells.get(nid)
            if ncell and ncell.zone < AttentionZone.PERIFOVEA:
                ncell.zone = AttentionZone.PERIFOVEA
                ncell.promoted_at = now

        # Enforce perifovea budget
        peri_cells = [(cid, c) for cid, c in self._cells.items() if c.zone == AttentionZone.PERIFOVEA]
        if len(peri_cells) > MAX_PERIFOVEA_CELLS:
            peri_cells.sort(key=lambda x: x[1].promoted_at)
            excess = len(peri_cells) - MAX_PERIFOVEA_CELLS
            for cid, c in peri_cells[:excess]:
                c.zone = AttentionZone.PERIPHERY

        event = SaccadeEvent(
            cell_id=cell_id,
            trigger=trigger,
            severity=severity,
            source=source,
            from_zone=from_zone,
            to_zone=AttentionZone.FOVEA,
        )
        self._saccades.append(event)
        self._stats.saccade_count += 1

        log.info(
            "SACCADE %s → FOVEA (trigger=%s, severity=%d, source=%s, neighbors=%d)",
            cell_id, trigger, severity, source, len(neighbors),
        )
        return event

    # ── Manual Focus Request ─────────────────────────────────────────────────

    def request_focus(self, cell_id: str, reason: str) -> SaccadeEvent | None:
        """Allow agents to manually request focus on a cell."""
        if cell_id not in self._cells:
            return None
        return self._execute_saccade(cell_id, f"manual:{reason}", 3, "agent")

    # ── Decay / Maintenance ──────────────────────────────────────────────────

    def tick_decay(self) -> int:
        """Decay attention zones. Called every 5 min in maintenance.

        Fovea cells with no anomaly for 30 min → perifovea.
        Perifovea cells quiet for 60 min → periphery.
        Returns count of demoted cells.
        """
        now = time.monotonic()
        demoted = 0

        for cell_id, cell in self._cells.items():
            if cell.pinned:
                continue
            if cell.zone == AttentionZone.FOVEA:
                quiet_time = now - max(cell.last_anomaly_at, cell.promoted_at)
                if quiet_time >= FOVEA_DECAY_SECONDS:
                    cell.zone = AttentionZone.PERIFOVEA
                    cell.interest_score *= 0.5  # decay interest
                    demoted += 1
            elif cell.zone == AttentionZone.PERIFOVEA:
                quiet_time = now - max(cell.last_anomaly_at, cell.promoted_at)
                if quiet_time >= PERIFOVEA_DECAY_SECONDS:
                    cell.zone = AttentionZone.PERIPHERY
                    cell.interest_score *= 0.25
                    demoted += 1

        if demoted:
            log.info("Retina decay: %d cells demoted", demoted)
        return demoted

    # ── Startup Seeding ──────────────────────────────────────────────────────

    def seed_initial_foci(self, seeds: list[tuple[str | None, str]]) -> None:
        """Seed known hotspots as fovea at startup."""
        now = time.monotonic()
        seeded = 0
        for cell_id, reason in seeds:
            if not cell_id or cell_id not in self._cells:
                continue
            cell = self._cells[cell_id]
            cell.zone = AttentionZone.FOVEA
            cell.promoted_at = now
            cell.promote_reason = f"seed:{reason}"
            seeded += 1

            # Neighbors to perifovea
            for nid in self._graph.get_nearby_cells(cell_id, radius=3):
                ncell = self._cells.get(nid)
                if ncell and ncell.zone == AttentionZone.PERIPHERY:
                    ncell.zone = AttentionZone.PERIFOVEA
                    ncell.promoted_at = now

        log.info("Retina seeded %d fovea cells", seeded)

    # ── State Export ─────────────────────────────────────────────────────────

    def get_retina_state(self) -> dict[str, Any]:
        """Full state for /api/retina endpoint."""
        cells_out: list[dict[str, Any]] = []
        fovea_count = 0
        peri_count = 0
        periphery_count = 0

        for cell_id, cell in self._cells.items():
            gc = self._graph.cells.get(cell_id)
            if not gc:
                continue

            if cell.zone == AttentionZone.FOVEA:
                fovea_count += 1
            elif cell.zone == AttentionZone.PERIFOVEA:
                peri_count += 1
            else:
                periphery_count += 1

            # Only emit non-periphery cells to keep response size manageable
            if cell.zone > AttentionZone.PERIPHERY:
                cells_out.append({
                    "id": cell_id,
                    "zone": cell.zone.name,
                    "zone_level": int(cell.zone),
                    "lat": gc.center_lat,
                    "lon": gc.center_lon,
                    "lat_min": gc.lat_min,
                    "lat_max": gc.lat_max,
                    "lon_min": gc.lon_min,
                    "lon_max": gc.lon_max,
                    "interest": round(cell.interest_score, 2),
                    "obs_total": cell.obs_total,
                    "obs_stored": cell.obs_stored,
                    "obs_dropped": cell.obs_dropped,
                    "anomaly_count": cell.anomaly_count,
                    "reason": cell.promote_reason,
                })

        total_cells = len(self._cells)
        return {
            "cells": cells_out,
            "stats": {
                "fovea_count": fovea_count,
                "perifovea_count": peri_count,
                "periphery_count": periphery_count,
                "total_cells": total_cells,
                "fovea_pct": round(100 * fovea_count / max(total_cells, 1), 1),
                "perifovea_pct": round(100 * peri_count / max(total_cells, 1), 1),
                "periphery_pct": round(100 * periphery_count / max(total_cells, 1), 1),
                "compression_ratio": round(self._stats.compression_ratio * 100, 1),
                "total_obs": self._stats.total_obs,
                "stored_obs": self._stats.stored_obs,
                "dropped_obs": self._stats.dropped_obs,
                "saccade_count": self._stats.saccade_count,
                "peripheral_detections": self._stats.peripheral_detections,
            },
            "recent_saccades": [s.to_dict() for s in list(self._saccades)[-20:]],
        }
