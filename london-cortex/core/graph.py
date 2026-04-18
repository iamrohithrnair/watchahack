"""CortexGraph — NetworkX knowledge graph with spatial grid and anomaly index."""

from __future__ import annotations

import json
import logging
import math
from collections import defaultdict
from pathlib import Path
from typing import Any

import networkx as nx

from .config import GRID_CELL_SIZE_M, LONDON_BBOX, STATIC_DIR
from .models import Anomaly, GridCell

log = logging.getLogger("cortex.graph")

# Approximate meters per degree at London's latitude
M_PER_DEG_LAT = 111_320
M_PER_DEG_LON = 111_320 * math.cos(math.radians(51.5))


class CortexGraph:
    """Knowledge graph with spatial index over London grid cells."""

    def __init__(self):
        self.G = nx.Graph()
        self.cells: dict[str, GridCell] = {}
        self.spatial_index: dict[str, list[Anomaly]] = defaultdict(list)
        self._lat_step = GRID_CELL_SIZE_M / M_PER_DEG_LAT
        self._lon_step = GRID_CELL_SIZE_M / M_PER_DEG_LON
        self.retina = None  # set externally
        self._build_grid()

    def _build_grid(self) -> None:
        """Build 500m grid cells covering Greater London."""
        grid_file = STATIC_DIR / "london_grid.json"
        if grid_file.exists():
            self._load_grid(grid_file)
            return

        bbox = LONDON_BBOX
        lat = bbox["min_lat"]
        row = 0
        while lat < bbox["max_lat"]:
            lon = bbox["min_lon"]
            col = 0
            while lon < bbox["max_lon"]:
                cell_id = f"cell_{row}_{col}"
                cell = GridCell(
                    id=cell_id,
                    lat_min=lat,
                    lat_max=lat + self._lat_step,
                    lon_min=lon,
                    lon_max=lon + self._lon_step,
                )
                self.cells[cell_id] = cell
                self.G.add_node(cell_id, type="grid_cell", **{
                    "lat": cell.center_lat, "lon": cell.center_lon,
                })
                lon += self._lon_step
                col += 1
            lat += self._lat_step
            row += 1

        # Add adjacency edges
        for cid, cell in self.cells.items():
            parts = cid.split("_")
            r, c = int(parts[1]), int(parts[2])
            for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1),
                           (-1, -1), (-1, 1), (1, -1), (1, 1)]:
                neighbor = f"cell_{r + dr}_{c + dc}"
                if neighbor in self.cells:
                    self.G.add_edge(cid, neighbor, type="adjacent_to")

        self._save_grid(grid_file)
        log.info("Built grid: %d cells", len(self.cells))

    def _save_grid(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {}
        for cid, cell in self.cells.items():
            data[cid] = {
                "lat_min": cell.lat_min, "lat_max": cell.lat_max,
                "lon_min": cell.lon_min, "lon_max": cell.lon_max,
                "borough": cell.borough,
            }
        path.write_text(json.dumps(data))
        log.info("Saved grid to %s", path)

    def _load_grid(self, path: Path) -> None:
        data = json.loads(path.read_text())
        for cid, d in data.items():
            cell = GridCell(id=cid, **d)
            self.cells[cid] = cell
            self.G.add_node(cid, type="grid_cell", lat=cell.center_lat, lon=cell.center_lon)
        # Rebuild adjacency
        for cid in self.cells:
            parts = cid.split("_")
            r, c = int(parts[1]), int(parts[2])
            for dr, dc in [(-1, 0), (1, 0), (0, -1), (0, 1),
                           (-1, -1), (-1, 1), (1, -1), (1, 1)]:
                neighbor = f"cell_{r + dr}_{c + dc}"
                if neighbor in self.cells:
                    self.G.add_edge(cid, neighbor, type="adjacent_to")
        log.info("Loaded grid: %d cells from %s", len(self.cells), path)

    def latlon_to_cell(self, lat: float, lon: float) -> str | None:
        """Convert lat/lon to grid cell ID. O(1)."""
        bbox = LONDON_BBOX
        if not (bbox["min_lat"] <= lat <= bbox["max_lat"] and
                bbox["min_lon"] <= lon <= bbox["max_lon"]):
            return None
        row = int((lat - bbox["min_lat"]) / self._lat_step)
        col = int((lon - bbox["min_lon"]) / self._lon_step)
        cell_id = f"cell_{row}_{col}"
        return cell_id if cell_id in self.cells else None

    def get_cell(self, cell_id: str) -> GridCell | None:
        """Get a grid cell by ID."""
        return self.cells.get(cell_id)

    def get_neighbors(self, cell_id: str) -> list[str]:
        """Get all neighboring cell IDs (8-connected)."""
        if cell_id not in self.G:
            return []
        return [n for n in self.G.neighbors(cell_id)
                if self.G[cell_id][n].get("type") == "adjacent_to"]

    def get_nearby_cells(self, cell_id: str, radius: int = 1) -> list[str]:
        """Get cells within `radius` hops."""
        if cell_id not in self.G:
            return []
        nearby = set()
        current = {cell_id}
        for _ in range(radius):
            next_ring = set()
            for c in current:
                for n in self.get_neighbors(c):
                    if n not in nearby and n != cell_id:
                        next_ring.add(n)
                        nearby.add(n)
            current = next_ring
        return list(nearby)

    # ── Anomaly Spatial Index ──────────────────────────────────────────────────

    def add_anomaly_to_cell(self, anomaly: Anomaly) -> None:
        """Index an anomaly into its spatial grid cell."""
        cell_id = anomaly.location_id
        if cell_id:
            self.spatial_index[cell_id].append(anomaly)

    def register_anomaly(self, anomaly: Anomaly) -> None:
        """Register an anomaly into the spatial index (alias for add_anomaly_to_cell)."""
        cell_id = anomaly.location_id
        if cell_id:
            self.spatial_index[cell_id].append(anomaly)

    def get_anomalies_at(self, cell_id: str, include_neighbors: bool = True) -> list[Anomaly]:
        """Get all active anomalies in this cell and optionally its neighbors."""
        result = list(self.spatial_index.get(cell_id, []))
        if include_neighbors:
            for n in self.get_neighbors(cell_id):
                result.extend(self.spatial_index.get(n, []))
        return result

    def get_colocated_anomalies(
        self, cell_id: str, include_neighbors: bool = True
    ) -> list[Anomaly]:
        """Get all active anomalies in this cell and optionally its neighbors."""
        result = list(self.spatial_index.get(cell_id, []))
        if include_neighbors:
            for n in self.get_neighbors(cell_id):
                result.extend(self.spatial_index.get(n, []))
        return result

    def prune_expired_anomalies(self) -> int:
        from datetime import datetime, timedelta, timezone
        now = datetime.now(timezone.utc)
        pruned = 0
        for cell_id in list(self.spatial_index.keys()):
            before = len(self.spatial_index[cell_id])
            self.spatial_index[cell_id] = [
                a for a in self.spatial_index[cell_id]
                if now - a.timestamp < timedelta(hours=a.ttl_hours)
            ]
            pruned += before - len(self.spatial_index[cell_id])
            if not self.spatial_index[cell_id]:
                del self.spatial_index[cell_id]
        return pruned

    # ── POI / Entity Management ───────────────────────────────────────────────

    def add_poi(self, poi_id: str, lat: float, lon: float, poi_type: str,
                metadata: dict[str, Any] | None = None) -> str | None:
        cell_id = self.latlon_to_cell(lat, lon)
        if not cell_id:
            return None
        self.G.add_node(poi_id, type="poi", poi_type=poi_type,
                        lat=lat, lon=lon, **(metadata or {}))
        self.G.add_edge(poi_id, cell_id, type="located_in")
        return cell_id

    def add_entity(self, entity_id: str, entity_type: str,
                   metadata: dict[str, Any] | None = None) -> None:
        self.G.add_node(entity_id, type="entity", entity_type=entity_type,
                        **(metadata or {}))

    def link(self, a: str, b: str, edge_type: str, **attrs: Any) -> None:
        self.G.add_edge(a, b, type=edge_type, **attrs)

    def stats(self) -> dict[str, int]:
        return {
            "nodes": self.G.number_of_nodes(),
            "edges": self.G.number_of_edges(),
            "cells": len(self.cells),
            "active_anomaly_cells": len(self.spatial_index),
        }
