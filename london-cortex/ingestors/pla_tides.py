"""PLA (Port of London Authority) tidal Thames ingestor — real-time tide gauge data.

Scrapes the PLA's live tide overview table which provides observed and predicted
water levels plus surge (observed − predicted) for ~12 gauges along the tidal
Thames, updated roughly every minute.

This is an independent data source from the Environment Agency level/flow gauges,
using PLA-operated instruments. Valuable for corroborating or refuting anomalous
readings from EA gauges on the tidal Thames.

Brain suggestion origin: 'API access to PLA real-time hydrographic survey data
and operational alerts to corroborate Environment Agency gauge readings.'
"""

from __future__ import annotations

import logging
import re
from html.parser import HTMLParser
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.pla_tides")

PLA_OVERVIEW_URL = "https://www.pla.co.uk/hydrographics/ltoverview_table.cfm"

# Approximate lat/lon for PLA tide gauges on the tidal Thames.
# From PLA tide tables and Admiralty charts.
_GAUGE_COORDS: dict[str, tuple[float, float]] = {
    "silvertown":    (51.5050, 0.0280),
    "chelsea":       (51.4850, -0.1710),
    "richmond":      (51.4590, -0.3060),
    "tower pier":    (51.5060, -0.0760),
    "westminster":   (51.5010, -0.1240),
    "greenwich":     (51.4840, -0.0090),
    "albert bridge": (51.4780, -0.1670),
    "battersea":     (51.4810, -0.1490),
    "wandsworth":    (51.4660, -0.1890),
    "hammersmith":   (51.4870, -0.2300),
    "putney":        (51.4670, -0.2170),
    "cadogan pier":  (51.4860, -0.1610),
    "erith":         (51.4820, 0.1760),
    "tilbury":       (51.4490, 0.3530),
    "southend":      (51.5140, 0.7210),
    "coryton":       (51.5120, 0.5090),
    "canvey island": (51.5120, 0.5700),
    "gallions point": (51.5060, 0.0690),
    "barrier":       (51.4950, 0.0350),
}

# CD-to-AOD offsets (metres) per gauge, from PLA tide tables.
# AOD = CD_value - offset. These are standard published values.
_CD_TO_AOD_OFFSET: dict[str, float] = {
    "silvertown":    3.29,
    "chelsea":       3.07,
    "richmond":      2.35,
    "tower pier":    3.20,
    "westminster":   3.12,
    "greenwich":     3.28,
    "albert bridge": 3.07,
    "battersea":     3.07,
    "wandsworth":    3.07,
    "hammersmith":   2.83,
    "putney":        2.90,
    "cadogan pier":  3.07,
    "erith":         3.35,
    "tilbury":       3.44,
    "southend":      2.90,
    "coryton":       3.20,
    "canvey island": 3.20,
    "gallions point": 3.29,
    "barrier":       3.29,
}


class _PLATableParser(HTMLParser):
    """Parse the PLA overview HTML table into a list of gauge readings."""

    def __init__(self):
        super().__init__()
        self._in_table = False
        self._in_row = False
        self._in_cell = False
        self._current_row: list[str] = []
        self._rows: list[list[str]] = []
        self._cell_text = ""

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag == "table":
            self._in_table = True
        elif tag == "tr" and self._in_table:
            self._in_row = True
            self._current_row = []
        elif tag in ("td", "th") and self._in_row:
            self._in_cell = True
            self._cell_text = ""

    def handle_endtag(self, tag: str) -> None:
        if tag in ("td", "th") and self._in_cell:
            self._in_cell = False
            self._current_row.append(self._cell_text.strip())
        elif tag == "tr" and self._in_row:
            self._in_row = False
            if self._current_row:
                self._rows.append(self._current_row)
        elif tag == "table":
            self._in_table = False

    def handle_data(self, data: str) -> None:
        if self._in_cell:
            self._cell_text += data

    def get_rows(self) -> list[list[str]]:
        return self._rows


def _parse_float(s: str) -> float | None:
    """Parse a float from a string, returning None on failure."""
    s = s.strip().replace(",", "")
    if not s or s == "-" or s.lower() in ("n/a", "offline", ""):
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_pla_html(html: str) -> list[dict[str, Any]]:
    """Extract gauge readings from the PLA overview HTML table.

    Expected columns (order may vary slightly):
      Gauge Name | Observed (CD) | Predicted (CD) | Surge | Next HW Time | Next HW (CD) | Next LW Time | Next LW (CD)
    """
    parser = _PLATableParser()
    parser.feed(html)
    rows = parser.get_rows()

    if len(rows) < 2:
        return []

    # Find column indices from header row
    header = [c.lower().strip() for c in rows[0]]

    def _find_col(*keywords: str) -> int | None:
        for i, h in enumerate(header):
            if all(k in h for k in keywords):
                return i
        return None

    col_name = _find_col("gauge") or _find_col("name") or _find_col("station") or 0
    col_observed = _find_col("observed") or _find_col("actual")
    col_predicted = _find_col("predicted") or _find_col("pred")
    col_surge = _find_col("surge") or _find_col("residual")

    results: list[dict[str, Any]] = []
    for row in rows[1:]:
        if len(row) <= max(col_name or 0, col_observed or 0, col_predicted or 0):
            continue

        name = row[col_name].strip()
        if not name or name.lower() in ("gauge name", "station", ""):
            continue

        observed_cd = _parse_float(row[col_observed]) if col_observed is not None and col_observed < len(row) else None
        predicted_cd = _parse_float(row[col_predicted]) if col_predicted is not None and col_predicted < len(row) else None
        surge = _parse_float(row[col_surge]) if col_surge is not None and col_surge < len(row) else None

        # If surge not provided but observed and predicted are, calculate it
        if surge is None and observed_cd is not None and predicted_cd is not None:
            surge = round(observed_cd - predicted_cd, 2)

        # Convert CD to AOD if offset available
        name_lower = name.lower()
        offset = _CD_TO_AOD_OFFSET.get(name_lower)
        observed_aod = round(observed_cd - offset, 2) if observed_cd is not None and offset is not None else None
        predicted_aod = round(predicted_cd - offset, 2) if predicted_cd is not None and offset is not None else None

        results.append({
            "gauge_name": name,
            "observed_cd": observed_cd,
            "predicted_cd": predicted_cd,
            "surge": surge,
            "observed_aod": observed_aod,
            "predicted_aod": predicted_aod,
        })

    return results


class PLATidesIngestor(BaseIngestor):
    source_name = "pla_tides"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(PLA_OVERVIEW_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            self.log.warning("Expected HTML string from PLA, got %s", type(data))
            return

        gauges = _parse_pla_html(data)
        if not gauges:
            self.log.warning("No gauge data parsed from PLA HTML")
            return

        processed = 0
        for gauge in gauges:
            name = gauge["gauge_name"]
            observed_cd = gauge["observed_cd"]
            surge = gauge["surge"]
            observed_aod = gauge["observed_aod"]

            if observed_cd is None:
                continue

            # Look up coordinates
            name_lower = name.lower()
            coords = _GAUGE_COORDS.get(name_lower)
            lat = coords[0] if coords else None
            lon = coords[1] if coords else None
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            # Primary value: observed level in metres AOD if available, else CD
            value = observed_aod if observed_aod is not None else observed_cd
            unit = "mAOD" if observed_aod is not None else "mCD"

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=value,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "gauge_name": name,
                    "observed_cd": observed_cd,
                    "predicted_cd": gauge["predicted_cd"],
                    "surge": surge,
                    "observed_aod": observed_aod,
                    "predicted_aod": gauge["predicted_aod"],
                    "unit": unit,
                    "data_source": "pla",
                },
            )
            await self.board.store_observation(obs)

            surge_str = f" surge={surge:+.2f}m" if surge is not None else ""
            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"PLA tide [{name}] level={value:.2f} {unit}{surge_str}"
                ),
                data={
                    "gauge_name": name,
                    "level": value,
                    "unit": unit,
                    "surge": surge,
                    "observed_cd": observed_cd,
                    "predicted_cd": gauge["predicted_cd"],
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("PLA tides: processed=%d gauges from %d rows", processed, len(gauges))


async def ingest_pla_tides(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one PLA tides fetch cycle."""
    ingestor = PLATidesIngestor(board, graph, scheduler)
    await ingestor.run()
