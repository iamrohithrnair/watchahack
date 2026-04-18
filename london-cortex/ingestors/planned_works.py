"""TfL Planned Works ingestor — scheduled closures and engineering works on tube/rail.

Fetches line status for tube, DLR, Overground, Elizabeth line, and tram modes,
extracting planned closures, part-closures, and engineering works. This provides
ground truth for infrastructure anomalies — if a tube line is closed for weekend
engineering works, downstream agents can immediately discard it as a causal factor
in malicious-event hypotheses.

Complements road_disruptions.py (which covers road-level planned works) by adding
rail/metro planned works.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.planned_works")

# TfL Line Status endpoint — no API key required
# Covers: tube, dlr, overground, elizabeth-line, tram
TFL_LINE_STATUS_URL = (
    "https://api.tfl.gov.uk/Line/Mode/tube,dlr,overground,elizabeth-line,tram/Status"
)

# Status severity descriptions that indicate planned works (not live disruptions)
_PLANNED_STATUS_TYPES = {
    "PlannedClosure",
    "PartClosure",
    "PartSuspended",
    "ServiceClosed",
}

# Reason categories that indicate scheduled maintenance/engineering
_ENGINEERING_REASON_TYPES = {
    "PlannedWork",
    "PlannedClosure",
}

# Map TfL line status severity (lower = worse) to our 1-5 scale
# TfL uses: 0=SpecialService, 1=Closed, 5=PartClosure, 6=SevereDelays, ...
# 10=GoodService, 20=Information
_SEVERITY_MAP = {
    0: 3,   # Special service
    1: 5,   # Closed
    2: 4,   # Suspended
    3: 4,   # Part suspended
    4: 3,   # Planned closure
    5: 3,   # Part closure
    6: 4,   # Severe delays
    7: 3,   # Reduced service
    8: 2,   # Bus service
    9: 2,   # Minor delays
    10: 1,  # Good service
    20: 1,  # Information
}


class PlannedWorksIngestor(BaseIngestor):
    source_name = "tfl_planned_works"
    rate_limit_name = "tfl"

    async def fetch_data(self) -> Any:
        return await self.fetch(TFL_LINE_STATUS_URL)

    async def process(self, data: Any) -> None:
        if not isinstance(data, list):
            self.log.warning("Unexpected data format: %s", type(data))
            return

        planned_count = 0
        live_disruption_count = 0

        for line in data:
            line_id = line.get("id", "unknown")
            line_name = line.get("name", line_id)
            mode = line.get("modeName", "unknown")

            statuses = line.get("lineStatuses", [])
            for status in statuses:
                status_severity = status.get("statusSeverity", 10)
                status_desc = status.get("statusSeverityDescription", "")
                reason = status.get("reason", "")
                disruption = status.get("disruption", {}) or {}

                # Determine if this is a planned/scheduled work
                is_planned = (
                    status_desc in _PLANNED_STATUS_TYPES
                    or disruption.get("categoryDescription", "") in _ENGINEERING_REASON_TYPES
                    or "planned closure" in reason.lower()
                    or "engineering work" in reason.lower()
                    or "weekend closure" in reason.lower()
                    or "improvement work" in reason.lower()
                )

                # Skip good service — nothing to report
                if status_severity >= 10:
                    continue

                severity_score = _SEVERITY_MAP.get(status_severity, 2)

                # Extract affected stops/sections from disruption detail
                affected_stops = []
                affected_sections = []
                if disruption:
                    for route in disruption.get("affectedRoutes", []):
                        for section in route.get("routeSections", []):
                            orig = section.get("originator", "")
                            dest = section.get("destination", "")
                            if orig and dest:
                                affected_sections.append(f"{orig} to {dest}")

                    for stop in disruption.get("affectedStops", []):
                        stop_name = stop.get("commonName", "")
                        if stop_name:
                            affected_stops.append(stop_name)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=severity_score,
                    location_id=None,  # Line-level, not point-level
                    lat=None,
                    lon=None,
                    metadata={
                        "line_id": line_id,
                        "line_name": line_name,
                        "mode": mode,
                        "status": status_desc,
                        "status_severity": status_severity,
                        "reason": reason[:500],
                        "is_planned": is_planned,
                        "affected_sections": affected_sections[:10],
                        "affected_stops": affected_stops[:20],
                        "closure_text": disruption.get("closureText", ""),
                    },
                )
                await self.board.store_observation(obs)

                tag = " [PLANNED]" if is_planned else " [LIVE]"
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Line status{tag} [{line_name}] {status_desc} "
                        f"severity={severity_score}"
                        + (f" — {reason[:200]}" if reason else "")
                    ),
                    data={
                        "line_id": line_id,
                        "line_name": line_name,
                        "mode": mode,
                        "status": status_desc,
                        "severity_score": severity_score,
                        "is_planned": is_planned,
                        "observation_id": obs.id,
                        "affected_sections": affected_sections[:5],
                    },
                    location_id=None,
                )
                await self.board.post(msg)

                if is_planned:
                    planned_count += 1
                else:
                    live_disruption_count += 1

        self.log.info(
            "TfL planned works: planned=%d live_disruptions=%d",
            planned_count, live_disruption_count,
        )


async def ingest_planned_works(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TfL planned works fetch cycle."""
    ingestor = PlannedWorksIngestor(board, graph, scheduler)
    await ingestor.run()
