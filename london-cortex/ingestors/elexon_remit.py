"""Elexon BMRS REMIT notices and Capacity Market Notices (CMN) ingestor.

Cross-references declared telemetry outages (REMIT UMMs) with physical
Capacity Market Notices to detect divergence between reported and actual
generation/demand capacity status on the GB grid.

Endpoints (no API key required):
  - REMIT UMMs:  /remit/messages/active
  - CMNs:        /balancing/capacity/market-notice
"""

from __future__ import annotations

import logging
from datetime import date
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.elexon_remit")

ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
REMIT_URL = ELEXON_BASE + "/remit/messages/active"
CMN_URL = ELEXON_BASE + "/balancing/capacity/market-notice"

LONDON_LAT = 51.5
LONDON_LON = -0.12

# Capacity Market Notice types that indicate physical capacity stress
CMN_STRESS_TYPES = {"CAPACITY_MARKET_NOTICE", "ELECTRICITY_CAPACITY_REPORT"}


class ElexonRemitIngestor(BaseIngestor):
    source_name = "elexon_remit"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        today = date.today().isoformat()
        remit_data = await self.fetch(REMIT_URL, params={"format": "json"})
        cmn_data = await self.fetch(CMN_URL, params={"from": today, "to": today})
        return {"remit": remit_data, "cmn": cmn_data}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        processed = 0

        # -- REMIT UMMs (Urgent Market Messages) --
        remit_resp = data.get("remit")
        if isinstance(remit_resp, dict):
            entries = remit_resp.get("data", [])
            if isinstance(entries, dict):
                entries = [entries]

            for entry in entries:
                asset = entry.get("assetName") or entry.get("affectedAssetName", "unknown")
                msg_type = entry.get("messageType", "")
                msg_id = entry.get("messageId", "")
                unavail_type = entry.get("unavailabilityType", "")
                unavail_mw = entry.get("unavailableCapacity")
                available_mw = entry.get("availableCapacity")
                cause = entry.get("cause") or entry.get("eventType", "")
                start = entry.get("eventStartTime", "")
                end = entry.get("eventEndTime", "")
                fuel_type = entry.get("fuelType", "")

                capacity_val = None
                if unavail_mw is not None:
                    try:
                        capacity_val = float(unavail_mw)
                    except (TypeError, ValueError):
                        pass

                if capacity_val is not None:
                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=capacity_val,
                        location_id=cell_id,
                        lat=LONDON_LAT,
                        lon=LONDON_LON,
                        metadata={
                            "metric": "remit_unavailable_capacity_mw",
                            "asset": asset,
                            "message_type": msg_type,
                            "message_id": msg_id,
                            "unavailability_type": unavail_type,
                            "available_capacity_mw": available_mw,
                            "cause": cause,
                            "fuel_type": fuel_type,
                            "event_start": start,
                            "event_end": end,
                        },
                    )
                    await self.board.store_observation(obs)

                    summary = (
                        f"REMIT outage [{asset}] {unavail_type}: "
                        f"{capacity_val:.0f}MW unavailable"
                        + (f" ({cause})" if cause else "")
                        + (f" [{fuel_type}]" if fuel_type else "")
                    )
                    await self.board.post(AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=summary,
                        data={
                            "asset": asset,
                            "message_type": msg_type,
                            "message_id": msg_id,
                            "unavailable_mw": capacity_val,
                            "available_mw": available_mw,
                            "cause": cause,
                            "fuel_type": fuel_type,
                            "event_start": start,
                            "event_end": end,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    ))
                    processed += 1

        # -- Capacity Market Notices (physical stress signals) --
        cmn_resp = data.get("cmn")
        if isinstance(cmn_resp, dict):
            entries = cmn_resp.get("data", [])
            if isinstance(entries, dict):
                entries = [entries]

            for entry in entries:
                notice_type = entry.get("capacityMarketNoticeType", "")
                publish_time = entry.get("publishTime", "")
                delivery_start = entry.get("deliveryStart", "")
                delivery_end = entry.get("deliveryEnd", "")
                volume = entry.get("volume")
                message = entry.get("message") or entry.get("noticeText", "")

                is_stress = any(t in notice_type.upper() for t in CMN_STRESS_TYPES)

                vol_val = None
                if volume is not None:
                    try:
                        vol_val = float(volume)
                    except (TypeError, ValueError):
                        pass

                obs_value = vol_val if vol_val is not None else (1.0 if is_stress else 0.0)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=obs_value,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "metric": "capacity_market_notice",
                        "notice_type": notice_type,
                        "is_stress_signal": is_stress,
                        "publish_time": publish_time,
                        "delivery_start": delivery_start,
                        "delivery_end": delivery_end,
                        "message": message[:200] if message else "",
                    },
                )
                await self.board.store_observation(obs)

                channel = "#alerts" if is_stress else "#raw"
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel=channel,
                    content=(
                        f"Capacity Market Notice [{notice_type}]: "
                        + (f"{vol_val:.0f}MW" if vol_val is not None else "")
                        + (f" — {message[:120]}" if message else "")
                    ),
                    data={
                        "notice_type": notice_type,
                        "is_stress_signal": is_stress,
                        "volume_mw": vol_val,
                        "publish_time": publish_time,
                        "delivery_start": delivery_start,
                        "delivery_end": delivery_end,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                ))
                processed += 1

        self.log.info("Elexon REMIT/CMN: processed=%d records", processed)


async def ingest_elexon_remit(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Elexon REMIT/CMN fetch cycle."""
    ingestor = ElexonRemitIngestor(board, graph, scheduler)
    await ingestor.run()
