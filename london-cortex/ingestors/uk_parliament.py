"""UK Parliament ingestor — recent bills, divisions (votes), and Commons business."""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.uk_parliament")

# Parliament APIs
_BILLS_URL = "https://bills-api.parliament.uk/api/v1/Bills"
_DIVISIONS_COMMONS_URL = "https://commonsvotes-api.parliament.uk/data/divisions.json/search"
_MEMBERS_URL = "https://members-api.parliament.uk/api/Members/Search"

# Westminster coordinates
_WESTMINSTER_LAT = 51.4995
_WESTMINSTER_LON = -0.1248


class UKParliamentIngestor(BaseIngestor):
    source_name = "uk_parliament"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        # Fetch recent bills
        bills = await self.fetch(
            _BILLS_URL,
            params={
                "SortOrder": "DateUpdatedDescending",
                "Take": "10",
                "CurrentHouse": "All",
            },
        )

        # Fetch recent Commons divisions (votes)
        divisions = await self.fetch(
            _DIVISIONS_COMMONS_URL,
            params={"queryParameters.take": "10"},
        )

        return {"bills": bills, "divisions": divisions}

    async def process(self, data: dict[str, Any]) -> None:
        cell_id = self.graph.latlon_to_cell(_WESTMINSTER_LAT, _WESTMINSTER_LON)
        await self._process_bills(data.get("bills"), cell_id)
        await self._process_divisions(data.get("divisions"), cell_id)

    async def _process_bills(self, data: Any, cell_id: str | None) -> None:
        if not isinstance(data, dict):
            self.log.info("No bills data: %s", type(data))
            return

        items = data.get("items", [])
        processed = 0

        for bill in items:
            try:
                bill_id = bill.get("billId", "")
                title = bill.get("shortTitle", "") or bill.get("longTitle", "")
                bill_type = bill.get("billTypeDescription", "")
                current_house = bill.get("currentHouse", "")
                last_update = bill.get("lastUpdate", "")
                is_act = bill.get("isAct", False)
                sponsors = []
                for member in bill.get("sponsors", []):
                    m = member.get("member", {})
                    if m:
                        sponsors.append(m.get("name", ""))

                stage = ""
                stages = bill.get("currentStages", [])
                if stages:
                    last_stage = stages[-1]
                    stage_info = last_stage.get("stageSitting", {}) or {}
                    stage = last_stage.get("description", "")

                obs = Observation(
                    source="uk_parliament",
                    obs_type=ObservationType.TEXT,
                    value=f"Bill: {title} [{bill_type}]",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "bill_id": bill_id,
                        "bill_type": bill_type,
                        "current_house": current_house,
                        "stage": stage,
                        "is_act": is_act,
                        "sponsors": sponsors,
                        "last_update": last_update,
                    },
                )
                await self.board.store_observation(obs)

                status = "Act of Parliament" if is_act else f"in {current_house}"
                msg = AgentMessage(
                    from_agent="uk_parliament",
                    channel="#raw",
                    content=(
                        f"Parliament Bill: {title} ({bill_type}) — "
                        f"{status}, stage: {stage}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "bill_id": bill_id,
                        "title": title,
                        "bill_type": bill_type,
                        "current_house": current_house,
                        "stage": stage,
                        "sponsors": sponsors,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing bill: %s", bill.get("billId"))

        self.log.info("Parliament bills: processed=%d", processed)

    async def _process_divisions(self, data: Any, cell_id: str | None) -> None:
        if not isinstance(data, list):
            self.log.info("No divisions data: %s", type(data))
            return

        processed = 0
        for div in data[:10]:
            try:
                div_id = div.get("DivisionId", "")
                date = div.get("Date", "")
                title = div.get("Title", "")
                aye_count = div.get("AyeCount", 0)
                noe_count = div.get("NoCount", 0)
                total = aye_count + noe_count

                obs = Observation(
                    source="uk_parliament",
                    obs_type=ObservationType.TEXT,
                    value=f"Division: {title} — Ayes {aye_count}, Noes {noe_count}",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "division_id": div_id,
                        "date": date,
                        "aye_count": aye_count,
                        "noe_count": noe_count,
                        "total_votes": total,
                    },
                )
                await self.board.store_observation(obs)

                result = "PASSED" if aye_count > noe_count else "REJECTED"
                margin = abs(aye_count - noe_count)
                msg = AgentMessage(
                    from_agent="uk_parliament",
                    channel="#raw",
                    content=(
                        f"Commons vote: {title} — {result} "
                        f"(Ayes {aye_count} / Noes {noe_count}, margin {margin})"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "division_id": div_id,
                        "title": title,
                        "aye_count": aye_count,
                        "noe_count": noe_count,
                        "result": result,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing division: %s", div.get("DivisionId"))

        self.log.info("Parliament divisions: processed=%d", processed)


async def ingest_uk_parliament(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = UKParliamentIngestor(board, graph, scheduler)
    await ingestor.run()
