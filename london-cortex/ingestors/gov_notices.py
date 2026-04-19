"""Government notices ingestor — The Gazette + Parliamentary Bills API.

Fetches official public/legal notices from The Gazette (thegazette.co.uk)
and active Parliamentary Bills from the UK Parliament Bills API. Provides
advance notice of scheduled information releases, enabling predictive
rather than reactive posture for bureaucratic cascades.

Neither API requires authentication.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.gov_notices")

# The Gazette — recent notices feed (JSON-LD)
# We use the Atom feed converted to JSON for recent notices.
GAZETTE_URL = "https://www.thegazette.co.uk/all-notices/notice?results-page-size=20&categorycode=all&requestedMimeType=application/json"

# Parliamentary Bills API — currently active bills
BILLS_URL = "https://bills-api.parliament.uk/api/v1/Bills"
BILLS_PARAMS = {
    "CurrentHouse": "All",
    "SortOrder": "DateUpdatedDescending",
    "Take": 20,
}

# Bill stage sittings — upcoming scheduled events
SITTINGS_URL = "https://bills-api.parliament.uk/api/v1/Sittings"


class GovNoticesIngestor(BaseIngestor):
    source_name = "gov_notices"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        gazette = await self.fetch(GAZETTE_URL)
        bills = await self.fetch(BILLS_URL, params=BILLS_PARAMS)
        sittings = await self.fetch(SITTINGS_URL, params={"Take": 10})

        if gazette is None and bills is None:
            return None
        return {"gazette": gazette, "bills": bills, "sittings": sittings}

    async def process(self, data: Any) -> None:
        gazette_count = await self._process_gazette(data.get("gazette"))
        bills_count = await self._process_bills(data.get("bills"))
        sittings_count = await self._process_sittings(data.get("sittings"))
        self.log.info(
            "Gov notices: gazette=%d bills=%d sittings=%d",
            gazette_count, bills_count, sittings_count,
        )

    async def _process_gazette(self, data: Any) -> int:
        if not data:
            return 0

        # The Gazette JSON response structure varies; handle both feed and results
        entries = []
        if isinstance(data, dict):
            # Try common response shapes
            entries = data.get("results", data.get("entry", data.get("items", [])))
        if isinstance(entries, dict):
            entries = [entries]
        if not isinstance(entries, list):
            self.log.debug("Unexpected Gazette structure: %s", type(data))
            return 0

        count = 0
        for entry in entries:
            try:
                title = (
                    entry.get("title", "")
                    or entry.get("notice-title", "")
                    or "Untitled notice"
                )
                if isinstance(title, dict):
                    title = title.get("value", str(title))

                notice_type = entry.get("notice-type", entry.get("category", ""))
                if isinstance(notice_type, dict):
                    notice_type = notice_type.get("value", str(notice_type))
                if isinstance(notice_type, list) and notice_type:
                    notice_type = notice_type[0].get("term", str(notice_type[0]))

                notice_id = entry.get("id", entry.get("notice-code", ""))
                published = entry.get("published", entry.get("publication-date", ""))

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=title[:500],
                    metadata={
                        "sub_source": "gazette",
                        "notice_type": str(notice_type)[:200],
                        "notice_id": str(notice_id),
                        "published": str(published),
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Gazette notice [{notice_type}]: {title[:200]}",
                    data={
                        "sub_source": "gazette",
                        "notice_type": str(notice_type),
                        "title": title[:300],
                        "notice_id": str(notice_id),
                        "observation_id": obs.id,
                    },
                )
                await self.board.post(msg)
                count += 1
            except Exception:
                self.log.debug("Skipping malformed Gazette entry", exc_info=True)
        return count

    async def _process_bills(self, data: Any) -> int:
        if not data:
            return 0

        items = []
        if isinstance(data, dict):
            items = data.get("items", data.get("results", []))
        if not isinstance(items, list):
            self.log.debug("Unexpected Bills structure: %s", type(data))
            return 0

        count = 0
        for bill in items:
            try:
                title = bill.get("shortTitle", bill.get("longTitle", "Unknown Bill"))
                bill_id = bill.get("billId", "")
                bill_type = bill.get("billTypeId", "")
                current_house = bill.get("currentHouse", "")
                last_update = bill.get("lastUpdate", "")
                is_act = bill.get("isAct", False)

                # Extract current stage info
                current_stage = bill.get("currentStage", {})
                stage_name = ""
                if isinstance(current_stage, dict):
                    stage_info = current_stage.get("stageName", "")
                    stage_name = stage_info

                status_label = "Act" if is_act else f"Stage: {stage_name}"

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=f"{title} — {status_label}",
                    metadata={
                        "sub_source": "parliament_bills",
                        "bill_id": str(bill_id),
                        "bill_type": str(bill_type),
                        "current_house": str(current_house),
                        "stage": stage_name,
                        "is_act": is_act,
                        "last_update": str(last_update),
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Parliament Bill [{current_house}]: {title} — {status_label}",
                    data={
                        "sub_source": "parliament_bills",
                        "bill_id": str(bill_id),
                        "title": title,
                        "stage": stage_name,
                        "is_act": is_act,
                        "observation_id": obs.id,
                    },
                )
                await self.board.post(msg)
                count += 1
            except Exception:
                self.log.debug("Skipping malformed bill entry", exc_info=True)
        return count

    async def _process_sittings(self, data: Any) -> int:
        """Process upcoming parliamentary sittings — scheduled future events."""
        if not data:
            return 0

        items = []
        if isinstance(data, dict):
            items = data.get("items", data.get("results", []))
        if not isinstance(items, list):
            return 0

        count = 0
        now = datetime.now(timezone.utc)
        for sitting in items:
            try:
                date_str = sitting.get("date", "")
                bill_id = sitting.get("billId", "")
                bill_title = sitting.get("billShortTitle", sitting.get("billLongTitle", ""))
                stage_name = sitting.get("billStageName", "")
                house = sitting.get("house", "")

                # Only emit future or very recent sittings
                if date_str:
                    try:
                        sit_date = datetime.fromisoformat(date_str.replace("Z", "+00:00"))
                        hours_away = (sit_date - now).total_seconds() / 3600
                        if hours_away < -24:
                            continue  # skip old sittings
                    except (ValueError, TypeError):
                        pass

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=f"Sitting: {bill_title} — {stage_name} on {date_str}",
                    metadata={
                        "sub_source": "parliament_sittings",
                        "bill_id": str(bill_id),
                        "stage": stage_name,
                        "house": str(house),
                        "date": date_str,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Parliament Sitting [{house}]: {bill_title} — {stage_name} on {date_str}",
                    data={
                        "sub_source": "parliament_sittings",
                        "bill_id": str(bill_id),
                        "title": bill_title,
                        "stage": stage_name,
                        "date": date_str,
                        "observation_id": obs.id,
                    },
                    priority=2,  # slightly elevated — scheduled future event
                )
                await self.board.post(msg)
                count += 1
            except Exception:
                self.log.debug("Skipping malformed sitting entry", exc_info=True)
        return count


async def ingest_gov_notices(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one gov notices fetch cycle."""
    ingestor = GovNoticesIngestor(board, graph, scheduler)
    await ingestor.run()
