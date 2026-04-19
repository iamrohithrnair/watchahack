"""UK Parliament Calendar ingestor — forward-looking committee schedules, debates,
oral questions, and chamber business.

Complements uk_parliament.py (which covers backward-looking bills/votes) by providing
predictive data: what Parliament will discuss in the coming week. This enables the
'Smokescreen' hypothesis — anticipating data drops timed to the political agenda.

APIs used (all free, no key required):
- What's On Calendar: committee hearings, debates, oral questions
- Oral Questions API: specific questions tabled for upcoming sessions
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.parliament_calendar")

_CALENDAR_URL = "https://whatson-api.parliament.uk/calendar/events/list.json"
_ORAL_QUESTIONS_URL = (
    "https://oralquestionsandmotions-api.parliament.uk/oralquestions/list"
)

# Westminster coordinates
_WESTMINSTER_LAT = 51.4995
_WESTMINSTER_LON = -0.1248

# Event types that signal potential policy/data significance
_SIGNIFICANT_CATEGORIES = {
    "Oral evidence",
    "Debate",
    "Oral questions",
    "Westminster Hall debate",
    "Statement",
    "Ministerial statement",
}


class ParliamentCalendarIngestor(BaseIngestor):
    source_name = "parliament_calendar"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        today = datetime.utcnow().strftime("%Y-%m-%d")
        end = (datetime.utcnow() + timedelta(days=7)).strftime("%Y-%m-%d")

        calendar = await self.fetch(
            _CALENDAR_URL,
            params={
                "startdate": today,
                "enddate": end,
                "house": "Commons",
            },
        )

        oral_questions = await self.fetch(
            _ORAL_QUESTIONS_URL,
            params={
                "parameters.answeringDateStart": today,
                "parameters.answeringDateEnd": end,
                "parameters.take": "20",
            },
        )

        return {"calendar": calendar, "oral_questions": oral_questions}

    async def process(self, data: dict[str, Any]) -> None:
        cell_id = self.graph.latlon_to_cell(_WESTMINSTER_LAT, _WESTMINSTER_LON)
        await self._process_calendar(data.get("calendar"), cell_id)
        await self._process_oral_questions(data.get("oral_questions"), cell_id)

    async def _process_calendar(self, data: Any, cell_id: str | None) -> None:
        if not isinstance(data, list):
            self.log.info("No calendar data: %s", type(data))
            return

        processed = 0
        for event in data:
            try:
                category = event.get("Category", "")
                if category not in _SIGNIFICANT_CATEGORIES:
                    continue

                title = event.get("Description", "") or event.get("Title", "")
                start = event.get("StartDate", "")
                committee = event.get("Committee", "") or ""
                if isinstance(committee, dict):
                    committee = committee.get("Name", "")
                location = event.get("Location", "")
                inquiry = event.get("InquiryName", "") or ""

                # Build a description that captures what's being discussed
                parts = [f"[{category}]"]
                if committee:
                    parts.append(f"Committee: {committee}")
                if inquiry:
                    parts.append(f"Inquiry: {inquiry}")
                if title:
                    parts.append(title)

                description = " | ".join(parts)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=description,
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "category": category,
                        "title": title,
                        "committee": committee,
                        "inquiry": inquiry,
                        "start_date": start,
                        "location": location,
                        "event_type": "scheduled",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Parliament agenda ({start[:10] if start else 'TBC'}): "
                        f"{description}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "category": category,
                        "title": title,
                        "committee": committee,
                        "inquiry": inquiry,
                        "start_date": start,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing calendar event")

        self.log.info("Parliament calendar events: processed=%d", processed)

    async def _process_oral_questions(self, data: Any, cell_id: str | None) -> None:
        if not isinstance(data, dict):
            self.log.info("No oral questions data: %s", type(data))
            return

        results = data.get("Response", [])
        if not isinstance(results, list):
            self.log.info("Unexpected oral questions format: %s", type(results))
            return

        processed = 0
        for question in results:
            try:
                q_text = question.get("QuestionText", "")
                department = question.get("AnsweringBodyName", "")
                answer_date = question.get("AnsweringWhen", "")
                asking_member = question.get("AskingMemberName", "")
                asking_party = question.get("AskingMemberParty", "")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"Oral Q to {department}: {q_text[:200]}",
                    location_id=cell_id,
                    lat=_WESTMINSTER_LAT,
                    lon=_WESTMINSTER_LON,
                    metadata={
                        "question_text": q_text,
                        "department": department,
                        "answer_date": answer_date,
                        "asking_member": asking_member,
                        "asking_party": asking_party,
                        "event_type": "oral_question",
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Oral question ({answer_date[:10] if answer_date else 'TBC'}) "
                        f"to {department}: {q_text[:150]}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "department": department,
                        "answer_date": answer_date,
                        "asking_member": asking_member,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing oral question")

        self.log.info("Parliament oral questions: processed=%d", processed)


async def ingest_parliament_calendar(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Parliament calendar fetch cycle."""
    ingestor = ParliamentCalendarIngestor(board, graph, scheduler)
    await ingestor.run()
