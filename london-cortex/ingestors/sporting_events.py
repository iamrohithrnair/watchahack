"""TheSportsDB sporting events ingestor — major UK sporting events calendar.

Fetches today's sporting events from TheSportsDB free API, focusing on
UK-relevant leagues (Premier League, Championship, FA Cup, Six Nations,
Wimbledon, The Ashes, etc.). Allows the system to cross-reference energy
demand spikes, transport anomalies, and sentiment shifts with scheduled
sporting events.

API docs: https://www.thesportsdb.com/free_sports_api
Free tier: API key "3", 30 requests/min, eventsday limited to 5/min.
No registration required.
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

log = logging.getLogger("cortex.ingestors.sporting_events")

EVENTS_DAY_URL = "https://www.thesportsdb.com/api/v1/json/3/eventsday.php"

# UK leagues/competitions we care about (matched against strLeague)
UK_LEAGUES = {
    "English Premier League",
    "English League Championship",
    "English League 1",
    "English League 2",
    "FA Cup",
    "EFL Cup",
    "Scottish Premiership",
    "Six Nations",
    "English Premiership Rugby",
    "Wimbledon",
    "The Ashes",
    "ICC Cricket World Cup",
    "Formula 1",
    "UEFA Champions League",
    "UEFA Europa League",
}

# UK city coordinates for venue-based location mapping
UK_CITY_COORDS: dict[str, tuple[float, float]] = {
    "london": (51.5074, -0.1278),
    "manchester": (53.4808, -2.2426),
    "liverpool": (53.4084, -2.9916),
    "birmingham": (52.4862, -1.8904),
    "leeds": (53.8008, -1.5491),
    "sheffield": (53.3811, -1.4701),
    "glasgow": (55.8642, -4.2518),
    "edinburgh": (55.9533, -3.1883),
    "cardiff": (51.4816, -3.1791),
    "newcastle": (54.9783, -1.6178),
    "brighton": (50.8225, -0.1372),
    "nottingham": (52.9548, -1.1581),
    "leicester": (52.6369, -1.1398),
    "southampton": (50.9097, -1.4044),
    "wolverhampton": (52.5870, -2.1288),
    "bristol": (51.4545, -2.5879),
    "silverstone": (52.0786, -1.0169),
    "twickenham": (51.4559, -0.3415),
    "wembley": (51.5560, -0.2795),
}


def _city_from_venue(venue: str | None, city: str | None) -> tuple[float, float] | None:
    """Try to resolve lat/lon from venue or city name."""
    for text in (city, venue):
        if not text:
            continue
        lower = text.lower()
        for name, coords in UK_CITY_COORDS.items():
            if name in lower:
                return coords
    return None


class SportingEventsIngestor(BaseIngestor):
    source_name = "sporting_events"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        today = date.today().isoformat()
        # Fetch all sports for today — free tier allows this
        return await self.fetch(EVENTS_DAY_URL, params={"d": today})

    async def process(self, data: Any) -> None:
        events = data.get("events") if isinstance(data, dict) else None
        if not events:
            self.log.info("Sporting events: no events today")
            return

        processed = 0
        skipped = 0

        for ev in events:
            league = ev.get("strLeague", "")

            # Filter to UK-relevant leagues
            if league not in UK_LEAGUES:
                skipped += 1
                continue

            event_name = ev.get("strEvent", "Unknown")
            sport = ev.get("strSport", "")
            home_team = ev.get("strHomeTeam", "")
            away_team = ev.get("strAwayTeam", "")
            venue = ev.get("strVenue", "")
            city = ev.get("strCity", "")
            country = ev.get("strCountry", "")
            event_time = ev.get("strTimeLocal") or ev.get("strTime", "")
            event_date = ev.get("dateEventLocal") or ev.get("dateEvent", "")
            status = ev.get("strStatus", "")
            home_score = ev.get("intHomeScore")
            away_score = ev.get("intAwayScore")
            spectators = ev.get("intSpectators")

            # Resolve location
            coords = _city_from_venue(venue, city)
            lat = coords[0] if coords else None
            lon = coords[1] if coords else None
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            # Spectator count as numeric value (proxy for event size)
            obs_value = float(spectators) if spectators else 0.0

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=obs_value,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "event": event_name,
                    "sport": sport,
                    "league": league,
                    "home_team": home_team,
                    "away_team": away_team,
                    "venue": venue,
                    "city": city,
                    "country": country,
                    "time": event_time,
                    "date": event_date,
                    "status": status,
                    "home_score": home_score,
                    "away_score": away_score,
                    "spectators": spectators,
                },
            )
            await self.board.store_observation(obs)

            score_str = ""
            if home_score is not None and away_score is not None:
                score_str = f" ({home_score}-{away_score})"

            venue_str = f" at {venue}" if venue else ""

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Sport [{sport}] {event_name}{score_str}{venue_str} "
                    f"({league}, {event_time or 'TBA'})"
                ),
                data={
                    "event": event_name,
                    "sport": sport,
                    "league": league,
                    "home_team": home_team,
                    "away_team": away_team,
                    "venue": venue,
                    "time": event_time,
                    "spectators": spectators,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Sporting events: processed=%d skipped=%d (non-UK leagues)",
            processed,
            skipped,
        )


async def ingest_sporting_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one sporting events fetch cycle."""
    ingestor = SportingEventsIngestor(board, graph, scheduler)
    await ingestor.run()
