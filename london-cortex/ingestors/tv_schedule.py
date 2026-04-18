"""TVmaze UK broadcast & streaming schedule ingestor.

Fetches today's UK TV schedule (broadcast + streaming) from TVmaze's free API.
Posts observations with show names, networks, genres, and air times so the
system can correlate cultural events with sentiment/grid spikes.

API docs: https://www.tvmaze.com/api
No API key required. Rate limit: 20 calls / 10 seconds.
Country code for UK is 'GB' (ISO 3166-1).
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

log = logging.getLogger("cortex.ingestors.tv_schedule")

TVMAZE_SCHEDULE_URL = "https://api.tvmaze.com/schedule"
TVMAZE_WEB_SCHEDULE_URL = "https://api.tvmaze.com/schedule/web"
COUNTRY_CODE = "GB"


class TVScheduleIngestor(BaseIngestor):
    source_name = "tv_schedule"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        today = date.today().isoformat()
        params = {"country": COUNTRY_CODE, "date": today}

        broadcast = await self.fetch(TVMAZE_SCHEDULE_URL, params=params)
        streaming = await self.fetch(TVMAZE_WEB_SCHEDULE_URL, params=params)

        return {
            "broadcast": broadcast if isinstance(broadcast, list) else [],
            "streaming": streaming if isinstance(streaming, list) else [],
        }

    async def process(self, data: Any) -> None:
        processed = 0

        for schedule_type, episodes in data.items():
            for ep in episodes:
                show = ep.get("show") or ep.get("_embedded", {}).get("show", {})
                if not show:
                    continue

                show_name = show.get("name", "Unknown")
                network = (show.get("network") or {}).get("name", "")
                web_channel = (show.get("webChannel") or {}).get("name", "")
                channel_name = network or web_channel or "Unknown"
                genres = show.get("genres", [])
                show_type = show.get("type", "")
                rating = (show.get("rating") or {}).get("average")
                airtime = ep.get("airtime", "")
                ep_name = ep.get("name", "")
                season = ep.get("season", 0)
                episode_num = ep.get("number", 0)
                runtime = ep.get("runtime") or show.get("runtime") or 0

                # Use rating as numeric value (popularity proxy)
                obs_value = rating if rating else 0.0

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=obs_value,
                    location_id=None,  # TV is city-wide, not grid-specific
                    lat=None,
                    lon=None,
                    metadata={
                        "show": show_name,
                        "episode": ep_name,
                        "season": season,
                        "episode_number": episode_num,
                        "channel": channel_name,
                        "schedule_type": schedule_type,
                        "genres": genres,
                        "show_type": show_type,
                        "rating": rating,
                        "airtime": airtime,
                        "runtime_min": runtime,
                    },
                )
                await self.board.store_observation(obs)

                genre_str = ", ".join(genres) if genres else "unclassified"
                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"TV [{schedule_type}] {show_name} S{season:02d}E{episode_num:02d} "
                        f"on {channel_name} at {airtime or 'TBA'} ({genre_str})"
                    ),
                    data={
                        "show": show_name,
                        "episode": ep_name,
                        "channel": channel_name,
                        "schedule_type": schedule_type,
                        "genres": genres,
                        "rating": rating,
                        "airtime": airtime,
                        "observation_id": obs.id,
                    },
                    location_id=None,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info(
            "TV schedule: processed=%d (broadcast=%d, streaming=%d)",
            processed,
            len(data.get("broadcast", [])),
            len(data.get("streaming", [])),
        )


async def ingest_tv_schedule(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one TV schedule fetch cycle."""
    ingestor = TVScheduleIngestor(board, graph, scheduler)
    await ingestor.run()
