"""GDELT discourse monitoring — tone & theme trends for London news verification.

Complements the existing GDELT article ingestor (news.py) by tracking
**aggregate tone** and **theme frequency** over time. This structured
discourse data enables the Brain and Validator to verify predictions
about media sentiment shifts and public discourse trends.

Uses the GDELT DOC v2 API with timelinetone and timelinetheme modes.
No API key required — free public API.
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

log = logging.getLogger("cortex.ingestors.gdelt_discourse")

GDELT_DOC_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

# Central London coords for non-geolocated discourse observations
LONDON_LAT = 51.5074
LONDON_LON = -0.1278

# Themes we track for London-relevant discourse
TRACKED_THEMES = [
    "PROTEST",
    "TERROR",
    "CLIMATE",
    "TRANSPORT",
    "HEALTH",
    "ECONOMY",
    "CRIME",
    "ELECTION",
    "STRIKE",
    "HOUSING",
]


class GdeltDiscourseIngestor(BaseIngestor):
    source_name = "gdelt_discourse"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        """Fetch tone timeline and theme data for London news."""
        # Tone timeline: average tone of London-related articles over last 24h
        tone_params = {
            "query": "london",
            "mode": "timelinetone",
            "format": "json",
            "timespan": "24h",
        }
        tone_data = await self.fetch(GDELT_DOC_URL, params=tone_params)

        # Theme timeline: which themes are trending in London coverage
        theme_params = {
            "query": "london",
            "mode": "timelinetheme",
            "format": "json",
            "timespan": "24h",
            "maxthemes": "20",
        }
        theme_data = await self.fetch(GDELT_DOC_URL, params=theme_params)

        if tone_data is None and theme_data is None:
            return None
        return {"tone": tone_data, "themes": theme_data}

    async def process(self, data: Any) -> None:
        processed = 0

        # --- Process tone timeline ---
        tone_data = data.get("tone")
        if isinstance(tone_data, dict):
            timeline = tone_data.get("timeline", [])
            if isinstance(timeline, list):
                processed += await self._process_tone(timeline)

        # --- Process theme data ---
        theme_data = data.get("themes")
        if isinstance(theme_data, dict):
            timeline = theme_data.get("timeline", [])
            if isinstance(timeline, list):
                processed += await self._process_themes(timeline)

        self.log.info("GDELT discourse: processed=%d observations", processed)

    async def _process_tone(self, timeline: list) -> int:
        """Extract recent tone scores from the timeline."""
        count = 0

        for series in timeline:
            if not isinstance(series, dict):
                continue
            data_points = series.get("data", [])
            if not data_points:
                continue

            # Take the most recent data points (last 3)
            recent = data_points[-3:] if len(data_points) > 3 else data_points

            for point in recent:
                if not isinstance(point, dict):
                    continue
                date_str = point.get("date", "")
                value = point.get("value")
                if value is None:
                    continue
                try:
                    tone_score = float(value)
                except (ValueError, TypeError):
                    continue

                cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=tone_score,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "metric": "media_tone",
                        "date": date_str,
                        "description": (
                            "Average tone of London news coverage "
                            "(negative=negative tone, positive=positive)"
                        ),
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"London media tone={tone_score:.2f} "
                        f"({'positive' if tone_score > 0 else 'negative'} sentiment) "
                        f"[{date_str}]"
                    ),
                    data={
                        "source": self.source_name,
                        "metric": "media_tone",
                        "tone": tone_score,
                        "date": date_str,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                count += 1

        return count

    async def _process_themes(self, timeline: list) -> int:
        """Extract theme frequencies, focusing on tracked themes."""
        count = 0
        theme_totals: dict[str, float] = {}

        for series in timeline:
            if not isinstance(series, dict):
                continue
            series_name = series.get("series", "")
            data_points = series.get("data", [])

            # Check if this theme is one we track
            theme_match = None
            for tracked in TRACKED_THEMES:
                if tracked.lower() in series_name.lower():
                    theme_match = tracked
                    break
            if not theme_match:
                continue

            # Sum recent values for this theme
            recent = data_points[-3:] if len(data_points) > 3 else data_points
            total = 0.0
            for point in recent:
                if isinstance(point, dict):
                    try:
                        total += float(point.get("value", 0))
                    except (ValueError, TypeError):
                        pass
            if total > 0:
                theme_totals[theme_match] = total

        if not theme_totals:
            return 0

        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # Post observation with all tracked theme frequencies
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.TEXT,
            value=f"London discourse themes: {', '.join(f'{k}={v:.0f}' for k, v in sorted(theme_totals.items(), key=lambda x: -x[1]))}",
            location_id=cell_id,
            lat=LONDON_LAT,
            lon=LONDON_LON,
            metadata={
                "metric": "discourse_themes",
                "themes": theme_totals,
                "timestamp": datetime.now(timezone.utc).isoformat(),
            },
        )
        await self.board.store_observation(obs)

        # Post top themes to board
        top_themes = sorted(theme_totals.items(), key=lambda x: -x[1])[:5]
        summary = ", ".join(f"{k}({v:.0f})" for k, v in top_themes)
        msg = AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=f"London discourse trending: {summary}",
            data={
                "source": self.source_name,
                "metric": "discourse_themes",
                "themes": theme_totals,
                "top_themes": [t[0] for t in top_themes],
                "observation_id": obs.id,
            },
            location_id=cell_id,
        )
        await self.board.post(msg)
        count += 1

        return count


async def ingest_gdelt_discourse(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GDELT discourse fetch cycle."""
    ingestor = GdeltDiscourseIngestor(board, graph, scheduler)
    await ingestor.run()
