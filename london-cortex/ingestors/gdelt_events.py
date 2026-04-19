"""GDELT Event Database ingestor — structured geopolitical events in London.

Downloads the latest GDELT 2.0 event export (updated every 15 min),
filters for events geo-located within Greater London, and publishes
structured event observations with CAMEO codes, Goldstein scale, and tone.

No API key required — GDELT data is fully open.
"""

from __future__ import annotations

import csv
import io
import logging
import zipfile
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.gdelt_events")

GDELT_LASTUPDATE_URL = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"

# Greater London bounding box
LONDON_LAT_MIN, LONDON_LAT_MAX = 51.28, 51.70
LONDON_LON_MIN, LONDON_LON_MAX = -0.51, 0.33

# GDELT 2.0 export column indices (59 columns, 0-indexed)
COL_GLOBAL_EVENT_ID = 0
COL_ACTOR1_NAME = 6
COL_ACTOR1_COUNTRY = 7
COL_ACTOR2_NAME = 16
COL_ACTOR2_COUNTRY = 17
COL_EVENT_CODE = 26
COL_EVENT_ROOT_CODE = 28
COL_QUAD_CLASS = 29
COL_GOLDSTEIN = 30
COL_NUM_MENTIONS = 31
COL_NUM_ARTICLES = 33
COL_AVG_TONE = 34
COL_ACTION_GEO_FULLNAME = 50
COL_ACTION_GEO_COUNTRY = 51
COL_ACTION_GEO_LAT = 55
COL_ACTION_GEO_LONG = 56
COL_SOURCEURL = 58

# CAMEO root code descriptions (subset of most relevant)
CAMEO_ROOT_CODES = {
    "01": "Make public statement",
    "02": "Appeal",
    "03": "Express intent to cooperate",
    "04": "Consult",
    "05": "Engage in diplomatic cooperation",
    "06": "Engage in material cooperation",
    "07": "Provide aid",
    "08": "Yield",
    "09": "Investigate",
    "10": "Demand",
    "11": "Disapprove",
    "12": "Reject",
    "13": "Threaten",
    "14": "Protest",
    "15": "Exhibit force posture",
    "16": "Reduce relations",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Use unconventional mass violence",
}

QUAD_CLASS_LABELS = {
    "1": "Verbal Cooperation",
    "2": "Material Cooperation",
    "3": "Verbal Conflict",
    "4": "Material Conflict",
}


def _safe_float(val: str) -> float | None:
    try:
        return float(val) if val else None
    except (ValueError, TypeError):
        return None


def _safe_int(val: str) -> int | None:
    try:
        return int(val) if val else None
    except (ValueError, TypeError):
        return None


def _in_london(lat: float | None, lon: float | None) -> bool:
    if lat is None or lon is None:
        return False
    return LONDON_LAT_MIN <= lat <= LONDON_LAT_MAX and LONDON_LON_MIN <= lon <= LONDON_LON_MAX


class GdeltEventsIngestor(BaseIngestor):
    source_name = "gdelt_events"
    rate_limit_name = "default"

    def __init__(self, board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler) -> None:
        super().__init__(board, graph, scheduler)
        self._seen_ids: set[str] = set()
        self._max_seen = 5000

    async def fetch_data(self) -> Any:
        # Step 1: Get the latest export file URL
        lastupdate = await self.fetch(GDELT_LASTUPDATE_URL)
        if not lastupdate or not isinstance(lastupdate, str):
            self.log.warning("Could not fetch GDELT lastupdate.txt")
            return None

        # Parse: each line is "size url hash", we want the .export.CSV.zip line
        export_url = None
        for line in lastupdate.strip().split("\n"):
            parts = line.strip().split(" ")
            if len(parts) >= 2 and ".export.CSV" in parts[1]:
                export_url = parts[1]
                break

        if not export_url:
            self.log.warning("No export URL found in GDELT lastupdate.txt")
            return None

        # Step 2: Download the zip file (binary)
        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        try:
            timeout = aiohttp.ClientTimeout(total=60, connect=10)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(export_url) as resp:
                    if resp.status != 200:
                        self.log.warning("HTTP %d fetching GDELT export: %s", resp.status, export_url)
                        return None
                    zip_bytes = await resp.read()
        except (aiohttp.ClientError, Exception) as exc:
            self.log.warning("Error downloading GDELT export: %s", exc)
            return None

        # Step 3: Decompress and parse TSV
        try:
            with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
                names = zf.namelist()
                if not names:
                    return None
                with zf.open(names[0]) as f:
                    text = f.read().decode("utf-8", errors="replace")
        except (zipfile.BadZipFile, Exception) as exc:
            self.log.warning("Error decompressing GDELT export: %s", exc)
            return None

        # Step 4: Filter for London events
        london_events = []
        reader = csv.reader(io.StringIO(text), delimiter="\t")
        for row in reader:
            if len(row) < 59:
                continue
            lat = _safe_float(row[COL_ACTION_GEO_LAT])
            lon = _safe_float(row[COL_ACTION_GEO_LONG])
            if not _in_london(lat, lon):
                continue
            event_id = row[COL_GLOBAL_EVENT_ID]
            if event_id in self._seen_ids:
                continue
            london_events.append(row)

        return london_events

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("GDELT events: 0 London events in latest update")
            return

        processed = 0
        skipped = 0

        for row in data:
            try:
                event_id = row[COL_GLOBAL_EVENT_ID]
                lat = _safe_float(row[COL_ACTION_GEO_LAT])
                lon = _safe_float(row[COL_ACTION_GEO_LONG])
                goldstein = _safe_float(row[COL_GOLDSTEIN])
                avg_tone = _safe_float(row[COL_AVG_TONE])
                num_mentions = _safe_int(row[COL_NUM_MENTIONS])
                num_articles = _safe_int(row[COL_NUM_ARTICLES])
                event_code = row[COL_EVENT_CODE].strip()
                root_code = row[COL_EVENT_ROOT_CODE].strip()
                quad_class = row[COL_QUAD_CLASS].strip()
                actor1 = row[COL_ACTOR1_NAME].strip()
                actor2 = row[COL_ACTOR2_NAME].strip()
                geo_name = row[COL_ACTION_GEO_FULLNAME].strip()
                source_url = row[COL_SOURCEURL].strip() if len(row) > COL_SOURCEURL else ""

                cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

                event_label = CAMEO_ROOT_CODES.get(root_code, f"Event-{event_code}")
                quad_label = QUAD_CLASS_LABELS.get(quad_class, "Unknown")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.EVENT,
                    value=goldstein if goldstein is not None else 0.0,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "gdelt_event_id": event_id,
                        "event_code": event_code,
                        "root_code": root_code,
                        "event_label": event_label,
                        "quad_class": quad_class,
                        "quad_label": quad_label,
                        "goldstein_scale": goldstein,
                        "avg_tone": avg_tone,
                        "num_mentions": num_mentions,
                        "num_articles": num_articles,
                        "actor1": actor1,
                        "actor2": actor2,
                        "location_name": geo_name,
                        "source_url": source_url,
                    },
                )
                await self.board.store_observation(obs)

                # Build concise message
                actors = " vs ".join(filter(None, [actor1, actor2])) or "Unknown actors"
                content = (
                    f"GDELT event [{geo_name}] {event_label} ({quad_label}): "
                    f"{actors}, Goldstein={goldstein}, tone={avg_tone:.1f}"
                    if avg_tone is not None
                    else f"GDELT event [{geo_name}] {event_label} ({quad_label}): "
                    f"{actors}, Goldstein={goldstein}"
                )

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=content,
                    data={
                        "source": self.source_name,
                        "obs_type": "event",
                        "gdelt_event_id": event_id,
                        "event_code": event_code,
                        "event_label": event_label,
                        "quad_label": quad_label,
                        "goldstein_scale": goldstein,
                        "avg_tone": avg_tone,
                        "actor1": actor1,
                        "actor2": actor2,
                        "location_name": geo_name,
                        "lat": lat,
                        "lon": lon,
                        "source_url": source_url,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)

                # Track seen IDs for deduplication
                self._seen_ids.add(event_id)
                processed += 1

            except Exception:
                self.log.exception("Error processing GDELT event row")
                skipped += 1

        # Trim seen IDs to prevent unbounded growth
        if len(self._seen_ids) > self._max_seen:
            excess = len(self._seen_ids) - self._max_seen
            it = iter(self._seen_ids)
            for _ in range(excess):
                self._seen_ids.discard(next(it))

        self.log.info("GDELT events: processed=%d skipped=%d", processed, skipped)


async def ingest_gdelt_events(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GDELT events fetch cycle."""
    ingestor = GdeltEventsIngestor(board, graph, scheduler)
    await ingestor.run()
