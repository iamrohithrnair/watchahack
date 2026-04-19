"""Flickr geo-fenced media ingestor — recent geo-tagged photos from key London locations.

Uses the Flickr API to search for recently uploaded, geo-tagged photos within
a radius of notable London locations. Photo metadata (including URLs) is posted
to #raw so that Vision/Explorer agents can fetch and analyze images for crowd
density, events, weather conditions, etc.

Directly addresses the Brain's 'Prediction Verification' blind spot: the system
can cross-reference hypotheses (e.g., 'large crowd at Parliament Square') against
real photos uploaded from that area in the last hour.

Requires a free Flickr API key: https://www.flickr.com/services/apps/create/apply/
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.flickr_geo_media")

FLICKR_API_KEY = os.environ.get("FLICKR_API_KEY", "")

# flickr.photos.search endpoint
FLICKR_SEARCH_URL = "https://api.flickr.com/services/rest/"

# Key London locations to monitor — high-footfall or event-prone areas
# (lat, lon, label)
MONITOR_LOCATIONS: list[tuple[float, float, str]] = [
    (51.5007, -0.1246, "Parliament Square"),
    (51.5081, -0.0759, "Tower Bridge"),
    (51.5074, -0.1278, "Westminster"),
    (51.5099, -0.1342, "Trafalgar Square"),
    (51.5033, -0.1195, "London Eye"),
    (51.5155, -0.1415, "Oxford Circus"),
    (51.5194, -0.1270, "British Museum"),
    (51.5353, -0.0553, "Olympic Park"),
    (51.5025, -0.0034, "O2 Arena"),
    (51.4816, -0.1909, "Earl's Court"),
    (51.4613, -0.1156, "Brixton"),
    (51.5416, -0.0033, "Stratford"),
]

# Search radius in km (Flickr max is 32km, we use small radius for precision)
SEARCH_RADIUS_KM = 1

# How far back to search (seconds) — 2 hours
LOOKBACK_SECONDS = 7200

# Max photos per location to avoid flooding
MAX_PER_LOCATION = 5


class FlickrGeoMediaIngestor(BaseIngestor):
    source_name = "flickr_geo_media"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        if not FLICKR_API_KEY:
            self.log.debug("FLICKR_API_KEY not set — skipping")
            return []

        import asyncio

        min_upload = int(time.time()) - LOOKBACK_SECONDS
        all_results: list[dict] = []

        for i, (lat, lon, label) in enumerate(MONITOR_LOCATIONS):
            if i > 0:
                await asyncio.sleep(1)  # respect rate limits

            params = {
                "method": "flickr.photos.search",
                "api_key": FLICKR_API_KEY,
                "lat": str(lat),
                "lon": str(lon),
                "radius": str(SEARCH_RADIUS_KM),
                "radius_units": "km",
                "min_upload_date": str(min_upload),
                "sort": "date-posted-desc",
                "per_page": str(MAX_PER_LOCATION),
                "extras": "geo,url_m,url_s,date_upload,description,tags",
                "format": "json",
                "nojsoncallback": "1",
                "content_types": "0",  # photos only (not screenshots/etc)
                "safe_search": "1",
            }

            data = await self.fetch(FLICKR_SEARCH_URL, params=params)
            if not data or not isinstance(data, dict):
                continue

            photos = data.get("photos", {}).get("photo", [])
            for photo in photos:
                all_results.append({
                    "id": photo.get("id", ""),
                    "title": photo.get("title", ""),
                    "tags": photo.get("tags", ""),
                    "lat": float(photo.get("latitude", lat)),
                    "lon": float(photo.get("longitude", lon)),
                    "url_m": photo.get("url_m", ""),  # medium 500px
                    "url_s": photo.get("url_s", ""),  # small 240px
                    "upload_date": photo.get("dateupload", ""),
                    "description": (
                        photo.get("description", {}).get("_content", "")[:200]
                        if isinstance(photo.get("description"), dict)
                        else str(photo.get("description", ""))[:200]
                    ),
                    "monitor_location": label,
                    "monitor_lat": lat,
                    "monitor_lon": lon,
                })

        if not all_results:
            # Return empty list (not None) — no photos is normal, not an error
            return []
        return all_results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("Flickr geo-media: no recent photos found")
            return

        processed = 0
        for photo in data:
            lat = photo["lat"]
            lon = photo["lon"]
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            image_url = photo.get("url_m") or photo.get("url_s") or ""

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.IMAGE if image_url else ObservationType.TEXT,
                value=1.0,  # presence indicator
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "flickr_id": photo["id"],
                    "title": photo["title"][:200],
                    "tags": photo["tags"][:300],
                    "image_url": image_url,
                    "monitor_location": photo["monitor_location"],
                    "upload_date": photo["upload_date"],
                    "description": photo["description"],
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Geo-tagged photo near {photo['monitor_location']}: "
                    f"'{photo['title'][:80]}' "
                    f"[tags: {photo['tags'][:100]}]"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "image" if image_url else "text",
                    "flickr_id": photo["id"],
                    "title": photo["title"][:200],
                    "tags": photo["tags"][:300],
                    "image_url": image_url,
                    "monitor_location": photo["monitor_location"],
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Flickr geo-media: processed=%d photos across %d locations",
            processed,
            len(MONITOR_LOCATIONS),
        )


async def ingest_flickr_geo_media(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one Flickr geo-media fetch cycle."""
    ingestor = FlickrGeoMediaIngestor(board, graph, scheduler)
    await ingestor.run()
