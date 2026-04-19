"""OpenStreetMap Points-of-Interest ingestor via Overpass API.

Tags grid cells with building/land-use functions (media HQ, government,
stadium, hospital, etc.) so downstream agents can down-weight anomalies
that originate from cells with known explanatory context — e.g. news
anomalies near a media headquarters.

Uses the public Overpass API (no key required). Data is relatively static,
so we fetch infrequently (every 6 hours).
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.osm_poi")

OVERPASS_URL = "https://overpass-api.de/api/interpreter"

# Overpass QL query: fetch POIs within Greater London bbox that belong to
# categories useful for anomaly context.  We use a union of tagged features.
_BBOX = "51.28,-0.51,51.70,0.33"

_OVERPASS_QUERY = f"""
[out:json][timeout:60];
(
  // Media & News
  node["office"="newspaper"]({_BBOX});
  way["office"="newspaper"]({_BBOX});
  node["office"="broadcaster"]({_BBOX});
  way["office"="broadcaster"]({_BBOX});
  node["amenity"="studio"]({_BBOX});
  way["amenity"="studio"]({_BBOX});
  node["office"="news_agency"]({_BBOX});
  way["office"="news_agency"]({_BBOX});

  // Government & Diplomacy
  node["office"="government"]({_BBOX});
  way["office"="government"]({_BBOX});
  node["amenity"="townhall"]({_BBOX});
  way["amenity"="townhall"]({_BBOX});
  node["office"="diplomatic"]({_BBOX});
  way["office"="diplomatic"]({_BBOX});

  // Transport Hubs
  node["aeroway"="terminal"]({_BBOX});
  way["aeroway"="terminal"]({_BBOX});
  node["railway"="station"]["station"!="subway"]({_BBOX});
  way["railway"="station"]["station"!="subway"]({_BBOX});
  node["amenity"="bus_station"]({_BBOX});
  way["amenity"="bus_station"]({_BBOX});

  // Emergency Services
  node["amenity"="hospital"]({_BBOX});
  way["amenity"="hospital"]({_BBOX});
  node["amenity"="fire_station"]({_BBOX});
  way["amenity"="fire_station"]({_BBOX});
  node["amenity"="police"]({_BBOX});
  way["amenity"="police"]({_BBOX});

  // Entertainment & Sports
  node["leisure"="stadium"]({_BBOX});
  way["leisure"="stadium"]({_BBOX});
  node["tourism"="attraction"]({_BBOX});
  way["tourism"="attraction"]({_BBOX});
  node["amenity"="theatre"]({_BBOX});
  way["amenity"="theatre"]({_BBOX});

  // Education
  node["amenity"="university"]({_BBOX});
  way["amenity"="university"]({_BBOX});

  // Markets & Retail
  node["amenity"="marketplace"]({_BBOX});
  way["amenity"="marketplace"]({_BBOX});
);
out center tags;
"""

# Map OSM tags to human-readable POI category for the system
_CATEGORY_MAP: dict[tuple[str, str], str] = {
    ("office", "newspaper"): "media_headquarters",
    ("office", "broadcaster"): "media_headquarters",
    ("amenity", "studio"): "media_headquarters",
    ("office", "news_agency"): "news_agency",
    ("office", "government"): "government_office",
    ("amenity", "townhall"): "government_office",
    ("office", "diplomatic"): "embassy",
    ("aeroway", "terminal"): "airport_terminal",
    ("railway", "station"): "railway_station",
    ("amenity", "bus_station"): "bus_station",
    ("amenity", "hospital"): "hospital",
    ("amenity", "fire_station"): "fire_station",
    ("amenity", "police"): "police_station",
    ("leisure", "stadium"): "stadium",
    ("tourism", "attraction"): "tourist_attraction",
    ("amenity", "theatre"): "theatre",
    ("amenity", "university"): "university",
    ("amenity", "marketplace"): "marketplace",
}


def _classify_element(tags: dict[str, str]) -> str | None:
    """Return the POI category for an OSM element, or None."""
    for (key, val), category in _CATEGORY_MAP.items():
        if tags.get(key) == val:
            return category
    return None


def _element_latlon(el: dict) -> tuple[float | None, float | None]:
    """Extract lat/lon from an Overpass element (node or way center)."""
    if el.get("type") == "node":
        return el.get("lat"), el.get("lon")
    # For ways, Overpass 'out center' puts coords in a 'center' sub-object
    center = el.get("center", {})
    return center.get("lat"), center.get("lon")


class OsmPoiIngestor(BaseIngestor):
    source_name = "osm_poi"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        return await self.fetch(
            OVERPASS_URL,
            params={"data": _OVERPASS_QUERY},
        )

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.warning("Unexpected Overpass response type: %s", type(data))
            return

        elements = data.get("elements", [])
        if not elements:
            self.log.info("Overpass returned 0 elements")
            return

        processed = 0
        skipped = 0
        # Track POI categories per grid cell for summary message
        cell_categories: dict[str, set[str]] = {}

        for el in elements:
            tags = el.get("tags", {})
            category = _classify_element(tags)
            if not category:
                skipped += 1
                continue

            lat, lon = _element_latlon(el)
            if lat is None or lon is None:
                skipped += 1
                continue

            cell_id = self.graph.latlon_to_cell(lat, lon)
            if not cell_id:
                skipped += 1
                continue

            name = tags.get("name", "unnamed")

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.CATEGORICAL,
                value=category,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "name": name,
                    "category": category,
                    "osm_id": el.get("id"),
                    "osm_type": el.get("type"),
                    "tags_subset": {
                        k: v
                        for k, v in tags.items()
                        if k in ("name", "operator", "brand", "description")
                    },
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"POI [{name}] category={category} cell={cell_id}",
                data={
                    "name": name,
                    "category": category,
                    "lat": lat,
                    "lon": lon,
                    "cell_id": cell_id,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

            cell_categories.setdefault(cell_id, set()).add(category)

        # Post a summary so agents can easily look up cell->category mappings
        if cell_categories:
            summary = {
                cid: sorted(cats) for cid, cats in cell_categories.items()
            }
            summary_msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"POI scan complete: {processed} POIs across "
                    f"{len(cell_categories)} grid cells"
                ),
                data={"cell_poi_summary": summary, "total_pois": processed},
            )
            await self.board.post(summary_msg)

        self.log.info(
            "OSM POI ingest: processed=%d skipped=%d cells_tagged=%d",
            processed,
            skipped,
            len(cell_categories),
        )


async def ingest_osm_poi(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one OSM POI fetch cycle."""
    ingestor = OsmPoiIngestor(board, graph, scheduler)
    await ingestor.run()
