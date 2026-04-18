"""Land Registry Price Paid — recent London property sales."""

from __future__ import annotations

import logging
import random
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.land_registry")

# SPARQL endpoint for Land Registry linked data
_SPARQL_URL = "https://landregistry.data.gov.uk/landregistry/query"

# Query template — district is filled per-cycle to avoid timeout on full GREATER LONDON
_SPARQL_TEMPLATE = """
PREFIX lrppi: <http://landregistry.data.gov.uk/def/ppi/>
PREFIX lrcommon: <http://landregistry.data.gov.uk/def/common/>

SELECT ?price ?date ?propertyType ?estateType ?street ?town ?district ?postcode
WHERE {{
  ?item lrppi:pricePaid ?price ;
        lrppi:transactionDate ?date ;
        lrppi:propertyAddress ?addr .
  ?addr lrcommon:district "{district}"^^<http://www.w3.org/2001/XMLSchema#string> ;
        lrcommon:street ?street ;
        lrcommon:town ?town ;
        lrcommon:district ?district ;
        lrcommon:postcode ?postcode .
  OPTIONAL {{ ?item lrppi:propertyType ?propertyType . }}
  OPTIONAL {{ ?item lrppi:estateType ?estateType . }}
}}
ORDER BY DESC(?date)
LIMIT 15
"""

# London boroughs to lat/lon (approximate centers for geo-tagging)
_BOROUGH_COORDS: dict[str, tuple[float, float]] = {
    "CITY OF LONDON": (51.5155, -0.0922),
    "CITY OF WESTMINSTER": (51.4975, -0.1357),
    "CAMDEN": (51.5290, -0.1255),
    "HACKNEY": (51.5450, -0.0553),
    "TOWER HAMLETS": (51.5099, -0.0059),
    "ISLINGTON": (51.5362, -0.1033),
    "SOUTHWARK": (51.5035, -0.0804),
    "LAMBETH": (51.4571, -0.1231),
    "WANDSWORTH": (51.4571, -0.1918),
    "KENSINGTON AND CHELSEA": (51.5020, -0.1947),
    "HAMMERSMITH AND FULHAM": (51.4927, -0.2339),
    "GREENWICH": (51.4892, 0.0648),
    "LEWISHAM": (51.4415, -0.0117),
    "NEWHAM": (51.5077, 0.0469),
    "BARKING AND DAGENHAM": (51.5397, 0.1231),
    "REDBRIDGE": (51.5590, 0.0741),
    "HAVERING": (51.5779, 0.2121),
    "BEXLEY": (51.4549, 0.1505),
    "BROMLEY": (51.4039, 0.0198),
    "CROYDON": (51.3762, -0.0982),
    "SUTTON": (51.3618, -0.1945),
    "MERTON": (51.4098, -0.2108),
    "KINGSTON UPON THAMES": (51.4085, -0.3064),
    "RICHMOND UPON THAMES": (51.4613, -0.3037),
    "HOUNSLOW": (51.4746, -0.3680),
    "HILLINGDON": (51.5441, -0.4760),
    "EALING": (51.5130, -0.3089),
    "BRENT": (51.5588, -0.2817),
    "HARROW": (51.5898, -0.3346),
    "BARNET": (51.6252, -0.1517),
    "HARINGEY": (51.5906, -0.1110),
    "ENFIELD": (51.6538, -0.0799),
    "WALTHAM FOREST": (51.5886, -0.0117),
}


class LandRegistryIngestor(BaseIngestor):
    source_name = "land_registry"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import aiohttp
        district = random.choice(list(_BOROUGH_COORDS.keys()))
        query = _SPARQL_TEMPLATE.format(district=district)
        headers = {"Accept": "application/sparql-results+json"}
        params = {"query": query}

        await self.scheduler.rate_limiter.acquire(self.rate_limit_name)
        try:
            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=30)) as session:
                async with session.get(_SPARQL_URL, params=params, headers=headers) as resp:
                    if resp.status == 200:
                        data = await resp.json(content_type=None)
                        if isinstance(data, dict):
                            data["_district"] = district
                        return data
                    else:
                        self.log.warning("Land Registry HTTP %d for %s", resp.status, district)
                        return None
        except Exception:
            self.log.exception("Error fetching Land Registry data for %s", district)
            return None

    async def process(self, data: Any) -> None:
        if not isinstance(data, dict):
            self.log.info("Unexpected Land Registry response: %s", type(data))
            return

        bindings = data.get("results", {}).get("bindings", [])
        if not bindings:
            self.log.info("Land Registry returned 0 results")
            return

        processed = 0
        for row in bindings:
            try:
                price = int(row.get("price", {}).get("value", "0"))
                date = row.get("date", {}).get("value", "")
                street = row.get("street", {}).get("value", "")
                town = row.get("town", {}).get("value", "")
                district = row.get("district", {}).get("value", "")
                postcode = row.get("postcode", {}).get("value", "")
                prop_type_uri = row.get("propertyType", {}).get("value", "")
                estate_type_uri = row.get("estateType", {}).get("value", "")

                # Extract property type from URI
                prop_type = prop_type_uri.split("/")[-1] if prop_type_uri else "unknown"
                estate_type = estate_type_uri.split("/")[-1] if estate_type_uri else "unknown"

                # Geo-tag by district
                district_upper = district.upper()
                lat, lon = _BOROUGH_COORDS.get(district_upper, (51.5074, -0.1278))
                cell_id = self.graph.latlon_to_cell(lat, lon)

                obs = Observation(
                    source="land_registry",
                    obs_type=ObservationType.NUMERIC,
                    value=price,
                    location_id=cell_id,
                    lat=lat,
                    lon=lon,
                    metadata={
                        "date": date,
                        "street": street,
                        "town": town,
                        "district": district,
                        "postcode": postcode,
                        "property_type": prop_type,
                        "estate_type": estate_type,
                    },
                )
                await self.board.store_observation(obs)

                price_str = f"£{price:,.0f}"
                msg = AgentMessage(
                    from_agent="land_registry",
                    channel="#raw",
                    content=(
                        f"Property sale: {street}, {town} ({district}) — "
                        f"{price_str} [{prop_type}/{estate_type}] on {date}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "numeric",
                        "price": price,
                        "date": date,
                        "street": street,
                        "district": district,
                        "postcode": postcode,
                        "property_type": prop_type,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing Land Registry row")

        self.log.info("Land Registry: processed=%d sales", processed)


async def ingest_land_registry(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    ingestor = LandRegistryIngestor(board, graph, scheduler)
    await ingestor.run()
