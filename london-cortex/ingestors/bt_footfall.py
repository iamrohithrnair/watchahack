"""BT Footfall ingestor — anonymized mobile-network-derived footfall counts.

Uses the BT Developer API (https://developer.bt.com/products/footfall-data)
which provides hourly footfall volumes at MSOA level, derived from anonymized
BT mobile network activity.  This gives a direct measure of city-wide human
presence and movement to contextualize anomalies.

Auth: OAuth2 client-credentials flow (register free sandbox at developer.bt.com).
"""

from __future__ import annotations

import base64
import csv
import io
import logging
import os
import time
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.bt_footfall")

# BT API endpoints
BT_TOKEN_URL = "https://api.bt.com/oauth/token"
BT_FOOTFALL_URL = "https://api.bt.com/bt-footfall/footfall"

# London MSOA codes start with E02000001–E02000983 (approx).
# We filter to Inner/Outer London borough MSOAs.
LONDON_MSOA_PREFIXES = (
    "E02000001", "E0200",  # will match most London MSOAs
)

# Cache token to avoid re-authenticating every cycle
_token_cache: dict[str, Any] = {"token": None, "expires_at": 0.0}


class BTFootfallIngestor(BaseIngestor):
    """Ingest hourly footfall counts from BT's mobile network data."""

    source_name = "bt_footfall"
    rate_limit_name = "default"

    def __init__(
        self,
        board: MessageBoard,
        graph: CortexGraph,
        scheduler: AsyncScheduler,
    ) -> None:
        super().__init__(board, graph, scheduler)
        self.client_id = os.getenv("BT_FOOTFALL_CLIENT_ID", "")
        self.client_secret = os.getenv("BT_FOOTFALL_CLIENT_SECRET", "")

    async def _get_token(self) -> str | None:
        """Obtain or reuse an OAuth2 access token via client credentials."""
        now = time.time()
        if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
            return _token_cache["token"]

        if not self.client_id or not self.client_secret:
            self.log.warning("BT_FOOTFALL_CLIENT_ID / BT_FOOTFALL_CLIENT_SECRET not set")
            return None

        creds = base64.b64encode(
            f"{self.client_id}:{self.client_secret}".encode()
        ).decode()

        headers = {
            "Authorization": f"Basic {creds}",
            "Content-Type": "application/x-www-form-urlencoded",
        }
        body = "grant_type=client_credentials"

        try:
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    BT_TOKEN_URL, headers=headers, data=body,
                    timeout=aiohttp.ClientTimeout(total=15),
                ) as resp:
                    if resp.status != 200:
                        self.log.warning("BT OAuth token request failed: HTTP %d", resp.status)
                        return None
                    data = await resp.json(content_type=None)
                    token = data.get("access_token")
                    expires_in = data.get("expires_in", 3600)
                    _token_cache["token"] = token
                    _token_cache["expires_at"] = now + expires_in
                    return token
        except Exception as exc:
            self.log.warning("BT OAuth error: %s", exc)
            return None

    async def fetch_data(self) -> Any:
        """Fetch hourly footfall data for London MSOAs."""
        token = await self._get_token()
        if not token:
            return None

        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "geographyType": "msoa",
            "frequency": "hourly",
            "metrics": "footfall,footfall_density",
        }

        # The BT API returns a signed URL pointing to a CSV file
        data = await self.fetch(BT_FOOTFALL_URL, params=params, headers=headers)
        if data is None:
            return None

        # If the response is a JSON with a signed URL, fetch the CSV
        if isinstance(data, dict):
            signed_url = data.get("url") or data.get("signed_url") or data.get("download_url")
            if signed_url:
                csv_text = await self.fetch(signed_url)
                return csv_text
            # Direct JSON data — return as-is
            return data

        # If already a string (CSV), return directly
        if isinstance(data, str):
            return data

        return data

    async def process(self, data: Any) -> None:
        """Parse footfall data and store observations."""
        rows = []

        if isinstance(data, str):
            # CSV text
            reader = csv.DictReader(io.StringIO(data))
            rows = list(reader)
        elif isinstance(data, dict):
            # JSON response with embedded data
            rows = data.get("data", data.get("results", []))
            if not isinstance(rows, list):
                rows = [rows]
        elif isinstance(data, list):
            rows = data
        else:
            self.log.warning("Unexpected BT footfall data type: %s", type(data))
            return

        processed = 0
        skipped = 0

        for row in rows:
            # Normalize field names (CSV headers vary)
            msoa = (
                row.get("msoa_code")
                or row.get("msoa")
                or row.get("geography_code")
                or row.get("area_code", "")
            )

            # Filter to London MSOAs only
            if not self._is_london_msoa(msoa):
                skipped += 1
                continue

            footfall_str = (
                row.get("footfall")
                or row.get("footfall_count")
                or row.get("count")
                or row.get("value")
            )
            density_str = (
                row.get("footfall_density")
                or row.get("density")
            )
            hour = row.get("hour") or row.get("time_period") or row.get("date_hour", "")

            try:
                footfall = float(footfall_str) if footfall_str else None
            except (ValueError, TypeError):
                footfall = None

            try:
                density = float(density_str) if density_str else None
            except (ValueError, TypeError):
                density = None

            if footfall is None:
                skipped += 1
                continue

            # Map MSOA centroid to grid cell (approximate — MSOAs are ~ward-sized)
            lat, lon = self._msoa_to_approx_latlon(msoa)
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=footfall,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "msoa": msoa,
                    "density": density,
                    "hour": hour,
                    "metric": "footfall",
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=f"Footfall [{msoa}] count={footfall:.0f}"
                + (f" density={density:.1f}/km²" if density else "")
                + (f" hour={hour}" if hour else ""),
                data={
                    "msoa": msoa,
                    "footfall": footfall,
                    "density": density,
                    "hour": hour,
                    "lat": lat,
                    "lon": lon,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("BT footfall: processed=%d skipped=%d", processed, skipped)

    @staticmethod
    def _is_london_msoa(code: str) -> bool:
        """Check if an MSOA code belongs to a London borough.

        London MSOAs fall in the range E02000001–E02000983 (approximate).
        A more precise filter would use a borough lookup table.
        """
        if not code or not code.startswith("E02"):
            return False
        try:
            num = int(code[3:])
            # London MSOA numeric range (approximate)
            return 1 <= num <= 983
        except ValueError:
            return False

    @staticmethod
    def _msoa_to_approx_latlon(msoa: str) -> tuple[float | None, float | None]:
        """Return approximate centroid for a London MSOA.

        Ideally this would use a lookup table of MSOA centroids.
        For now, return central London as a fallback so observations
        are at least placed on the graph — the MSOA code in metadata
        allows proper mapping downstream.
        """
        # Central London fallback (Trafalgar Square area)
        return 51.508, -0.128


async def ingest_bt_footfall(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one BT footfall fetch cycle."""
    ingestor = BTFootfallIngestor(board, graph, scheduler)
    await ingestor.run()
