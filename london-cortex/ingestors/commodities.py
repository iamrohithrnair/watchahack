"""Commodity futures ingestor — Brent Crude, metals, natural gas via yfinance.

Provides a financial sensor for geopolitical/crime event market reactions.
Uses COMEX/NYMEX futures as proxies for LME metals (LME API requires paid access).
"""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.commodities")

# Commodity futures tickers (yfinance)
COMMODITY_TICKERS = {
    "BZ=F": {"name": "Brent Crude", "unit": "USD/bbl", "category": "energy"},
    "CL=F": {"name": "WTI Crude", "unit": "USD/bbl", "category": "energy"},
    "NG=F": {"name": "Natural Gas", "unit": "USD/MMBtu", "category": "energy"},
    "HG=F": {"name": "Copper", "unit": "USD/lb", "category": "metal"},
    "GC=F": {"name": "Gold", "unit": "USD/oz", "category": "metal"},
    "SI=F": {"name": "Silver", "unit": "USD/oz", "category": "metal"},
    "ALI=F": {"name": "Aluminum", "unit": "USD/lb", "category": "metal"},
}

# Central London cell for commodity data
LONDON_LAT = 51.5
LONDON_LON = -0.12


def _fetch_commodities_sync() -> dict[str, Any]:
    """Fetch commodity futures data synchronously (runs in thread executor)."""
    try:
        import yfinance as yf  # type: ignore
    except ImportError:
        log.error("yfinance not installed")
        return {}

    results: dict[str, Any] = {}
    for ticker_sym, meta in COMMODITY_TICKERS.items():
        try:
            ticker = yf.Ticker(ticker_sym)
            info = ticker.fast_info
            price = getattr(info, "last_price", None)
            prev_close = getattr(info, "previous_close", None)
            results[ticker_sym] = {
                "price": float(price) if price is not None else None,
                "prev_close": float(prev_close) if prev_close is not None else None,
                "name": meta["name"],
                "unit": meta["unit"],
                "category": meta["category"],
            }
        except Exception as exc:
            log.warning("yfinance commodity error for %s: %s", ticker_sym, exc)
    return results


class CommoditiesIngestor(BaseIngestor):
    source_name = "commodities"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        return await asyncio.to_thread(_fetch_commodities_sync)

    async def process(self, data: dict[str, Any]) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        processed = 0

        for ticker_sym, info in data.items():
            price = info.get("price")
            if price is None:
                continue

            prev = info.get("prev_close")
            pct_change = None
            if prev and prev != 0:
                pct_change = round((price - prev) / prev * 100, 3)

            name = info.get("name", ticker_sym)
            unit = info.get("unit", "USD")
            category = info.get("category", "commodity")

            obs = Observation(
                source="commodities",
                obs_type=ObservationType.NUMERIC,
                value=price,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "ticker": ticker_sym,
                    "name": name,
                    "unit": unit,
                    "category": category,
                    "prev_close": prev,
                    "pct_change": pct_change,
                },
            )
            await self.board.store_observation(obs)

            change_str = f" ({pct_change:+.2f}%)" if pct_change is not None else ""
            msg = AgentMessage(
                from_agent="commodities",
                channel="#raw",
                content=f"Commodity [{name}] price={price:.2f} {unit}{change_str}",
                data={
                    "ticker": ticker_sym,
                    "name": name,
                    "price": price,
                    "unit": unit,
                    "category": category,
                    "prev_close": prev,
                    "pct_change": pct_change,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info("Commodities: processed=%d tickers", processed)


async def ingest_commodities(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one commodity fetch cycle."""
    ingestor = CommoditiesIngestor(board, graph, scheduler)
    await ingestor.run()
