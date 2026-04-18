"""Financial data ingestor — FTSE stocks (yfinance), crypto (CoinGecko), Polymarket."""

from __future__ import annotations

import asyncio
import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.financial")

COINGECKO_URL = "https://api.coingecko.com/api/v3/simple/price"
COINGECKO_PARAMS = {
    "ids": "bitcoin,ethereum",
    "vs_currencies": "gbp",
}

POLYMARKET_URL = "https://gamma-api.polymarket.com/events"
POLYMARKET_PARAMS = {
    "tag": "Politics",
    "closed": "false",
}

FTSE_TICKERS = ["^FTSE", "SHEL.L", "HSBA.L", "BP.L", "AZN.L", "ULVR.L", "RIO.L", "GSK.L"]

# Central London cell for financial data with no geo
LONDON_LAT = 51.5
LONDON_LON = -0.12


def _fetch_yfinance_sync() -> dict[str, Any]:
    """Fetch FTSE stock data synchronously (runs in thread executor)."""
    try:
        import yfinance as yf  # type: ignore
    except ImportError:
        log.error("yfinance not installed")
        return {}

    results: dict[str, Any] = {}
    for ticker_sym in FTSE_TICKERS:
        try:
            ticker = yf.Ticker(ticker_sym)
            info = ticker.fast_info
            price = getattr(info, "last_price", None)
            prev_close = getattr(info, "previous_close", None)
            currency = getattr(info, "currency", "GBP")
            results[ticker_sym] = {
                "price": float(price) if price is not None else None,
                "prev_close": float(prev_close) if prev_close is not None else None,
                "currency": currency,
            }
        except Exception as exc:
            log.warning("yfinance error for %s: %s", ticker_sym, exc)
            results[ticker_sym] = {"price": None, "prev_close": None, "currency": "GBP"}
    return results


class FinancialIngestor(BaseIngestor):
    source_name = "financial"
    rate_limit_name = "default"

    async def fetch_data(self) -> dict[str, Any]:
        # Run all fetches concurrently where possible
        stocks_task = asyncio.to_thread(_fetch_yfinance_sync)
        crypto_task = self.fetch(COINGECKO_URL, params=COINGECKO_PARAMS)
        poly_task = self.fetch(POLYMARKET_URL, params=POLYMARKET_PARAMS)

        stocks, crypto, polymarket = await asyncio.gather(
            stocks_task, crypto_task, poly_task, return_exceptions=True
        )

        return {
            "stocks": stocks if not isinstance(stocks, Exception) else {},
            "crypto": crypto if not isinstance(crypto, Exception) else {},
            "polymarket": polymarket if not isinstance(polymarket, Exception) else [],
        }

    async def process(self, data: dict[str, Any]) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)

        # ── Stocks ──────────────────────────────────────────────────────────
        stocks: dict[str, Any] = data.get("stocks") or {}
        for ticker_sym, info in stocks.items():
            price = info.get("price")
            if price is None:
                continue
            prev = info.get("prev_close")
            pct_change = None
            if prev and prev != 0:
                pct_change = round((price - prev) / prev * 100, 3)

            obs = Observation(
                source="yfinance",
                obs_type=ObservationType.NUMERIC,
                value=price,
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={
                    "ticker": ticker_sym,
                    "currency": info.get("currency", "GBP"),
                    "prev_close": prev,
                    "pct_change": pct_change,
                },
            )
            await self.board.store_observation(obs)

            change_str = f" ({pct_change:+.2f}%)" if pct_change is not None else ""
            msg = AgentMessage(
                from_agent="yfinance",
                channel="#raw",
                content=f"Stock [{ticker_sym}] price={price:.2f} GBP{change_str}",
                data={
                    "ticker": ticker_sym,
                    "price": price,
                    "prev_close": prev,
                    "pct_change": pct_change,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        # ── Crypto ──────────────────────────────────────────────────────────
        crypto: dict[str, Any] = data.get("crypto") or {}
        for coin_id, prices in crypto.items():
            if not isinstance(prices, dict):
                continue
            gbp_price = prices.get("gbp")
            if gbp_price is None:
                continue

            obs = Observation(
                source="coingecko",
                obs_type=ObservationType.NUMERIC,
                value=float(gbp_price),
                location_id=cell_id,
                lat=LONDON_LAT,
                lon=LONDON_LON,
                metadata={"coin": coin_id, "currency": "GBP"},
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent="coingecko",
                channel="#raw",
                content=f"Crypto [{coin_id.upper()}] price={gbp_price:.2f} GBP",
                data={
                    "coin": coin_id,
                    "price_gbp": float(gbp_price),
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)

        # ── Polymarket ──────────────────────────────────────────────────────
        poly_data = data.get("polymarket") or []
        if isinstance(poly_data, list):
            events = poly_data
        elif isinstance(poly_data, dict):
            events = poly_data.get("events", poly_data.get("data", []))
        else:
            events = []

        for event in events[:20]:  # cap at 20
            try:
                title = event.get("title", "")
                slug = event.get("slug", "")
                volume = event.get("volume") or event.get("liquidityNum")

                obs = Observation(
                    source="polymarket",
                    obs_type=ObservationType.TEXT,
                    value=title,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "slug": slug,
                        "volume": volume,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent="polymarket",
                    channel="#raw",
                    content=f"Polymarket [{slug}] {title[:100]}",
                    data={
                        "title": title,
                        "slug": slug,
                        "volume": volume,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
            except Exception:
                self.log.exception("Error processing Polymarket event")

        self.log.info(
            "Financial: stocks=%d crypto=%d polymarket_events=%d",
            len(stocks),
            len(crypto),
            len(events),
        )


async def ingest_financial(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one financial fetch cycle."""
    ingestor = FinancialIngestor(board, graph, scheduler)
    await ingestor.run()
