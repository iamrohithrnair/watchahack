"""Electricity wholesale price ingestor — GB system prices and market index data.

Uses the Elexon BMRS Insights Solution API (free, no API key required).
Provides half-hourly system sell/buy prices (£/MWh) and market index prices
for the GB electricity market — the economic signal behind grid behaviour.

Endpoints:
  - System prices: /balancing/settlement/system-prices/{date}/{period}
  - Market index:  /balancing/pricing/market-index?from={date}&to={date}
"""

from __future__ import annotations

import logging
from datetime import date, datetime, timezone
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.electricity_prices")

ELEXON_BASE = "https://data.elexon.co.uk/bmrs/api/v1"
SYSTEM_PRICES_URL = ELEXON_BASE + "/balancing/settlement/system-prices/{date}/{period}"
MARKET_INDEX_URL = ELEXON_BASE + "/balancing/pricing/market-index"

LONDON_LAT = 51.5
LONDON_LON = -0.12


def _current_settlement_period() -> int:
    """GB settlement periods: 1-48, each 30 min starting at midnight UTC."""
    now = datetime.now(timezone.utc)
    minutes_since_midnight = now.hour * 60 + now.minute
    return max(1, minutes_since_midnight // 30 + 1)


class ElectricityPriceIngestor(BaseIngestor):
    source_name = "electricity_prices"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        today = date.today().isoformat()
        period = _current_settlement_period()

        # Fetch both system prices and market index in parallel-ish
        system_data = await self.fetch(
            SYSTEM_PRICES_URL.format(date=today, period=period),
        )
        market_data = await self.fetch(
            MARKET_INDEX_URL,
            params={"from": today, "to": today},
        )
        return {"system": system_data, "market": market_data}

    async def process(self, data: Any) -> None:
        cell_id = self.graph.latlon_to_cell(LONDON_LAT, LONDON_LON)
        processed = 0

        # -- System prices (£/MWh) --
        system_resp = data.get("system")
        if isinstance(system_resp, dict):
            entries = system_resp.get("data", [])
            if isinstance(entries, dict):
                entries = [entries]
            for entry in entries:
                sell_price = entry.get("systemSellPrice")
                buy_price = entry.get("systemBuyPrice")
                period = entry.get("settlementPeriod")
                sdate = entry.get("settlementDate", "")
                net_imbalance = entry.get("netImbalanceVolume")

                if sell_price is not None:
                    obs = Observation(
                        source=self.source_name,
                        obs_type=ObservationType.NUMERIC,
                        value=float(sell_price),
                        location_id=cell_id,
                        lat=LONDON_LAT,
                        lon=LONDON_LON,
                        metadata={
                            "metric": "system_sell_price_gbp_mwh",
                            "settlement_period": period,
                            "settlement_date": sdate,
                            "buy_price": buy_price,
                            "net_imbalance_volume": net_imbalance,
                        },
                    )
                    await self.board.store_observation(obs)

                    msg = AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"GB electricity system price: sell={sell_price:.2f}/MWh "
                            f"buy={buy_price:.2f}/MWh "
                            f"(period {period}, imbalance={net_imbalance:.1f}MW)"
                        ),
                        data={
                            "system_sell_price": sell_price,
                            "system_buy_price": buy_price,
                            "settlement_period": period,
                            "settlement_date": sdate,
                            "net_imbalance_volume": net_imbalance,
                            "observation_id": obs.id,
                        },
                        location_id=cell_id,
                    )
                    await self.board.post(msg)
                    processed += 1

        # -- Market index prices --
        market_resp = data.get("market")
        if isinstance(market_resp, dict):
            entries = market_resp.get("data", [])
            if isinstance(entries, dict):
                entries = [entries]

            # Pick the best available provider (APXMIDP preferred over N2EXMIDP)
            best = None
            for entry in entries:
                price = entry.get("price")
                volume = entry.get("volume", 0)
                if price is not None and float(price) > 0:
                    if best is None or entry.get("dataProvider") == "APXMIDP":
                        best = entry

            if best:
                price = float(best["price"])
                volume = float(best.get("volume", 0))
                provider = best.get("dataProvider", "")
                period = best.get("settlementPeriod")

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=price,
                    location_id=cell_id,
                    lat=LONDON_LAT,
                    lon=LONDON_LON,
                    metadata={
                        "metric": "market_index_price_gbp_mwh",
                        "volume_mwh": volume,
                        "data_provider": provider,
                        "settlement_period": period,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"GB market index price: {price:.2f}/MWh "
                        f"(vol={volume:.0f}MWh, provider={provider}, period {period})"
                    ),
                    data={
                        "market_index_price": price,
                        "volume_mwh": volume,
                        "data_provider": provider,
                        "settlement_period": period,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

        self.log.info("Electricity prices: processed=%d observations", processed)


async def ingest_electricity_prices(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one electricity price fetch cycle."""
    ingestor = ElectricityPriceIngestor(board, graph, scheduler)
    await ingestor.run()
