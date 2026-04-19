"""GOV.UK press releases + Met Police news ingestor.

Fetches official UK Government press releases via the GOV.UK Content API
and Metropolitan Police news from news.met.police.uk (HTML scraping of
JSON-LD structured data). Neither source requires API keys.

This complements the existing gov_notices ingestor (Gazette + Bills) by
adding press releases and official police statements — addressing the
verification gap for predictions about government actions.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.gov_press")

# GOV.UK Content Search API — press releases and news
GOVUK_API_URL = "https://www.gov.uk/api/search.json"
GOVUK_PARAMS = {
    "filter_content_purpose_supergroup": "news_and_communications",
    "count": "20",
    "order": "-public_timestamp",
    "fields": "title,description,link,public_timestamp,organisations,format,document_type",
}

# Met Police newsroom (Mynewsdesk platform) — no API, parse JSON-LD from HTML
MET_POLICE_URL = "https://news.met.police.uk/latest_news"

# London-relevant GOV.UK organisations (filter for London focus)
LONDON_RELEVANT_ORGS = {
    "metropolitan-police-service",
    "mayor-of-london",
    "transport-for-london",
    "city-of-london-police",
    "home-office",
    "ministry-of-housing-communities-and-local-government",
    "department-for-transport",
    "environment-agency",
    "greater-london-authority",
    "nhs-england",
    "uk-health-security-agency",
}

# Keywords that make a press release London-relevant even without org match
LONDON_KEYWORDS = re.compile(
    r"\blondon\b|\bmetropolitan\b|\bthames\b|\bcity of london\b|\btfl\b",
    re.IGNORECASE,
)


class GovPressIngestor(BaseIngestor):
    source_name = "gov_press"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        govuk = await self.fetch(GOVUK_API_URL, params=GOVUK_PARAMS)
        met_html = await self._fetch_met_police()
        if govuk is None and met_html is None:
            return None
        return {"govuk": govuk, "met_police": met_html}

    async def _fetch_met_police(self) -> list[dict] | None:
        """Fetch Met Police news and extract JSON-LD structured data from HTML."""
        raw = await self.fetch(MET_POLICE_URL)
        if raw is None or not isinstance(raw, str):
            return None
        # Extract JSON-LD blocks from HTML
        entries = []
        for match in re.finditer(
            r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            raw,
            re.DOTALL,
        ):
            try:
                obj = json.loads(match.group(1))
                if isinstance(obj, list):
                    entries.extend(obj)
                elif isinstance(obj, dict):
                    entries.append(obj)
            except (json.JSONDecodeError, ValueError):
                continue
        return entries if entries else None

    async def process(self, data: Any) -> None:
        govuk_count = await self._process_govuk(data.get("govuk"))
        met_count = await self._process_met_police(data.get("met_police"))
        self.log.info(
            "Gov press: govuk=%d met_police=%d", govuk_count, met_count,
        )

    async def _process_govuk(self, data: Any) -> int:
        if not data or not isinstance(data, dict):
            return 0

        results = data.get("results", [])
        if not isinstance(results, list):
            return 0

        count = 0
        for item in results:
            try:
                title = item.get("title", "")
                description = item.get("description", "")
                link = item.get("link", "")
                timestamp = item.get("public_timestamp", "")
                doc_type = item.get("document_type", item.get("format", ""))

                # Check London relevance
                orgs = item.get("organisations", [])
                org_slugs = set()
                if isinstance(orgs, list):
                    for org in orgs:
                        if isinstance(org, dict):
                            org_slugs.add(org.get("slug", ""))
                        elif isinstance(org, str):
                            org_slugs.add(org)

                is_london = bool(org_slugs & LONDON_RELEVANT_ORGS)
                text = f"{title} {description}"
                if not is_london:
                    is_london = bool(LONDON_KEYWORDS.search(text))

                # Still ingest non-London items but tag them
                relevance = "london" if is_london else "national"

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"{title}: {description}"[:500],
                    metadata={
                        "sub_source": "govuk_press",
                        "doc_type": doc_type,
                        "link": f"https://www.gov.uk{link}" if link.startswith("/") else link,
                        "timestamp": timestamp,
                        "orgs": list(org_slugs)[:5],
                        "relevance": relevance,
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"GOV.UK [{doc_type}] {title[:200]}",
                    data={
                        "sub_source": "govuk_press",
                        "title": title[:300],
                        "description": description[:300],
                        "doc_type": doc_type,
                        "relevance": relevance,
                        "observation_id": obs.id,
                    },
                    priority=2 if is_london else 1,
                )
                await self.board.post(msg)
                count += 1
            except Exception:
                self.log.debug("Skipping malformed GOV.UK entry", exc_info=True)
        return count

    async def _process_met_police(self, entries: list[dict] | None) -> int:
        if not entries:
            return 0

        count = 0
        for entry in entries:
            try:
                # JSON-LD NewsArticle schema
                entry_type = entry.get("@type", "")
                if isinstance(entry_type, list):
                    entry_type = entry_type[0] if entry_type else ""
                if entry_type not in ("NewsArticle", "Article", "WebPage", ""):
                    continue

                headline = entry.get("headline", entry.get("name", ""))
                if not headline:
                    continue

                description = entry.get("description", entry.get("articleBody", ""))
                url = entry.get("url", entry.get("mainEntityOfPage", ""))
                if isinstance(url, dict):
                    url = url.get("@id", url.get("url", ""))
                date_published = entry.get("datePublished", entry.get("dateCreated", ""))

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"{headline}: {description}"[:500],
                    metadata={
                        "sub_source": "met_police",
                        "url": str(url)[:500],
                        "date_published": str(date_published),
                    },
                )
                await self.board.store_observation(obs)

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=f"Met Police News: {headline[:200]}",
                    data={
                        "sub_source": "met_police",
                        "headline": headline[:300],
                        "description": str(description)[:300],
                        "url": str(url)[:500],
                        "observation_id": obs.id,
                    },
                    priority=2,
                )
                await self.board.post(msg)
                count += 1
            except Exception:
                self.log.debug("Skipping malformed Met Police entry", exc_info=True)
        return count


async def ingest_gov_press(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one GOV.UK press + Met Police cycle."""
    ingestor = GovPressIngestor(board, graph, scheduler)
    await ingestor.run()
