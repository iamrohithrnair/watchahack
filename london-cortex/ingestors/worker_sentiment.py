"""Transport worker sentiment ingestor — Reddit forums frequented by workers.

Monitors worker-oriented subreddits (r/UKRail, r/TrainDrivers, r/tfl) for
grassroots labor discontent signals: pay disputes, roster grievances, morale
drops, management complaints, and early ballot chatter. Provides a leading
indicator for industrial action — moving detection earlier than official
union announcements (covered by union_feeds.py / union_industrial_action.py).

Differs from social_sentiment.py which monitors *passenger* experience.
This ingestor focuses on *worker* experience and labor relations.

No API keys required — uses Reddit's public JSON API.

Brain suggestion origin: Integrate anonymized sentiment analysis from public
online forums and subreddits frequented by London transport workers to create
a leading indicator for labor discontent.
"""

from __future__ import annotations

import logging
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.worker_sentiment")

# Worker-frequented subreddits (distinct from social_sentiment's passenger subs)
SUBREDDITS = ["UKRail", "TrainDrivers", "tfl"]

REDDIT_SEARCH_URL = "https://www.reddit.com/r/{subreddit}/new.json"

# Search queries targeting worker concerns (not passenger disruption)
_WORKER_QUERIES = [
    "pay OR wage OR salary OR pension",
    "ballot OR strike OR mandate OR picket",
    "management OR roster OR shift OR overtime",
    "morale OR burnout OR staffing OR shortage",
    "safety OR conditions OR training OR workload",
]

# Reddit User-Agent (required by their API policy)
_HEADERS = {"User-Agent": "LondonCortex/1.0 (transport research)"}

# Labor discontent keywords — scored by severity
_HIGH_DISCONTENT = {
    "strike", "walkout", "ballot", "mandate", "picket", "stoppage",
    "work to rule", "overtime ban", "industrial action",
}
_MEDIUM_DISCONTENT = {
    "dispute", "rejected", "deadlock", "breakdown", "fed up", "enough",
    "furious", "unacceptable", "disgusted", "morale", "burnout", "quit",
    "leaving", "resignation", "mass exodus", "understaffed", "unsafe",
}
_LOW_DISCONTENT = {
    "pay", "wage", "salary", "pension", "roster", "shift", "overtime",
    "management", "conditions", "workload", "training", "staffing",
    "shortage", "vacancy", "recruitment", "cuts", "redundancy",
}

# Transport employer keywords for London relevance
_LONDON_EMPLOYERS = {
    "tfl", "transport for london", "london underground", "tube",
    "overground", "elizabeth line", "dlr", "crossrail",
    "arriva", "abellio", "go-ahead", "stagecoach", "metroline",
    "london bus", "southern", "southeastern", "thameslink",
    "great northern", "c2c", "greater anglia", "chiltern",
    "network rail", "govia", "mtr", "keolis",
}

# Central London coords for non-geo-located observations
_LONDON_CENTRE = (51.5074, -0.1278)


def _discontent_score(text: str) -> float:
    """Score 0-1 measuring labor discontent intensity."""
    text_lower = text.lower()
    score = 0.0
    high = sum(1 for kw in _HIGH_DISCONTENT if kw in text_lower)
    med = sum(1 for kw in _MEDIUM_DISCONTENT if kw in text_lower)
    low = sum(1 for kw in _LOW_DISCONTENT if kw in text_lower)

    score += high * 0.25
    score += med * 0.15
    score += low * 0.05

    # Boost for London-specific mentions
    if any(kw in text_lower for kw in _LONDON_EMPLOYERS):
        score += 0.1

    return min(score, 1.0)


def _classify_discontent(text: str) -> str:
    """Classify the type of worker discontent."""
    text_lower = text.lower()
    if any(kw in text_lower for kw in ("strike", "walkout", "picket", "stoppage")):
        return "action_imminent"
    if any(kw in text_lower for kw in ("ballot", "mandate", "vote")):
        return "ballot_chatter"
    if any(kw in text_lower for kw in ("dispute", "rejected", "deadlock")):
        return "dispute_escalation"
    if any(kw in text_lower for kw in ("morale", "burnout", "quit", "leaving", "exodus")):
        return "morale_decline"
    if any(kw in text_lower for kw in ("pay", "wage", "salary", "pension")):
        return "pay_grievance"
    if any(kw in text_lower for kw in ("safety", "unsafe", "conditions", "workload")):
        return "safety_concern"
    if any(kw in text_lower for kw in ("roster", "shift", "overtime", "staffing")):
        return "staffing_issue"
    return "general_discontent"


def _is_worker_relevant(text: str) -> bool:
    """Check if a post is about worker concerns (not passenger complaints)."""
    text_lower = text.lower()
    # Must mention at least one discontent keyword
    all_keywords = _HIGH_DISCONTENT | _MEDIUM_DISCONTENT | _LOW_DISCONTENT
    return any(kw in text_lower for kw in all_keywords)


def _is_london_relevant(text: str) -> bool:
    """Check if post is relevant to London transport."""
    text_lower = text.lower()
    # r/tfl is always London-relevant
    # For other subs, check employer keywords or generic London mention
    return any(kw in text_lower for kw in _LONDON_EMPLOYERS) or "london" in text_lower


class WorkerSentimentIngestor(BaseIngestor):
    source_name = "worker_sentiment"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        import asyncio as _asyncio

        results = []
        fetch_failures = 0

        for i, sub in enumerate(SUBREDDITS):
            if i > 0:
                await _asyncio.sleep(3)  # conservative rate limit

            url = REDDIT_SEARCH_URL.format(subreddit=sub)
            params = {"limit": "50", "raw_json": "1"}

            try:
                data = await self.fetch(url, params=params, headers=_HEADERS)
            except Exception as exc:
                if "429" in str(exc):
                    self.log.warning(
                        "Reddit 429 rate limit on r/%s — backing off", sub
                    )
                    await _asyncio.sleep(15)
                    fetch_failures += 1
                    continue
                raise

            if data and isinstance(data, dict):
                children = data.get("data", {}).get("children", [])
                for child in children:
                    post = child.get("data", {})
                    results.append({
                        "title": post.get("title", ""),
                        "selftext": post.get("selftext", ""),
                        "subreddit": post.get("subreddit", sub),
                        "score": post.get("score", 0),
                        "created_utc": post.get("created_utc", 0),
                        "num_comments": post.get("num_comments", 0),
                        "permalink": post.get("permalink", ""),
                        "upvote_ratio": post.get("upvote_ratio", 0.5),
                    })
            else:
                fetch_failures += 1

        if fetch_failures == len(SUBREDDITS):
            return None
        return results

    async def process(self, data: Any) -> None:
        if not data:
            self.log.info("No worker sentiment posts found")
            return

        processed = 0
        skipped = 0

        for post in data:
            full_text = f"{post['title']} {post['selftext']}"

            if not _is_worker_relevant(full_text):
                skipped += 1
                continue

            # For r/tfl all posts are London-relevant; for others check
            is_london = (
                post["subreddit"].lower() == "tfl"
                or _is_london_relevant(full_text)
            )

            score = _discontent_score(full_text)
            discontent_type = _classify_discontent(full_text)

            # Only emit observations above a minimum discontent threshold
            if score < 0.05:
                skipped += 1
                continue

            lat, lon = _LONDON_CENTRE if is_london else (None, None)
            cell_id = self.graph.latlon_to_cell(lat, lon) if lat and lon else None

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.TEXT,
                value=score,
                location_id=cell_id,
                lat=lat,
                lon=lon,
                metadata={
                    "title": post["title"][:200],
                    "subreddit": post["subreddit"],
                    "reddit_score": post["score"],
                    "num_comments": post["num_comments"],
                    "upvote_ratio": post["upvote_ratio"],
                    "discontent_score": score,
                    "discontent_type": discontent_type,
                    "is_london_relevant": is_london,
                },
            )
            await self.board.store_observation(obs)

            msg = AgentMessage(
                from_agent=self.source_name,
                channel="#raw",
                content=(
                    f"Worker sentiment [r/{post['subreddit']}] "
                    f"{discontent_type} (discontent={score:.2f}): "
                    f"{post['title'][:150]}"
                ),
                data={
                    "source": self.source_name,
                    "obs_type": "text",
                    "title": post["title"][:200],
                    "subreddit": post["subreddit"],
                    "discontent_score": score,
                    "discontent_type": discontent_type,
                    "reddit_score": post["score"],
                    "num_comments": post["num_comments"],
                    "is_london_relevant": is_london,
                    "observation_id": obs.id,
                },
                location_id=cell_id,
            )
            await self.board.post(msg)
            processed += 1

        self.log.info(
            "Worker sentiment: processed=%d skipped=%d from %d subreddits",
            processed, skipped, len(SUBREDDITS),
        )


async def ingest_worker_sentiment(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one worker sentiment fetch cycle."""
    ingestor = WorkerSentimentIngestor(board, graph, scheduler)
    await ingestor.run()
