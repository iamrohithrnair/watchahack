"""InvestigatorAgent — on-demand interactive investigation via natural language.

Architectural improvements over v1:
1. Context-aware planning — pre-fetches system stats so planner knows what data exists
2. Wave-based execution — independent steps run in parallel, dependent steps wait
3. Programmatic lead extraction — follows chain_ids, new keywords, locations without LLM
4. Deep web fetch — reads top article pages, not just DDG snippets
5. Result caching — thread accumulates evidence across follow-up questions
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp

from google.genai import types as genai_types

from .base import BaseAgent
from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL, LLM_PRO_MODEL
from ..core.graph import CortexGraph
from ..core.locations import resolve_location
from ..core.memory import MemoryManager

log = logging.getLogger("cortex.agents.investigator")

# ── Web config ────────────────────────────────────────────────────────────────
_FETCH_TIMEOUT = aiohttp.ClientTimeout(total=10)

# ── System prompts ────────────────────────────────────────────────────────────

PLANNING_SYSTEM = """You are the investigation planner for the London Cortex — an AI monitoring London in real-time with 60+ data sources.

SYSTEM CONTEXT (live stats about what data exists right now):
{system_context}

Given a user question, plan investigation steps. You have TWO categories of tools:

INTERNAL (search the system's own accumulated intelligence):
- search_messages: keyword search across agent messages. Params: keyword (required), channel (optional, e.g. "#discoveries", "#anomalies", "#hypotheses"), location (optional place name), since_hours (default 48)
- search_anomalies: search anomaly descriptions. Params: keyword (optional), source (optional data source name), location (optional), since_hours (default 48)
- search_observations: search raw observations. Params: keyword (optional), source (optional, e.g. "tfl_jamcam", "laqn", "carbon_intensity", "gdelt", "open_meteo", "financial_stocks", "uk_headlines", "social_sentiment"), location (optional), since_hours (default 48)
- get_connections: connections involving a data source. Params: source (required), since_hours (default 48)
- get_chain: follow a connection chain. Params: chain_id (required). Use ONLY when you have a specific chain_id from prior results. Mark as depends_on the step index that provides it.
- location_anomalies: all anomalies near a place. Params: location (required place name), radius (optional, default 2), since_hours (default 48)
- location_messages: all messages about a place. Params: location (required place name), since_hours (default 24)
- recent_discoveries: latest Brain discoveries. Params: limit (default 10)
- check_narratives: search evolving narratives. Params: keyword (required)
- check_predictions: find predictions about a topic. Params: keyword (required)

EXTERNAL (search the live web for current information):
- web_search: search the web via Google Search. Params: queries (required, list of 1-3 search query strings). The top 2-3 articles will be fetched and read in full. Use for current events, context, or anything the system may not track.

STRATEGY:
- ALWAYS include at least one web_search step for broader context.
- Use the SYSTEM CONTEXT above to choose which sources and keywords to search — only search sources that actually have data.
- Use multiple keyword variants for internal searches.
- For location-related questions, try both keyword search AND location-based search.
- Mark steps with "depends_on": [step_index] if they need results from a prior step (e.g. get_chain depends on a step that discovers chain_ids). Steps without depends_on run in parallel.

Respond with ONLY valid JSON (no markdown):
{{"steps": [{{"type": "step_type", "params": {{}}, "reason": "why", "depends_on": []}}], "sources_of_interest": ["source1"], "locations_of_interest": ["place1"]}}

Plan 4-8 steps. Mix internal and external."""

SYNTHESIS_SYSTEM = """You are synthesizing investigation findings for the London Cortex — an AI that monitors all of London in real-time.

You have evidence from BOTH internal sensors/agents AND live web searches (including full article text). Combine them into a comprehensive answer.

When internal data is sparse, lean on web results but note what the system does/doesn't track.
When internal data is rich, use web results to provide broader context.
Always be direct and answer the user's actual question.

Respond with ONLY valid JSON (no markdown):
{{
  "headline": "One-sentence key finding — directly answer the question",
  "confidence": 0.0 to 1.0,
  "summary": "2-3 paragraph narrative. Lead with the answer. Weave together web context and internal sensor data. Be specific and informative.",
  "key_evidence": ["evidence point 1", "evidence point 2", ...],
  "blind_spots": ["what we couldn't verify or didn't check"],
  "follow_up_questions": ["suggested follow-up question 1", "suggested follow-up question 2"]
}}"""


# ── Evidence Cache (shared across follow-ups in a thread) ─────────────────────

class EvidenceCache:
    """Accumulates evidence across multiple queries in a conversation thread."""

    def __init__(self):
        self.results: list[dict] = []  # all step results
        self.seen_ids: set[str] = set()  # dedup item IDs
        self.chain_ids: set[str] = set()  # discovered chain_ids to follow
        self.keywords: set[str] = set()  # new keywords discovered from results
        self.locations: set[str] = set()  # locations mentioned in results

    def add_result(self, result: dict) -> None:
        """Add a step result, extracting leads for programmatic follow-up."""
        self.results.append(result)
        for item in result.get("items", []):
            item_id = item.get("id")
            if item_id:
                self.seen_ids.add(item_id)
            # Extract chain_ids
            chain_id = item.get("chain_id")
            if chain_id:
                self.chain_ids.add(chain_id)
            # Extract location_ids
            loc = item.get("location_id")
            if loc:
                self.locations.add(loc)

    @property
    def total_evidence(self) -> int:
        return sum(r.get("count", 0) for r in self.results)

    def get_unfollowed_chains(self, followed: set[str]) -> set[str]:
        return self.chain_ids - followed

    def merge_from(self, other: "EvidenceCache") -> None:
        """Merge evidence from a prior thread turn."""
        self.results.extend(other.results)
        self.seen_ids |= other.seen_ids
        self.chain_ids |= other.chain_ids
        self.keywords |= other.keywords
        self.locations |= other.locations


class InvestigatorAgent(BaseAgent):
    """On-demand investigation agent. Not poll-based — runs per user query."""

    name = "investigator"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager | None = None):
        super().__init__(board, graph, memory or MemoryManager())
        self._daemon: Any = None  # set externally

    async def investigate(
        self, question: str,
        context: list[dict] | None = None,
        prior_evidence: EvidenceCache | None = None,
    ) -> AsyncGenerator[dict, None]:
        """Run a full investigation. Yields SSE-compatible event dicts."""
        cache = EvidenceCache()
        if prior_evidence:
            cache.merge_from(prior_evidence)

        try:
            # Phase 1: Context-aware planning
            yield {"event": "status", "data": {"phase": "planning", "message": "Scanning system state..."}}
            system_ctx = await self._build_system_context()

            yield {"event": "status", "data": {"phase": "planning", "message": "Planning investigation..."}}
            plan = await self._plan(question, context, system_ctx)
            if not plan or "steps" not in plan:
                yield {"event": "error", "data": {"error": "Could not plan investigation"}}
                return

            yield {"event": "plan", "data": {"steps": plan["steps"]}}

            # Phase 2: Wave-based execution
            yield {"event": "status", "data": {"phase": "investigating", "message": "Searching internal knowledge and the web..."}}
            step_results = await self._execute_in_waves(plan["steps"], cache)
            step_idx = 0
            for step, result in zip(plan["steps"], step_results):
                yield {"event": "step_result", "data": {"index": step_idx, "step": step, "result": result}}
                cache.add_result(result)
                step_idx += 1

            # Phase 3: Programmatic lead extraction (no LLM needed)
            followed_chains: set[str] = set()
            for round_num in range(2):
                auto_steps = self._extract_leads(cache, followed_chains, plan["steps"])
                if not auto_steps:
                    break
                reason = self._lead_reason(auto_steps)
                yield {"event": "followup", "data": {"reason": reason, "steps": auto_steps}}

                fresults = await self._execute_in_waves(auto_steps, cache)
                for step, result in zip(auto_steps, fresults):
                    yield {"event": "step_result", "data": {"index": step_idx, "step": step, "result": result}}
                    cache.add_result(result)
                    step_idx += 1
                plan["steps"].extend(auto_steps)

            # Phase 4: Deep web fetch — read top article pages
            web_articles = await self._deep_fetch_articles(cache)
            if web_articles:
                yield {"event": "status", "data": {"phase": "reading", "message": f"Reading {len(web_articles)} article(s)..."}}
                article_result = {
                    "type": "web_articles",
                    "count": len(web_articles),
                    "items": web_articles,
                }
                cache.add_result(article_result)
                yield {"event": "step_result", "data": {
                    "index": step_idx,
                    "step": {"type": "deep_read", "params": {}, "reason": "Reading full article text from top web results"},
                    "result": article_result,
                }}
                step_idx += 1

            # Phase 5: Claude Code (optional, only when very sparse)
            if cache.total_evidence < 3 and self._daemon and hasattr(self._daemon, "claude_runner"):
                try:
                    runner = self._daemon.claude_runner
                    if len([i for i in runner._instances.values() if i.is_running]) < runner._max_concurrent:
                        findings_summary = self._summarize_results(cache.results)
                        prompt = (
                            f"Investigate this question about London: {question}\n\n"
                            f"Initial findings (sparse):\n{findings_summary}\n\n"
                            f"Search the codebase and data for deeper patterns. "
                            f"Post findings to #discoveries channel."
                        )
                        instance_id = await runner.spawn(prompt)
                        yield {"event": "claude_spawned", "data": {"instance_id": instance_id[:8]}}
                except Exception as e:
                    log.warning("Claude spawn failed: %s", e)

            # Phase 6: Synthesis
            yield {"event": "status", "data": {"phase": "synthesizing", "message": "Synthesizing findings..."}}
            synthesis = await self._synthesize(question, plan["steps"], cache.results)
            yield {"event": "synthesis", "data": synthesis}

            # Attach cache to the yielded data so dashboard can store it
            yield {"event": "_cache", "data": {"cache": cache}}

        except Exception as e:
            log.error("Investigation failed: %s", e, exc_info=True)
            yield {"event": "error", "data": {"error": str(e)}}

    # ── Improvement #1: Context-aware planning ────────────────────────────────

    async def _build_system_context(self) -> str:
        """Pre-fetch system stats so the planner knows what data actually exists."""
        lines = []
        try:
            db = self.board.get_db()

            # Table counts
            for table in ("observations", "anomalies", "connections", "messages"):
                cursor = await db.execute(f"SELECT COUNT(*) FROM {table}")
                row = await cursor.fetchone()
                lines.append(f"- {table}: {row[0]:,} total rows")

            # Top observation sources (last 48h)
            since = (datetime.now(timezone.utc) - timedelta(hours=48)).isoformat()
            cursor = await db.execute(
                """SELECT source, COUNT(*) as cnt FROM observations
                   WHERE timestamp > ? GROUP BY source ORDER BY cnt DESC LIMIT 15""",
                (since,),
            )
            rows = await cursor.fetchall()
            if rows:
                sources_str = ", ".join(f"{r[0]}({r[1]})" for r in rows)
                lines.append(f"- Top observation sources (48h): {sources_str}")

            # Top anomaly sources
            cursor = await db.execute(
                """SELECT source, COUNT(*) as cnt FROM anomalies
                   WHERE timestamp > ? GROUP BY source ORDER BY cnt DESC LIMIT 10""",
                (since,),
            )
            rows = await cursor.fetchall()
            if rows:
                anom_str = ", ".join(f"{r[0]}({r[1]})" for r in rows)
                lines.append(f"- Top anomaly sources (48h): {anom_str}")

            # Active channels with message counts
            cursor = await db.execute(
                """SELECT channel, COUNT(*) as cnt FROM messages
                   WHERE timestamp > ? GROUP BY channel ORDER BY cnt DESC""",
                (since,),
            )
            rows = await cursor.fetchall()
            if rows:
                chan_str = ", ".join(f"{r[0]}({r[1]})" for r in rows)
                lines.append(f"- Active channels (48h): {chan_str}")

            # Recent anomaly keywords (sample descriptions)
            cursor = await db.execute(
                """SELECT description FROM anomalies
                   WHERE timestamp > ? ORDER BY timestamp DESC LIMIT 5""",
                (since,),
            )
            rows = await cursor.fetchall()
            if rows:
                lines.append("- Recent anomaly examples:")
                for r in rows:
                    lines.append(f"    \"{r[0][:100]}\"")

            # Connections with chain_ids
            cursor = await db.execute(
                """SELECT COUNT(DISTINCT chain_id) FROM connections
                   WHERE chain_id IS NOT NULL AND timestamp > ?""",
                (since,),
            )
            row = await cursor.fetchone()
            lines.append(f"- Active connection chains (48h): {row[0]}")

        except Exception as e:
            lines.append(f"- [Context fetch error: {e}]")

        return "\n".join(lines) if lines else "No system context available."

    async def _plan(self, question: str, context: list[dict] | None = None, system_ctx: str = "") -> dict | None:
        context_str = ""
        if context:
            context_str = "\n\nConversation context:\n" + "\n".join(
                f"{c.get('role', 'user')}: {c.get('content', '')}" for c in context[-5:]
            )

        system_prompt = PLANNING_SYSTEM.format(system_context=system_ctx)
        prompt = f"User question: {question}{context_str}"
        raw = await self.call_llm(
            prompt, model=LLM_FLASH_MODEL,
            trigger="investigate_plan", system_instruction=system_prompt,
        )
        return self._parse_json(raw)

    # ── Improvement #2: Wave-based execution ──────────────────────────────────

    async def _execute_in_waves(self, steps: list[dict], cache: EvidenceCache) -> list[dict]:
        """Execute steps in dependency waves. Independent steps run in parallel."""
        results: list[dict] = [None] * len(steps)  # type: ignore[list-item]

        # Build dependency graph: step_idx -> set of step_idxs it depends on
        deps: dict[int, set[int]] = {}
        for i, step in enumerate(steps):
            raw_deps = step.get("depends_on", [])
            if isinstance(raw_deps, int):
                raw_deps = [raw_deps]
            deps[i] = {d for d in raw_deps if isinstance(d, int) and 0 <= d < len(steps)}

        completed: set[int] = set()
        max_waves = 5  # safety limit

        for _ in range(max_waves):
            # Find steps whose dependencies are all completed
            ready = [
                i for i in range(len(steps))
                if i not in completed and deps.get(i, set()).issubset(completed)
            ]
            if not ready:
                break

            # Execute ready steps in parallel
            async def _run(idx: int) -> tuple[int, dict]:
                # Pass prior results so dependent steps can use them
                prior = [results[d] for d in deps.get(idx, set()) if results[d] is not None]
                r = await self._execute_single_step(steps[idx], prior, cache)
                return idx, r

            gathered = await asyncio.gather(*[_run(i) for i in ready], return_exceptions=True)
            for item in gathered:
                if isinstance(item, Exception):
                    # Find which index failed — we can't know, so mark first unfinished ready
                    for i in ready:
                        if results[i] is None:
                            results[i] = {"type": "error", "count": 0, "items": [], "error": str(item)}
                            completed.add(i)
                            break
                else:
                    idx, result = item
                    results[idx] = result
                    completed.add(idx)

        # Fill any unexecuted steps (circular deps or exceeded max_waves)
        for i in range(len(steps)):
            if results[i] is None:
                results[i] = {"type": "skipped", "count": 0, "items": [], "note": "Dependencies not met"}

        return results

    async def _execute_single_step(self, step: dict, prior_results: list[dict], cache: EvidenceCache) -> dict:
        """Execute one investigation step."""
        step_type = step.get("type", "")
        params = step.get("params", {})

        try:
            if step_type == "web_search":
                return await self._do_web_search(params)

            elif step_type == "search_messages":
                keyword = params.get("keyword", "")
                channel = params.get("channel")
                location = self._resolve_loc(params.get("location"))
                since = params.get("since_hours", 48)
                items = await self.board.search_messages(
                    keyword, channel=channel,
                    location_id=location.get("cell_id") if location else None,
                    since_hours=since,
                )
                return {"type": "messages", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "search_anomalies":
                keyword = params.get("keyword")
                source = params.get("source")
                location = self._resolve_loc(params.get("location"))
                since = params.get("since_hours", 48)
                items = await self.board.search_anomalies(
                    keyword=keyword, source=source,
                    location_id=location.get("cell_id") if location else None,
                    since_hours=since,
                )
                return {"type": "anomalies", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "search_observations":
                keyword = params.get("keyword")
                source = params.get("source")
                location = self._resolve_loc(params.get("location"))
                since = params.get("since_hours", 48)
                items = await self.board.search_observations(
                    keyword=keyword, source=source,
                    location_id=location.get("cell_id") if location else None,
                    since_hours=since,
                )
                return {"type": "observations", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "get_connections":
                source = params.get("source", "")
                since = params.get("since_hours", 48)
                items = await self.board.get_connections_involving(source, since_hours=since)
                return {"type": "connections", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "get_chain":
                chain_id = params.get("chain_id", "")
                items = await self.board.get_chain(chain_id)
                return {"type": "connections", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "location_anomalies":
                location = self._resolve_loc(params.get("location"))
                if not location:
                    return {"type": "anomalies", "count": 0, "items": [], "note": "Location not resolved"}
                radius = params.get("radius", 2)
                since = params.get("since_hours", 48)
                cell_id = location["cell_id"]
                cells = [cell_id] + self.graph.get_nearby_cells(cell_id, radius=radius)
                all_items = []
                for c in cells:
                    items = await self.board.search_anomalies(location_id=c, since_hours=since)
                    all_items.extend(items)
                deduped = self._dedup(all_items, cache)
                return {"type": "anomalies", "count": len(deduped), "items": deduped[:10], "location": location}

            elif step_type == "location_messages":
                location = self._resolve_loc(params.get("location"))
                if not location:
                    return {"type": "messages", "count": 0, "items": [], "note": "Location not resolved"}
                since = params.get("since_hours", 24)
                items = await self.board.read_location(location["cell_id"], since_hours=since)
                return {"type": "messages", "count": len(items), "items": self._dedup(items, cache)[:10], "location": location}

            elif step_type == "recent_discoveries":
                limit = params.get("limit", 10)
                items = await self.board.read_channel("#discoveries", limit=limit)
                return {"type": "messages", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "check_narratives":
                keyword = params.get("keyword", "")
                items = await self.board.search_messages(keyword, channel="#hypotheses", since_hours=48)
                return {"type": "narratives", "count": len(items), "items": self._dedup(items, cache)[:10]}

            elif step_type == "check_predictions":
                keyword = params.get("keyword", "")
                db = self.board.get_db()
                cursor = await db.execute(
                    """SELECT * FROM connections WHERE prediction IS NOT NULL
                       AND (description LIKE ? OR prediction LIKE ?)
                       ORDER BY timestamp DESC LIMIT 20""",
                    (f"%{keyword}%", f"%{keyword}%"),
                )
                rows = await cursor.fetchall()
                items = [self.board._row_to_dict(r) for r in rows]
                return {"type": "predictions", "count": len(items), "items": self._dedup(items, cache)[:10]}

            else:
                return {"type": "unknown", "count": 0, "items": [], "note": f"Unknown step type: {step_type}"}

        except Exception as e:
            log.warning("Step %s failed: %s", step_type, e)
            return {"type": "error", "count": 0, "items": [], "error": str(e)}

    @staticmethod
    def _dedup(items: list[dict], cache: EvidenceCache) -> list[dict]:
        """Remove items already seen in this investigation thread."""
        out = []
        for item in items:
            item_id = item.get("id")
            if item_id and item_id in cache.seen_ids:
                continue
            out.append(item)
        return out

    # ── Improvement #3: Programmatic lead extraction ──────────────────────────

    def _extract_leads(self, cache: EvidenceCache, followed_chains: set[str], existing_steps: list[dict]) -> list[dict]:
        """Extract actionable leads from results — no LLM round-trip needed."""
        auto_steps: list[dict] = []
        existing_types = {(s.get("type"), json.dumps(s.get("params", {}), sort_keys=True)) for s in existing_steps}

        # 1. Follow unfollowed chain_ids
        for chain_id in cache.get_unfollowed_chains(followed_chains):
            step = {"type": "get_chain", "params": {"chain_id": chain_id}, "reason": f"Following discovered chain {chain_id[:12]}"}
            key = (step["type"], json.dumps(step["params"], sort_keys=True))
            if key not in existing_types:
                auto_steps.append(step)
                followed_chains.add(chain_id)
            if len(auto_steps) >= 3:
                break

        # 2. Search locations discovered in results that weren't already queried
        queried_locs = set()
        for s in existing_steps:
            loc = s.get("params", {}).get("location")
            if loc:
                queried_locs.add(loc)
            lid = s.get("params", {}).get("location_id")
            if lid:
                queried_locs.add(lid)

        for loc_id in cache.locations:
            if loc_id in queried_locs or len(auto_steps) >= 3:
                break
            # Only follow up on locations that appeared multiple times (signal vs noise)
            loc_count = sum(
                1 for r in cache.results for item in r.get("items", [])
                if item.get("location_id") == loc_id
            )
            if loc_count >= 2:
                step = {"type": "location_anomalies", "params": {"location": loc_id, "since_hours": 48},
                        "reason": f"Multiple hits at {loc_id}, checking for anomaly cluster"}
                # For cell_id based locations, we need a different approach
                # Use search_anomalies directly with location_id
                step = {"type": "search_anomalies", "params": {"location_id": loc_id, "since_hours": 48},
                        "reason": f"Multiple hits at {loc_id}, checking for anomaly cluster"}
                key = (step["type"], json.dumps(step["params"], sort_keys=True))
                if key not in existing_types:
                    auto_steps.append(step)

        return auto_steps[:3]

    @staticmethod
    def _lead_reason(steps: list[dict]) -> str:
        parts = []
        for s in steps:
            if s["type"] == "get_chain":
                parts.append("following a connection chain")
            elif "location" in s.get("params", {}) or "location_id" in s.get("params", {}):
                parts.append("checking a location hotspot")
            else:
                parts.append(s.get("reason", "exploring a lead"))
        return "Automatically following leads: " + ", ".join(parts[:3])

    # ── Web Search ────────────────────────────────────────────────────────────

    async def _do_web_search(self, params: dict) -> dict:
        """Execute web searches and return results."""
        queries = params.get("queries", [])
        if isinstance(queries, str):
            queries = [queries]
        if not queries:
            queries = [params.get("query", "")]

        all_results: list[dict] = []
        for query in queries[:3]:
            if not query:
                continue
            results = await self._search_web(query)
            all_results.extend(results)

        return {
            "type": "web",
            "count": len(all_results),
            "items": all_results[:10],
            "queries": queries[:3],
        }

    async def _search_web(self, query: str) -> list[dict]:
        """Search using Gemini's built-in Google Search grounding."""
        try:
            google_search_tool = genai_types.Tool(
                google_search=genai_types.GoogleSearch(),
            )
            response = await self._client.aio.models.generate_content(
                model=LLM_FLASH_MODEL,
                contents=f"Search the web for: {query}\n\nReturn the key facts and findings.",
                config=genai_types.GenerateContentConfig(
                    tools=[google_search_tool],
                ),
            )

            results = []
            # Extract grounding metadata (URLs, titles, snippets)
            if hasattr(response, "candidates") and response.candidates:
                candidate = response.candidates[0]
                grounding = getattr(candidate, "grounding_metadata", None)
                if grounding:
                    chunks = getattr(grounding, "grounding_chunks", None) or []
                    for chunk in chunks[:8]:
                        web = getattr(chunk, "web", None)
                        if web:
                            results.append({
                                "title": getattr(web, "title", "") or "",
                                "snippet": "",
                                "url": getattr(web, "uri", "") or "",
                            })
                    support = getattr(grounding, "grounding_supports", None) or []
                    for s in support:
                        segment = getattr(s, "segment", None)
                        if segment:
                            text = getattr(segment, "text", "")
                            if text:
                                for r in results:
                                    if not r["snippet"]:
                                        r["snippet"] = text[:400]
                                        break

            # If no grounding metadata, use the text response as a single result
            if not results and response.text:
                results.append({
                    "title": f"Search: {query[:80]}",
                    "snippet": response.text[:500],
                    "url": "",
                })

            log.info("[Investigator] Google Search returned %d results for: %s", len(results), query[:60])
            return results

        except Exception as exc:
            log.warning("[Investigator] Google Search error: %s", exc)
            return []

    # ── Improvement #4: Deep web fetch ────────────────────────────────────────

    async def _deep_fetch_articles(self, cache: EvidenceCache) -> list[dict]:
        """Fetch full text of top 2-3 web result URLs for richer context."""
        # Collect URLs from web results
        urls: list[tuple[str, str]] = []  # (url, title)
        for r in cache.results:
            if r.get("type") != "web":
                continue
            for item in r.get("items", []):
                url = item.get("url", "")
                title = item.get("title", "")
                if url and url.startswith("http") and len(urls) < 3:
                    urls.append((url, title))

        if not urls:
            return []

        async def _fetch_one(url: str, title: str) -> dict | None:
            try:
                headers = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36"}
                async with aiohttp.ClientSession(timeout=_FETCH_TIMEOUT) as session:
                    async with session.get(url, headers=headers, allow_redirects=True) as resp:
                        if resp.status != 200:
                            return None
                        ct = resp.content_type or ""
                        if "html" not in ct and "text" not in ct:
                            return None
                        html = await resp.text(errors="replace")
                        text = self._extract_article_text(html)
                        if len(text) < 100:
                            return None
                        return {
                            "title": title,
                            "url": url,
                            "text": text[:3000],  # cap for LLM context
                            "length": len(text),
                        }
            except Exception as e:
                log.debug("Article fetch failed for %s: %s", url, e)
                return None

        fetched = await asyncio.gather(*[_fetch_one(u, t) for u, t in urls], return_exceptions=True)
        articles = [a for a in fetched if isinstance(a, dict) and a is not None]
        return articles

    @staticmethod
    def _extract_article_text(html: str) -> str:
        """Extract readable text from HTML. Simple approach: strip tags, collapse whitespace."""
        # Remove script and style blocks
        text = re.sub(r'<script[^>]*>.*?</script>', ' ', html, flags=re.DOTALL | re.IGNORECASE)
        text = re.sub(r'<style[^>]*>.*?</style>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
        # Remove nav, header, footer, aside
        for tag in ('nav', 'header', 'footer', 'aside'):
            text = re.sub(rf'<{tag}[^>]*>.*?</{tag}>', ' ', text, flags=re.DOTALL | re.IGNORECASE)
        # Strip remaining tags
        text = re.sub(r'<[^>]+>', ' ', text)
        # Decode common entities
        text = text.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>')
        text = text.replace('&quot;', '"').replace('&#39;', "'").replace('&nbsp;', ' ')
        # Collapse whitespace
        text = re.sub(r'\s+', ' ', text).strip()
        return text

    # ── Synthesis ─────────────────────────────────────────────────────────────

    async def _synthesize(
        self, question: str, steps: list[dict], results: list[dict],
    ) -> dict:
        summary = self._summarize_results(results)
        step_descriptions = "\n".join(
            f"- {s.get('type')}: {s.get('reason', 'N/A')}" for s in steps
        )
        prompt = (
            f"Question: {question}\n\n"
            f"Investigation steps:\n{step_descriptions}\n\n"
            f"Evidence collected:\n{summary}"
        )
        raw = await self.call_llm(
            prompt, model=LLM_PRO_MODEL,
            trigger="investigate_synthesis", system_instruction=SYNTHESIS_SYSTEM,
        )
        self.record_pro_call()
        result = self._parse_json(raw)
        if result:
            return result
        return {
            "headline": "Investigation complete",
            "confidence": 0.5,
            "summary": raw[:2000] if raw else "No synthesis could be generated.",
            "key_evidence": [],
            "blind_spots": ["Synthesis JSON parsing failed"],
            "follow_up_questions": [],
        }

    def _resolve_loc(self, location_name: str | None) -> dict | None:
        if not location_name:
            return None
        return resolve_location(location_name, self.graph)

    def _summarize_results(self, results: list[dict]) -> str:
        lines = []
        for i, r in enumerate(results):
            rtype = r.get("type", "unknown")
            count = r.get("count", 0)
            lines.append(f"\nResult {i+1} ({rtype}, {count} items):")

            for item in r.get("items", [])[:5]:
                if rtype == "web":
                    lines.append(f"  - [WEB] {item.get('title', '?')}: {item.get('snippet', '')[:200]}")
                elif rtype == "web_articles":
                    lines.append(f"  - [ARTICLE] {item.get('title', '?')} ({item.get('length', 0)} chars): {item.get('text', '')[:400]}")
                elif rtype == "messages":
                    lines.append(f"  - [{item.get('from_agent', '?')}] {item.get('content', '')[:150]}")
                elif rtype == "anomalies":
                    z = item.get('z_score', 0)
                    z_str = f"{z:.1f}" if isinstance(z, (int, float)) else str(z)
                    lines.append(f"  - [{item.get('source', '?')}] sev={item.get('severity', '?')} z={z_str}: {item.get('description', '')[:120]}")
                elif rtype == "connections":
                    lines.append(f"  - {item.get('source_a', '?')} <-> {item.get('source_b', '?')}: {item.get('description', '')[:120]} (conf={item.get('confidence', '?')})")
                elif rtype == "predictions":
                    outcome = item.get("prediction_outcome")
                    status = "CORRECT" if outcome == 1 else "WRONG" if outcome == 0 else "PENDING"
                    lines.append(f"  - [{status}] {item.get('prediction', '')[:120]}")
                elif rtype == "observations":
                    lines.append(f"  - [{item.get('source', '?')}] {str(item.get('value', ''))[:100]}")
                elif rtype == "narratives":
                    lines.append(f"  - [{item.get('from_agent', '?')}] {item.get('content', '')[:150]}")
                else:
                    lines.append(f"  - {str(item)[:150]}")

            if count > 5:
                lines.append(f"  ... and {count - 5} more")
        return "\n".join(lines) if lines else "No results found."

    @staticmethod
    def _parse_json(raw: str) -> dict | None:
        if not raw:
            return None
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            lines = lines[1:]
            if lines and lines[-1].strip() == "```":
                lines = lines[:-1]
            text = "\n".join(lines)
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            start = text.find("{")
            end = text.rfind("}") + 1
            if start >= 0 and end > start:
                try:
                    return json.loads(text[start:end])
                except json.JSONDecodeError:
                    pass
            log.warning("Failed to parse JSON from Gemini response: %s", text[:200])
            return None
