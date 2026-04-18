"""
WebSearchAgent -- web search for hypothesis verification and investigation threads.

Watches for investigation threads assigned to it + directed requests.
Uses the LLM backend for query generation and result synthesis.
Falls back to DuckDuckGo for actual web searches when Gemini search grounding is unavailable.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.web_searcher")

_MAX_RESULTS = 5


class WebSearchAgent(BaseAgent):
    """Web search agent for investigation threads and directed requests."""

    name = "web_searcher"

    async def run(self) -> None:
        # 1. Check investigation threads assigned to me
        try:
            pending = await self.get_my_investigations()
            for inv in pending[:3]:  # max 3 per cycle
                await self._investigate(inv)
        except Exception as exc:
            log.debug("[WebSearcher] Investigation check failed: %s", exc)

        # 2. Check directed requests
        try:
            requests = await self.board.read_directed(self.name, limit=5)
            for req in requests[:3]:
                await self._handle_request(req)
        except Exception as exc:
            log.debug("[WebSearcher] Directed request check failed: %s", exc)

    async def _investigate(self, inv_thread: list[dict]) -> None:
        """Handle an investigation thread."""
        if not inv_thread:
            return
        last_step = inv_thread[-1]
        thread_id = last_step.get("thread_id", "")
        question = last_step.get("next_question", "")
        if not question:
            return

        # Generate search queries via LLM
        queries = await self._generate_search_queries(question, inv_thread)
        if not queries:
            queries = [question[:100]]

        # Execute searches
        all_results: list[dict] = []
        for query in queries[:3]:
            results = await self._search_web(query)
            all_results.extend(results)

        if not all_results:
            await self.continue_investigation(
                thread_id=thread_id,
                action=f"Web searched: {', '.join(queries[:2])}",
                finding="No search results found.",
                next_question=None,
                next_agent=None,
            )
            return

        # Synthesize results with LLM
        synthesis = await self._synthesize_results(question, all_results, inv_thread)

        follow_up = synthesis.get("follow_up_question")
        next_agent = self._suggest_next_agent(synthesis) if follow_up else None

        await self.continue_investigation(
            thread_id=thread_id,
            action=f"Web searched: {', '.join(queries[:2])}",
            finding=synthesis.get("finding", "No significant findings."),
            next_question=follow_up,
            next_agent=next_agent,
        )
        log.info("[WebSearcher] Investigation step for thread %s: %s", thread_id[:8], synthesis.get("finding", "")[:80])

    async def _handle_request(self, req: dict) -> None:
        """Handle a directed request message."""
        data = req.get("data", {})
        question = data.get("question", req.get("content", ""))
        if not question or data.get("request_type") == "agent_response":
            return

        queries = [question[:100]]
        results = await self._search_ddg(queries[0])
        if results:
            synthesis = await self._synthesize_results(question, results, [])
            await self.respond_to(
                original_msg_id=req.get("id", ""),
                response=synthesis.get("finding", "No findings."),
            )

    async def _generate_search_queries(self, question: str, inv_thread: list[dict]) -> list[str]:
        """Use LLM to generate effective search queries."""
        context = "\n".join(
            f"- {s.get('agent_name', '?')}: {s.get('finding', 'N/A')[:100]}"
            for s in inv_thread[-3:]
        )

        prompt = f"""Generate 2-3 web search queries to investigate this question about London:
Question: {question}

Previous findings:
{context}

Respond with a JSON array of search query strings:
["query 1", "query 2", "query 3"]"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger="websearch:queries")
        parsed = parse_json_response(result)
        if isinstance(parsed, list):
            return [str(q) for q in parsed[:3]]
        return []

    async def _search_web(self, query: str) -> list[dict]:
        """Search using Gemini's built-in Google Search grounding, fallback to DDG."""
        full_query = f"{query} London"

        # Try Gemini Google Search grounding first
        try:
            from google import genai
            from google.genai import types as genai_types

            from ..core.llm import _get_backend
            backend = _get_backend()

            # Only GeminiBackend supports Google Search grounding
            if hasattr(backend, '_client'):
                google_search_tool = genai_types.Tool(
                    google_search=genai_types.GoogleSearch(),
                )
                response = await backend._client.aio.models.generate_content(
                    model=backend._flash_model,
                    contents=f"Search the web for: {full_query}\n\nReturn the key facts and findings.",
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
                        for chunk in chunks[:_MAX_RESULTS]:
                            web = getattr(chunk, "web", None)
                            if web:
                                results.append({
                                    "title": getattr(web, "title", "") or "",
                                    "snippet": "",
                                    "url": getattr(web, "uri", "") or "",
                                })
                        # Add search entry point snippets if available
                        support = getattr(grounding, "grounding_supports", None) or []
                        for s in support:
                            segment = getattr(s, "segment", None)
                            if segment:
                                text = getattr(segment, "text", "")
                                if text:
                                    # Attach snippet to first result without one, or add as new
                                    for r in results:
                                        if not r["snippet"]:
                                            r["snippet"] = text[:300]
                                            break

                # If no grounding metadata, use the text response as a single result
                if not results and response.text:
                    results.append({
                        "title": f"Search: {query[:80]}",
                        "snippet": response.text[:500],
                        "url": "",
                    })

                log.info("[WebSearcher] Google Search returned %d results for: %s", len(results), full_query[:60])
                return results

        except ImportError:
            pass  # google-genai not available
        except Exception as exc:
            log.warning("[WebSearcher] Google Search error: %s", exc)

        # Fallback to DuckDuckGo
        return await self._search_ddg(full_query)

    async def _search_ddg(self, query: str) -> list[dict]:
        """Search using DuckDuckGo HTML API as fallback."""
        import aiohttp

        results: list[dict] = []
        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=15),
                headers={"User-Agent": "Cortex/1.0"},
            ) as session:
                url = f"https://html.duckduckgo.com/html/?q={query.replace(' ', '+')}"
                async with session.get(url) as resp:
                    if resp.status == 200:
                        text = await resp.text()
                        # Parse basic results from DDG HTML
                        titles = re.findall(r'class="result__a"[^>]*>(.*?)</a>', text, re.DOTALL)
                        snippets = re.findall(r'class="result__snippet"[^>]*>(.*?)</[at]', text, re.DOTALL)
                        for i, (title, snippet) in enumerate(zip(titles[:_MAX_RESULTS], snippets[:_MAX_RESULTS])):
                            results.append({
                                "title": re.sub(r'<[^>]+>', '', title).strip(),
                                "snippet": re.sub(r'<[^>]+>', '', snippet).strip()[:300],
                                "url": "",
                            })
        except Exception as exc:
            log.warning("[WebSearcher] DuckDuckGo search error: %s", exc)

        return results

    async def _synthesize_results(
        self, question: str, results: list[dict], inv_thread: list[dict],
    ) -> dict:
        """Synthesize search results into a finding."""
        results_text = "\n\n".join(
            f"[{i+1}] {r.get('title', '')}\n{r.get('snippet', '')}"
            for i, r in enumerate(results[:_MAX_RESULTS])
        )

        prev_context = ""
        if inv_thread:
            prev_context = "\nPrevious findings:\n" + "\n".join(
                f"- {s.get('agent_name', '?')}: {s.get('finding', '')[:100]}"
                for s in inv_thread[-3:]
            )

        prompt = f"""Synthesize these web search results to answer the question.

Question: {question}
{prev_context}

Search results:
{results_text}

Respond with JSON:
{{"finding": "concise synthesis of what you found (2-3 sentences)",
  "confidence": 0.0-1.0,
  "follow_up_question": "what should be investigated next, or null if resolved",
  "sources_useful": true/false}}"""

        result = await self.call_llm(prompt=prompt, model=LLM_FLASH_MODEL, trigger="websearch:synthesize")
        parsed = parse_json_response(result)
        if isinstance(parsed, dict):
            return parsed
        return {"finding": "Could not synthesize results.", "confidence": 0.0}

    def _suggest_next_agent(self, synthesis: dict) -> str | None:
        """Suggest which agent should handle the follow-up."""
        follow_up = synthesis.get("follow_up_question", "")
        if not follow_up:
            return None
        fl = follow_up.lower()
        if any(w in fl for w in ("camera", "image", "visual", "cctv")):
            return "vision_interpreter"
        if any(w in fl for w in ("spatial", "nearby", "location", "area")):
            return "spatial_connector"
        if any(w in fl for w in ("data", "correlation", "statistic", "trend")):
            return "statistical_connector"
        if any(w in fl for w in ("narrative", "explain", "story")):
            return "narrative_connector"
        return "curiosity_engine"


# -- Async entry point ----------------------------------------------------------

async def run_web_searcher(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager,
) -> None:
    agent = WebSearchAgent(board, graph, memory)
    await agent.run()
