"""UK Court Judgments ingestor — published judgments from the National Archives.

Uses the Find Case Law Atom feed (caselaw.nationalarchives.gov.uk) to ingest
recently published court judgments. No API key required.

Courts covered include EWHC (Admin, Planning, Ch, KB, Fam, Comm), EWCA (Civ, Crim),
UKSC, UKUT, and others. Particularly valuable for detecting legal decisions that
affect city dynamics (e.g. LTN rulings, planning decisions, judicial reviews).
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.court_judgments")

_ATOM_URL = "https://caselaw.nationalarchives.gov.uk/atom.xml"
_ATOM_NS = "http://www.w3.org/2005/Atom"
_UK_NS = "https://caselaw.nationalarchives.gov.uk"

# Royal Courts of Justice location (Strand, London)
_RCJ_LAT = 51.5138
_RCJ_LON = -0.1134

# Courts of particular interest for London city dynamics
_LONDON_RELEVANT_COURTS = {
    "ewhc/admin",   # Administrative Court — judicial reviews (LTNs, planning)
    "ewhc/ch",      # Chancery — property, companies, trusts
    "ewhc/kb",      # King's Bench — civil, personal injury
    "ewhc/comm",    # Commercial Court — City of London business disputes
    "ewhc/fam",     # Family Division
    "ewhc/tcc",     # Technology & Construction Court
    "ewca/civ",     # Court of Appeal (Civil)
    "ewca/crim",    # Court of Appeal (Criminal)
    "uksc",         # Supreme Court
    "eat",          # Employment Appeal Tribunal
}

# Keywords that may indicate London-relevant judgments
_LONDON_KEYWORDS = {
    "london", "tfl", "transport for london", "metropolitan police",
    "greater london", "city of london", "westminster", "camden", "islington",
    "hackney", "tower hamlets", "southwark", "lambeth", "lewisham",
    "greenwich", "newham", "barking", "havering", "redbridge", "waltham",
    "haringey", "enfield", "barnet", "harrow", "brent", "ealing",
    "hounslow", "richmond", "kingston", "merton", "sutton", "croydon",
    "bromley", "bexley", "hillingdon", "hammersmith", "kensington", "chelsea",
    "wandsworth", "ltn", "ulez", "congestion charge", "thames",
    "planning permission", "compulsory purchase",
}


def _extract_court_from_uri(uri: str) -> str:
    """Extract court identifier from a case URI like 'ewhc/admin/2026/123'."""
    parts = uri.strip("/").split("/")
    if len(parts) >= 2:
        court = parts[0]
        if len(parts) >= 3 and not parts[1].isdigit():
            court = f"{parts[0]}/{parts[1]}"
        return court
    return uri


class CourtJudgmentsIngestor(BaseIngestor):
    source_name = "court_judgments"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        text = await self.fetch(
            _ATOM_URL,
            params={"per_page": "20", "order": "-date"},
        )
        if text is None:
            return None
        # The base fetch returns text for non-JSON responses
        if isinstance(text, str):
            return text
        # If somehow parsed as JSON (shouldn't happen for XML), return as-is
        return text

    async def process(self, data: Any) -> None:
        if not isinstance(data, str):
            self.log.warning("Expected XML string, got %s", type(data))
            return

        try:
            root = ET.fromstring(data)
        except ET.ParseError as e:
            self.log.error("Failed to parse Atom XML: %s", e)
            return

        cell_id = self.graph.latlon_to_cell(_RCJ_LAT, _RCJ_LON)
        entries = root.findall(f"{{{_ATOM_NS}}}entry")
        processed = 0

        for entry in entries:
            try:
                title = _text(entry, f"{{{_ATOM_NS}}}title") or "Untitled"
                published = _text(entry, f"{{{_ATOM_NS}}}published") or ""
                updated = _text(entry, f"{{{_ATOM_NS}}}updated") or ""
                court = _text(entry, f"{{{_ATOM_NS}}}author/{{{_ATOM_NS}}}name") or ""
                case_id = _text(entry, f"{{{_UK_NS}}}identifier") or ""
                fclid = _text(entry, f"{{{_UK_NS}}}fclid") or ""

                # Get the URI from the link
                uri = ""
                for link in entry.findall(f"{{{_ATOM_NS}}}link"):
                    if link.get("type") == "application/xml":
                        href = link.get("href", "")
                        # Extract path from full URL
                        if "caselaw.nationalarchives.gov.uk/" in href:
                            uri = href.split("caselaw.nationalarchives.gov.uk/")[1]
                            uri = uri.replace("/data.xml", "")
                        break

                court_code = _extract_court_from_uri(uri) if uri else ""

                # Check London relevance
                title_lower = title.lower()
                is_london_relevant = any(kw in title_lower for kw in _LONDON_KEYWORDS)
                is_key_court = court_code in _LONDON_RELEVANT_COURTS

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.TEXT,
                    value=f"Judgment: {title} [{case_id}]",
                    location_id=cell_id,
                    lat=_RCJ_LAT,
                    lon=_RCJ_LON,
                    metadata={
                        "case_id": case_id,
                        "fclid": fclid,
                        "court": court,
                        "court_code": court_code,
                        "published": published,
                        "updated": updated,
                        "uri": uri,
                        "london_relevant": is_london_relevant,
                        "key_court": is_key_court,
                    },
                )
                await self.board.store_observation(obs)

                relevance_tag = ""
                if is_london_relevant:
                    relevance_tag = " [LONDON-RELEVANT]"
                elif is_key_court:
                    relevance_tag = " [KEY-COURT]"

                msg = AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Court judgment: {title} — {court} "
                        f"({case_id}){relevance_tag}"
                    ),
                    data={
                        "source": self.source_name,
                        "obs_type": "text",
                        "case_id": case_id,
                        "title": title,
                        "court": court,
                        "court_code": court_code,
                        "london_relevant": is_london_relevant,
                        "observation_id": obs.id,
                    },
                    location_id=cell_id,
                )
                await self.board.post(msg)
                processed += 1

            except Exception:
                self.log.exception("Error processing judgment entry")

        self.log.info(
            "Court judgments: processed=%d from %d entries", processed, len(entries)
        )


def _text(element: ET.Element, path: str) -> str | None:
    """Safely extract text from an XML element path."""
    el = element.find(path)
    return el.text.strip() if el is not None and el.text else None


async def ingest_court_judgments(
    board: MessageBoard, graph: CortexGraph, scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one court judgments fetch cycle."""
    ingestor = CourtJudgmentsIngestor(board, graph, scheduler)
    await ingestor.run()
