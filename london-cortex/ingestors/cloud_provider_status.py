"""Cloud provider status ingestor — AWS, GCP, and Azure health feeds.

Correlates API rate limit spikes and data-source gaps with scheduled cloud
maintenance windows or active incidents. No API keys required.

Sources:
- AWS Service Health Dashboard: https://status.aws.amazon.com/data.json
- GCP Status: https://status.cloud.google.com/incidents.json
- Azure Status: https://azure.status.microsoft/en-us/status/feed/ (Atom RSS)
"""

from __future__ import annotations

import logging
import xml.etree.ElementTree as ET
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.cloud_provider_status")

_AWS_URL = "https://status.aws.amazon.com/data.json"
_GCP_URL = "https://status.cloud.google.com/incidents.json"
_AZURE_FEED_URL = "https://azure.status.microsoft/en-us/status/feed/"

_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10)

# EU/UK-relevant AWS service substrings (best-effort filter)
_AWS_EU_KEYWORDS = ("eu-west", "eu-central", "europe", "London", "Frankfurt", "Ireland")


class CloudProviderStatusIngestor(BaseIngestor):
    source_name = "cloud_provider_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            results["aws"] = await self._fetch_aws(session)
            results["gcp"] = await self._fetch_gcp(session)
            results["azure"] = await self._fetch_azure(session)
        return results

    async def _fetch_aws(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        try:
            async with session.get(_AWS_URL) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                data = await resp.json(content_type=None)
                current = data.get("current", [])
                # Filter for EU/UK-region incidents where possible
                eu_incidents = [
                    i for i in current
                    if any(kw.lower() in str(i).lower() for kw in _AWS_EU_KEYWORDS)
                ]
                return {
                    "ok": True,
                    "label": "AWS",
                    "total_active": len(current),
                    "eu_active": len(eu_incidents),
                    "severity": min(3, len(current)),
                    "incident_names": [
                        i.get("service_name", i.get("summary", ""))[:80]
                        for i in current[:5]
                    ],
                    "eu_incident_names": [
                        i.get("service_name", i.get("summary", ""))[:80]
                        for i in eu_incidents[:3]
                    ],
                }
        except Exception as exc:
            self.log.warning("AWS status fetch failed: %s", exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def _fetch_gcp(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        try:
            async with session.get(_GCP_URL) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                incidents = await resp.json(content_type=None)
                # Active incidents have no end timestamp
                active = [i for i in incidents if not i.get("end")]
                return {
                    "ok": True,
                    "label": "GCP",
                    "total_active": len(active),
                    "severity": min(3, len(active)),
                    "incident_names": [
                        f"{i.get('service_name', '')} — {i.get('external_desc', '')[:60]}"
                        for i in active[:5]
                    ],
                }
        except Exception as exc:
            self.log.warning("GCP status fetch failed: %s", exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def _fetch_azure(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        try:
            async with session.get(_AZURE_FEED_URL) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                text = await resp.text()
                return self._parse_azure_feed(text)
        except Exception as exc:
            self.log.warning("Azure status fetch failed: %s", exc)
            return {"ok": False, "error": str(exc)[:200]}

    def _parse_azure_feed(self, xml_text: str) -> dict[str, Any]:
        try:
            root = ET.fromstring(xml_text)
            # Atom namespace
            ns = {"atom": "http://www.w3.org/2005/Atom"}
            entries = root.findall("atom:entry", ns) or root.findall("entry")
            if not entries:
                # Try RSS format
                entries = root.findall(".//item")
            titles = []
            for entry in entries[:5]:
                title_el = entry.find("{http://www.w3.org/2005/Atom}title") or entry.find("title")
                if title_el is not None and title_el.text:
                    titles.append(title_el.text[:80])
            return {
                "ok": True,
                "label": "Azure",
                "total_active": len(entries),
                "severity": min(3, len(entries)),
                "incident_names": titles,
            }
        except ET.ParseError as exc:
            return {"ok": False, "error": f"XML parse error: {exc}"}

    async def process(self, data: Any) -> None:
        results: dict[str, dict] = data
        healthy_count = 0
        max_severity = 0

        for name, result in results.items():
            if not result.get("ok"):
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=-1.0,
                    location_id=None,
                    lat=None,
                    lon=None,
                    metadata={
                        "metric": "cloud_status",
                        "provider": name,
                        "reachable": False,
                        "error": result.get("error", "unknown"),
                    },
                )
                await self.board.store_observation(obs)
                continue

            severity = result.get("severity", 0)
            label = result.get("label", name)
            total_active = result.get("total_active", 0)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(severity),
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "cloud_status",
                    "provider": name,
                    "label": label,
                    "total_active_incidents": total_active,
                    "eu_active": result.get("eu_active", total_active),
                    "incident_names": result.get("incident_names", []),
                },
            )
            await self.board.store_observation(obs)

            if severity > 0:
                incidents_str = "; ".join(result.get("incident_names", [])[:3]) or "unknown"
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Cloud provider incident: {label} — "
                        f"{total_active} active incident(s): {incidents_str}"
                    ),
                    data={
                        "metric": "cloud_status",
                        "provider": name,
                        "severity": severity,
                        "total_active": total_active,
                        "observation_id": obs.id,
                    },
                ))

            if severity > max_severity:
                max_severity = severity
            if severity == 0:
                healthy_count += 1

        total_providers = len(results)
        overall = healthy_count / total_providers if total_providers else 0.0
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=overall,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "cloud_health_overall",
                "healthy_count": healthy_count,
                "total_providers": total_providers,
                "max_severity": max_severity,
            },
        )
        await self.board.store_observation(obs)

        await self.board.post(AgentMessage(
            from_agent=self.source_name,
            channel="#raw",
            content=(
                f"Cloud provider status: {healthy_count}/{total_providers} healthy "
                f"(AWS/GCP/Azure), max severity={max_severity}"
            ),
            data={
                "metric": "cloud_health_overall",
                "overall_health": round(overall, 3),
                "healthy_count": healthy_count,
                "total_providers": total_providers,
                "max_severity": max_severity,
                "observation_id": obs.id,
            },
        ))

        self.log.info(
            "Cloud provider status: %d/%d healthy, max_severity=%d",
            healthy_count, total_providers, max_severity,
        )


async def ingest_cloud_provider_status(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one cloud provider status check cycle."""
    ingestor = CloudProviderStatusIngestor(board, graph, scheduler)
    await ingestor.run()
