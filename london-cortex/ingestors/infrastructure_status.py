"""Infrastructure status ingestor — monitors UK & cloud infrastructure outages.

Addresses the Brain's 'System Intentionality' blind spot: differentiating between
routine data outages/maintenance and genuine infrastructure failures by tracking
public status pages of key providers.

Sources (all free, no API keys required):
- UKPN (UK Power Networks) live faults via OpenDataSoft
- Cloudflare status (Statuspage.io)
- GitHub status (Statuspage.io)
- Fastly status (Statuspage.io)
- Azure DevOps status

Statuspage.io provides a standard API at /api/v2/summary.json returning:
  {"status": {"indicator": "none|minor|major|critical"}, "components": [...], "incidents": [...]}

UKPN exposes live power faults (planned + unplanned) via:
  https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets/ukpn-live-faults/records
"""

from __future__ import annotations

import logging
from typing import Any

import aiohttp

from ..core.board import MessageBoard
from ..core.graph import CortexGraph
from ..core.models import AgentMessage, Observation, ObservationType
from ..core.scheduler import AsyncScheduler
from .base import BaseIngestor

log = logging.getLogger("cortex.ingestors.infrastructure_status")

# ── Statuspage.io-based providers ────────────────────────────────────────────
_STATUSPAGE_PROVIDERS: list[dict[str, str]] = [
    {
        "name": "cloudflare",
        "label": "Cloudflare",
        "url": "https://www.cloudflarestatus.com/api/v2/summary.json",
    },
    {
        "name": "github",
        "label": "GitHub",
        "url": "https://www.githubstatus.com/api/v2/summary.json",
    },
    {
        "name": "fastly",
        "label": "Fastly CDN",
        "url": "https://www.fastlystatus.com/api/v2/summary.json",
    },
]

# ── UKPN live faults (London coverage) ───────────────────────────────────────
_UKPN_URL = (
    "https://ukpowernetworks.opendatasoft.com/api/explore/v2.1/catalog/datasets"
    "/ukpn-live-faults/records?limit=50&order_by=incidentreference%20desc"
)

# ── Azure DevOps status ──────────────────────────────────────────────────────
_AZURE_DEVOPS_URL = (
    "https://status.dev.azure.com/_apis/status/health?api-version=7.1-preview.1"
)

_TIMEOUT = aiohttp.ClientTimeout(total=20, connect=10)

# Map Statuspage.io indicator to a numeric severity (0=ok, 1=minor, 2=major, 3=critical)
_INDICATOR_SEVERITY = {"none": 0, "minor": 1, "major": 2, "critical": 3}


class InfrastructureStatusIngestor(BaseIngestor):
    source_name = "infrastructure_status"
    rate_limit_name = "default"

    async def fetch_data(self) -> Any:
        results: dict[str, Any] = {}
        async with aiohttp.ClientSession(timeout=_TIMEOUT) as session:
            # Statuspage.io providers
            for provider in _STATUSPAGE_PROVIDERS:
                results[provider["name"]] = await self._fetch_statuspage(
                    session, provider
                )
            # UKPN
            results["ukpn"] = await self._fetch_ukpn(session)
            # Azure DevOps
            results["azure_devops"] = await self._fetch_azure_devops(session)
        return results

    async def _fetch_statuspage(
        self, session: aiohttp.ClientSession, provider: dict[str, str]
    ) -> dict[str, Any]:
        try:
            async with session.get(provider["url"]) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                data = await resp.json(content_type=None)
                indicator = data.get("status", {}).get("indicator", "unknown")
                # Count degraded components
                components = data.get("components", [])
                degraded = [
                    c for c in components if c.get("status") != "operational"
                ]
                # Active incidents
                incidents = data.get("incidents", [])
                return {
                    "ok": True,
                    "label": provider["label"],
                    "indicator": indicator,
                    "severity": _INDICATOR_SEVERITY.get(indicator, 0),
                    "total_components": len(components),
                    "degraded_components": len(degraded),
                    "degraded_names": [c.get("name", "") for c in degraded[:5]],
                    "active_incidents": len(incidents),
                    "incident_names": [
                        i.get("name", "")[:100] for i in incidents[:3]
                    ],
                }
        except Exception as exc:
            self.log.warning("Statuspage fetch failed for %s: %s", provider["name"], exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def _fetch_ukpn(self, session: aiohttp.ClientSession) -> dict[str, Any]:
        try:
            async with session.get(_UKPN_URL) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                data = await resp.json(content_type=None)
                records = data.get("results", [])
                # Classify faults
                planned = [
                    r for r in records
                    if "planned" in str(r.get("incidenttype", "")).lower()
                ]
                unplanned = [r for r in records if r not in planned]
                # Extract London-area faults (postcodes starting with E, N, S, W, SE, SW, NW, EC, WC)
                london_prefixes = {"E", "N", "S", "W", "SE", "SW", "NW", "EC", "WC"}
                london_faults = []
                for r in records:
                    postcode = str(r.get("postcodesaffected", ""))
                    first_part = postcode.split(",")[0].strip().split(" ")[0] if postcode else ""
                    # Check if the alphabetic prefix is a London postcode
                    alpha = "".join(c for c in first_part if c.isalpha()).upper()
                    if alpha in london_prefixes:
                        london_faults.append(r)

                return {
                    "ok": True,
                    "label": "UKPN (UK Power Networks)",
                    "total_faults": len(records),
                    "planned": len(planned),
                    "unplanned": len(unplanned),
                    "london_faults": len(london_faults),
                    "fault_details": [
                        {
                            "ref": r.get("incidentreference", ""),
                            "type": r.get("incidenttype", ""),
                            "status": r.get("powercutstatus", ""),
                            "affected": r.get("customersaffected", 0),
                            "postcode": str(r.get("postcodesaffected", ""))[:60],
                        }
                        for r in london_faults[:5]
                    ],
                }
        except Exception as exc:
            self.log.warning("UKPN fetch failed: %s", exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def _fetch_azure_devops(
        self, session: aiohttp.ClientSession
    ) -> dict[str, Any]:
        try:
            async with session.get(_AZURE_DEVOPS_URL) as resp:
                if resp.status != 200:
                    return {"ok": False, "error": f"HTTP {resp.status}"}
                data = await resp.json(content_type=None)
                status = data.get("status", {})
                health = status.get("health", "unknown")
                services = data.get("services", [])
                degraded = [
                    s for s in services
                    if s.get("health", "healthy") != "healthy"
                ]
                return {
                    "ok": True,
                    "label": "Azure DevOps",
                    "health": health,
                    "severity": 0 if health == "healthy" else 2,
                    "total_services": len(services),
                    "degraded_services": len(degraded),
                    "degraded_names": [
                        s.get("id", "") for s in degraded[:5]
                    ],
                }
        except Exception as exc:
            self.log.warning("Azure DevOps fetch failed: %s", exc)
            return {"ok": False, "error": str(exc)[:200]}

    async def process(self, data: Any) -> None:
        results: dict[str, dict] = data
        total_providers = len(results)
        healthy_count = 0
        max_severity = 0

        for name, result in results.items():
            if not result.get("ok"):
                # Couldn't reach the status page — emit as unreachable
                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=-1.0,
                    location_id=None,
                    lat=None,
                    lon=None,
                    metadata={
                        "metric": "infra_status",
                        "provider": name,
                        "reachable": False,
                        "error": result.get("error", "unknown"),
                    },
                )
                await self.board.store_observation(obs)
                continue

            # ── UKPN special handling ────────────────────────────────────
            if name == "ukpn":
                severity = 0
                london_faults = result.get("london_faults", 0)
                unplanned = result.get("unplanned", 0)
                if london_faults > 10:
                    severity = 3
                elif london_faults > 5:
                    severity = 2
                elif london_faults > 0:
                    severity = 1

                obs = Observation(
                    source=self.source_name,
                    obs_type=ObservationType.NUMERIC,
                    value=float(london_faults),
                    location_id=None,
                    lat=51.5074,
                    lon=-0.1278,
                    metadata={
                        "metric": "infra_power_faults",
                        "provider": "ukpn",
                        "total_faults": result.get("total_faults", 0),
                        "planned": result.get("planned", 0),
                        "unplanned": unplanned,
                        "london_faults": london_faults,
                        "fault_details": result.get("fault_details", []),
                    },
                )
                await self.board.store_observation(obs)

                if london_faults > 0:
                    detail_str = "; ".join(
                        f"{f['ref']} ({f['type']}, {f['affected']} affected)"
                        for f in result.get("fault_details", [])[:3]
                    )
                    await self.board.post(AgentMessage(
                        from_agent=self.source_name,
                        channel="#raw",
                        content=(
                            f"UKPN: {london_faults} London power faults "
                            f"({result.get('planned', 0)} planned, "
                            f"{unplanned} unplanned). {detail_str}"
                        ),
                        data={
                            "metric": "infra_power_faults",
                            "provider": "ukpn",
                            "london_faults": london_faults,
                            "severity": severity,
                            "observation_id": obs.id,
                        },
                    ))
                if severity > max_severity:
                    max_severity = severity
                if severity == 0:
                    healthy_count += 1
                continue

            # ── Statuspage.io / Azure DevOps ─────────────────────────────
            severity = result.get("severity", 0)
            label = result.get("label", name)

            obs = Observation(
                source=self.source_name,
                obs_type=ObservationType.NUMERIC,
                value=float(severity),
                location_id=None,
                lat=None,
                lon=None,
                metadata={
                    "metric": "infra_status",
                    "provider": name,
                    "label": label,
                    "indicator": result.get("indicator") or result.get("health", ""),
                    "degraded_components": result.get("degraded_components")
                    or result.get("degraded_services", 0),
                    "degraded_names": result.get("degraded_names", []),
                    "active_incidents": result.get("active_incidents", 0),
                    "incident_names": result.get("incident_names", []),
                },
            )
            await self.board.store_observation(obs)

            if severity > 0:
                degraded_str = ", ".join(
                    result.get("degraded_names", [])[:3]
                ) or "unknown"
                await self.board.post(AgentMessage(
                    from_agent=self.source_name,
                    channel="#raw",
                    content=(
                        f"Infrastructure alert: {label} "
                        f"status={result.get('indicator') or result.get('health', '?')} "
                        f"(degraded: {degraded_str})"
                    ),
                    data={
                        "metric": "infra_status",
                        "provider": name,
                        "severity": severity,
                        "observation_id": obs.id,
                    },
                ))

            if severity > max_severity:
                max_severity = severity
            if severity == 0:
                healthy_count += 1

        # Overall summary observation
        overall = healthy_count / total_providers if total_providers else 0.0
        obs = Observation(
            source=self.source_name,
            obs_type=ObservationType.NUMERIC,
            value=overall,
            location_id=None,
            lat=None,
            lon=None,
            metadata={
                "metric": "infra_health_overall",
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
                f"Infrastructure status: {healthy_count}/{total_providers} healthy, "
                f"max severity={max_severity}"
            ),
            data={
                "metric": "infra_health_overall",
                "overall_health": round(overall, 3),
                "healthy_count": healthy_count,
                "total_providers": total_providers,
                "max_severity": max_severity,
                "observation_id": obs.id,
            },
        ))

        self.log.info(
            "Infrastructure status: %d/%d healthy, max_severity=%d",
            healthy_count,
            total_providers,
            max_severity,
        )


async def ingest_infrastructure_status(
    board: MessageBoard,
    graph: CortexGraph,
    scheduler: AsyncScheduler,
) -> None:
    """Standalone async ingest function for one infrastructure status check cycle."""
    ingestor = InfrastructureStatusIngestor(board, graph, scheduler)
    await ingestor.run()
