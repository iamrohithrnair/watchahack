"""Live web dashboard — aiohttp server with WebSocket log streaming and API routes.

Backend API server for London Cortex. Frontend is served separately on port 3000.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import uuid4

from aiohttp import web

from .config import DATA_DIR
from .image_store import THUMBS_DIR, FULL_DIR
from .memory import MemoryManager

if TYPE_CHECKING:
    from .board import MessageBoard
    from .coordinator import Coordinator

log = logging.getLogger("cortex.dashboard")

DASHBOARD_PORT = 8000


@web.middleware
async def cors_middleware(request: web.Request, handler):
    """CORS middleware to allow frontend on port 3000."""
    response = await handler(request)
    response.headers["Access-Control-Allow-Origin"] = "*"
    response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
    response.headers["Access-Control-Allow-Headers"] = "Content-Type"
    return response


class DashboardLogHandler(logging.Handler):
    """Captures log records and broadcasts to WebSocket clients."""

    def __init__(self, buffer_size: int = 500):
        super().__init__()
        self.buffer: deque[dict[str, Any]] = deque(maxlen=buffer_size)
        self._clients: set[web.WebSocketResponse] = set()

    @staticmethod
    async def _safe_send(ws: web.WebSocketResponse, data: dict) -> bool:
        """Send JSON to a WebSocket client, silently handling disconnects."""
        try:
            if not ws.closed:
                await ws.send_json(data)
                return True
        except Exception:
            pass  # client gone, will be cleaned up
        return False

    def emit(self, record: logging.LogRecord) -> None:
        entry = {
            "time": datetime.fromtimestamp(record.created, tz=timezone.utc).strftime("%H:%M:%S"),
            "level": record.levelname,
            "name": record.name.replace("cortex.", ""),
            "msg": record.getMessage(),
        }
        self.buffer.append(entry)
        # Fire-and-forget broadcast with safe wrapper
        dead = set()
        payload = {"type": "log", "data": entry}
        for ws in self._clients:
            if ws.closed:
                dead.add(ws)
                continue
            try:
                asyncio.get_event_loop().call_soon_threadsafe(
                    asyncio.ensure_future,
                    self._safe_send(ws, payload),
                )
            except Exception:
                dead.add(ws)
        self._clients -= dead

    def add_client(self, ws: web.WebSocketResponse) -> None:
        self._clients.add(ws)

    def remove_client(self, ws: web.WebSocketResponse) -> None:
        self._clients.discard(ws)


class DashboardServer:
    """aiohttp web server serving the Cortex dashboard API."""

    def __init__(self):
        self._board: MessageBoard | None = None
        self._coordinator: Coordinator | None = None
        self._memory: MemoryManager | None = None
        self._graph: Any = None
        self._retina: Any = None
        self._daemon: Any = None  # set by run.py after daemon creation
        self._investigator: Any = None
        self._investigations: dict[str, dict] = {}  # thread_id -> {context, evidence_cache}
        self._app: web.Application | None = None
        self._runner: web.AppRunner | None = None
        self.log_handler = DashboardLogHandler()

    async def start(self, board: MessageBoard, coordinator: Coordinator, memory: MemoryManager | None = None, graph: Any = None, retina: Any = None) -> None:
        self._board = board
        self._coordinator = coordinator
        self._memory = memory
        self._graph = graph
        self._retina = retina

        # Initialize investigator if graph is available
        if graph:
            from ..agents.investigator import InvestigatorAgent
            self._investigator = InvestigatorAgent(board, graph, memory)

        app = web.Application(middlewares=[cors_middleware])

        # Health check
        app.router.add_get("/api/health", self._api_health)

        # WebSocket log streaming
        app.router.add_get("/ws", self._websocket_handler)

        # API routes
        app.router.add_get("/api/tasks", self._api_tasks)
        app.router.add_get("/api/anomalies", self._api_anomalies)
        app.router.add_get("/api/observations", self._api_observations)
        app.router.add_get("/api/discoveries", self._api_discoveries)
        app.router.add_get("/api/stats", self._api_stats)
        app.router.add_get("/api/images", self._api_images)
        app.router.add_get("/api/cameras", self._api_cameras)
        app.router.add_get("/api/claude_instances", self._api_claude_instances)
        app.router.add_get("/api/claude_instance", self._api_claude_instance)
        app.router.add_get("/api/map_data", self._api_map_data)
        app.router.add_get("/api/channels", self._api_channels)
        app.router.add_get("/api/retina", self._api_retina)
        app.router.add_get("/stream", self._api_stream)
        app.router.add_get("/api/investigate", self._api_investigate_sse)

        # Static image routes
        app.router.add_static("/images/thumbs", str(THUMBS_DIR), show_index=False)
        app.router.add_static("/images/full", str(FULL_DIR), show_index=False)

        # CORS preflight handler
        app.router.add_options("/api/{path:.*}", self._cors_preflight)

        self._app = app

        # Frontend is served by Next.js on port 3000 — no static server needed

        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "0.0.0.0", DASHBOARD_PORT)
        await site.start()
        log.info("Dashboard API running at http://localhost:%d", DASHBOARD_PORT)

    async def stop(self) -> None:
        if self._runner:
            await self._runner.cleanup()

    # -- CORS preflight --

    async def _cors_preflight(self, request: web.Request) -> web.Response:
        """Handle CORS preflight OPTIONS requests."""
        response = web.Response(status=204)
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type"
        return response

    # -- Routes --

    async def _api_health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({"status": "ok"})

    async def _api_investigate_sse(self, request: web.Request) -> web.Response:
        question = request.query.get("q", "").strip()
        if not question:
            return web.json_response({"error": "Missing query parameter 'q'"}, status=400)
        if not self._investigator:
            return web.json_response({"error": "Investigator not ready"}, status=503)

        thread_id = request.query.get("thread_id")

        response = web.StreamResponse()
        response.content_type = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        response.headers["Access-Control-Allow-Origin"] = "*"
        await response.prepare(request)

        thread_data = self._investigations.get(thread_id, {}) if thread_id else {}
        context = thread_data.get("context", [])
        prior_evidence = thread_data.get("evidence_cache")

        evidence_cache = None
        async for event in self._investigator.investigate(question, context, prior_evidence):
            event_type = event.get("event", "message")
            # Intercept the internal _cache event (not sent to client)
            if event_type == "_cache":
                evidence_cache = event.get("data", {}).get("cache")
                continue
            data = json.dumps(event.get("data", {}), default=str)
            await response.write(f"event: {event_type}\ndata: {data}\n\n".encode())

        # Store context + evidence cache for follow-ups
        new_thread_id = thread_id or uuid4().hex[:12]
        if new_thread_id not in self._investigations:
            self._investigations[new_thread_id] = {"context": [], "evidence_cache": None}
        self._investigations[new_thread_id]["context"].append({"role": "user", "content": question})
        if evidence_cache:
            self._investigations[new_thread_id]["evidence_cache"] = evidence_cache
        # Cap at 20 threads
        while len(self._investigations) > 20:
            del self._investigations[next(iter(self._investigations))]

        # Send done event with thread_id
        done_data = json.dumps({"thread_id": new_thread_id})
        await response.write(f"event: done\ndata: {done_data}\n\n".encode())

        return response

    async def _websocket_handler(self, request: web.Request) -> web.WebSocketResponse:
        ws = web.WebSocketResponse()
        await ws.prepare(request)
        self.log_handler.add_client(ws)

        # Send buffered logs
        for entry in self.log_handler.buffer:
            if not await DashboardLogHandler._safe_send(ws, {"type": "log", "data": entry}):
                break  # client disconnected during catchup

        try:
            async for msg in ws:
                pass  # We only send, don't receive
        except Exception:
            pass  # client disconnected
        finally:
            self.log_handler.remove_client(ws)
        return ws

    async def _api_tasks(self, request: web.Request) -> web.Response:
        if not self._coordinator:
            return web.json_response({})
        return web.json_response(self._coordinator.get_task_health())

    async def _api_anomalies(self, request: web.Request) -> web.Response:
        if not self._board:
            return web.json_response([])
        rows = await self._board.get_active_anomalies(since_hours=2.0)
        # Trim to last 30
        return web.json_response(rows[:30], dumps=_json_dumps)

    async def _api_observations(self, request: web.Request) -> web.Response:
        if not self._board:
            return web.json_response([])
        db = self._board.get_db()
        cursor = await db.execute(
            "SELECT * FROM observations ORDER BY timestamp DESC LIMIT 50"
        )
        rows = await cursor.fetchall()
        return web.json_response(
            [self._board._row_to_dict(r) for r in rows], dumps=_json_dumps
        )

    async def _api_discoveries(self, request: web.Request) -> web.Response:
        if not self._board:
            return web.json_response([])
        rows = await self._board.read_channel("#discoveries", limit=20)
        return web.json_response(rows, dumps=_json_dumps)

    async def _api_channels(self, request: web.Request) -> web.Response:
        """Return messages from agent conversation channels for the Slack-style view."""
        if not self._board:
            return web.json_response({})

        # Only serve channels with meaningful agent-to-agent conversation
        result = {}
        for ch in ("#discoveries", "#hypotheses", "#anomalies", "#requests"):
            rows = await self._board.read_channel(ch, limit=50)
            result[ch] = rows

        # Filter #meta to only substantive reasoning, not routine status pings
        meta_rows = await self._board.read_channel("#meta", limit=80)
        _INTERESTING_META_TYPES = {
            "brain_cycle", "brain_synthesis", "self_improvement",
            "explorer_complete", "chronicler_observation",
            "quiet_period", "all_suppressed", "daemon_action",
        }
        filtered_meta = [
            m for m in meta_rows
            if (m.get("data") or {}).get("type") in _INTERESTING_META_TYPES
            or m.get("from_agent", "") in ("brain", "chronicler", "daemon")
            or "[SELF-IMPROVEMENT]" in (m.get("content") or "")
            or "[EXPLORER" in (m.get("content") or "")
            or "[CHRONICLER" in (m.get("content") or "")
        ]
        result["#meta"] = filtered_meta[:50]

        return web.json_response(result, dumps=_json_dumps)

    async def _api_stats(self, request: web.Request) -> web.Response:
        if not self._board:
            return web.json_response({})
        db = self._board.get_db()
        stats = {}
        for table in ("observations", "anomalies", "connections", "messages"):
            cursor = await db.execute(f"SELECT COUNT(*) FROM {table}")
            row = await cursor.fetchone()
            stats[table] = row[0] if row else 0
        return web.json_response(stats)

    async def _api_images(self, request: web.Request) -> web.Response:
        """Return list of recent thumbnails with timestamps."""
        thumbs = sorted(
            THUMBS_DIR.glob("*.jpg"),
            key=lambda p: p.stat().st_mtime,
            reverse=True,
        )[:24]
        return web.json_response([
            {"name": p.name, "mtime": p.stat().st_mtime}
            for p in thumbs
        ])

    async def _api_cameras(self, request: web.Request) -> web.Response:
        """Return camera health summary."""
        if not self._memory:
            return web.json_response({"counts": {"active": 0, "suspect": 0, "dead": 0}, "total_tracked": 0, "dead_cameras": [], "top_active": []})
        return web.json_response(self._memory.get_camera_summary(), dumps=_json_dumps)


    async def _api_claude_instances(self, request: web.Request) -> web.Response:
        """Return running + recent Claude Code instances."""
        if not self._daemon or not hasattr(self._daemon, "claude_runner"):
            return web.json_response([])
        runner = self._daemon.claude_runner
        instances = []
        for iid, inst in runner._instances.items():
            elapsed = (datetime.now(timezone.utc) - inst.started_at).total_seconds()
            instances.append({
                "id": iid[:8],
                "prompt": inst.prompt[:120],
                "running": inst.is_running,
                "started": inst.started_at.isoformat(),
                "elapsed_s": round(elapsed),
                "return_code": inst.return_code,
                "completed": inst.completed_at.isoformat() if inst.completed_at else None,
            })
        # Most recent first
        instances.sort(key=lambda x: x["started"], reverse=True)
        return web.json_response(instances[:20], dumps=_json_dumps)


    async def _api_claude_instance(self, request: web.Request) -> web.Response:
        """Return detail for a single Claude Code instance."""
        short_id = request.query.get("id", "")
        if not self._daemon or not hasattr(self._daemon, "claude_runner"):
            return web.json_response({"error": "No daemon"})
        runner = self._daemon.claude_runner
        # Match by prefix
        match = None
        for iid, inst in runner._instances.items():
            if iid.startswith(short_id):
                match = (iid, inst)
                break
        if not match:
            return web.json_response({"error": f"Instance {short_id} not found"})
        iid, inst = match
        elapsed = (datetime.now(timezone.utc) - inst.started_at).total_seconds()
        duration = None
        if inst.completed_at:
            duration = round((inst.completed_at - inst.started_at).total_seconds())
        return web.json_response({
            "id": iid[:8],
            "prompt": inst.prompt,
            "running": inst.is_running,
            "started": inst.started_at.isoformat(),
            "elapsed_s": round(elapsed),
            "duration_s": duration,
            "return_code": inst.return_code,
            "completed": inst.completed_at.isoformat() if inst.completed_at else None,
            "stdout": inst.stdout_lines[-200:] if inst.stdout_lines else [],
            "stderr": inst.stderr_lines[-50:] if inst.stderr_lines else [],
        }, dumps=_json_dumps)


    async def _api_map_data(self, request: web.Request) -> web.Response:
        """Return geolocated anomalies, observations, and discoveries for the map."""
        if not self._board:
            return web.json_response({"anomalies": [], "observations": [], "discoveries": []})
        db = self._board.get_db()

        # Active anomalies with coordinates
        anomalies = await self._board.get_active_anomalies(since_hours=6.0)
        geo_anomalies = [
            {
                "id": a["id"],
                "lat": a.get("lat"),
                "lon": a.get("lon"),
                "source": a.get("source", ""),
                "description": a.get("description", ""),
                "severity": a.get("severity", 1),
                "z_score": a.get("z_score", 0),
                "timestamp": a.get("timestamp", ""),
                "location_id": a.get("location_id", ""),
            }
            for a in anomalies
            if a.get("lat") and a.get("lon")
        ]

        # Recent observations with coordinates (last 2h, limit 200)
        cursor = await db.execute(
            """SELECT id, source, obs_type, location_id, lat, lon, timestamp,
                      value, metadata
               FROM observations
               WHERE lat IS NOT NULL AND lon IS NOT NULL
                 AND timestamp > datetime('now', '-2 hours')
               ORDER BY timestamp DESC LIMIT 200"""
        )
        rows = await cursor.fetchall()
        geo_obs = []
        total_all = len(rows)
        total_focused = 0
        for r in rows:
            # Tag each observation with its retina zone
            loc_id = r["location_id"]
            zone = 0  # periphery default
            if self._retina and loc_id:
                zone = int(self._retina.get_zone(loc_id))
            if zone >= 1:
                total_focused += 1

            entry = {
                "id": r["id"],
                "lat": r["lat"],
                "lon": r["lon"],
                "source": r["source"],
                "obs_type": r["obs_type"],
                "timestamp": r["timestamp"],
                "location_id": loc_id,
                "value": r["value"],
                "zone": zone,  # 0=periphery, 1=perifovea, 2=fovea
            }
            # Parse metadata JSON for extra detail
            meta_raw = r["metadata"]
            if meta_raw:
                try:
                    meta = json.loads(meta_raw) if isinstance(meta_raw, str) else meta_raw
                    # Include select useful fields
                    for k in ("station", "species", "unit", "site_name", "camera_id",
                              "description", "pollutant", "road", "borough",
                              "status", "level", "category", "title", "name"):
                        if k in meta:
                            entry[k] = meta[k]
                except Exception:
                    pass
            geo_obs.append(entry)

        # Recent discoveries with location data
        discs = await self._board.read_channel("#discoveries", limit=30)
        geo_discs = []
        for d in discs:
            loc = d.get("location_id", "")
            data = d.get("data", {}) or {}
            lat = data.get("lat") or d.get("lat")
            lon = data.get("lon") or d.get("lon")
            if lat and lon:
                geo_discs.append({
                    "lat": lat,
                    "lon": lon,
                    "from_agent": d.get("from_agent", ""),
                    "content": d.get("content", ""),
                    "timestamp": d.get("timestamp", ""),
                    "location_id": loc,
                })

        return web.json_response(
            {
                "anomalies": geo_anomalies,
                "observations": geo_obs,
                "discoveries": geo_discs,
                "obs_total": total_all,
                "obs_focused": total_focused,
            },
            dumps=_json_dumps,
        )


    async def _api_retina(self, request: web.Request) -> web.Response:
        """Return retina attention state for the map overlay."""
        if not self._retina:
            return web.json_response({"cells": [], "stats": {}, "recent_saccades": []})
        return web.json_response(self._retina.get_retina_state(), dumps=_json_dumps)

    async def _api_stream(self, request: web.Request) -> web.Response:
        """SSE endpoint for real-time log streaming."""
        response = web.StreamResponse()
        response.content_type = "text/event-stream"
        response.headers["Cache-Control"] = "no-cache"
        response.headers["X-Accel-Buffering"] = "no"
        response.headers["Access-Control-Allow-Origin"] = "*"
        await response.prepare(request)

        last_idx = len(self.log_handler.buffer)

        try:
            while True:
                current = list(self.log_handler.buffer)
                if len(current) > last_idx:
                    for entry in current[last_idx:]:
                        try:
                            data = json.dumps(entry, default=str)
                            await response.write(f"data: {data}\n\n".encode())
                        except Exception:
                            break
                    last_idx = len(current)
                elif len(current) < last_idx:
                    last_idx = len(current)
                await asyncio.sleep(0.1)
        except (ConnectionError, asyncio.CancelledError):
            pass
        return response


def _json_dumps(obj: Any) -> str:
    return json.dumps(obj, default=str)
