"""London Cortex — autonomous city intelligence. Main entry point."""

from __future__ import annotations

import asyncio
import importlib
import json
import logging
import signal
import sys
from datetime import datetime, timezone
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parent / ".env")

from .core.board import MessageBoard
from .core.config import (
    BACKEND_PORT, DATA_DIR, LOG_DIR, MEMORY_DIR,
    RATE_LIMITS, get_interval,
)
from .core.coordinator import Coordinator
from .core.dashboard import DashboardServer
from .core.graph import CortexGraph
from .core.llm import create_llm_backend, create_rate_limiter
from .core.memory import MemoryManager
from .core.scheduler import AsyncScheduler

log = logging.getLogger("cortex.run")

_dashboard = DashboardServer()


def setup_logging() -> None:
    root = logging.getLogger("cortex")
    if root.handlers:
        return
    fmt = logging.Formatter(
        '{"time":"%(asctime)s","level":"%(levelname)s","name":"%(name)s","msg":"%(message)s"}',
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    console = logging.StreamHandler(sys.stdout)
    console.setFormatter(fmt)
    console.setLevel(logging.INFO)
    file_h = logging.FileHandler(LOG_DIR / "cortex.log")
    file_h.setFormatter(fmt)
    file_h.setLevel(logging.DEBUG)

    _dashboard.log_handler.setLevel(logging.INFO)

    root.setLevel(logging.DEBUG)
    root.addHandler(console)
    root.addHandler(file_h)
    root.addHandler(_dashboard.log_handler)


async def main() -> str:
    setup_logging()
    log.info("=== London Cortex starting ===")

    # Initialize core systems
    board = MessageBoard()
    await board.init()
    graph = CortexGraph()
    memory = MemoryManager()
    scheduler = AsyncScheduler()
    coordinator = Coordinator()

    # Configure rate limits
    for rl in (scheduler, coordinator):
        rl.configure_rate_limit("gemini_flash", RATE_LIMITS.gemini_flash_rpm)
        rl.configure_rate_limit("gemini_pro", RATE_LIMITS.gemini_pro_rpm)
        rl.configure_rate_limit("tfl", RATE_LIMITS.tfl_rpm)
        rl.configure_rate_limit("default", RATE_LIMITS.default_rpm)

    # Initialize LLM backend and rate limiter
    llm = create_llm_backend()
    llm_limiter = create_rate_limiter()

    # Initialize retina
    from .core.retina import AttentionManager
    retina = AttentionManager(graph)
    graph.retina = retina
    board.set_retina(retina)

    # Restore baselines
    restored = await memory.restore_baselines(board.get_db())
    log.info("Restored %d baselines from previous run.", restored)

    # Seed retina foci
    retina.seed_initial_foci([
        (graph.latlon_to_cell(51.505, -0.09), "city_center"),
        (graph.latlon_to_cell(51.515, -0.14), "west_end"),
        (graph.latlon_to_cell(51.503, -0.02), "canary_wharf"),
    ])

    # Persist system start time
    meta_path = Path(MEMORY_DIR) / "system_meta.json"
    system_meta = {}
    if meta_path.exists():
        try:
            system_meta = json.loads(meta_path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    system_meta["last_start_time"] = datetime.now(timezone.utc).isoformat()
    if "first_start_time" not in system_meta:
        system_meta["first_start_time"] = system_meta["last_start_time"]
    meta_path.write_text(json.dumps(system_meta, indent=2))

    log.info("Core systems initialized. Graph: %s", graph.stats())

    # ── Register ingestors from registry ──────────────────────────────────────
    from .ingestors.registry import INGESTOR_REGISTRY, AGENT_REGISTRY, check_env_requirements

    ingestor_args = (board, graph, scheduler)
    agent_args = (board, graph, memory)

    active_ingestors: list[str] = []
    skipped_ingestors: list[tuple[str, list[str]]] = []

    for func_name, module_path, interval_key, priority, sources, env_vars in INGESTOR_REGISTRY:
        ok, missing = check_env_requirements(func_name, env_vars)
        if not ok:
            skipped_ingestors.append((func_name, missing))
            continue
        try:
            mod = importlib.import_module(module_path)
            func = getattr(mod, func_name)
        except (ImportError, AttributeError) as e:
            log.warning("Could not import %s from %s: %s", func_name, module_path, e)
            skipped_ingestors.append((func_name, [str(e)]))
            continue
        min_i, max_i = get_interval(interval_key)
        coordinator.register(func_name, func,
            min_interval=min_i, max_interval=max_i,
            priority=priority, args=ingestor_args,
            source_names=sources)
        active_ingestors.append(func_name)

    # ── Register agents from registry ─────────────────────────────────────────
    for task_name, module_path, func_name, interval_key, priority in AGENT_REGISTRY:
        try:
            mod = importlib.import_module(module_path)
            func = getattr(mod, func_name)
        except (ImportError, AttributeError) as e:
            log.warning("Could not import %s from %s: %s", func_name, module_path, e)
            continue
        min_i, max_i = get_interval(interval_key)
        coordinator.register(task_name, func,
            min_interval=min_i, max_interval=max_i,
            priority=priority, args=agent_args)

    # ── Startup report ────────────────────────────────────────────────────────
    log.info("=== Startup Report ===")
    log.info("Active: %d ingestors, %d agents registered", len(active_ingestors), len(AGENT_REGISTRY))
    for name, missing in sorted(skipped_ingestors):
        log.warning("SKIPPED %s — missing: %s", name, ", ".join(missing))
    log.info("=== End Startup Report ===")

    # Post source availability
    from .core.models import AgentMessage
    await board.post(AgentMessage(
        from_agent="startup",
        channel="#system",
        content=f"London Cortex started with {len(active_ingestors)} active sources.",
        data={
            "active_ingestors": sorted(active_ingestors),
            "skipped_ingestors": [{"name": n, "missing_keys": k} for n, k in skipped_ingestors],
            "event": "startup_source_report",
        },
    ))

    # ── Graceful shutdown ─────────────────────────────────────────────────────
    stop_event = asyncio.Event()

    def handle_signal():
        if stop_event.is_set():
            log.warning("Second shutdown signal — forcing exit")
            os._exit(1)
        log.info("Shutdown signal received (Ctrl+C again to force)")
        stop_event.set()

    import os
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_signal)

    # Start dashboard (backend API on port 8000)
    await _dashboard.start(board, coordinator, memory=memory, graph=graph, retina=retina)

    # Startup probes
    await coordinator.run_startup_probes()

    # Start all tasks
    await coordinator.start()
    log.info("=== All %d coordinator tasks started ===", len(coordinator._tasks))

    # Start daemon
    from .core.daemon import Daemon
    daemon = Daemon(board, coordinator)
    _dashboard._daemon = daemon
    if hasattr(_dashboard, '_investigator') and _dashboard._investigator:
        _dashboard._investigator._daemon = daemon
    daemon_task = asyncio.create_task(daemon.run(), name="daemon")

    # Maintenance loop
    from .core.image_store import ImageStore
    image_store = ImageStore()
    last_daily_snapshot = datetime.now(timezone.utc)

    async def maintenance():
        nonlocal last_daily_snapshot
        while not stop_event.is_set():
            await asyncio.sleep(300)
            pruned = graph.prune_expired_anomalies()
            retina.tick_decay()
            expired = await board.cleanup_expired()
            saved = await memory.persist_baselines(board.get_db())
            image_store.cleanup()
            if pruned or expired or saved:
                log.info("Maintenance: pruned %d anomalies, %d expired, %d baselines saved", pruned, expired, saved)
            now = datetime.now(timezone.utc)
            if (now - last_daily_snapshot).total_seconds() > 86400:
                await memory.snapshot_daily_baselines(board.get_db())
                last_daily_snapshot = now

    maint_task = asyncio.create_task(maintenance())

    # Wait for shutdown
    await stop_event.wait()
    log.info("Shutting down...")
    maint_task.cancel()
    daemon_task.cancel()
    try:
        await asyncio.wait_for(memory.persist_baselines(board.get_db()), timeout=3.0)
    except Exception:
        pass
    try:
        await asyncio.wait_for(coordinator.stop(), timeout=5.0)
    except asyncio.TimeoutError:
        log.warning("Coordinator stop timed out, forcing exit")
    await _dashboard.stop()
    await board.close()
    log.info("=== London Cortex stopped ===")

    sentinel = Path(__file__).resolve().parent / "data" / ".restart"
    if sentinel.exists():
        sentinel.unlink(missing_ok=True)
        return "restart"
    return "stop"


def run_with_auto_restart() -> None:
    """Run main() in a loop, auto-restarting when daemon requests it."""
    import time as _time

    restart_count = 0
    max_rapid_restarts = 10
    last_start = 0.0

    while True:
        now = _time.monotonic()
        if now - last_start > 60:
            restart_count = 0
        last_start = now

        result = asyncio.run(main())

        if result == "restart":
            restart_count += 1
            if restart_count >= max_rapid_restarts:
                log.info("Too many rapid restarts (%d), stopping.", restart_count)
                break
            _time.sleep(2)
            log.info("=== Auto-restarting (restart #%d) ===", restart_count)
            continue
        else:
            break


if __name__ == "__main__":
    run_with_auto_restart()
