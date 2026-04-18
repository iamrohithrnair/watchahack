"""Daemon — persistent event-driven watcher that tails system state.

NOT a periodic agent. Started as a top-level asyncio.Task in run.py.
Watches board channels, coordinator health, and log files.
Spawns Claude Code instances to auto-fix issues.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import os
import signal

from .board import MessageBoard
from .claude_runner import ClaudeRunner
from .config import DATA_DIR, LOG_DIR, MEMORY_DIR
from .coordinator import Coordinator
from .models import DaemonAction

_RESTART_SENTINEL = Path(DATA_DIR) / ".restart"

log = logging.getLogger("cortex.daemon")

_DAEMON_ACTIONS_PATH = Path(MEMORY_DIR) / "daemon_actions.json"
_ARCHITECTURE_PATH = Path(__file__).resolve().parent.parent / "ARCHITECTURE.md"
_STAGING_PATH = Path(MEMORY_DIR) / "pending_registrations.json"

# Guidance for spawned Claude instances about core file modifications
_PROTECTED_FILES_NOTICE = (
    "\n\nNOTE ON CORE FILES: Modifying these files triggers a full process restart, "
    "so only edit them if truly necessary for your fix:\n"
    "  cortex/run.py, cortex/core/board.py, cortex/core/coordinator.py, "
    "cortex/core/scheduler.py, cortex/core/models.py\n"
    "PREFERRED: To register a new ingestor WITHOUT restarting, write a JSON entry to "
    "cortex/data/memory/pending_registrations.json:\n"
    '[{"task_name": "ingest_foo", "module": "cortex.ingestors.foo", "func": "ingest_foo", '
    '"source_names": ["foo_source"], "interval": 1800}]\n'
    "The daemon will auto-register it. Only modify run.py directly if you must change "
    "something beyond simple registration (e.g. fixing imports, changing agent wiring).\n"
)

_MIN_RESTART_GAP = 10800  # 3 hours minimum between restarts

# Error thresholds
_CONSECUTIVE_ERROR_THRESHOLD = 3
_ACCURACY_DROP_THRESHOLD = 0.3
_EMPTY_DATA_MULTIPLIER = 3  # task returns empty for 3x its interval


class Daemon:
    """Persistent async watcher — event-driven, not timer-based."""

    def __init__(
        self,
        board: MessageBoard,
        coordinator: Coordinator,
    ):
        self.board = board
        self.coordinator = coordinator
        self.claude_runner = ClaudeRunner(max_concurrent=3)

        # Internal state
        self._last_check: dict[str, datetime] = {}  # channel -> last checked
        self._error_counts: dict[str, int] = defaultdict(int)
        self._known_anomaly_types: set[str] = set()
        self._actions: list[dict[str, Any]] = []
        self._load_actions()
        self._last_registration_check: float = 0.0  # monotonic time
        self._last_restart_request: float = 0.0  # monotonic time

        # Git HEAD at startup for change detection
        self._startup_head = self._get_git_head()

    def _load_actions(self) -> None:
        """Load past daemon actions from disk."""
        try:
            if _DAEMON_ACTIONS_PATH.exists():
                data = json.loads(_DAEMON_ACTIONS_PATH.read_text())
                if isinstance(data, list):
                    self._actions = data[-200:]  # keep last 200
        except (json.JSONDecodeError, OSError):
            pass

    def _save_actions(self) -> None:
        """Persist daemon actions to disk."""
        try:
            _DAEMON_ACTIONS_PATH.parent.mkdir(parents=True, exist_ok=True)
            _DAEMON_ACTIONS_PATH.write_text(
                json.dumps(self._actions[-200:], indent=2, default=str)
            )
        except OSError as exc:
            log.warning("Failed to save daemon actions: %s", exc)

    async def run(self) -> None:
        """Main daemon loop — runs for lifetime of process."""
        log.info("[Daemon] Starting persistent watcher")
        poll_interval = 10  # seconds between checks
        self._completed_instance_ids: set[str] = set()

        while True:
            try:
                await self._check_board_channels()
                await self._check_coordinator_health()
                await self._check_log_errors()
                await self._check_completed_instances()
                await self._check_pending_registrations()
                self.claude_runner.cleanup_old()
            except asyncio.CancelledError:
                log.info("[Daemon] Shutting down")
                return
            except Exception:
                log.exception("[Daemon] Error in watch cycle")

            await asyncio.sleep(poll_interval)

    async def _check_board_channels(self) -> None:
        """Watch board channels for actionable messages."""
        now = datetime.now(timezone.utc)

        # Check #meta for self-improvement suggestions
        since = self._last_check.get("#meta", now - timedelta(minutes=5))
        self._last_check["#meta"] = now
        try:
            meta_msgs = await self.board.read_channel("#meta", since=since, limit=20)
        except Exception:
            return

        for msg in meta_msgs:
            data = msg.get("data", {})
            msg_type = data.get("type", "")

            # Brain's self-improvement suggestion -> consider spawning Claude
            if msg_type == "self_improvement":
                new_source = data.get("new_data_source_suggestion", "")
                if new_source and len(new_source) > 10:
                    await self._maybe_spawn_for_new_source(new_source, msg)

            # Prediction accuracy drop
            if msg_type == "accuracy_metrics":
                recent_accuracy = data.get("recent_accuracy", 1.0)
                if recent_accuracy < _ACCURACY_DROP_THRESHOLD:
                    await self._spawn_for_accuracy_drop(recent_accuracy, data)

        # Check #requests for investigation requests
        since_req = self._last_check.get("#requests", now - timedelta(minutes=5))
        self._last_check["#requests"] = now
        try:
            req_msgs = await self.board.read_channel("#requests", since=since_req, limit=10)
        except Exception:
            return

        for msg in req_msgs:
            data = msg.get("data", {})
            if data.get("request_type") == "claude_investigation":
                question = data.get("question", "")
                context = data.get("context", "")
                from_agent = data.get("from_agent", "unknown")
                if question:
                    await self._spawn_for_investigation(question, context, from_agent, msg)
            elif data.get("request_type") == "brain_question":
                question = data.get("question", "")
                if question:
                    log.debug("[Daemon] Brain question: %s", question[:100])

        # Track anomaly types for novelty detection
        since_anom = self._last_check.get("#anomalies", now - timedelta(minutes=5))
        self._last_check["#anomalies"] = now
        try:
            anom_msgs = await self.board.read_channel("#anomalies", since=since_anom, limit=30)
        except Exception:
            return

        for msg in anom_msgs:
            data = msg.get("data", {})
            source = data.get("source", "")
            metric = data.get("metric", "")
            anom_type = f"{source}:{metric}" if metric else source
            if anom_type and anom_type not in self._known_anomaly_types:
                self._known_anomaly_types.add(anom_type)
                if len(self._known_anomaly_types) > 5:  # skip initial warmup
                    log.info("[Daemon] New anomaly type detected: %s", anom_type)

    async def _check_coordinator_health(self) -> None:
        """Watch coordinator task health for errors and staleness."""
        health = self.coordinator.get_task_health()

        for task_name, info in health.items():
            errors = info.get("consecutive_errors", 0)

            # Consecutive errors threshold
            if errors >= _CONSECUTIVE_ERROR_THRESHOLD:
                prev_count = self._error_counts.get(task_name, 0)
                if errors > prev_count:
                    self._error_counts[task_name] = errors
                    await self._spawn_for_task_errors(task_name, errors, info)

            # Check staleness: no success for 3x expected interval
            last_success = info.get("last_success")
            max_interval = info.get("max_interval", 600)
            if last_success:
                try:
                    last_dt = datetime.fromisoformat(last_success)
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                    elapsed = (datetime.now(timezone.utc) - last_dt).total_seconds()
                    if elapsed > max_interval * _EMPTY_DATA_MULTIPLIER:
                        log.warning(
                            "[Daemon] Task %s stale: no success for %.0fs (expected max %.0fs)",
                            task_name, elapsed, max_interval,
                        )
                except (ValueError, TypeError):
                    pass

    async def _check_log_errors(self) -> None:
        """Tail the log file for ERROR/WARNING patterns."""
        log_path = LOG_DIR / "cortex.log"
        if not log_path.exists():
            return

        # Simple approach: read last 50 lines
        try:
            content = log_path.read_text()
            lines = content.strip().split("\n")
            recent = lines[-50:]

            error_count = sum(1 for l in recent if '"ERROR"' in l)
            if error_count > 10:
                log.warning("[Daemon] High error rate in logs: %d errors in last 50 lines", error_count)
        except (OSError, UnicodeDecodeError):
            pass

    async def _spawn_for_task_errors(
        self, task_name: str, error_count: int, health_info: dict
    ) -> None:
        """Spawn Claude Code to investigate consecutive task errors."""
        prompt = (
            f"The London Cortex task '{task_name}' has failed {error_count} "
            f"consecutive times. Health info: {json.dumps(health_info)}\n\n"
            f"Please investigate the task code, check the error logs at "
            f"cortex/data/logs/cortex.log, and fix the issue.\n\n"
            f"Read cortex/ARCHITECTURE.md first for system overview."
            f"{_PROTECTED_FILES_NOTICE}"
        )

        context_files = [str(_ARCHITECTURE_PATH)]

        # Use coordinator metadata to find the exact source file
        entry = self.coordinator._tasks.get(task_name)
        if entry and entry.module_name:
            file_path = entry.module_name.replace(".", "/") + ".py"
            context_files.append(file_path)
        elif "ingest" in task_name:
            source = task_name.replace("ingest_", "")
            context_files.append(f"cortex/ingestors/{source}.py")

        instance = await self.claude_runner.spawn(prompt, context_files)
        if instance:
            action = DaemonAction(
                trigger=f"consecutive_errors:{task_name}:{error_count}",
                action_type="spawn_claude",
                prompt=prompt[:500],
                context_files=context_files,
                instance_id=instance.instance_id,
            )
            self._record_action(action)
            await self._post_daemon_message(
                f"[DAEMON] Spawned Claude Code to investigate {task_name} "
                f"({error_count} consecutive errors)",
                {"task_name": task_name, "error_count": error_count},
            )

    async def _spawn_for_accuracy_drop(
        self, accuracy: float, metrics: dict
    ) -> None:
        """Spawn Claude Code to review brain/connectors when accuracy drops."""
        prompt = (
            f"The London Cortex prediction accuracy has dropped to {accuracy:.0%}. "
            f"Metrics: {json.dumps(metrics)}\n\n"
            f"Please review the brain (cortex/agents/brain.py) and connectors "
            f"(cortex/agents/connectors.py) to identify why predictions are failing. "
            f"Check if the epistemic engine (cortex/core/epistemics.py) thresholds need adjustment.\n\n"
            f"Read cortex/ARCHITECTURE.md first for system overview."
            f"{_PROTECTED_FILES_NOTICE}"
        )

        context_files = [
            str(_ARCHITECTURE_PATH),
            "cortex/agents/brain.py",
            "cortex/agents/connectors.py",
            "cortex/core/epistemics.py",
        ]

        instance = await self.claude_runner.spawn(prompt, context_files)
        if instance:
            action = DaemonAction(
                trigger=f"accuracy_drop:{accuracy:.2f}",
                action_type="spawn_claude",
                prompt=prompt[:500],
                context_files=context_files,
                instance_id=instance.instance_id,
            )
            self._record_action(action)
            await self._post_daemon_message(
                f"[DAEMON] Spawned Claude Code to review prediction accuracy ({accuracy:.0%})",
                {"accuracy": accuracy},
            )

    def _recent_actions_context(self, action_type_prefix: str, hours: float = 2.0) -> str:
        """Collect recent actions of a given type for LLM context."""
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        recent = []
        for a in reversed(self._actions[-20:]):
            trigger = a.get("trigger", "")
            if not trigger.startswith(action_type_prefix):
                continue
            started = a.get("started_at", "")
            try:
                ts = datetime.fromisoformat(started)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts < cutoff:
                    continue
            except (ValueError, TypeError):
                continue
            outcome = a.get("outcome", "pending")
            recent.append(f"- {trigger} (outcome: {outcome})")
            if len(recent) >= 5:
                break
        if not recent:
            return ""
        return (
            "\n\nRecent investigations (avoid duplicating):\n"
            + "\n".join(recent)
        )

    def _rate_limited(self, action_type: str, min_gap_seconds: float = 120) -> bool:
        """Return True if an action of this type was spawned too recently."""
        cutoff = datetime.now(timezone.utc) - timedelta(seconds=min_gap_seconds)
        for a in reversed(self._actions[-10:]):
            if not a.get("trigger", "").startswith(action_type):
                continue
            started = a.get("started_at", "")
            try:
                ts = datetime.fromisoformat(started)
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=timezone.utc)
                if ts > cutoff:
                    return True
            except (ValueError, TypeError):
                continue
        return False

    async def _spawn_for_investigation(
        self, question: str, context: str, from_agent: str, msg: dict
    ) -> None:
        """Spawn Claude Code to investigate a question from an agent."""
        if self._rate_limited("investigation:", min_gap_seconds=120):
            return

        recent_context = self._recent_actions_context("investigation:")

        prompt = (
            f"An agent ({from_agent}) in the London Cortex needs help investigating:\n\n"
            f"Question: {question}\n\n"
            f"Context: {context}\n\n"
            f"Read cortex/ARCHITECTURE.md for system overview. "
            f"Investigate this question using the system's data and code. "
            f"Post your findings back to the board.\n"
            f"{recent_context}\n"
            f"If this question has already been investigated recently (see above), "
            f"skip it and respond with 'SKIP: already investigated'."
            f"{_PROTECTED_FILES_NOTICE}"
        )

        context_files = [str(_ARCHITECTURE_PATH)]
        instance = await self.claude_runner.spawn(prompt, context_files)
        if instance:
            action = DaemonAction(
                trigger=f"investigation:{from_agent}:{question[:50]}",
                action_type="spawn_claude",
                prompt=prompt[:500],
                context_files=context_files,
                instance_id=instance.instance_id,
            )
            self._record_action(action)
            await self._post_daemon_message(
                f"[DAEMON] Spawned Claude for investigation from {from_agent}: {question[:80]}",
                {"from_agent": from_agent, "question": question[:200]},
            )

    async def _maybe_spawn_for_new_source(
        self, source_suggestion: str, msg: dict
    ) -> None:
        """Consider spawning Claude Code to implement a new data source."""
        if self._rate_limited("new_source_suggestion:", min_gap_seconds=120):
            return

        recent_context = self._recent_actions_context("new_source_suggestion:")

        prompt = (
            f"The Brain agent suggested adding a new data source: '{source_suggestion}'\n\n"
            f"Please evaluate this suggestion and if feasible, implement a new ingestor "
            f"following the BaseIngestor pattern. See cortex/ARCHITECTURE.md for how to "
            f"add an ingestor.\n\n"
            f"Steps:\n"
            f"1. Check if the API/data source exists and is accessible\n"
            f"2. Create a new ingestor file in cortex/ingestors/\n"
            f"3. If the API requires a key, add it to .env.example\n"
            f"{recent_context}\n"
            f"If this source was already suggested recently (see above), skip it."
            f"{_PROTECTED_FILES_NOTICE}"
        )

        context_files = [
            str(_ARCHITECTURE_PATH),
            "cortex/ingestors/air_quality.py",  # example ingestor
        ]

        instance = await self.claude_runner.spawn(prompt, context_files)
        if instance:
            action = DaemonAction(
                trigger=f"new_source_suggestion:{source_suggestion[:50]}",
                action_type="spawn_claude",
                prompt=prompt[:500],
                context_files=context_files,
                instance_id=instance.instance_id,
            )
            self._record_action(action)
            await self._post_daemon_message(
                f"[DAEMON] Evaluating new data source suggestion: {source_suggestion[:80]}",
                {"suggestion": source_suggestion},
            )

    def _record_action(self, action: DaemonAction) -> None:
        """Record a daemon action to memory."""
        self._actions.append({
            "action_id": action.action_id,
            "trigger": action.trigger,
            "action_type": action.action_type,
            "prompt": action.prompt[:300],
            "context_files": action.context_files,
            "instance_id": action.instance_id,
            "started_at": action.started_at.isoformat(),
        })
        self._save_actions()

    def _get_git_head(self) -> str:
        """Get current git HEAD commit hash."""
        try:
            import subprocess
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                capture_output=True, text=True, timeout=5,
                cwd=str(Path(__file__).resolve().parent.parent.parent),
            )
            return result.stdout.strip() if result.returncode == 0 else ""
        except Exception:
            return ""

    async def _get_changed_py_files(self) -> list[str]:
        """Use git to detect .py file changes vs HEAD (modified + untracked)."""
        try:
            repo_root = Path(__file__).resolve().parent.parent.parent

            # Modified/added tracked files vs HEAD
            proc = await asyncio.create_subprocess_exec(
                "git", "diff", "--name-only", "HEAD",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(repo_root),
            )
            stdout, _ = await asyncio.wait_for(proc.communicate(), timeout=10)
            tracked = stdout.decode().strip().splitlines() if stdout else []

            # New untracked files
            proc2 = await asyncio.create_subprocess_exec(
                "git", "ls-files", "--others", "--exclude-standard",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(repo_root),
            )
            stdout2, _ = await asyncio.wait_for(proc2.communicate(), timeout=10)
            untracked = stdout2.decode().strip().splitlines() if stdout2 else []

            all_changed = tracked + untracked
            # Only .py files under cortex/
            return [f for f in all_changed if f.endswith(".py") and f.startswith("cortex/")]
        except Exception as exc:
            log.warning("[Daemon] Failed to check git for changes: %s", exc)
            return []

    async def _check_completed_instances(self) -> None:
        """Check if any completed Claude instances modified code — hot-reload affected tasks."""
        for iid, instance in self.claude_runner._instances.items():
            # Use completed_at (not is_running) to avoid race with _monitor_instance
            if instance.completed_at is None or iid in self._completed_instance_ids:
                continue
            self._completed_instance_ids.add(iid)

            if instance.return_code != 0:
                continue

            # Check git for actual .py file changes — definitive
            changed_files = await self._get_changed_py_files()
            if changed_files:
                log.info(
                    "[Daemon] %d file(s) changed vs git HEAD: %s",
                    len(changed_files), ", ".join(changed_files[:5]),
                )
                # Auto-commit
                await self._git_auto_commit(changed_files, iid)

                # Hot-reload changed modules and restart only affected tasks
                reloaded, restarted = await self._hot_reload(changed_files)

                await self._post_daemon_message(
                    f"[DAEMON] Hot-reloaded {reloaded} module(s), restarted {restarted} task(s): "
                    f"{', '.join(changed_files[:5])}",
                    {"type": "hot_reload", "instance_id": iid,
                     "changed_files": changed_files,
                     "modules_reloaded": reloaded, "tasks_restarted": restarted},
                )

                # Only fall back to full restart for core infrastructure changes
                core_files = [f for f in changed_files if any(
                    c in f for c in ("core/board.py", "core/coordinator.py",
                                      "core/scheduler.py", "core/models.py", "run.py")
                )]
                if core_files:
                    log.warning(
                        "[Daemon] Core infrastructure changed (%s) — full restart required",
                        ", ".join(core_files),
                    )
                    self._request_restart(f"Core file changed: {core_files[0]}")

                return  # Only one reload per check cycle

    async def _git_auto_commit(self, changed_files: list[str], instance_id: str) -> None:
        """Auto-commit changed files so next restart has a clean HEAD."""
        try:
            repo_root = Path(__file__).resolve().parent.parent.parent

            # Stage the changed cortex/ .py files
            proc = await asyncio.create_subprocess_exec(
                "git", "add", *changed_files,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(repo_root),
            )
            await asyncio.wait_for(proc.communicate(), timeout=10)

            # Also stage any modified files (config, run.py, etc.)
            proc2 = await asyncio.create_subprocess_exec(
                "git", "add", "-u", "cortex/",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(repo_root),
            )
            await asyncio.wait_for(proc2.communicate(), timeout=10)

            # Commit
            msg = (
                f"[auto] daemon: {len(changed_files)} file(s) added/modified by Claude instance {instance_id[:8]}\n\n"
                f"Files: {', '.join(changed_files[:10])}"
            )
            proc3 = await asyncio.create_subprocess_exec(
                "git", "commit", "-m", msg,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(repo_root),
            )
            stdout, stderr = await asyncio.wait_for(proc3.communicate(), timeout=10)
            if proc3.returncode == 0:
                log.info("[Daemon] Auto-committed %d file(s)", len(changed_files))
            else:
                log.warning("[Daemon] Auto-commit failed: %s", (stderr or b"").decode()[:200])
        except Exception as exc:
            log.warning("[Daemon] Auto-commit error: %s", exc)

    async def _hot_reload(self, changed_files: list[str]) -> tuple[int, int]:
        """Hot-reload changed Python modules and restart affected coordinator tasks.

        Uses coordinator metadata (module_name/func_name) instead of hardcoded
        mappings. Also auto-registers new ingestors that follow the convention.

        Returns (modules_reloaded, tasks_restarted).
        """
        import importlib
        import sys

        modules_reloaded = 0
        tasks_to_restart: dict[str, Any] = {}

        # Separate files into categories for ordered reload
        base_changed = any("agents/base.py" in f for f in changed_files)
        util_modules: list[str] = []
        agent_modules: list[str] = []
        ingestor_modules: list[str] = []
        new_ingestor_files: list[str] = []

        for filepath in changed_files:
            module_name = filepath.replace("/", ".").removesuffix(".py")
            if "core/" in filepath and filepath not in (
                "cortex/core/board.py", "cortex/core/coordinator.py",
                "cortex/core/scheduler.py", "cortex/core/models.py",
            ):
                util_modules.append(module_name)
            elif "agents/" in filepath:
                agent_modules.append(module_name)
            elif "ingestors/" in filepath:
                if module_name in sys.modules:
                    ingestor_modules.append(module_name)
                else:
                    new_ingestor_files.append(filepath)

        # Phase 1: Reload utility modules
        for mod_name in util_modules:
            if mod_name in sys.modules:
                try:
                    importlib.reload(sys.modules[mod_name])
                    modules_reloaded += 1
                    log.info("[Daemon] Reloaded utility: %s", mod_name)
                except Exception as exc:
                    log.warning("[Daemon] Failed to reload %s: %s", mod_name, exc)

        # Phase 2: Agent modules — use coordinator metadata for cascade
        all_agent_modules = {
            m for m in self.coordinator.get_all_agent_modules()
            if m.startswith("cortex.agents.")
        }

        if base_changed:
            base_mod = "cortex.agents.base"
            if base_mod in sys.modules:
                try:
                    importlib.reload(sys.modules[base_mod])
                    modules_reloaded += 1
                    log.info("[Daemon] Reloaded base agent: %s", base_mod)
                except Exception as exc:
                    log.warning("[Daemon] Failed to reload base: %s — full restart", exc)
                    self._request_restart(f"base.py reload failed: {exc}")
                    return modules_reloaded, 0

            # Reload ALL agent modules (they inherit from BaseAgent)
            for mod_name in all_agent_modules:
                if mod_name in sys.modules:
                    try:
                        importlib.reload(sys.modules[mod_name])
                        modules_reloaded += 1
                        log.info("[Daemon] Reloaded (base cascade): %s", mod_name)
                    except Exception as exc:
                        log.warning("[Daemon] Failed to reload %s: %s", mod_name, exc)
        else:
            # Reload only the specific agent modules that changed
            for mod_name in agent_modules:
                if mod_name == "cortex.agents.base":
                    continue
                if mod_name in sys.modules:
                    try:
                        importlib.reload(sys.modules[mod_name])
                        modules_reloaded += 1
                        log.info("[Daemon] Reloaded agent: %s", mod_name)
                    except Exception as exc:
                        log.warning("[Daemon] Failed to reload %s: %s", mod_name, exc)

        # Phase 3: Reload existing ingestor modules
        for mod_name in ingestor_modules:
            if mod_name in sys.modules:
                try:
                    importlib.reload(sys.modules[mod_name])
                    modules_reloaded += 1
                    log.info("[Daemon] Reloaded ingestor: %s", mod_name)
                except Exception as exc:
                    log.warning("[Daemon] Failed to reload %s: %s", mod_name, exc)

        # Phase 4: Auto-register new ingestors
        for filepath in new_ingestor_files:
            registered = await self._try_register_new_ingestor(filepath)
            if registered:
                modules_reloaded += 1

        # Phase 5: Build task->new_func map from coordinator metadata
        reloaded_modules = set(util_modules + agent_modules + ingestor_modules)
        if base_changed:
            reloaded_modules.update(all_agent_modules)

        for mod_name in reloaded_modules:
            entries = self.coordinator.get_tasks_for_module(mod_name)
            mod = sys.modules.get(mod_name)
            if mod and entries:
                for entry in entries:
                    if entry.func_name:
                        new_func = getattr(mod, entry.func_name, None)
                        if new_func:
                            tasks_to_restart[entry.name] = new_func

        # Phase 6: Restart affected coordinator tasks with NEW function references
        tasks_restarted = 0
        for task_name, new_func in tasks_to_restart.items():
            if self.coordinator.restart_task(task_name, new_func=new_func):
                tasks_restarted += 1
                log.info("[Daemon] Restarted task with new code: %s", task_name)
            else:
                log.debug("[Daemon] Task %s not found in coordinator (may be OK)", task_name)

        if modules_reloaded or tasks_restarted:
            log.info(
                "[Daemon] Hot-reload complete: %d modules reloaded, %d tasks restarted",
                modules_reloaded, tasks_restarted,
            )

        return modules_reloaded, tasks_restarted

    async def _try_register_new_ingestor(self, filepath: str) -> bool:
        """Auto-register a new ingestor that follows the ingest_<name> convention.

        Only handles ingestors (simple convention). New agents post a board
        notification and require a restart.
        """
        import importlib

        module_name = filepath.replace("/", ".").removesuffix(".py")
        ingestor_name = filepath.split("/")[-1].removesuffix(".py")
        func_name = f"ingest_{ingestor_name}"
        task_name = func_name

        # Already registered?
        if task_name in self.coordinator._tasks:
            return False

        try:
            mod = importlib.import_module(module_name)
        except Exception as exc:
            log.warning("[Daemon] Failed to import new ingestor %s: %s", module_name, exc)
            return False

        func = getattr(mod, func_name, None)
        if func is None:
            log.warning(
                "[Daemon] New ingestor %s has no %s() function — skipping",
                module_name, func_name,
            )
            return False

        # Get ingestor args from coordinator (set by run.py)
        ingestor_args = getattr(self.coordinator, "_ingestor_args", None)
        if ingestor_args is None:
            log.warning("[Daemon] No _ingestor_args on coordinator — cannot register %s", task_name)
            return False

        # Register with sensible defaults
        self.coordinator.register(
            task_name, func,
            min_interval=300, max_interval=1200,
            priority=7, args=ingestor_args,
        )

        # Start the task
        import asyncio
        entry = self.coordinator._tasks[task_name]
        task = asyncio.create_task(
            self.coordinator._run_loop(entry),
            name=task_name,
        )
        self.coordinator._async_tasks[task_name] = task

        log.info("[Daemon] Auto-registered and started new ingestor: %s", task_name)
        await self._post_daemon_message(
            f"[DAEMON] Auto-registered new ingestor: {task_name}",
            {"type": "new_ingestor", "task_name": task_name, "module": module_name},
        )
        return True

    def _request_restart(self, reason: str) -> None:
        """Write restart sentinel and terminate the process so the loop wrapper restarts it.

        Rate-limited: at most one restart per 3 hours to avoid restart loops.
        """
        import time as _time
        now = _time.monotonic()
        if now - self._last_restart_request < _MIN_RESTART_GAP:
            elapsed_min = (now - self._last_restart_request) / 60
            log.info(
                "[Daemon] Restart requested (%s) but too soon (%.0f min since last) — "
                "deferring. Changes will take effect on next natural restart.",
                reason, elapsed_min,
            )
            return

        self._last_restart_request = now
        try:
            _RESTART_SENTINEL.parent.mkdir(parents=True, exist_ok=True)
            _RESTART_SENTINEL.write_text(reason)
            log.info("[Daemon] Restart sentinel written: %s", reason)
        except OSError as exc:
            log.warning("[Daemon] Failed to write restart sentinel: %s", exc)
            return

        # Send SIGINT to ourselves — triggers graceful shutdown in run.py
        # The run_loop.sh wrapper will see the sentinel file and restart
        log.info("[Daemon] Sending SIGINT for graceful restart...")
        os.kill(os.getpid(), signal.SIGINT)

    async def _check_pending_registrations(self) -> None:
        """Check for pending ingestor registrations every 3 hours.

        Claude instances write to pending_registrations.json instead of
        modifying run.py directly, avoiding disruptive restarts.
        """
        import time as _time

        now = _time.monotonic()
        if now - self._last_registration_check < 10800:  # 3 hours
            return
        self._last_registration_check = now

        if not _STAGING_PATH.exists():
            return

        try:
            pending = json.loads(_STAGING_PATH.read_text())
            if not isinstance(pending, list) or not pending:
                return
        except (json.JSONDecodeError, OSError):
            return

        registered = 0
        for entry in pending:
            task_name = entry.get("task_name", "")
            module_path = entry.get("module", "")
            func_name = entry.get("func", "")
            source_names = entry.get("source_names", [])
            interval = entry.get("interval", 1800)

            if not task_name or not module_path or not func_name:
                continue
            if task_name in self.coordinator._tasks:
                continue  # already registered

            try:
                import importlib
                mod = importlib.import_module(module_path)
                func = getattr(mod, func_name, None)
                if not func:
                    log.warning("[Daemon] Pending registration %s: no %s() in %s", task_name, func_name, module_path)
                    continue

                ingestor_args = getattr(self.coordinator, "_ingestor_args", None)
                if not ingestor_args:
                    continue

                self.coordinator.register(
                    task_name, func,
                    min_interval=interval * 0.5, max_interval=interval * 2,
                    priority=7, args=ingestor_args,
                    source_names=source_names,
                )

                # Start the task
                entry_obj = self.coordinator._tasks[task_name]
                task = asyncio.create_task(
                    self.coordinator._run_loop(entry_obj),
                    name=task_name,
                )
                self.coordinator._async_tasks[task_name] = task
                registered += 1
                log.info("[Daemon] Registered pending ingestor: %s", task_name)
            except Exception as exc:
                log.warning("[Daemon] Failed to register pending %s: %s", task_name, exc)

        if registered:
            await self._post_daemon_message(
                f"[DAEMON] Registered {registered} pending ingestor(s) from staging file",
                {"type": "pending_registration", "count": registered},
            )

        # Clear the staging file
        try:
            _STAGING_PATH.write_text("[]")
        except OSError:
            pass

    async def _post_daemon_message(self, content: str, data: dict) -> None:
        """Post a message to #meta from the daemon."""
        try:
            from .models import AgentMessage
            msg = AgentMessage(
                from_agent="daemon",
                channel="#meta",
                content=content,
                data={"type": "daemon_action", **data},
                priority=3,
                ttl_hours=24,
            )
            await self.board.post(msg)
        except Exception as exc:
            log.warning("[Daemon] Failed to post message: %s", exc)
