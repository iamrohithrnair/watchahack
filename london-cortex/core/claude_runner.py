"""Claude Code CLI instance manager.

Spawns and manages `claude` CLI instances as async subprocesses.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import DATA_DIR, LOG_DIR
from .models import _uuid

log = logging.getLogger("cortex.claude_runner")

_CLAUDE_LOGS_DIR = LOG_DIR / "claude_runs"


class ClaudeInstance:
    """Handle for a running Claude Code CLI instance."""

    def __init__(self, instance_id: str, prompt: str, process: asyncio.subprocess.Process):
        self.instance_id = instance_id
        self.prompt = prompt
        self.process = process
        self.started_at = datetime.now(timezone.utc)
        self.completed_at: datetime | None = None
        self.stdout_lines: list[str] = []
        self.stderr_lines: list[str] = []
        self.return_code: int | None = None

    @property
    def is_running(self) -> bool:
        return self.process.returncode is None

    @property
    def log_path(self) -> Path:
        return _CLAUDE_LOGS_DIR / f"{self.instance_id}.log"


class ClaudeRunner:
    """Utility for spawning and managing Claude Code CLI instances."""

    def __init__(self, max_concurrent: int = 3):
        self._max_concurrent = max_concurrent
        self._instances: dict[str, ClaudeInstance] = {}
        _CLAUDE_LOGS_DIR.mkdir(parents=True, exist_ok=True)

    async def spawn(
        self,
        prompt: str,
        context_files: list[str] | None = None,
        timeout: int = 300,
    ) -> ClaudeInstance | None:
        """Spawn a Claude Code CLI instance. Returns handle or None if at capacity."""
        # Check capacity
        active = [i for i in self._instances.values() if i.is_running]
        if len(active) >= self._max_concurrent:
            log.warning(
                "Claude runner at capacity (%d/%d). Skipping spawn.",
                len(active), self._max_concurrent,
            )
            return None

        instance_id = _uuid()

        # Build prompt with context
        full_prompt = prompt
        if context_files:
            file_list = "\n".join(f"- {f}" for f in context_files)
            full_prompt = f"Relevant files:\n{file_list}\n\n{prompt}"

        # Build command
        cmd = [
            "claude", "-p",
            "--dangerously-skip-permissions",
            "--no-session-persistence",
            full_prompt,
        ]

        # Build clean env: unset CLAUDECODE to allow spawning from within a session
        env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                cwd=str(Path(__file__).resolve().parent.parent.parent),
                env=env,
            )
        except FileNotFoundError:
            log.error("'claude' CLI not found in PATH. Cannot spawn instance.")
            return None
        except Exception as exc:
            log.error("Failed to spawn Claude instance: %s", exc)
            return None

        instance = ClaudeInstance(instance_id, prompt, process)
        self._instances[instance_id] = instance

        # Start async monitoring
        asyncio.create_task(
            self._monitor_instance(instance, timeout),
            name=f"claude_monitor:{instance_id[:8]}",
        )

        log.info(
            "Spawned Claude instance %s: %s",
            instance_id[:8], prompt[:100],
        )
        return instance

    async def _monitor_instance(self, instance: ClaudeInstance, timeout: int) -> None:
        """Monitor a Claude instance, collect output, enforce timeout."""
        try:
            stdout_data, stderr_data = await asyncio.wait_for(
                instance.process.communicate(),
                timeout=timeout,
            )
            instance.stdout_lines = (stdout_data or b"").decode(errors="replace").splitlines()
            instance.stderr_lines = (stderr_data or b"").decode(errors="replace").splitlines()
            instance.return_code = instance.process.returncode
        except asyncio.TimeoutError:
            log.warning("Claude instance %s timed out after %ds", instance.instance_id[:8], timeout)
            instance.process.kill()
            try:
                await instance.process.wait()
            except Exception:
                pass
            instance.return_code = -1
            instance.stderr_lines.append(f"TIMEOUT after {timeout}s")
        except Exception as exc:
            log.error("Error monitoring Claude instance %s: %s", instance.instance_id[:8], exc)
            instance.return_code = -2

        instance.completed_at = datetime.now(timezone.utc)

        # Write log file
        try:
            log_content = {
                "instance_id": instance.instance_id,
                "prompt": instance.prompt,
                "started_at": instance.started_at.isoformat(),
                "completed_at": instance.completed_at.isoformat(),
                "return_code": instance.return_code,
                "stdout": instance.stdout_lines,
                "stderr": instance.stderr_lines,
            }
            instance.log_path.write_text(json.dumps(log_content, indent=2))
        except Exception as exc:
            log.warning("Failed to write Claude instance log: %s", exc)

        log.info(
            "Claude instance %s completed (rc=%s, %d stdout lines)",
            instance.instance_id[:8],
            instance.return_code,
            len(instance.stdout_lines),
        )

    def active_instances(self) -> list[ClaudeInstance]:
        """List currently running instances."""
        return [i for i in self._instances.values() if i.is_running]

    def kill(self, instance_id: str) -> bool:
        """Terminate a running instance."""
        instance = self._instances.get(instance_id)
        if instance and instance.is_running:
            instance.process.kill()
            log.info("Killed Claude instance %s", instance_id[:8])
            return True
        return False

    def get_instance(self, instance_id: str) -> ClaudeInstance | None:
        return self._instances.get(instance_id)

    def cleanup_old(self, max_kept: int = 50) -> None:
        """Remove old completed instances from memory."""
        completed = [
            (iid, inst) for iid, inst in self._instances.items()
            if not inst.is_running
        ]
        completed.sort(key=lambda x: x[1].completed_at or x[1].started_at)
        for iid, _ in completed[:-max_kept]:
            del self._instances[iid]
