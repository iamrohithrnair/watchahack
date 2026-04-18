"""MemoryManager — rolling baselines, long-term JSON memory files."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .config import ANOMALY_Z_THRESHOLD, MEMORY_DIR

log = logging.getLogger("cortex.memory")


class MemoryManager:
    """Manages rolling baselines (working memory) and persistent JSON (long-term)."""

    def __init__(self, memory_dir: str | None = None):
        self._dir = Path(memory_dir or MEMORY_DIR)
        self._dir.mkdir(parents=True, exist_ok=True)
        # Working memory: rolling EMA baselines keyed by (source, location_id, metric)
        self._baselines: dict[tuple[str, str, str], dict[str, float]] = defaultdict(
            lambda: {"mean": 0.0, "var": 1.0, "count": 0}
        )
        self._alpha = 0.05  # EMA smoothing factor

    def update_baseline(self, source: str, location_id: str, metric: str, value: float) -> float:
        """Update rolling EMA baseline, return z-score."""
        key = (source, location_id, metric)
        b = self._baselines[key]
        b["count"] += 1
        if b["count"] == 1:
            b["mean"] = value
            b["var"] = 0.0
            return 0.0
        if b["count"] < 5:
            # Warmup: simple running average
            old_mean = b["mean"]
            b["mean"] = old_mean + (value - old_mean) / b["count"]
            b["var"] = b["var"] + (value - old_mean) * (value - b["mean"])
            std = (b["var"] / (b["count"] - 1)) ** 0.5 if b["count"] > 1 else 1.0
            return (value - b["mean"]) / std if std > 0 else 0.0
        if b["count"] == 5:
            # Convert Welford's sum of squared deviations to variance
            b["var"] = b["var"] / (b["count"] - 1)
        # EMA update
        alpha = self._alpha
        old_mean = b["mean"]
        b["mean"] = alpha * value + (1 - alpha) * old_mean
        b["var"] = alpha * (value - b["mean"]) ** 2 + (1 - alpha) * b["var"]
        std = b["var"] ** 0.5
        return (value - b["mean"]) / std if std > 0 else 0.0

    def is_anomalous(self, z_score: float) -> bool:
        return abs(z_score) >= ANOMALY_Z_THRESHOLD

    # ── Long-term Memory (JSON files) ─────────────────────────────────────────

    def _path(self, name: str) -> Path:
        return self._dir / name

    def load_json(self, name: str) -> Any:
        p = self._path(name)
        if p.exists():
            return json.loads(p.read_text())
        return {}

    def save_json(self, name: str, data: Any) -> None:
        self._path(name).write_text(json.dumps(data, indent=2, default=str))

    def update_connection_registry(self, source_a: str, source_b: str, hit: bool) -> None:
        """Track hit rate per source combination."""
        registry = self.load_json("connection_registry.json")
        key = f"{source_a}|{source_b}"
        if key not in registry:
            registry[key] = {"hits": 0, "misses": 0, "total": 0}
        registry[key]["total"] += 1
        if hit:
            registry[key]["hits"] += 1
        else:
            registry[key]["misses"] += 1
        self.save_json("connection_registry.json", registry)

    def add_learned_correlation(self, correlation_data: dict[str, Any]) -> None:
        correlations = self.load_json("learned_correlations.json")
        if not isinstance(correlations, list):
            correlations = []
        correlations.append({
            **correlation_data,
            "discovered_at": datetime.now(timezone.utc).isoformat(),
        })
        # Keep last 1000
        correlations = correlations[-1000:]
        self.save_json("learned_correlations.json", correlations)

    def log_backtesting_result(self, result: dict[str, Any]) -> None:
        results = self.load_json("backtesting_results.json")
        if not isinstance(results, list):
            results = []
        results.append({
            **result,
            "checked_at": datetime.now(timezone.utc).isoformat(),
        })
        results = results[-1000:]
        self.save_json("backtesting_results.json", results)

    def get_camera_scores(self) -> dict[str, float]:
        return self.load_json("camera_scores.json")

    # ── Baseline Persistence ─────────────────────────────────────────────────

    async def persist_baselines(self, db) -> int:
        """Persist in-memory baselines to the database. Returns count saved."""
        count = 0
        now = datetime.now(timezone.utc).isoformat()
        for (source, location_id, metric), b in self._baselines.items():
            if b["count"] < 2:
                continue  # skip warmup entries
            key = f"{source}|{location_id}|{metric}"
            try:
                await db.execute(
                    """INSERT OR REPLACE INTO baselines (key, mean, var, count, updated_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (key, b["mean"], b["var"], int(b["count"]), now),
                )
                count += 1
            except Exception as exc:
                log.warning("Failed to persist baseline %s: %s", key, exc)
        if count:
            await db.commit()
        log.info("Persisted %d baselines to database.", count)
        return count

    async def restore_baselines(self, db) -> int:
        """Restore baselines from database into memory. Returns count restored."""
        try:
            cursor = await db.execute("SELECT key, mean, var, count FROM baselines")
            rows = await cursor.fetchall()
        except Exception as exc:
            log.warning("Failed to restore baselines: %s", exc)
            return 0

        count = 0
        for row in rows:
            key_str = row["key"] if isinstance(row, dict) else row[0]
            parts = key_str.split("|", 2)
            if len(parts) != 3:
                log.warning("Skipping malformed baseline key: %s", key_str[:80])
                continue
            source, location_id, metric = parts
            mean_val = row["mean"] if isinstance(row, dict) else row[1]
            var_val = row["var"] if isinstance(row, dict) else row[2]
            count_val = row["count"] if isinstance(row, dict) else row[3]
            self._baselines[(source, location_id, metric)] = {
                "mean": float(mean_val),
                "var": float(var_val),
                "count": int(count_val),
            }
            count += 1

        log.info("Restored %d baselines from database.", count)
        return count

    async def snapshot_daily_baselines(self, db) -> int:
        """Snapshot current baselines into baseline_history table. Called once per day."""
        count = 0
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        for (source, location_id, metric), b in self._baselines.items():
            if b["count"] < 5:
                continue
            key = f"{source}|{location_id}|{metric}"
            try:
                await db.execute(
                    """INSERT OR REPLACE INTO baseline_history (key, date, mean, var, count)
                       VALUES (?, ?, ?, ?, ?)""",
                    (key, today, b["mean"], b["var"], int(b["count"])),
                )
                count += 1
            except Exception as exc:
                log.warning("Failed to snapshot baseline %s: %s", key, exc)
        if count:
            await db.commit()
        log.info("Snapshotted %d baselines for %s.", count, today)
        return count

    def update_camera_score(self, camera_id: str, interesting: bool) -> None:
        scores = self.get_camera_scores()
        if camera_id not in scores:
            scores[camera_id] = 0.5
        alpha = 0.1
        scores[camera_id] = alpha * (1.0 if interesting else 0.0) + (1 - alpha) * scores[camera_id]
        self.save_json("camera_scores.json", scores)

    # ── Camera Health Tracking ────────────────────────────────────────────────

    def get_camera_health(self) -> dict[str, Any]:
        return self.load_json("camera_health.json")

    def update_camera_health(self, camera_id: str, success: bool) -> None:
        health = self.get_camera_health()
        now = datetime.now(timezone.utc).isoformat()
        if camera_id not in health:
            health[camera_id] = {
                "consecutive_failures": 0,
                "total_successes": 0,
                "total_failures": 0,
                "last_success": None,
                "last_failure": None,
                "status": "active",
                "last_checked": now,
            }
        cam = health[camera_id]
        cam["last_checked"] = now
        if success:
            cam["consecutive_failures"] = 0
            cam["total_successes"] = cam.get("total_successes", 0) + 1
            cam["last_success"] = now
        else:
            cam["consecutive_failures"] = cam.get("consecutive_failures", 0) + 1
            cam["total_failures"] = cam.get("total_failures", 0) + 1
            cam["last_failure"] = now
        # Auto-compute status — failure count + time-based staleness
        cf = cam["consecutive_failures"]
        hours_since_success = None
        if cam.get("last_success"):
            try:
                last_ok = datetime.fromisoformat(cam["last_success"])
                hours_since_success = (datetime.now(timezone.utc) - last_ok).total_seconds() / 3600
            except (ValueError, TypeError):
                pass

        if cf >= 10 or (hours_since_success is not None and hours_since_success > 6):
            cam["status"] = "dead"
        elif cf >= 3 or (hours_since_success is not None and hours_since_success > 2):
            cam["status"] = "suspect"
        else:
            cam["status"] = "active"
        self.save_json("camera_health.json", health)

    def get_active_cameras(self) -> set[str]:
        health = self.get_camera_health()
        return {cid for cid, info in health.items() if info.get("status") != "dead"}

    def get_camera_summary(self) -> dict[str, Any]:
        health = self.get_camera_health()
        counts = {"active": 0, "suspect": 0, "dead": 0}
        dead_cameras = []
        for cid, info in health.items():
            status = info.get("status", "active")
            counts[status] = counts.get(status, 0) + 1
            if status == "dead":
                dead_cameras.append({
                    "camera_id": cid,
                    "last_success": info.get("last_success"),
                    "consecutive_failures": info.get("consecutive_failures", 0),
                })
        scores = self.get_camera_scores()
        top_active = sorted(
            ((cid, score) for cid, score in scores.items()
             if health.get(cid, {}).get("status", "active") != "dead"),
            key=lambda x: x[1],
            reverse=True,
        )[:10]
        return {
            "counts": counts,
            "total_tracked": len(health),
            "dead_cameras": dead_cameras,
            "top_active": [{"camera_id": cid, "score": score} for cid, score in top_active],
        }
