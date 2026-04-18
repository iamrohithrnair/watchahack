"""MessageBoard — SQLite-backed agent communication hub."""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

import asyncio

import aiosqlite

from .config import DB_PATH, DEFAULT_MESSAGE_TTL_HOURS
from .models import AgentConversation, AgentMessage, Anomaly, Connection, Observation

log = logging.getLogger("cortex.board")


class MessageBoard:
    """Central message board — all agents read/write through this."""

    def __init__(self, db_path: str | None = None):
        self._db_path = str(db_path or DB_PATH)
        self._db: aiosqlite.Connection | None = None
        self._flush_task: asyncio.Task | None = None
        self._retina = None  # AttentionManager — set via set_retina()

    async def init(self) -> None:
        # timeout=10 sets SQLite's busy timeout at the C level — it will wait
        # up to 10s for a lock instead of failing immediately. This is critical
        # because even PRAGMA statements need a lock.
        self._db = await aiosqlite.connect(self._db_path, timeout=10)
        self._db.row_factory = aiosqlite.Row
        try:
            await self._db.execute("PRAGMA journal_mode=WAL")
        except Exception as exc:
            log.warning("Could not set WAL mode (non-fatal): %s", exc)
        await self._create_tables()
        self._flush_task = asyncio.create_task(self._auto_flush())
        log.info("MessageBoard initialized at %s", self._db_path)

    def get_db(self) -> aiosqlite.Connection:
        """Return the database connection, raising if not initialized."""
        if not self._db:
            raise RuntimeError("Board not initialized — call board.init() first")
        return self._db

    async def _ensure_connection(self) -> None:
        """Reconnect to SQLite if the connection has been lost."""
        if self._db is not None:
            try:
                # Quick health check — if this fails, the connection is dead
                await self._db.execute("SELECT 1")
                return
            except Exception:
                log.warning("SQLite connection lost, reconnecting...")
                try:
                    await self._db.close()
                except Exception:
                    pass
                self._db = None

        self._db = await aiosqlite.connect(self._db_path, timeout=10)
        self._db.row_factory = aiosqlite.Row
        try:
            await self._db.execute("PRAGMA journal_mode=WAL")
        except Exception:
            pass
        log.info("SQLite connection re-established")

    async def flush(self) -> None:
        """Commit any pending writes to the database."""
        if self._db:
            await self._db.commit()

    async def _auto_flush(self) -> None:
        """Background task that commits pending writes every 2 seconds."""
        try:
            while True:
                await asyncio.sleep(2)
                if self._db:
                    try:
                        await self._db.commit()
                    except Exception:
                        log.warning("Auto-flush commit failed", exc_info=True)
        except asyncio.CancelledError:
            pass

    # Desired schema: table -> [(column, type, default), ...]
    # Used by _create_tables and _migrate_schema to converge existing DBs.
    _SCHEMA: dict[str, list[tuple[str, str, str | None]]] = {
        "messages": [
            ("id", "TEXT PRIMARY KEY", None),
            ("timestamp", "TEXT NOT NULL", None),
            ("from_agent", "TEXT NOT NULL", None),
            ("to_agent", "TEXT", None),
            ("channel", "TEXT NOT NULL", None),
            ("priority", "INTEGER", "1"),
            ("content", "TEXT NOT NULL", None),
            ("data", "TEXT", "'{}'"),
            ("references_json", "TEXT", "'[]'"),
            ("location_id", "TEXT", None),
            ("ttl_hours", "INTEGER", "6"),
        ],
        "observations": [
            ("id", "TEXT PRIMARY KEY", None),
            ("timestamp", "TEXT NOT NULL", None),
            ("source", "TEXT NOT NULL", None),
            ("obs_type", "TEXT NOT NULL", None),
            ("value", "TEXT", None),
            ("location_id", "TEXT", None),
            ("lat", "REAL", None),
            ("lon", "REAL", None),
            ("metadata", "TEXT", "'{}'"),
        ],
        "anomalies": [
            ("id", "TEXT PRIMARY KEY", None),
            ("timestamp", "TEXT NOT NULL", None),
            ("source", "TEXT NOT NULL", None),
            ("observation_id", "TEXT", None),
            ("description", "TEXT NOT NULL", None),
            ("z_score", "REAL", "0"),
            ("location_id", "TEXT", None),
            ("lat", "REAL", None),
            ("lon", "REAL", None),
            ("severity", "INTEGER", "1"),
            ("metadata", "TEXT", "'{}'"),
            ("ttl_hours", "INTEGER", "6"),
        ],
        "connections": [
            ("id", "TEXT PRIMARY KEY", None),
            ("timestamp", "TEXT NOT NULL", None),
            ("source_a", "TEXT NOT NULL", None),
            ("source_b", "TEXT NOT NULL", None),
            ("description", "TEXT NOT NULL", None),
            ("confidence", "REAL", "0.5"),
            ("evidence", "TEXT", "'[]'"),
            ("prediction", "TEXT", None),
            ("prediction_deadline", "TEXT", None),
            ("prediction_outcome", "INTEGER", None),
            ("location_id", "TEXT", None),
            ("metadata", "TEXT", "'{}'"),
            ("relationship_type", "TEXT", "'correlation'"),
            ("chain_id", "TEXT", None),
            ("is_permanent", "INTEGER", "0"),
        ],
        "baselines": [
            ("key", "TEXT PRIMARY KEY", None),
            ("mean", "REAL", None),
            ("var", "REAL", None),
            ("count", "INTEGER", None),
            ("updated_at", "TEXT", None),
        ],
        "conversations": [
            ("id", "TEXT PRIMARY KEY", None),
            ("timestamp", "TEXT NOT NULL", None),
            ("agent_name", "TEXT NOT NULL", None),
            ("trigger", "TEXT", None),
            ("model_used", "TEXT", None),
            ("prompt", "TEXT", None),
            ("response", "TEXT", None),
            ("tokens_used", "INTEGER", "0"),
            ("messages_read", "TEXT", "'[]'"),
            ("actions_taken", "TEXT", "'[]'"),
            ("posted_messages", "TEXT", "'[]'"),
        ],
        "investigations": [
            ("id", "TEXT PRIMARY KEY", None),
            ("thread_id", "TEXT NOT NULL", None),
            ("step_number", "INTEGER NOT NULL", None),
            ("agent_name", "TEXT NOT NULL", None),
            ("action", "TEXT NOT NULL", None),
            ("finding", "TEXT", None),
            ("next_question", "TEXT", None),
            ("next_agent", "TEXT", None),
            ("data", "TEXT", "'{}'"),
            ("timestamp", "TEXT NOT NULL", None),
        ],
        "baseline_history": [
            ("key", "TEXT NOT NULL", None),
            ("date", "TEXT NOT NULL", None),
            ("mean", "REAL", None),
            ("var", "REAL", None),
            ("count", "INTEGER", None),
        ],
    }

    _INDEXES = [
        "CREATE INDEX IF NOT EXISTS idx_messages_channel ON messages(channel)",
        "CREATE INDEX IF NOT EXISTS idx_messages_timestamp ON messages(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_messages_location ON messages(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_messages_to_agent ON messages(to_agent)",
        "CREATE INDEX IF NOT EXISTS idx_obs_source ON observations(source)",
        "CREATE INDEX IF NOT EXISTS idx_obs_location ON observations(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_obs_timestamp ON observations(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_anom_location ON anomalies(location_id)",
        "CREATE INDEX IF NOT EXISTS idx_anom_timestamp ON anomalies(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_conn_chain ON connections(chain_id)",
        "CREATE INDEX IF NOT EXISTS idx_conn_sources ON connections(source_a, source_b)",
        "CREATE INDEX IF NOT EXISTS idx_conv_agent ON conversations(agent_name)",
        "CREATE INDEX IF NOT EXISTS idx_inv_thread ON investigations(thread_id)",
        "CREATE INDEX IF NOT EXISTS idx_inv_next_agent ON investigations(next_agent)",
    ]

    async def _create_tables(self) -> None:
        if not self._db:
            raise RuntimeError("Board not initialized")

        # Create tables with full schema
        for table, columns in self._SCHEMA.items():
            col_defs = []
            for col_name, col_type, default in columns:
                defn = f"{col_name} {col_type}"
                if default is not None:
                    defn += f" DEFAULT {default}"
                col_defs.append(defn)
            # baseline_history has a composite primary key
            if table == "baseline_history":
                col_defs.append("PRIMARY KEY (key, date)")
            sql = f"CREATE TABLE IF NOT EXISTS {table} ({', '.join(col_defs)})"
            await self._db.execute(sql)

        # Migrate: add any missing columns to existing tables
        await self._migrate_schema()

        # Create indexes
        for idx_sql in self._INDEXES:
            await self._db.execute(idx_sql)

        await self._db.commit()

    async def _migrate_schema(self) -> None:
        """Add missing columns to existing tables via ALTER TABLE ADD COLUMN."""
        if not self._db:
            return
        for table, columns in self._SCHEMA.items():
            cursor = await self._db.execute(f"PRAGMA table_info({table})")
            existing = {row[1] for row in await cursor.fetchall()}
            for col_name, col_type, default in columns:
                if col_name in existing:
                    continue
                # Strip NOT NULL for ALTER TABLE (SQLite requires a default for new NOT NULL cols)
                safe_type = col_type.replace(" NOT NULL", "").replace(" PRIMARY KEY", "")
                alter = f"ALTER TABLE {table} ADD COLUMN {col_name} {safe_type}"
                if default is not None:
                    alter += f" DEFAULT {default}"
                try:
                    await self._db.execute(alter)
                    log.info("Migrated: %s.%s added (%s)", table, col_name, safe_type)
                except Exception as exc:
                    log.warning("Migration failed for %s.%s: %s", table, col_name, exc)

    # ── Post / Read Messages ──────────────────────────────────────────────────

    async def post(self, msg: AgentMessage) -> str:
        await self._ensure_connection()
        await self._db.execute(
            """INSERT INTO messages (id, timestamp, from_agent, to_agent, channel, priority,
               content, data, references_json, location_id, ttl_hours)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (msg.id, msg.timestamp.isoformat(), msg.from_agent, msg.to_agent,
             msg.channel, msg.priority, msg.content, json.dumps(msg.data),
             json.dumps(msg.references), msg.location_id, msg.ttl_hours),
        )
        log.debug("Posted message %s to %s from %s", msg.id, msg.channel, msg.from_agent)
        return msg.id

    async def read_channel(
        self, channel: str, since: datetime | None = None, limit: int = 100
    ) -> list[dict[str, Any]]:
        await self._ensure_connection()
        since = since or (datetime.now(timezone.utc) - timedelta(hours=DEFAULT_MESSAGE_TTL_HOURS))
        cursor = await self._db.execute(
            """SELECT * FROM messages WHERE channel = ? AND timestamp > ?
               ORDER BY timestamp DESC LIMIT ?""",
            (channel, since.isoformat(), limit),
        )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def read_location(
        self, location_id: str, since_hours: float = 2.0, limit: int = 50
    ) -> list[dict[str, Any]]:
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cursor = await self._db.execute(
            """SELECT * FROM messages WHERE location_id = ? AND timestamp > ?
               ORDER BY timestamp DESC LIMIT ?""",
            (location_id, since.isoformat(), limit),
        )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def get_message(self, msg_id: str) -> dict[str, Any] | None:
        if not self._db:
            raise RuntimeError("Board not initialized")
        cursor = await self._db.execute("SELECT * FROM messages WHERE id = ?", (msg_id,))
        row = await cursor.fetchone()
        return self._row_to_dict(row) if row else None

    def set_retina(self, retina) -> None:
        """Attach the AttentionManager for perceptual compression."""
        self._retina = retina

    # ── Observations ──────────────────────────────────────────────────────────

    async def store_observation(self, obs: Observation) -> str | None:
        # Retina gate: peripheral observations are compressed (not stored)
        # Caller should still run z-scores on all observations regardless
        if self._retina and not self._retina.should_store_observation(obs):
            return None
        await self._ensure_connection()
        value_str = json.dumps(obs.value) if not isinstance(obs.value, str) else obs.value
        await self._db.execute(
            """INSERT INTO observations (id, timestamp, source, obs_type, value,
               location_id, lat, lon, metadata) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (obs.id, obs.timestamp.isoformat(), obs.source, obs.obs_type.value,
             value_str, obs.location_id, obs.lat, obs.lon, json.dumps(obs.metadata)),
        )
        return obs.id

    async def get_observations(
        self, source: str, location_id: str | None = None,
        since_hours: float = 24.0, limit: int = 200
    ) -> list[dict[str, Any]]:
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        if location_id:
            cursor = await self._db.execute(
                """SELECT * FROM observations WHERE source = ? AND location_id = ?
                   AND timestamp > ? ORDER BY timestamp DESC LIMIT ?""",
                (source, location_id, since.isoformat(), limit),
            )
        else:
            cursor = await self._db.execute(
                """SELECT * FROM observations WHERE source = ? AND timestamp > ?
                   ORDER BY timestamp DESC LIMIT ?""",
                (source, since.isoformat(), limit),
            )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    async def get_recent_observations_multi(
        self, sources: list[str], location_id: str | None = None,
        since_hours: float = 2.0, limit: int = 200
    ) -> list[dict[str, Any]]:
        """Fetch observations from multiple sources at once."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        placeholders = ", ".join("?" for _ in sources)
        if location_id:
            cursor = await self._db.execute(
                f"""SELECT * FROM observations WHERE source IN ({placeholders})
                    AND location_id = ? AND timestamp > ?
                    ORDER BY timestamp DESC LIMIT ?""",
                (*sources, location_id, since.isoformat(), limit),
            )
        else:
            cursor = await self._db.execute(
                f"""SELECT * FROM observations WHERE source IN ({placeholders})
                    AND timestamp > ? ORDER BY timestamp DESC LIMIT ?""",
                (*sources, since.isoformat(), limit),
            )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ── Anomalies ──────────────────────────────────────────────────────────────

    async def store_anomaly(self, anomaly: Anomaly) -> str:
        if not self._db:
            raise RuntimeError("Board not initialized")
        await self._db.execute(
            """INSERT INTO anomalies (id, timestamp, source, observation_id, description,
               z_score, location_id, lat, lon, severity, metadata, ttl_hours)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (anomaly.id, anomaly.timestamp.isoformat(), anomaly.source,
             anomaly.observation_id, anomaly.description, anomaly.z_score,
             anomaly.location_id, anomaly.lat, anomaly.lon, anomaly.severity,
             json.dumps(anomaly.metadata), anomaly.ttl_hours),
        )
        # No immediate commit — let auto_flush handle it to avoid lock contention
        return anomaly.id

    async def get_active_anomalies(
        self, location_id: str | None = None, since_hours: float = 6.0
    ) -> list[dict[str, Any]]:
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        if location_id:
            cursor = await self._db.execute(
                """SELECT * FROM anomalies WHERE location_id = ? AND timestamp > ?
                   ORDER BY timestamp DESC""",
                (location_id, since.isoformat()),
            )
        else:
            cursor = await self._db.execute(
                "SELECT * FROM anomalies WHERE timestamp > ? ORDER BY timestamp DESC",
                (since.isoformat(),),
            )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ── Connections ───────────────────────────────────────────────────────────

    async def store_connection(self, conn: Connection) -> str:
        if not self._db:
            raise RuntimeError("Board not initialized")
        await self._db.execute(
            """INSERT INTO connections (id, timestamp, source_a, source_b, description,
               confidence, evidence, prediction, prediction_deadline, prediction_outcome,
               location_id, metadata, relationship_type, chain_id, is_permanent)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (conn.id, conn.timestamp.isoformat(), conn.source_a, conn.source_b,
             conn.description, conn.confidence, json.dumps(conn.evidence),
             conn.prediction, conn.prediction_deadline.isoformat() if conn.prediction_deadline else None,
             conn.prediction_outcome, conn.location_id, json.dumps(conn.metadata),
             conn.relationship_type, conn.chain_id, int(conn.is_permanent)),
        )
        return conn.id

    async def get_chain(self, chain_id: str) -> list[dict[str, Any]]:
        """Get all connections in a chain."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        cursor = await self._db.execute(
            "SELECT * FROM connections WHERE chain_id = ? ORDER BY timestamp",
            (chain_id,),
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def get_connections_involving(self, source: str, since_hours: float = 24.0) -> list[dict[str, Any]]:
        """Get connections where a source appears as source_a or source_b."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cursor = await self._db.execute(
            """SELECT * FROM connections WHERE (source_a = ? OR source_b = ?)
               AND timestamp > ? ORDER BY timestamp DESC LIMIT 100""",
            (source, source, since.isoformat()),
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def get_structural_facts(self) -> list[dict[str, Any]]:
        """Get permanent structural connections."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        cursor = await self._db.execute(
            "SELECT * FROM connections WHERE is_permanent = 1 ORDER BY timestamp DESC LIMIT 200"
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def get_recent_connections(self, since_hours: float = 6.0, limit: int = 50) -> list[dict[str, Any]]:
        """Get recent connections of any type."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cursor = await self._db.execute(
            "SELECT * FROM connections WHERE timestamp > ? ORDER BY timestamp DESC LIMIT ?",
            (since.isoformat(), limit),
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def get_pending_predictions(self) -> list[dict[str, Any]]:
        if not self._db:
            raise RuntimeError("Board not initialized")
        now = datetime.now(timezone.utc).isoformat()
        cursor = await self._db.execute(
            """SELECT * FROM connections WHERE prediction IS NOT NULL
               AND prediction_outcome IS NULL AND prediction_deadline < ?""",
            (now,),
        )
        rows = await cursor.fetchall()
        return [self._row_to_dict(r) for r in rows]

    # ── Conversations ─────────────────────────────────────────────────────────

    async def log_conversation(self, conv: AgentConversation) -> str:
        if not self._db:
            raise RuntimeError("Board not initialized")
        await self._db.execute(
            """INSERT INTO conversations (id, timestamp, agent_name, trigger, model_used,
               prompt, response, tokens_used, messages_read, actions_taken, posted_messages)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (conv.id, conv.timestamp.isoformat(), conv.agent_name, conv.trigger,
             conv.model_used, conv.prompt, conv.response, conv.tokens_used,
             json.dumps(conv.messages_read), json.dumps(conv.actions_taken),
             json.dumps(conv.posted_messages)),
        )
        return conv.id

    # ── Directed Messages ──────────────────────────────────────────────────────

    async def read_directed(self, agent_name: str, since: datetime | None = None, limit: int = 50) -> list[dict[str, Any]]:
        """Read messages directed to a specific agent."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        since = since or (datetime.now(timezone.utc) - timedelta(hours=DEFAULT_MESSAGE_TTL_HOURS))
        cursor = await self._db.execute(
            "SELECT * FROM messages WHERE to_agent = ? AND timestamp > ? ORDER BY timestamp DESC LIMIT ?",
            (agent_name, since.isoformat(), limit),
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    # ── Investigation Threads ────────────────────────────────────────────────

    async def start_investigation(
        self, agent_name: str, trigger: str, question: str, data: dict | None = None,
    ) -> str:
        """Create step 0 of a new investigation thread. Returns thread_id."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        thread_id = str(uuid.uuid4())[:12]
        step_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO investigations (id, thread_id, step_number, agent_name, action,
               finding, next_question, next_agent, data, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (step_id, thread_id, 0, agent_name, f"Triggered: {trigger}",
             None, question, None, json.dumps(data or {}), now),
        )
        log.info("Investigation started: thread=%s by %s question=%s", thread_id, agent_name, question[:80])
        return thread_id

    async def add_investigation_step(
        self, thread_id: str, agent_name: str, action: str, finding: str,
        next_question: str | None = None, next_agent: str | None = None, data: dict | None = None,
    ) -> str:
        """Add a step to an existing investigation thread."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        # Get current max step number
        cursor = await self._db.execute(
            "SELECT MAX(step_number) FROM investigations WHERE thread_id = ?",
            (thread_id,),
        )
        row = await cursor.fetchone()
        step_number = (row[0] or 0) + 1
        step_id = str(uuid.uuid4())
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """INSERT INTO investigations (id, thread_id, step_number, agent_name, action,
               finding, next_question, next_agent, data, timestamp)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (step_id, thread_id, step_number, agent_name, action,
             finding, next_question, next_agent, json.dumps(data or {}), now),
        )
        log.info("Investigation step %d added to thread=%s by %s", step_number, thread_id, agent_name)
        return step_id

    async def get_investigation(self, thread_id: str) -> list[dict[str, Any]]:
        """Get all steps of an investigation thread."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        cursor = await self._db.execute(
            "SELECT * FROM investigations WHERE thread_id = ? ORDER BY step_number",
            (thread_id,),
        )
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def get_pending_investigations(self, agent_name: str) -> list[list[dict[str, Any]]]:
        """Find investigation threads where next_agent matches and no subsequent step exists."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        # Find threads where the latest step has next_agent = agent_name
        cursor = await self._db.execute(
            """SELECT DISTINCT i1.thread_id FROM investigations i1
               WHERE i1.next_agent = ?
               AND i1.step_number = (
                   SELECT MAX(i2.step_number) FROM investigations i2
                   WHERE i2.thread_id = i1.thread_id
               )
               AND i1.timestamp > ?
               ORDER BY i1.timestamp DESC LIMIT 10""",
            (agent_name, (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()),
        )
        thread_ids = [r[0] for r in await cursor.fetchall()]

        result = []
        for tid in thread_ids:
            steps = await self.get_investigation(tid)
            if steps:
                result.append(steps)
        return result

    async def get_active_investigations(self, limit: int = 20) -> list[dict[str, Any]]:
        """Get a summary of active (incomplete) investigation threads."""
        if not self._db:
            raise RuntimeError("Board not initialized")
        cursor = await self._db.execute(
            """SELECT thread_id, COUNT(*) as steps,
                      MIN(timestamp) as started, MAX(timestamp) as last_update,
                      GROUP_CONCAT(agent_name, ' -> ') as agents
               FROM investigations
               WHERE timestamp > ?
               GROUP BY thread_id
               HAVING MAX(next_question) IS NOT NULL
               ORDER BY MAX(timestamp) DESC LIMIT ?""",
            ((datetime.now(timezone.utc) - timedelta(hours=24)).isoformat(), limit),
        )
        rows = await cursor.fetchall()
        return [dict(r) for r in rows]

    # ── Keyword Search ─────────────────────────────────────────────────────────

    async def search_messages(
        self, keyword: str, channel: str | None = None,
        location_id: str | None = None, since_hours: float = 24.0, limit: int = 50,
    ) -> list[dict[str, Any]]:
        await self._ensure_connection()
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        sql = "SELECT * FROM messages WHERE content LIKE ? AND timestamp > ?"
        params: list[Any] = [f"%{keyword}%", since.isoformat()]
        if channel:
            sql += " AND channel = ?"
            params.append(channel)
        if location_id:
            sql += " AND location_id = ?"
            params.append(location_id)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        cursor = await self._db.execute(sql, params)
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def search_anomalies(
        self, keyword: str | None = None, source: str | None = None,
        location_id: str | None = None, since_hours: float = 24.0, limit: int = 50,
    ) -> list[dict[str, Any]]:
        await self._ensure_connection()
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        sql = "SELECT * FROM anomalies WHERE timestamp > ?"
        params: list[Any] = [since.isoformat()]
        if keyword:
            sql += " AND description LIKE ?"
            params.append(f"%{keyword}%")
        if source:
            sql += " AND source = ?"
            params.append(source)
        if location_id:
            sql += " AND location_id = ?"
            params.append(location_id)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        cursor = await self._db.execute(sql, params)
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    async def search_observations(
        self, keyword: str | None = None, source: str | None = None,
        location_id: str | None = None, since_hours: float = 24.0, limit: int = 50,
    ) -> list[dict[str, Any]]:
        await self._ensure_connection()
        since = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        sql = "SELECT * FROM observations WHERE timestamp > ?"
        params: list[Any] = [since.isoformat()]
        if keyword:
            sql += " AND (value LIKE ? OR metadata LIKE ?)"
            params.extend([f"%{keyword}%", f"%{keyword}%"])
        if source:
            sql += " AND source = ?"
            params.append(source)
        if location_id:
            sql += " AND location_id = ?"
            params.append(location_id)
        sql += " ORDER BY timestamp DESC LIMIT ?"
        params.append(limit)
        cursor = await self._db.execute(sql, params)
        return [self._row_to_dict(r) for r in await cursor.fetchall()]

    # ── Cleanup ────────────────────────────────────────────────────────────────

    async def cleanup_expired(self) -> int:
        await self._ensure_connection()
        cursor = await self._db.execute(
            "DELETE FROM messages WHERE datetime(timestamp, '+' || ttl_hours || ' hours') < datetime('now')"
        )
        await self._db.commit()
        return cursor.rowcount

    async def close(self) -> None:
        if self._flush_task:
            self._flush_task.cancel()
            self._flush_task = None
        if self._db:
            try:
                await asyncio.wait_for(self._db.commit(), timeout=3.0)
            except Exception:
                pass
            try:
                await asyncio.wait_for(self._db.close(), timeout=3.0)
            except Exception:
                pass
            self._db = None

    @staticmethod
    def _row_to_dict(row: aiosqlite.Row) -> dict[str, Any]:
        d = dict(row)
        for k in ("data", "metadata", "references_json", "evidence",
                   "messages_read", "actions_taken", "posted_messages"):
            if k in d and isinstance(d[k], str):
                try:
                    d[k] = json.loads(d[k])
                except (json.JSONDecodeError, TypeError):
                    pass
        return d
