"""All dataclasses for the London Cortex."""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any


def _now() -> datetime:
    return datetime.now(timezone.utc)


def _uuid() -> str:
    return uuid.uuid4().hex[:12]


# ── Observations ───────────────────────────────────────────────────────────────

class ObservationType(str, Enum):
    IMAGE = "image"
    NUMERIC = "numeric"
    TEXT = "text"
    GEO = "geo"
    CATEGORICAL = "categorical"
    EVENT = "event"


@dataclass
class Observation:
    source: str                  # e.g. "tfl_jamcam", "laqn", "gdelt"
    obs_type: ObservationType
    value: Any                   # numeric value, text, image URL, etc.
    location_id: str | None = None  # grid cell ID
    lat: float | None = None
    lon: float | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)


# ── Anomalies ──────────────────────────────────────────────────────────────────

@dataclass
class Anomaly:
    source: str
    observation_id: str
    description: str
    z_score: float = 0.0
    location_id: str | None = None
    lat: float | None = None
    lon: float | None = None
    severity: int = 1            # 1-5
    metadata: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)
    ttl_hours: int = 6


# ── Agent Messages ─────────────────────────────────────────────────────────────

@dataclass
class AgentMessage:
    from_agent: str
    channel: str                 # "#raw", "#anomalies", etc.
    content: str
    data: dict[str, Any] = field(default_factory=dict)
    to_agent: str | None = None
    priority: int = 1            # 1=low, 5=urgent
    references: list[str] = field(default_factory=list)
    location_id: str | None = None
    ttl_hours: int = 6
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)


# ── Agent Conversation Log ────────────────────────────────────────────────────

@dataclass
class AgentConversation:
    agent_name: str
    trigger: str
    model_used: str
    prompt: str
    response: str
    tokens_used: int = 0
    messages_read: list[str] = field(default_factory=list)
    actions_taken: list[str] = field(default_factory=list)
    posted_messages: list[str] = field(default_factory=list)
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)


# ── Connections / Hypotheses ──────────────────────────────────────────────────

@dataclass
class Connection:
    source_a: str
    source_b: str
    description: str
    confidence: float = 0.5       # 0-1
    evidence: list[str] = field(default_factory=list)
    prediction: str | None = None
    prediction_deadline: datetime | None = None
    prediction_outcome: bool | None = None
    location_id: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)
    relationship_type: str = "correlation"  # "hypothesis" | "structural" | "correlation" | "chain"
    chain_id: str | None = None             # links connections into multi-hop chains
    is_permanent: bool = False              # structural facts don't expire
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)


# ── Grid Cell ─────────────────────────────────────────────────────────────────

@dataclass
class GridCell:
    id: str
    lat_min: float
    lat_max: float
    lon_min: float
    lon_max: float
    borough: str | None = None
    center_lat: float = 0.0
    center_lon: float = 0.0

    def __post_init__(self):
        self.center_lat = (self.lat_min + self.lat_max) / 2
        self.center_lon = (self.lon_min + self.lon_max) / 2


# ── Learned Correlation ──────────────────────────────────────────────────────

# ── Anomaly Cluster ──────────────────────────────────────────────────────────

@dataclass
class AnomalyCluster:
    """A group of related anomalies from the same source/location/time window."""
    cluster_id: str = field(default_factory=_uuid)
    source: str = ""
    location_id: str | None = None
    description: str = ""
    anomaly_ids: list[str] = field(default_factory=list)
    count: int = 0
    n_sources: int = 1
    mean_severity: float = 1.0
    max_severity: int = 1
    first_seen: datetime = field(default_factory=_now)
    last_seen: datetime = field(default_factory=_now)
    metadata: dict[str, Any] = field(default_factory=dict)


# ── Grounding Report ────────────────────────────────────────────────────────

@dataclass
class GroundingReport:
    """Hard facts about an anomaly cluster — pure code logic, no LLM."""
    cluster_id: str = ""
    n_independent_sources: int = 1
    is_single_source: bool = True
    base_rate: float = 0.0  # how often this type occurs (0-1)
    mundane_explanation: str = ""
    age_minutes: float = 0.0
    similar_past_events: int = 0
    source_reliability: float = 0.5

    @property
    def should_suppress(self) -> bool:
        """Suppress only truly mundane high-base-rate events.

        Single-source anomalies with mundane explanations are NOT suppressed
        — they are passed through as low-confidence curiosity items so the
        Brain can still notice genuinely odd single-source events.
        Only suppress when the base rate is very high (routine noise).
        """
        if self.base_rate > 0.40 and self.mundane_explanation:
            return True
        return False

    @property
    def max_expressible_severity(self) -> int:
        """Cap severity based on source count."""
        if self.n_independent_sources <= 1:
            return 3
        if self.n_independent_sources == 2:
            return 4
        return 5

    def as_context_for_llm(self) -> str:
        """Inject grounding FACTS into prompt so LLM can't ignore them."""
        lines = [
            "=== GROUNDING FACTS (from code analysis, NOT negotiable) ===",
            f"Independent sources confirming this: {self.n_independent_sources}",
            f"Single-source anomaly: {self.is_single_source}",
            f"Source reliability score: {self.source_reliability:.2f}",
        ]
        if self.mundane_explanation:
            lines.append(f"Most likely mundane explanation: {self.mundane_explanation}")
        if self.base_rate > 0:
            lines.append(f"Historical base rate (how often this happens): {self.base_rate:.0%}")
        if self.similar_past_events > 0:
            lines.append(f"Similar events in recent history: {self.similar_past_events}")
        lines.append(f"Maximum severity you may assign: {self.max_expressible_severity}")
        lines.append(f"Age of oldest observation: {self.age_minutes:.0f} minutes")
        if self.should_suppress:
            lines.append("WARNING: This cluster should be treated as ROUTINE, not alarming.")
        lines.append("=== END GROUNDING FACTS ===")
        return "\n".join(lines)


# ── Coordinator models ──────────────────────────────────────────────────────

@dataclass
class TaskEntry:
    """A registered task in the coordinator."""
    name: str
    func: Any  # Callable
    min_interval: float = 60.0
    max_interval: float = 600.0
    current_interval: float = 300.0
    priority: int = 5  # 1=highest, 10=lowest
    last_run: datetime | None = None
    last_success: datetime | None = None
    consecutive_errors: int = 0
    args: tuple = ()
    kwargs: dict[str, Any] = field(default_factory=dict)
    module_name: str = ""   # e.g. "cortex.agents.interpreters"
    func_name: str = ""     # e.g. "run_vision_interpreter"


@dataclass
class CoordinatorEvent:
    """An event in the coordinator's event queue."""
    event_type: str  # "data_request", "anomaly_detected", "error_backoff"
    source: str = ""
    reason: str = ""
    severity: int = 1
    timestamp: datetime = field(default_factory=_now)
    data: dict[str, Any] = field(default_factory=dict)


# ── Daemon models ───────────────────────────────────────────────────────────

@dataclass
class DaemonAction:
    """Record of a daemon action (e.g. spawning Claude Code)."""
    action_id: str = field(default_factory=_uuid)
    trigger: str = ""
    action_type: str = ""  # "spawn_claude", "restart_task", "log_anomaly"
    prompt: str = ""
    context_files: list[str] = field(default_factory=list)
    outcome: str = ""
    started_at: datetime = field(default_factory=_now)
    completed_at: datetime | None = None
    instance_id: str = ""


# ── Learned Correlation ──────────────────────────────────────────────────────

@dataclass
class LearnedCorrelation:
    source_a: str
    source_b: str
    metric_a: str
    metric_b: str
    correlation: float
    lag_hours: float
    p_value: float
    location_id: str | None = None
    sample_size: int = 0
    explanation: str | None = None
    timestamp: datetime = field(default_factory=_now)
    id: str = field(default_factory=_uuid)
