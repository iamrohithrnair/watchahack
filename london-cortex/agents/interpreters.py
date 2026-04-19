"""
Interpreter agents — first-pass analysis of raw sensor data.

VisionInterpreter    — TFL JamCam images -> LLM Flash -> anomalies
NumericInterpreter   — Air quality / weather / energy / env -> z-score -> anomalies
TextInterpreter      — GDELT news -> LLM Flash classification -> anomalies
FinancialInterpreter — Stocks / crypto / Polymarket -> returns / divergence -> anomalies
"""

from __future__ import annotations

import io
import json
import logging
import math
import random
from datetime import datetime, timedelta, timezone
from statistics import mean, stdev
from typing import Any

from ..core.board import MessageBoard
from ..core.config import LLM_FLASH_MODEL
from ..core.graph import CortexGraph
from ..core.memory import MemoryManager
from ..core.models import Anomaly, AgentMessage
from ..core.image_store import ImageStore, THUMBS_DIR
from ..core.utils import parse_json_response
from .base import BaseAgent

log = logging.getLogger("cortex.agents.interpreters")


def _is_blank_image(data: bytes) -> bool:
    """Check if an image is a blank/placeholder camera image.

    Detects:
    - Solid black/grey images (very low variance)
    - TFL-style grey placeholder images with "offline" text overlay
      (grey background ~140-200 avg, moderate variance from text)
    """
    try:
        from PIL import Image
        img = Image.open(io.BytesIO(data))

        # Check 1: grayscale variance on small sample (catches solid black/grey)
        grey = img.convert("L").resize((10, 10))
        pixels = list(grey.getdata())
        if not pixels:
            return True
        avg = sum(pixels) / len(pixels)
        variance = sum((p - avg) ** 2 for p in pixels) / len(pixels)
        std_dev = variance ** 0.5
        if std_dev < 10:
            return True  # nearly uniform = blank

        # Check 2: detect grey placeholder images with text overlay
        # TFL offline placeholders are grey (~140-200 avg) with low color saturation
        # and moderate variance (10-50) from the text. Real camera images have
        # much higher variance and color content.
        if 120 < avg < 210 and std_dev < 50:
            # Low saturation check: placeholder images are nearly grayscale
            # Sample color channels at small size
            rgb = img.convert("RGB").resize((10, 10))
            r_vals, g_vals, b_vals = [], [], []
            for r, g, b in rgb.getdata():
                r_vals.append(r)
                g_vals.append(g)
                b_vals.append(b)
            # Max channel spread per pixel — grayscale images have near-zero spread
            spreads = [max(r, g, b) - min(r, g, b)
                       for r, g, b in zip(r_vals, g_vals, b_vals)]
            avg_spread = sum(spreads) / len(spreads)
            if avg_spread < 15:
                # Grey, low-saturation, moderate variance = placeholder with text
                return True

        return False
    except Exception:
        return False


# ---------------------------------------------------------------------------
# VisionInterpreter
# ---------------------------------------------------------------------------

_VISION_SYSTEM = (
    "You are an expert urban analyst monitoring London via traffic cameras. "
    "You are precise, factual, and alert to unusual events."
)

_VISION_PROMPT = """\
Analyze this London traffic camera image. Describe:
1) Traffic conditions (free-flowing / congested / stationary / empty)
2) Any UNUSUAL events — accidents, fires, protests, police activity, emergency vehicles, large crowds, road blockages, flooding, structural damage, or anything that deviates from normal street life
3) Weather visible (clear / overcast / rain / fog / night)
4) Estimated counts: vehicles in frame, pedestrians visible
5) Overall assessment: NORMAL or UNUSUAL (pick one word)

Be concise. If everything is normal, say so briefly. Only elaborate if something is genuinely unusual.
Output JSON:
{
  "traffic": "...",
  "unusual_events": "...",
  "weather": "...",
  "vehicle_count": <int>,
  "pedestrian_count": <int>,
  "assessment": "NORMAL" | "UNUSUAL",
  "severity": <1-5>,
  "summary": "one sentence"
}
"""


class VisionInterpreter(BaseAgent):
    """Reads raw JamCam image messages and analyses them with LLM Flash."""

    name = "vision_interpreter"
    _cycle_count = 0

    def __init__(self, board, graph, memory, coordinator=None):
        super().__init__(board, graph, memory, coordinator=coordinator)
        self._image_store = ImageStore()

    async def run(self) -> None:
        VisionInterpreter._cycle_count += 1
        # Check for pending investigations assigned to me
        try:
            pending = await self.get_my_investigations()
            for inv in pending:
                await self._handle_vision_investigation(inv)
        except Exception as exc:
            log.debug("[VisionInterpreter] Investigation check failed: %s", exc)
        """One cycle: process unanalysed camera messages."""
        messages = await self.read_channel("#raw", since_hours=1.0, limit=1000)

        # Filter to tfl_jamcam image messages
        cam_messages = [
            m for m in messages
            if m.get("data", {}).get("source") == "tfl_jamcam"
            or "jamcam" in m.get("from_agent", "").lower()
            or m.get("data", {}).get("obs_type") == "image"
        ]

        if not cam_messages:
            log.debug("[VisionInterpreter] No camera messages to process.")
            return

        # Exclude dead cameras
        active_cameras = self.memory.get_active_cameras()
        health = self.memory.get_camera_health()
        dead_ids = {cid for cid, info in health.items() if info.get("status") == "dead"}

        # Re-probe: every 10th cycle, try 1 random dead camera
        reprobe_id = None
        if VisionInterpreter._cycle_count % 10 == 0 and dead_ids:
            reprobe_id = random.choice(list(dead_ids))
            log.info("[VisionInterpreter] Re-probing dead camera %s", reprobe_id)

        before_filter = len(cam_messages)
        cam_messages = [
            m for m in cam_messages
            if m.get("data", {}).get("camera_id", "") not in dead_ids
            or m.get("data", {}).get("camera_id", "") == reprobe_id
        ]
        skipped_dead = before_filter - len(cam_messages)
        if skipped_dead:
            log.info("[VisionInterpreter] Skipped %d dead cameras", skipped_dead)

        # Prioritise cameras with high historical interest scores
        camera_scores = self.memory.get_camera_scores()
        cam_messages.sort(
            key=lambda m: camera_scores.get(m.get("data", {}).get("camera_id", ""), 0.5),
            reverse=True,
        )

        # Process up to 5 cameras per cycle to conserve LLM quota
        for msg in cam_messages[:5]:
            data = msg.get("data", {})
            camera_id = data.get("camera_id", msg.get("id", "unknown"))
            image_url = data.get("image_url") or data.get("value")
            location_id = msg.get("location_id")
            lat = data.get("lat")
            lon = data.get("lon")

            if not image_url:
                continue

            # Download and save thumbnail regardless of retina zone
            img_bytes = await self._download_image(image_url) if isinstance(image_url, str) else None
            if not img_bytes or len(img_bytes) < 1000:
                log.debug("[VisionInterpreter] Skipping failed/tiny image for %s", camera_id)
                self.memory.update_camera_health(camera_id, success=False)
                continue
            if _is_blank_image(img_bytes):
                log.info("[VisionInterpreter] Skipping offline/blank camera %s", camera_id)
                self.memory.update_camera_health(camera_id, success=False)
                thumb = THUMBS_DIR / f"{camera_id}.jpg"
                if thumb.exists():
                    thumb.unlink(missing_ok=True)
                continue

            # Always store thumbnail so the camera grid shows feeds
            self._image_store.store_thumbnail(img_bytes, camera_id)
            self.memory.update_camera_health(camera_id, success=True)

            # Retina gate: only skip LLM analysis for peripheral cells
            retina = getattr(self.graph, 'retina', None)
            if retina and location_id:
                if not retina.should_agent_process(location_id, "vision"):
                    continue

            try:
                response_text = await self.call_llm(
                    prompt=_VISION_PROMPT,
                    model=LLM_FLASH_MODEL,
                    tier="flash",
                    images=[img_bytes],
                    trigger=f"vision:{camera_id}",
                    system_instruction=_VISION_SYSTEM,
                )

                if response_text.startswith("ERROR:"):
                    log.warning("[VisionInterpreter] LLM error for %s: %s", camera_id, response_text)
                    continue

                # Parse JSON response
                parsed = parse_json_response(response_text)
                if not parsed:
                    log.debug("[VisionInterpreter] Could not parse response for %s", camera_id)
                    continue

                assessment = parsed.get("assessment", "NORMAL").upper()
                severity = int(parsed.get("severity", 1))
                summary = parsed.get("summary", "")
                unusual_events = parsed.get("unusual_events", "")

                interesting = assessment == "UNUSUAL" and severity >= 2

                # Post normal frames to #observations for baseline building
                if not interesting:
                    await self.post(
                        channel="#observations",
                        content=(
                            f"[OBS] Camera {camera_id}: {parsed.get('traffic', 'unknown')} traffic, "
                            f"{parsed.get('weather', 'unknown')} weather, "
                            f"vehicles={parsed.get('vehicle_count', 0)}, "
                            f"pedestrians={parsed.get('pedestrian_count', 0)}"
                        ),
                        data={
                            "source": "tfl_jamcam",
                            "camera_id": camera_id,
                            "obs_type": "vision",
                            "assessment": assessment,
                            "traffic": parsed.get("traffic"),
                            "weather": parsed.get("weather"),
                            "vehicle_count": parsed.get("vehicle_count"),
                            "pedestrian_count": parsed.get("pedestrian_count"),
                            "lat": lat,
                            "lon": lon,
                        },
                        priority=1,
                        location_id=location_id,
                        ttl_hours=2,
                    )

                # Store full image if interesting; thumbnail already saved above
                if interesting:
                    self._image_store.store_full(img_bytes, camera_id)

                self.memory.update_camera_score(camera_id, interesting)

                if interesting:
                    # Store anomaly in DB
                    anomaly = Anomaly(
                        source="tfl_jamcam",
                        observation_id=msg.get("id", ""),
                        description=f"Camera {camera_id}: {summary}",
                        z_score=float(severity),
                        location_id=location_id,
                        lat=lat,
                        lon=lon,
                        severity=severity,
                        metadata={
                            "camera_id": camera_id,
                            "assessment": assessment,
                            "unusual_events": unusual_events,
                            "traffic": parsed.get("traffic"),
                            "weather": parsed.get("weather"),
                            "vehicle_count": parsed.get("vehicle_count"),
                            "pedestrian_count": parsed.get("pedestrian_count"),
                            "image_url": image_url,
                            "layer": "interpreted",
                        },
                    )
                    await self.board.store_anomaly(anomaly)

                    # Notify retina — may trigger saccade
                    retina = getattr(self.graph, 'retina', None)
                    if retina:
                        retina.notify_anomaly(anomaly)

                    # Register in spatial index
                    if location_id:
                        self.graph.register_anomaly(anomaly)

                    # Post to #anomalies
                    await self.post(
                        channel="#anomalies",
                        content=(
                            f"[VISION] Camera {camera_id} — {summary} "
                            f"(severity={severity}, events={unusual_events})"
                        ),
                        data={
                            "anomaly_id": anomaly.id,
                            "source": "tfl_jamcam",
                            "camera_id": camera_id,
                            "assessment": assessment,
                            "severity": severity,
                            "unusual_events": unusual_events,
                            "image_url": image_url,
                            "lat": lat,
                            "lon": lon,
                        },
                        priority=min(severity, 5),
                        location_id=location_id,
                        references=[msg.get("id", "")],
                    )
                    log.info(
                        "[VisionInterpreter] ANOMALY camera=%s severity=%d: %s",
                        camera_id, severity, summary,
                    )

            except Exception as exc:
                log.error("[VisionInterpreter] Unexpected error for %s: %s", camera_id, exc)

    async def _handle_vision_investigation(self, inv_thread: list[dict]) -> None:
        """Handle investigation threads requesting camera analysis."""
        if not inv_thread:
            return
        last_step = inv_thread[-1]
        thread_id = last_step.get("thread_id", "")
        question = last_step.get("next_question", "")
        if not question:
            return

        # Look for camera messages matching the investigation context
        cam_messages = await self.read_channel("#raw", since_hours=2.0, limit=200)
        cam_msgs = [
            m for m in cam_messages
            if m.get("data", {}).get("source") == "tfl_jamcam"
        ][:5]

        findings = []
        for msg in cam_msgs:
            data = msg.get("data", {})
            image_url = data.get("image_url") or data.get("value")
            camera_id = data.get("camera_id", "unknown")
            if not image_url:
                continue
            findings.append(f"Camera {camera_id} available for analysis")

        finding = f"Found {len(findings)} cameras. " + "; ".join(findings[:3]) if findings else "No camera data available."

        await self.continue_investigation(
            thread_id=thread_id,
            action=f"Vision scan for: {question[:60]}",
            finding=finding,
            next_question=None,
            next_agent=None,
        )


# ---------------------------------------------------------------------------
# NumericInterpreter
# ---------------------------------------------------------------------------

# Sources that produce numeric observations
_NUMERIC_SOURCES = {
    "laqn",           # air quality
    "open_meteo",     # weather
    "carbon_intensity",
    "environment_agency",  # river/flood

    "nature_inaturalist",
}

# Severity mapping based on z-score magnitude
def _z_to_severity(z: float) -> int:
    az = abs(z)
    if az < 2.5:
        return 1
    elif az < 3.5:
        return 2
    elif az < 5.0:
        return 3
    elif az < 7.0:
        return 4
    return 5


class NumericInterpreter(BaseAgent):
    """
    Pure statistics interpreter.
    Reads #raw numeric observations, updates EMA baselines, posts anomalies when z > 2.
    No LLM calls required.
    """

    name = "numeric_interpreter"

    async def run(self) -> None:
        messages = await self.read_channel("#raw", since_hours=0.25, limit=200)

        numeric_messages = [
            m for m in messages
            if m.get("data", {}).get("obs_type") in ("numeric", "NUMERIC")
            or m.get("data", {}).get("source") in _NUMERIC_SOURCES
        ]

        if not numeric_messages:
            log.debug("[NumericInterpreter] No numeric messages.")
            return

        for msg in numeric_messages:
            data = msg.get("data", {})
            source = data.get("source", msg.get("from_agent", "unknown"))
            location_id = msg.get("location_id") or data.get("location_id")
            lat = data.get("lat")
            lon = data.get("lon")
            metrics: dict[str, Any] = data.get("metrics", {})

            # If no metrics dict, try to treat the whole data as metrics
            if not metrics:
                # Some ingestors post value directly
                value = data.get("value")
                metric_name = data.get("metric", "value")
                if value is not None:
                    try:
                        metrics = {metric_name: float(value)}
                    except (TypeError, ValueError):
                        continue

            if not metrics:
                continue

            loc_key = location_id or "global"

            for metric_name, raw_value in metrics.items():
                try:
                    value = float(raw_value)
                except (TypeError, ValueError):
                    continue

                z = self.memory.update_baseline(source, loc_key, metric_name, value)

                # Post ALL observations to #observations (context, not alerts)
                await self.post(
                    channel="#observations",
                    content=(
                        f"[OBS] {source} {metric_name}={value:.3g} z={z:+.2f} at {loc_key}"
                    ),
                    data={
                        "source": source,
                        "metric": metric_name,
                        "value": value,
                        "z_score": z,
                        "location_id": location_id,
                        "obs_type": "numeric",
                    },
                    priority=1,
                    location_id=location_id,
                    ttl_hours=2,
                )

                if self.memory.is_anomalous(z):
                    severity = _z_to_severity(z)
                    direction = "HIGH" if z > 0 else "LOW"
                    description = (
                        f"{source} {metric_name} is anomalously {direction}: "
                        f"{value:.3g} (z={z:+.2f}) at {loc_key}"
                    )

                    anomaly = Anomaly(
                        source=source,
                        observation_id=msg.get("id", ""),
                        description=description,
                        z_score=z,
                        location_id=location_id,
                        lat=lat,
                        lon=lon,
                        severity=severity,
                        metadata={
                            "metric": metric_name,
                            "value": value,
                            "z_score": z,
                            "direction": direction,
                            "layer": "interpreted",
                        },
                    )
                    await self.board.store_anomaly(anomaly)

                    # Notify retina — may trigger saccade
                    retina = getattr(self.graph, 'retina', None)
                    if retina:
                        retina.notify_anomaly(anomaly)

                    if location_id:
                        self.graph.register_anomaly(anomaly)

                    await self.post(
                        channel="#anomalies",
                        content=(
                            f"[NUMERIC] {description}"
                        ),
                        data={
                            "anomaly_id": anomaly.id,
                            "source": source,
                            "metric": metric_name,
                            "value": value,
                            "z_score": z,
                            "severity": severity,
                            "direction": direction,
                        },
                        priority=severity,
                        location_id=location_id,
                        references=[msg.get("id", "")],
                    )
                    log.info(
                        "[NumericInterpreter] ANOMALY %s %s z=%+.2f severity=%d",
                        source, metric_name, z, severity,
                    )


# ---------------------------------------------------------------------------
# TextInterpreter
# ---------------------------------------------------------------------------

_TEXT_SYSTEM = (
    "You are an expert analyst monitoring London for significant events via news sources. "
    "You extract structured information from news articles accurately and concisely."
)

_TEXT_CLASSIFY_PROMPT = """\
Classify and analyse the following London news item(s). For each article, extract:

Articles:
{articles}

Respond with JSON array (one object per article):
[
  {{
    "headline": "...",
    "topic": "crime|protest|accident|fire|infrastructure|weather|health|politics|culture|other",
    "sentiment": "positive|neutral|negative",
    "urgency": <1-5>,
    "london_locations": ["list of specific London places mentioned"],
    "key_entities": ["people, organisations, events"],
    "is_unusual": true|false,
    "unusual_reason": "why it's unusual (or empty string)",
    "summary": "one sentence"
  }}
]
"""

_UNUSUAL_TOPICS = {"crime", "protest", "accident", "fire", "infrastructure", "health"}


class TextInterpreter(BaseAgent):
    """
    Reads #raw text observations (GDELT news) and classifies with LLM Flash.
    Posts to #anomalies on unusual volume or negative high-urgency content.
    """

    name = "text_interpreter"

    def __init__(self, board: MessageBoard, graph: CortexGraph, memory: MemoryManager) -> None:
        super().__init__(board, graph, memory)
        self._recent_headlines: set[str] = set()  # dedup

    async def run(self) -> None:
        messages = await self.read_channel("#raw", since_hours=0.5, limit=100)

        text_messages = [
            m for m in messages
            if m.get("data", {}).get("obs_type") in ("text", "TEXT")
            or m.get("data", {}).get("source") in ("gdelt", "gdelt_geo", "gdelt_doc", "news")
        ]

        if not text_messages:
            log.debug("[TextInterpreter] No text messages.")
            return

        # Batch articles for efficiency
        batch: list[dict[str, Any]] = []
        batch_ids: list[str] = []

        for msg in text_messages[:30]:
            data = msg.get("data", {})
            headline = data.get("title") or data.get("headline") or data.get("value", "")[:200]
            if not headline or headline in self._recent_headlines:
                continue
            self._recent_headlines.add(headline)
            # Keep dedup set bounded
            if len(self._recent_headlines) > 1000:
                self._recent_headlines = set(list(self._recent_headlines)[-500:])

            batch.append({
                "id": msg.get("id", ""),
                "headline": headline,
                "body": data.get("body", data.get("text", ""))[:500],
                "source": data.get("source", "news"),
                "location_id": msg.get("location_id"),
                "lat": data.get("lat"),
                "lon": data.get("lon"),
            })
            batch_ids.append(msg.get("id", ""))

            if len(batch) >= 10:
                await self._process_batch(batch, batch_ids)
                batch = []
                batch_ids = []

        if batch:
            await self._process_batch(batch, batch_ids)

        # Check for volume anomaly — many articles in short window = unusual
        if len(text_messages) >= 15:
            z = self.memory.update_baseline("gdelt", "global", "article_volume", len(text_messages))
            if self.memory.is_anomalous(z):
                await self.post(
                    channel="#anomalies",
                    content=(
                        f"[TEXT] High news volume detected: {len(text_messages)} articles in 30 min "
                        f"(z={z:+.2f}). Possible breaking news event."
                    ),
                    data={
                        "source": "gdelt",
                        "metric": "article_volume",
                        "value": len(text_messages),
                        "z_score": z,
                        "severity": _z_to_severity(z),
                    },
                    priority=3,
                )

    async def _process_batch(
        self, batch: list[dict[str, Any]], batch_ids: list[str]
    ) -> None:
        articles_text = "\n\n".join(
            f"[{i+1}] {a['headline']}\n{a['body'][:300]}" for i, a in enumerate(batch)
        )
        prompt = _TEXT_CLASSIFY_PROMPT.format(articles=articles_text)

        response_text = await self.call_llm(
            prompt=prompt,
            model=LLM_FLASH_MODEL,
            tier="flash",
            trigger="text:classify",
            system_instruction=_TEXT_SYSTEM,
        )

        if response_text.startswith("ERROR:"):
            log.warning("[TextInterpreter] LLM error: %s", response_text)
            return

        parsed = parse_json_response(response_text)
        if not isinstance(parsed, list):
            # Try wrapping
            if isinstance(parsed, dict):
                parsed = [parsed]
            else:
                log.debug("[TextInterpreter] Unexpected response format.")
                return

        for item, original in zip(parsed, batch):
            is_unusual = item.get("is_unusual", False)
            urgency = int(item.get("urgency", 1))
            sentiment = item.get("sentiment", "neutral")
            topic = item.get("topic", "other")
            summary = item.get("summary", original["headline"])
            unusual_reason = item.get("unusual_reason", "")
            locations = item.get("london_locations", [])

            # Anomaly conditions: unusual flag, or negative high-urgency in sensitive topics
            is_anomaly = (
                is_unusual
                or (sentiment == "negative" and urgency >= 4 and topic in _UNUSUAL_TOPICS)
            )

            # Post ALL classified articles to #observations
            if not is_anomaly:
                await self.post(
                    channel="#observations",
                    content=(
                        f"[OBS] News {topic} {sentiment}: {summary[:120]}"
                    ),
                    data={
                        "source": original.get("source", "gdelt"),
                        "obs_type": "text",
                        "topic": topic,
                        "sentiment": sentiment,
                        "urgency": urgency,
                        "headline": original["headline"],
                        "locations": locations,
                        "entities": item.get("key_entities", []),
                    },
                    priority=1,
                    location_id=original.get("location_id"),
                    ttl_hours=2,
                )

            if is_anomaly:
                location_id = original.get("location_id")
                lat = original.get("lat")
                lon = original.get("lon")

                # Try to geolocate from extracted locations if we don't have coords
                if not location_id and locations:
                    # We can't resolve text names to cells here without geocoder;
                    # store the location names in metadata for later resolution
                    pass

                anomaly = Anomaly(
                    source=original.get("source", "gdelt"),
                    observation_id=original.get("id", ""),
                    description=f"NEWS [{topic.upper()}]: {summary}",
                    z_score=float(urgency),
                    location_id=location_id,
                    lat=lat,
                    lon=lon,
                    severity=min(urgency, 5),
                    metadata={
                        "headline": original["headline"],
                        "topic": topic,
                        "sentiment": sentiment,
                        "urgency": urgency,
                        "unusual_reason": unusual_reason,
                        "london_locations": locations,
                        "key_entities": item.get("key_entities", []),
                    },
                )
                await self.board.store_anomaly(anomaly)

                if location_id:
                    self.graph.register_anomaly(anomaly)

                await self.post(
                    channel="#anomalies",
                    content=(
                        f"[TEXT] {topic.upper()} {sentiment.upper()} urgency={urgency}: "
                        f"{summary}. Reason: {unusual_reason or 'negative high-urgency'}"
                    ),
                    data={
                        "anomaly_id": anomaly.id,
                        "source": original.get("source", "gdelt"),
                        "topic": topic,
                        "sentiment": sentiment,
                        "urgency": urgency,
                        "locations": locations,
                        "entities": item.get("key_entities", []),
                        "headline": original["headline"],
                    },
                    priority=urgency,
                    location_id=location_id,
                    references=[original.get("id", "")],
                )
                log.info(
                    "[TextInterpreter] ANOMALY topic=%s urgency=%d: %s",
                    topic, urgency, summary,
                )


# ---------------------------------------------------------------------------
# FinancialInterpreter
# ---------------------------------------------------------------------------

_STOCK_MOVE_THRESHOLD = 0.015   # 1.5% single-period move triggers z-score check
_POLY_MOVE_THRESHOLD = 0.05     # 5% Polymarket probability shift
_SECTOR_DIVERGENCE_THRESHOLD = 0.03  # 3% sector vs market divergence


class FinancialInterpreter(BaseAgent):
    """
    Reads #raw financial observations.
    Detects: large single-stock moves, sector divergence, unusual Polymarket shifts.
    No LLM — pure statistics with rolling baselines.
    """

    name = "financial_interpreter"

    async def run(self) -> None:
        messages = await self.read_channel("#raw", since_hours=1.0, limit=200)

        financial_messages = [
            m for m in messages
            if m.get("data", {}).get("source") in (
                "financial_stocks", "financial_crypto", "polymarket",
                "coingecko", "alpha_vantage",
            )
            or m.get("data", {}).get("obs_type") in ("financial",)
        ]

        if not financial_messages:
            log.debug("[FinancialInterpreter] No financial messages.")
            return

        # Group by source type
        stocks: list[dict[str, Any]] = []
        crypto: list[dict[str, Any]] = []
        polymarket: list[dict[str, Any]] = []

        for msg in financial_messages:
            data = msg.get("data", {})
            source = data.get("source", "")
            if "stock" in source or "equity" in source:
                stocks.append(msg)
            elif "crypto" in source or "coingecko" in source:
                crypto.append(msg)
            elif "polymarket" in source or "prediction" in source:
                polymarket.append(msg)

        await self._process_stocks(stocks)
        await self._process_crypto(crypto)
        await self._process_polymarket(polymarket)

    async def _process_stocks(self, messages: list[dict[str, Any]]) -> None:
        """Detect large individual stock moves and sector divergence."""
        if not messages:
            return

        sector_returns: dict[str, list[float]] = {}
        market_returns: list[float] = []

        for msg in messages:
            data = msg.get("data", {})
            ticker = data.get("ticker") or data.get("symbol", "UNKNOWN")
            price = data.get("price") or data.get("value")
            prev_price = data.get("prev_price") or data.get("previous_close")
            sector = data.get("sector", "unknown")

            if price is None or prev_price is None:
                continue

            try:
                ret = (float(price) - float(prev_price)) / float(prev_price)
            except (ZeroDivisionError, TypeError, ValueError):
                continue

            market_returns.append(ret)
            sector_returns.setdefault(sector, []).append(ret)

            # Individual stock z-score
            z = self.memory.update_baseline("stocks", "london", ticker, ret)
            if abs(z) > 2.0 or abs(ret) > _STOCK_MOVE_THRESHOLD * 2:
                severity = min(int(abs(z)) + 1, 5)
                direction = "UP" if ret > 0 else "DOWN"
                await self._post_financial_anomaly(
                    source="financial_stocks",
                    obs_id=msg.get("id", ""),
                    description=(
                        f"{ticker} moved {ret:+.2%} ({direction}) — z={z:+.2f}. "
                        f"Price: {price} (prev: {prev_price})"
                    ),
                    metric=ticker,
                    value=ret,
                    z_score=z,
                    severity=severity,
                    metadata={"ticker": ticker, "sector": sector, "price": price, "return": ret},
                )

        # Sector divergence: if a sector moves much more than market
        if market_returns and len(market_returns) >= 3:
            market_avg = mean(market_returns)
            for sector, returns in sector_returns.items():
                if len(returns) < 2:
                    continue
                sector_avg = mean(returns)
                divergence = sector_avg - market_avg
                if abs(divergence) >= _SECTOR_DIVERGENCE_THRESHOLD:
                    direction = "outperforming" if divergence > 0 else "underperforming"
                    z = self.memory.update_baseline("stocks", "london", f"sector_{sector}", sector_avg)
                    await self._post_financial_anomaly(
                        source="financial_stocks",
                        obs_id="",
                        description=(
                            f"Sector {sector!r} is {direction} the market by {divergence:+.2%}. "
                            f"Sector avg: {sector_avg:+.2%}, market avg: {market_avg:+.2%} (z={z:+.2f})"
                        ),
                        metric=f"sector_divergence_{sector}",
                        value=divergence,
                        z_score=z,
                        severity=3,
                        metadata={"sector": sector, "divergence": divergence, "market_avg": market_avg},
                    )

    async def _process_crypto(self, messages: list[dict[str, Any]]) -> None:
        """Detect unusual crypto price moves."""
        for msg in messages:
            data = msg.get("data", {})
            coin = data.get("coin") or data.get("id", "UNKNOWN")
            price = data.get("price") or data.get("value")
            change_24h = data.get("price_change_percentage_24h") or data.get("change_24h")

            if change_24h is None and price is None:
                continue

            if change_24h is not None:
                try:
                    change = float(change_24h) / 100.0  # normalise to ratio
                except (TypeError, ValueError):
                    continue
                z = self.memory.update_baseline("crypto", "global", coin, change)
                if abs(z) > 2.5 or abs(change) > 0.10:  # 10% move
                    severity = min(int(abs(z)), 5) or 3
                    direction = "UP" if change > 0 else "DOWN"
                    await self._post_financial_anomaly(
                        source="financial_crypto",
                        obs_id=msg.get("id", ""),
                        description=(
                            f"Crypto {coin} 24h change: {change:+.2%} ({direction}), z={z:+.2f}"
                        ),
                        metric=coin,
                        value=change,
                        z_score=z,
                        severity=severity,
                        metadata={"coin": coin, "price": price, "change_24h": change},
                    )

    async def _process_polymarket(self, messages: list[dict[str, Any]]) -> None:
        """Detect sudden Polymarket probability shifts on London-relevant markets."""
        for msg in messages:
            data = msg.get("data", {})
            market_id = data.get("market_id") or data.get("id", "unknown")
            question = data.get("question") or data.get("title", "")
            prob = data.get("probability") or data.get("last_trade_price")
            prev_prob = data.get("prev_probability") or data.get("previous_probability")

            if prob is None:
                continue

            try:
                prob_f = float(prob)
            except (TypeError, ValueError):
                continue

            z = self.memory.update_baseline("polymarket", "global", market_id, prob_f)

            if prev_prob is not None:
                try:
                    shift = abs(prob_f - float(prev_prob))
                except (TypeError, ValueError):
                    shift = 0.0
            else:
                shift = 0.0

            if abs(z) > 2.0 or shift >= _POLY_MOVE_THRESHOLD:
                severity = 3 if shift >= 0.15 else 2
                await self._post_financial_anomaly(
                    source="polymarket",
                    obs_id=msg.get("id", ""),
                    description=(
                        f"Polymarket: '{question}' probability moved to {prob_f:.1%} "
                        f"(shift={shift:+.2%}, z={z:+.2f})"
                    ),
                    metric=market_id,
                    value=prob_f,
                    z_score=z,
                    severity=severity,
                    metadata={"market_id": market_id, "question": question, "probability": prob_f, "shift": shift},
                )

    async def _post_financial_anomaly(
        self,
        source: str,
        obs_id: str,
        description: str,
        metric: str,
        value: float,
        z_score: float,
        severity: int,
        metadata: dict[str, Any] | None = None,
    ) -> None:
        anomaly = Anomaly(
            source=source,
            observation_id=obs_id,
            description=description,
            z_score=z_score,
            severity=severity,
            metadata=metadata or {},
        )
        await self.board.store_anomaly(anomaly)
        await self.post(
            channel="#anomalies",
            content=f"[FINANCIAL] {description}",
            data={
                "anomaly_id": anomaly.id,
                "source": source,
                "metric": metric,
                "value": value,
                "z_score": z_score,
                "severity": severity,
                **(metadata or {}),
            },
            priority=severity,
            references=[obs_id] if obs_id else [],
        )
        log.info("[FinancialInterpreter] ANOMALY %s z=%+.2f: %s", source, z_score, description[:100])


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# -- Async entry points (called by scheduler) ---------------------------------

async def run_vision_interpreter(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = VisionInterpreter(board, graph, memory)
    await agent.run()


async def run_numeric_interpreter(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = NumericInterpreter(board, graph, memory)
    await agent.run()


async def run_text_interpreter(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = TextInterpreter(board, graph, memory)
    await agent.run()


async def run_financial_interpreter(
    board: MessageBoard, graph: CortexGraph, memory: MemoryManager
) -> None:
    agent = FinancialInterpreter(board, graph, memory)
    await agent.run()
