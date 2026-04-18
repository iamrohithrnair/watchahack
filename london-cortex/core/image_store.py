"""Two-tier image storage: thumbnails (always) + full-size (when interesting).

Thumbnails: 160x120 JPEG q=30 → data/images/thumbs/
Full-size:  640x480 JPEG q=60 → data/images/full/
Cleanup:    thumbs >48h, full >7d
"""

from __future__ import annotations

import io
import logging
import time
from pathlib import Path
from typing import Any

from .config import DATA_DIR

log = logging.getLogger("cortex.image_store")

IMAGES_DIR = DATA_DIR / "images"
THUMBS_DIR = IMAGES_DIR / "thumbs"
FULL_DIR = IMAGES_DIR / "full"

# Ensure dirs exist
for d in (IMAGES_DIR, THUMBS_DIR, FULL_DIR):
    d.mkdir(parents=True, exist_ok=True)

# Retention
THUMB_TTL_HOURS = 48
FULL_TTL_HOURS = 168  # 7 days


class ImageStore:
    """Two-tier image storage using Pillow."""

    def store_thumbnail(self, image_bytes: bytes, image_id: str) -> Path | None:
        """Store a 160x120 JPEG thumbnail. Always called for valid images."""
        try:
            from PIL import Image
            img = Image.open(io.BytesIO(image_bytes))
            img.thumbnail((160, 120))
            if img.mode != "RGB":
                img = img.convert("RGB")
            path = THUMBS_DIR / f"{image_id}.jpg"
            img.save(str(path), "JPEG", quality=30)
            return path
        except Exception as exc:
            log.debug("Failed to store thumbnail %s: %s", image_id, exc)
            return None

    def store_full(self, image_bytes: bytes, image_id: str) -> Path | None:
        """Store a 640x480 JPEG full-size image. Called when image is interesting."""
        try:
            from PIL import Image
            img = Image.open(io.BytesIO(image_bytes))
            img.thumbnail((640, 480))
            if img.mode != "RGB":
                img = img.convert("RGB")
            path = FULL_DIR / f"{image_id}.jpg"
            img.save(str(path), "JPEG", quality=60)
            return path
        except Exception as exc:
            log.debug("Failed to store full image %s: %s", image_id, exc)
            return None

    def cleanup(self) -> tuple[int, int]:
        """Remove expired thumbnails and full images. Returns (thumbs_removed, full_removed)."""
        now = time.time()
        thumbs_removed = self._cleanup_dir(THUMBS_DIR, THUMB_TTL_HOURS, now)
        full_removed = self._cleanup_dir(FULL_DIR, FULL_TTL_HOURS, now)
        if thumbs_removed or full_removed:
            log.info("Image cleanup: %d thumbs, %d full removed", thumbs_removed, full_removed)
        return thumbs_removed, full_removed

    def _cleanup_dir(self, directory: Path, ttl_hours: int, now: float) -> int:
        removed = 0
        cutoff = now - ttl_hours * 3600
        try:
            for f in directory.iterdir():
                if f.is_file() and f.stat().st_mtime < cutoff:
                    f.unlink()
                    removed += 1
        except Exception as exc:
            log.debug("Cleanup error in %s: %s", directory, exc)
        return removed
