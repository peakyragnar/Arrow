"""Common audio download — once we have an unauthenticated URL.

Most earnings-call audio URLs (Q4 edited recordings, YouTube via yt-dlp)
are public CloudFront / S3 hosted. We just stream them to disk with curl
and hash as we go.
"""

from __future__ import annotations

import hashlib
import shutil
import subprocess
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

from .contracts import AudioFetch, AudioRef


def _format_from_url(url: str) -> str:
    """Best-effort format inference from URL extension."""
    lower = url.split("?", 1)[0].lower()
    for ext in ("mp4", "mp3", "m4a", "wav", "webm"):
        if lower.endswith("." + ext):
            return ext
    return "mp4"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def _ffprobe_duration_sec(path: Path) -> int | None:
    """Best-effort duration via ffprobe; returns None if unavailable."""
    if shutil.which("ffprobe") is None:
        return None
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(path),
            ],
            timeout=30,
            stderr=subprocess.DEVNULL,
        )
        return int(float(out.decode().strip()))
    except (subprocess.SubprocessError, ValueError):
        return None


def download_audio(
    audio_ref: AudioRef,
    *,
    dest_path: Path,
    user_agent: str = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36",
) -> AudioFetch:
    """Stream audio to dest_path via curl. Returns a populated AudioFetch.

    No vendor-specific logic here — the URL is assumed to be reachable
    without auth. Vendor adapters handle auth/registration during URL
    discovery, not during download.
    """
    dest_path.parent.mkdir(parents=True, exist_ok=True)

    cmd = [
        "curl",
        "--fail",
        "--silent",
        "--show-error",
        "--location",            # follow redirects (CloudFront sometimes 302s)
        "--user-agent", user_agent,
        "--output", str(dest_path),
        audio_ref.source_url,
    ]
    subprocess.run(cmd, check=True)

    fmt = _format_from_url(audio_ref.source_url)
    return AudioFetch(
        audio_ref=audio_ref,
        local_path=dest_path,
        audio_format=fmt,
        audio_hash_sha256=_sha256_file(dest_path),
        audio_size_bytes=dest_path.stat().st_size,
        duration_sec=_ffprobe_duration_sec(dest_path),
        captured_at=datetime.now(UTC),
    )
