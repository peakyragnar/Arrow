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
    for ext in ("mp4", "mp3", "m4a", "wav", "webm", "ts"):
        if lower.endswith("." + ext):
            return ext
    return "mp4"


def _remux_ts_to_mp4(src: Path, dst: Path) -> None:
    """ffmpeg -c copy MPEG-TS -> MP4 container. No re-encoding (instant)."""
    subprocess.run(
        ["ffmpeg", "-y", "-i", str(src), "-c", "copy", str(dst)],
        check=True,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


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

    # MPEG-TS isn't in the audio_artifacts.audio_format CHECK list (mp3/mp4/m4a/wav/webm).
    # Remux to an MP4 container in place — no re-encoding, just a different
    # container around the same audio bytes. Hash + size shift after remux,
    # which is correct: the canonical artifact we keep on disk + reference
    # by sha256 is the post-remux file.
    if fmt == "ts":
        mp4_path = dest_path.with_suffix(".mp4")
        _remux_ts_to_mp4(dest_path, mp4_path)
        dest_path.unlink()
        dest_path = mp4_path
        fmt = "mp4"

    return AudioFetch(
        audio_ref=audio_ref,
        local_path=dest_path,
        audio_format=fmt,
        audio_hash_sha256=_sha256_file(dest_path),
        audio_size_bytes=dest_path.stat().st_size,
        duration_sec=_ffprobe_duration_sec(dest_path),
        captured_at=datetime.now(UTC),
    )
