"""YouTube audio acquisition adapter.

Used as a fallback when an issuer's IR audio is encrypted HLS (e.g. AMD's
Mediasite-hosted player) or otherwise inaccessible. IR-coverage YouTube
channels (EARNMOAR, Investing 101, etc.) routinely mirror full earnings
calls within hours of publication.

Implementation note: yt-dlp does the heavy lifting. We assume yt-dlp is
installed system-wide via brew (same as ffmpeg + curl).
"""

from __future__ import annotations

import json
import re
import subprocess
from datetime import UTC, datetime
from pathlib import Path

from .contracts import AudioFetch, AudioRef


_VIDEO_ID_RE = re.compile(r"^[A-Za-z0-9_-]{11}$")
_WATCH_URL_RE = re.compile(r"(?:youtube\.com/watch\?v=|youtu\.be/)([A-Za-z0-9_-]{11})")


def _extract_video_id(url_or_id: str) -> str:
    if _VIDEO_ID_RE.match(url_or_id):
        return url_or_id
    m = _WATCH_URL_RE.search(url_or_id)
    if m:
        return m.group(1)
    raise ValueError(f"Could not extract YouTube video_id from {url_or_id!r}")


def accept_video(url_or_id: str) -> AudioRef:
    """Accept either a YouTube URL or a bare 11-char video ID."""
    video_id = _extract_video_id(url_or_id)
    canonical = f"https://www.youtube.com/watch?v={video_id}"
    return AudioRef(
        vendor="youtube",
        event_id=video_id,
        source_url=canonical,
        source_uuid=video_id,
        discovered_via="manual_paste",
    )


def download_youtube_audio(audio_ref: AudioRef, *, dest_path: Path) -> AudioFetch:
    """Use yt-dlp to extract audio as mp3, write to dest_path."""
    if audio_ref.vendor != "youtube":
        raise ValueError(f"Expected vendor='youtube', got {audio_ref.vendor!r}")

    dest_path.parent.mkdir(parents=True, exist_ok=True)
    # yt-dlp wants an output template, not a literal path; the .%(ext)s
    # placeholder gets replaced after extraction.
    out_tpl = dest_path.with_suffix(".%(ext)s")

    cmd = [
        "yt-dlp",
        "-f", "bestaudio",
        "--extract-audio",
        "--audio-format", "mp3",
        "--audio-quality", "0",
        "-o", str(out_tpl),
        audio_ref.source_url,
    ]
    subprocess.run(cmd, check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)

    final = dest_path.with_suffix(".mp3")
    if not final.exists():
        raise RuntimeError(f"yt-dlp succeeded but expected output {final} not found")

    # Hash + size + duration
    import hashlib
    h = hashlib.sha256()
    with final.open("rb") as f:
        for chunk in iter(lambda: f.read(1 << 20), b""):
            h.update(chunk)

    duration_sec: int | None = None
    try:
        out = subprocess.check_output(
            [
                "ffprobe", "-v", "error",
                "-show_entries", "format=duration",
                "-of", "default=noprint_wrappers=1:nokey=1",
                str(final),
            ],
            timeout=30, stderr=subprocess.DEVNULL,
        )
        duration_sec = int(float(out.decode().strip()))
    except (subprocess.SubprocessError, ValueError):
        pass

    return AudioFetch(
        audio_ref=audio_ref,
        local_path=final,
        audio_format="mp3",
        audio_hash_sha256=h.hexdigest(),
        audio_size_bytes=final.stat().st_size,
        duration_sec=duration_sec,
        captured_at=datetime.now(UTC),
    )
