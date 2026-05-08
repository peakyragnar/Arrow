"""Typed contracts for the audio acquisition layer.

These are the shapes the orchestrator sees; vendor adapters return them.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


@dataclass(frozen=True)
class AudioRef:
    """A discovered, downloadable audio URL plus its provenance."""
    vendor: str
    event_id: str | None
    source_url: str
    source_uuid: str | None       # vendor-specific identifier embedded in the URL
    discovered_via: str           # 'playwright' | 'manual_paste' | 'youtube_extractor'


@dataclass(frozen=True)
class AudioFetch:
    """A successfully downloaded audio file on disk."""
    audio_ref: AudioRef
    local_path: Path
    audio_format: str             # 'mp3' | 'mp4' | 'm4a' | 'wav' | 'webm'
    audio_hash_sha256: str        # hex digest
    audio_size_bytes: int
    duration_sec: int | None
    captured_at: datetime
