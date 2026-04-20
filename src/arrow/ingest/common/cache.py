"""Filesystem cache writer — belt-and-suspenders for raw payloads.

Layout is endpoint-mirrored per docs/architecture/system.md § Raw Cache
Layout. DB (raw_responses) is the authoritative replay index; this cache
is convenience for offline replay and local grepping. Overwrites on
re-fetch — historical bytes live in Postgres.
"""

from __future__ import annotations

from pathlib import Path

# this file: src/arrow/ingest/common/cache.py
# parents: [0]=common [1]=ingest [2]=arrow [3]=src [4]=repo root
REPO_ROOT = Path(__file__).resolve().parents[4]
RAW_DIR = REPO_ROOT / "data" / "raw"


def cache_path(vendor: str, *segments: str) -> Path:
    """Build data/raw/{vendor}/{segment1}/{segment2}/...

    Segments may contain forward slashes; they are split so callers can
    pass endpoint paths verbatim (e.g. "submissions/CIK0000123.json").
    """
    p = RAW_DIR / vendor
    for s in segments:
        for piece in s.split("/"):
            if piece:
                p = p / piece
    return p
