"""Generic / non-vendor-specific audio acquisition.

Used as a fallback when an audio URL doesn't match a known vendor pattern
(e.g., AMD's media-server.com hosted .ts files). The operator pastes the
URL from their browser; we accept it as-is and let download.py handle the
HTTP + format detection.
"""

from __future__ import annotations

from urllib.parse import urlparse

from .contracts import AudioRef


def accept_pasted_url(url: str, *, vendor: str = "manual") -> AudioRef:
    """Accept an arbitrary pasted audio URL with minimal validation.

    Validates only that the URL parses as http(s). Pattern enforcement is
    intentionally absent — the calling adapter (q4inc, etc.) handles
    vendor-specific validation; this is the fallthrough for anything else.
    """
    parsed = urlparse(url)
    if parsed.scheme not in ("http", "https") or not parsed.netloc:
        raise ValueError(f"Pasted URL must be a valid http(s) URL, got: {url!r}")
    if vendor not in ("q4inc", "youtube", "manual", "other"):
        raise ValueError(
            f"vendor must be one of: q4inc, youtube, manual, other (got {vendor!r})"
        )
    return AudioRef(
        vendor=vendor,
        event_id=None,
        source_url=url,
        source_uuid=None,
        discovered_via="manual_paste",
    )
