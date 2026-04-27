"""Parse FMP earnings-call transcript content into speaker turns."""

from __future__ import annotations

import re
from dataclasses import dataclass

SPEAKER_RE = re.compile(
    r"^(?P<speaker>[A-Z][\w .,'\-]{1,80}):\s+(?P<text>.+)$",
    re.MULTILINE,
)

MIN_TURN_COVERAGE = 0.80


@dataclass(frozen=True)
class ParsedTurn:
    ordinal: int
    speaker: str
    text: str
    start_offset: int
    end_offset: int


def canonicalize_transcript_content(content: str) -> str:
    """Normalize line endings/NULs while preserving character offsets."""
    return content.replace("\r\n", "\n").replace("\r", "\n").replace("\x00", "")


def parse_speaker_turns(
    content: str,
    *,
    min_coverage: float = MIN_TURN_COVERAGE,
) -> list[ParsedTurn]:
    """Return deterministic speaker turns, or [] when parse quality is low.

    A speaker marker starts a turn; the turn extends until the next marker.
    Offsets are character offsets into the canonical transcript content.
    """
    canonical = canonicalize_transcript_content(content)
    if not canonical.strip():
        return []

    matches = list(SPEAKER_RE.finditer(canonical))
    if not matches:
        return []

    turns: list[ParsedTurn] = []
    covered_chars = 0
    for idx, match in enumerate(matches):
        start = match.start()
        next_start = matches[idx + 1].start() if idx + 1 < len(matches) else len(canonical)
        raw_text = canonical[start:next_start].rstrip()
        if not raw_text:
            continue
        end = start + len(raw_text)
        covered_chars += end - start
        turns.append(
            ParsedTurn(
                ordinal=len(turns) + 1,
                speaker=match.group("speaker").strip(),
                text=raw_text,
                start_offset=start,
                end_offset=end,
            )
        )

    if not turns:
        return []

    coverage = covered_chars / max(1, len(canonical.strip()))
    if coverage < min_coverage:
        return []
    return turns
