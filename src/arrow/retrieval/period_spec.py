"""Period spec language for the universe screener.

A small grammar that lets the agent specify "which period(s) to rank by"
across financials / estimates / valuation in one consistent string format.
The parser produces a `ParsedPeriod` consumed by `arrow.retrieval.screener`.

Forms:
- ``latest``                    — most recent non-null row per company
- ``FY{YYYY}``                  — single fiscal year (annual grain)
- ``{YYYY}-Q{N}``               — single fiscal quarter
- ``last_{N}{q|y}``             — trailing N quarters/years (window; for delta/relative_change agg)
- ``last_{N}{q|y}_avg``         — trailing N q/y averaged (single value per company)
- ``last_{N}{q|y}_sum``         — trailing N q/y summed
- ``next``  / ``next_q``        — first forward period (estimates)
- ``next_fy``                   — first forward annual period (estimates)
- ``forward_{N}{q|y}_avg``      — forward window of N periods, averaged
- ``forward_{N}{q|y}_sum``      — forward window of N periods, summed
- ``forward_{N}{q|y}``          — forward window (no aggregation; window for delta/etc)
- ``asof:{YYYY-MM-DD}``         — single date (valuation)

Returns ParsedPeriod with `kind` indicating shape and `details` carrying values.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class ParsedPeriod:
    """Normalized period spec.

    `kind`:
      - 'latest'         → use most recent row per company
      - 'single_fy'      → year is set
      - 'single_fq'      → year + quarter set
      - 'window'         → trailing window: n_periods + grain ('q'|'y'); agg in {None, 'avg', 'sum'}
      - 'forward_window' → forward window: n_periods + grain; agg in {None, 'avg', 'sum'}
      - 'forward_single' → first forward period; grain in {'q','y'}
      - 'asof'           → as_of date for daily/valuation grain
    """
    kind: str
    year: int | None = None
    quarter: int | None = None
    n_periods: int | None = None
    grain: str | None = None       # 'q' | 'y'
    agg: str | None = None         # None | 'avg' | 'sum'
    as_of: date | None = None
    raw: str = ""


_FY_RE = re.compile(r"^FY(\d{4})$", re.IGNORECASE)
_FQ_RE = re.compile(r"^(\d{4})-Q([1-4])$", re.IGNORECASE)
_WINDOW_RE = re.compile(
    r"^(?P<dir>last|forward)_(?P<n>\d+)(?P<grain>[qy])(?:_(?P<agg>avg|sum))?$",
    re.IGNORECASE,
)
_ASOF_RE = re.compile(r"^asof:(\d{4}-\d{2}-\d{2})$", re.IGNORECASE)


def parse_period(spec: str) -> ParsedPeriod:
    """Parse a period spec string. Raises ValueError on unrecognized input."""
    s = (spec or "").strip()
    if not s:
        raise ValueError("empty period spec")
    sl = s.lower()

    if sl == "latest":
        return ParsedPeriod(kind="latest", raw=s)

    if sl in ("next", "next_q"):
        return ParsedPeriod(kind="forward_single", grain="q", raw=s)
    if sl == "next_fy":
        return ParsedPeriod(kind="forward_single", grain="y", raw=s)

    m = _FY_RE.match(s)
    if m:
        return ParsedPeriod(kind="single_fy", year=int(m.group(1)), raw=s)

    m = _FQ_RE.match(s)
    if m:
        return ParsedPeriod(kind="single_fq", year=int(m.group(1)), quarter=int(m.group(2)), raw=s)

    m = _WINDOW_RE.match(s)
    if m:
        direction = m.group("dir").lower()
        n = int(m.group("n"))
        grain = m.group("grain").lower()
        agg = m.group("agg").lower() if m.group("agg") else None
        if n <= 0:
            raise ValueError(f"window length must be positive: {spec!r}")
        kind = "window" if direction == "last" else "forward_window"
        return ParsedPeriod(kind=kind, n_periods=n, grain=grain, agg=agg, raw=s)

    m = _ASOF_RE.match(s)
    if m:
        try:
            d = date.fromisoformat(m.group(1))
        except ValueError as exc:
            raise ValueError(f"invalid asof date in {spec!r}: {exc}") from exc
        return ParsedPeriod(kind="asof", as_of=d, raw=s)

    raise ValueError(
        f"unrecognized period spec: {spec!r}. "
        "Expected 'latest', 'FY2025', '2025-Q3', 'last_3y', 'last_4q_avg', "
        "'forward_4q_avg', 'next', 'next_fy', or 'asof:YYYY-MM-DD'."
    )


def is_window(p: ParsedPeriod) -> bool:
    return p.kind in ("window", "forward_window")


def supports_agg(p: ParsedPeriod, agg: str) -> bool:
    """Is the requested screen agg ('level'|'delta'|'relative_change') compatible?"""
    if agg == "level":
        return True
    # delta/relative_change need a multi-period window so we can split into
    # early-third vs late-third buckets.
    return is_window(p) and (p.n_periods or 0) >= 3
