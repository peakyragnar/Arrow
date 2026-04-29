"""Link steward findings to the operator actions that resolved them.

The steward's auto-resolve path knows *that* a finding is no longer
surfacing, but not *why*. Operator actions (correct_corrupted_q4_is.py,
backfill_cross_endpoint_period_end.py, repair_calendar_fields.py, etc.)
each open an ``ingest_runs`` row whose ``counts`` jsonb already carries
their scope (ticker, fiscal_year, fiscal_quarter, ...). We just need
to correlate.

This module provides one small primitive: ``find_resolving_runs`` —
given a finding's scope and the moment it was closed, return the
recent succeeded runs that plausibly fixed the underlying data.

Used by:
  - ``arrow.steward.runner._auto_resolve_cleared`` (forward-going:
    enriches resolution notes as findings auto-close)
  - ``scripts/annotate_resolution_history.py`` (backfill: rewrites
    closed_note for existing generic-noted resolutions)

The match is intentionally lenient — false positives (a transcript
ingest run correlated with a financial-statement finding) are noise,
not damage. They appear as "candidate runs" in the note, leaving the
operator (or the V2 LLM) to read the run's counts and judge.
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any

import psycopg


_FPK_FY = re.compile(r"^FY(\d{4})$")
_FPK_FY_Q = re.compile(r"^FY(\d{4})\s*Q([1-4])$")


def parse_fiscal_period_key(fpk: str | None) -> tuple[int | None, int | None]:
    """Parse 'FY2024' / 'FY2024 Q3' into (fiscal_year, fiscal_quarter)."""
    if not fpk:
        return None, None
    m = _FPK_FY_Q.match(fpk.strip())
    if m:
        return int(m.group(1)), int(m.group(2))
    m = _FPK_FY.match(fpk.strip())
    if m:
        return int(m.group(1)), None
    return None, None


def find_resolving_runs(
    conn: psycopg.Connection,
    *,
    ticker: str | None,
    fiscal_period_key: str | None,
    at: datetime,
    window_minutes: int = 60,
) -> list[dict[str, Any]]:
    """Find ingest_runs that plausibly resolved a finding closed at ``at``.

    Match rules:
      - run.status = 'succeeded'
      - run.finished_at within [at - window_minutes, at]
      - run.ticker_scope contains ``ticker`` (or ticker_scope is NULL,
        for universe-scoped repair scripts that don't pin to a ticker)
      - run.run_kind != 'reconciliation' (those are steward's own
        passes — they observe, they don't fix)
      - if fiscal_period_key is set and run.counts has a fiscal_year /
        fiscal_quarter, those must match the parsed fpk. If counts has
        no such keys, the run is included (we don't have evidence
        either way; the universe of LITE-scoped runs is small enough
        that the operator can read them).

    Returns: list of {run_id, run_kind, vendor, started_at, finished_at,
                      counts, action_label} — newest first.
    """
    fy, fq = parse_fiscal_period_key(fiscal_period_key)

    sql = [
        "SELECT id, run_kind, vendor, ticker_scope, started_at, finished_at, counts",
        "FROM ingest_runs",
        "WHERE status = 'succeeded'",
        "  AND run_kind <> 'reconciliation'",
        "  AND finished_at <= %s",
        "  AND finished_at >= %s",
    ]
    params: list[Any] = [at, at - timedelta(minutes=window_minutes)]

    if ticker:
        sql.append("  AND (ticker_scope IS NULL OR %s = ANY(ticker_scope))")
        params.append(ticker)

    sql.append("ORDER BY id DESC")

    with conn.cursor() as cur:
        cur.execute("\n".join(sql), params)
        rows = cur.fetchall()

    out: list[dict[str, Any]] = []
    for run_id, run_kind, vendor, ticker_scope, started_at, finished_at, counts in rows:
        c = counts or {}
        # Tighten by fiscal scope if both finding and run carry it
        if fy is not None:
            run_fy = c.get("fiscal_year")
            if isinstance(run_fy, int) and run_fy != fy:
                continue
        if fq is not None:
            run_fq = c.get("fiscal_quarter")
            if isinstance(run_fq, int) and run_fq != fq:
                continue
        out.append({
            "run_id": run_id,
            "run_kind": run_kind,
            "vendor": vendor,
            "ticker_scope": list(ticker_scope) if ticker_scope else None,
            "started_at": started_at,
            "finished_at": finished_at,
            "counts": c,
            "action_label": _label_run(run_kind, vendor, c),
        })
    return out


def _label_run(run_kind: str, vendor: str, counts: dict[str, Any]) -> str:
    """Best-effort human-readable label for what an ingest run did.

    Inferred from the shape of ``counts`` rather than an explicit
    ``action_kind`` field — the existing scripts set scope-y keys
    (ticker, fiscal_year, is_facts_superseded, ...) which are
    diagnostic enough.
    """
    # Explicit action_kind set by instrumented scripts wins.
    if counts.get("action_kind"):
        return str(counts["action_kind"])

    # XBRL audit promotion — checked before supersession because audit
    # runs also set is_facts_superseded; the audit framing is more
    # specific.
    if "audit_run_id" in counts:
        return "xbrl audit promotion"

    # Supersession scripts (correct_corrupted_q4_is.py, etc.)
    if (
        counts.get("is_facts_superseded")
        or counts.get("bs_facts_superseded")
        or counts.get("cf_facts_superseded")
    ):
        n = (
            (counts.get("is_facts_superseded") or 0)
            + (counts.get("bs_facts_superseded") or 0)
            + (counts.get("cf_facts_superseded") or 0)
        )
        fy = counts.get("fiscal_year")
        fq = counts.get("fiscal_quarter")
        scope = (f"FY{fy}" + (f" Q{fq}" if fq else "")) if fy else "—"
        return f"supersede ({scope}, {n} rows)"

    # Transcript ingest
    if "transcripts_fetched" in counts or "text_units_inserted" in counts:
        return f"transcripts ingest ({counts.get('transcripts_fetched', '?')} fetched)"

    # SEC qualitative
    if "filings_seen" in counts or "sections_written" in counts:
        return f"sec filings ({counts.get('artifacts_written', '?')} artifacts)"

    # FMP financials backfill
    if "is_facts_written" in counts or "bs_facts_written" in counts:
        return f"fmp financials backfill ({vendor})"

    # FMP segments / employees
    if "segments_processed" in counts:
        return f"fmp segments ({counts.get('facts_written', '?')} facts)"
    if vendor == "fmp" and "facts_written" in counts:
        return f"fmp employees ({counts.get('facts_written', '?')} facts)"

    # Schema / seed
    if "companies" in counts:
        return f"sec seed ({counts['companies']} companies)"

    return f"{run_kind}/{vendor}"


def format_resolution_note(
    base_note: str,
    runs: list[dict[str, Any]],
    *,
    max_runs: int = 5,
) -> str:
    """Append a 'recent operator actions' suffix to a generic resolution
    note. If no runs were correlated, return ``base_note`` unchanged."""
    if not runs:
        return base_note
    parts = [base_note, " | recent operator actions: "]
    bits = []
    for r in runs[:max_runs]:
        bits.append(f"run {r['run_id']} ({r['action_label']})")
    parts.append("; ".join(bits))
    if len(runs) > max_runs:
        parts.append(f" (+{len(runs) - max_runs} more)")
    return "".join(parts)
