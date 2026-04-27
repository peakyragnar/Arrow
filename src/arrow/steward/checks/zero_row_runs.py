"""zero_row_runs check: surface ingest_runs that succeeded but wrote nothing.

The most common silent-failure pattern: a vendor returned 200 OK with
empty data, the ingest code shrugged it off, and the run was marked
``succeeded`` despite producing no facts/artifacts/etc. The user has
explicitly named this as a thing to flag (`feedback_silent_failures`):
"missing data is a bug, not a happy accident."

What "wrote nothing" means
--------------------------
``ingest_runs.counts`` is a free-form jsonb whose keys vary by vendor.
Surveyed real shapes (2026-04-25):

  - FMP: rows_processed, raw_responses, *_facts_written,
         *_facts_superseded, segments_processed, *_flags_written
  - SEC: raw_responses, artifacts_written, documents_fetched,
         sections_written, text_units_written, files_fetched
  - FMP transcripts: raw_responses, transcripts_fetched, artifacts_inserted,
         text_units_inserted, text_chunks_inserted

We define "wrote something" as: the sum of any recognized OUTPUT keys
in ``OUTPUT_KEYS`` is > 0. If all of those keys are absent or zero, the
run produced nothing meaningful and is flagged.

Keys like ``since_date``, ``until_date``, ``forms``,
``limit_per_ticker``, ``min_fiscal_year_by_ticker``,
``max_fiscal_year_by_ticker``, ``artifacts_existing``,
``artifacts_by_type``, ``earnings_8k_only``, ``companies`` are
configuration echoes or informational metadata, NOT output counts —
they are deliberately excluded from the sum.

When new ingest paths land that emit different output keys, add them
to ``OUTPUT_KEYS``. Per the working rule "new verticals ship with
their expectations and steward checks," that change rides along with
the new vertical's PR.

Scope behavior
--------------
- ``scope.tickers`` set: only flag runs whose ``ticker_scope`` overlaps.
  Runs scoped to a single ticker yield findings with ``ticker`` set;
  multi-ticker / universe runs yield findings with ``ticker = NULL``
  (cross-cutting per-run).
- Cross-cutting check (vertical=None): always runs regardless of
  ``scope.verticals``.

Fingerprint
-----------
``(check_name="zero_row_runs", scope={"ingest_run_id": <id>}, params={})``

Including the ingest_run id means each problematic run gets exactly one
finding, idempotent across nightly sweeps. When the run gets re-fetched
or otherwise resolved, the finding's fingerprint stops surfacing and
auto-resolves.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register

#: Recent window for "did this just succeed but write nothing?"
RECENT_WINDOW_DAYS = 7

#: Output keys whose sum determines whether a run wrote anything.
#: Update when new vendors / paths add new output count keys.
OUTPUT_KEYS = (
    "rows_processed",
    "raw_responses",
    "facts_written",
    "is_facts_written",
    "bs_facts_written",
    "cf_facts_written",
    "facts_superseded",
    "is_facts_superseded",
    "bs_facts_superseded",
    "cf_facts_superseded",
    "segments_processed",
    "artifacts_written",
    "documents_fetched",
    "sections_written",
    "text_units_written",
    "text_chunks_inserted",
    "transcripts_fetched",
    "artifacts_inserted",
    "files_fetched",
)


@register
class ZeroRowRuns(Check):
    name = "zero_row_runs"
    severity = "warning"
    vertical = None  # cross-cutting

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql_parts = [
            "SELECT id, vendor, run_kind, ticker_scope, started_at, finished_at, counts",
            "FROM ingest_runs",
            "WHERE status = 'succeeded'",
            f"  AND finished_at > now() - interval '{RECENT_WINDOW_DAYS} days'",
            "  AND COALESCE((",
        ]
        # Sum all recognized OUTPUT_KEYS. SQL builds:
        #   COALESCE((counts->>'k1')::numeric, 0) + ... = 0
        sum_terms = " + ".join(
            f"COALESCE((counts->>'{k}')::numeric, 0)" for k in OUTPUT_KEYS
        )
        sql_parts.append(f"    {sum_terms}")
        sql_parts.append("  ), 0) = 0")

        params: list = []
        if scope.tickers is not None:
            sql_parts.append("  AND (ticker_scope IS NULL OR ticker_scope && %s::text[])")
            params.append([t.upper() for t in scope.tickers])

        sql_parts.append("ORDER BY finished_at DESC")
        sql = "\n".join(sql_parts)

        with conn.cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()
            cols = [d[0] for d in cur.description]

        for row in rows:
            r = dict(zip(cols, row))
            yield self._build_draft(r, scope)

    def _build_draft(self, r: dict, scope: Scope) -> FindingDraft:
        run_id = r["id"]
        vendor = r["vendor"]
        run_kind = r["run_kind"]
        ticker_scope = r["ticker_scope"]
        started_at = r["started_at"]
        counts = r["counts"] or {}

        # Single-ticker run → finding scoped to that ticker.
        # Multi-ticker / universe → cross-cutting finding (ticker=None).
        ticker: str | None = None
        company_id: int | None = None
        if ticker_scope and len(ticker_scope) == 1:
            ticker = ticker_scope[0].upper()

        fp = fingerprint(
            self.name,
            scope={"ingest_run_id": run_id},
            rule_params={"window_days": RECENT_WINDOW_DAYS},
        )

        scope_desc = (
            f"ticker={ticker}" if ticker
            else f"tickers={ticker_scope}" if ticker_scope
            else "universe"
        )
        summary = (
            f"Ingest run #{run_id} ({vendor} {run_kind}, {scope_desc}) succeeded "
            f"but wrote 0 rows across all output keys."
        )

        suggested = {
            "kind": "investigate_ingest_run",
            "params": {"ingest_run_id": run_id, "vendor": vendor, "ticker_scope": ticker_scope},
            "command": (
                f"uv run python -c "
                f"\"from arrow.db.connection import get_conn; "
                f"from psycopg.rows import dict_row; "
                f"with get_conn() as c, c.cursor(row_factory=dict_row) as cur: "
                f"cur.execute('SELECT * FROM ingest_runs WHERE id=%s', ({run_id},)); "
                f"print(cur.fetchone())\""
            ),
            "prose": (
                f"The {vendor} {run_kind} run on {started_at:%Y-%m-%d %H:%M} marked itself "
                f"succeeded but produced no facts, artifacts, or written rows. Likely causes: "
                f"vendor returned 200 with empty payload, ticker not found, fiscal-window "
                f"filter excluded everything, or a code path that swallowed an empty response. "
                f"Inspect the raw_responses linked to this run "
                f"(SELECT * FROM raw_responses WHERE ingest_run_id = {run_id}). "
                f"If the vendor truly has no data for this scope right now, suppress with "
                f"reason 'vendor empty' (set expires for the period to retry later); "
                f"otherwise re-run the ingest."
            ),
        }

        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=self.severity,
            company_id=company_id,
            ticker=ticker,
            vertical=None,
            fiscal_period_key=None,
            evidence={
                "ingest_run_id": run_id,
                "vendor": vendor,
                "run_kind": run_kind,
                "ticker_scope": ticker_scope,
                "started_at": started_at.isoformat() if started_at else None,
                "counts": counts,
            },
            summary=summary,
            suggested_action=suggested,
        )
