"""expected_coverage check: surface mismatches between what each
covered ticker SHOULD have and what it currently has.

For each ticker in ``coverage_membership``, resolves expectations
(via ``arrow.steward.expectations``) and evaluates them against the
ticker's actual per-vertical state (computed by
``arrow.steward.coverage.compute_coverage_matrix``). Yields one
finding per unmet expectation.

Why this matters:
  ``zero_row_runs``, ``unparsed_body_fallback``, ``broken_provenance``
  etc. all surface failures of things that ARE there. This check is
  the only one that surfaces failures of things that AREN'T there —
  the data we expected and don't have. Without it, the inbox can
  show "0 issues" while the dataset is materially incomplete.

Vertical: each finding is tagged with the vertical it's about
(financials / segments / employees / sec_qual / press_release / transcript) so
the dashboard's vertical filter works as expected.

Scope behavior:
  - ``scope.tickers`` set: only iterate coverage members in that
    list.
  - ``scope.tickers`` None: iterate all coverage members.

Fingerprint:
  ``(check_name='expected_coverage',
     scope={ticker, vertical, rule},
     params={effective_params})``
  Effective params are included so an expectations change (tightened
  threshold, override added) creates a different fingerprint and
  the prior finding auto-resolves. This is correct: a different rule
  is a different finding.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import psycopg

from arrow.steward.coverage import compute_coverage_matrix
from arrow.steward.expectations import (
    EvaluationResult,
    Expectation,
    evaluate_expectation,
    expectations_for,
)
from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


@register
class ExpectedCoverage(Check):
    name = "expected_coverage"
    severity = "warning"  # default; promoted per-rule below
    vertical = None       # cross-cutting (produces findings tagged per-vertical)

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        matrix = compute_coverage_matrix(conn)
        if not matrix:
            return  # no coverage members; nothing to evaluate

        scope_tickers: set[str] | None = (
            {t.upper() for t in scope.tickers} if scope.tickers is not None else None
        )

        now = datetime.now(timezone.utc)
        for row in matrix:
            if scope_tickers is not None and row.ticker not in scope_tickers:
                continue

            for exp in expectations_for(row.ticker):
                cell = row.by_vertical[exp.vertical]
                latest_age_days = self._age_days(cell.latest, now)
                result = evaluate_expectation(
                    exp,
                    has_data=cell.has_data,
                    period_count=cell.period_count,
                    latest_age_days=latest_age_days,
                )
                if result.met:
                    continue
                yield self._build_draft(row=row, exp=exp, result=result, cell=cell)

    @staticmethod
    def _age_days(latest: object | None, now: datetime) -> float | None:
        """Coerce a `date`/`datetime` to an age in days. Returns None
        when latest is None (no data)."""
        if latest is None:
            return None
        if isinstance(latest, datetime):
            ref = latest if latest.tzinfo is not None else latest.replace(tzinfo=timezone.utc)
            return (now - ref).total_seconds() / 86400.0
        # Plain `date`. Promote to UTC-midnight datetime for the diff.
        try:
            ref = datetime(latest.year, latest.month, latest.day, tzinfo=timezone.utc)
        except AttributeError:
            return None
        return (now - ref).total_seconds() / 86400.0

    def _build_draft(
        self,
        *,
        row,                # CoverageRow
        exp: Expectation,
        result: EvaluationResult,
        cell,               # VerticalCoverage
    ) -> FindingDraft:
        # Severity by failure mode: missing-entirely is investigate;
        # partial / stale is warning.
        if exp.rule in ("present", "recency") and not cell.has_data:
            severity = "investigate"
        else:
            severity = self.severity

        fp = fingerprint(
            self.name,
            scope={
                "ticker": row.ticker,
                "vertical": exp.vertical,
                "rule": exp.rule,
            },
            rule_params=exp.params,
        )

        summary = (
            f"{row.ticker} — {exp.vertical} fails {exp.rule}: "
            f"{result.detail}"
        )

        suggested_action = self._suggest(row=row, exp=exp, result=result, cell=cell)

        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity=severity,
            company_id=row.company_id,
            ticker=row.ticker,
            vertical=exp.vertical,
            fiscal_period_key=None,
            evidence={
                "rule": exp.rule,
                "rule_params": exp.params,
                "actual": result.actual,
                "expected": result.expected,
                "detail": result.detail,
                "vertical_state": {
                    "row_count": cell.row_count,
                    "period_count": cell.period_count,
                    "earliest": cell.earliest.isoformat() if cell.earliest else None,
                    "latest": cell.latest.isoformat() if cell.latest else None,
                },
            },
            summary=summary,
            suggested_action=suggested_action,
        )

    def _suggest(self, *, row, exp: Expectation, result, cell) -> dict:
        """Build a structured suggested_action with prose explaining
        the most likely fix per (vertical, rule)."""
        ticker = row.ticker
        v = exp.vertical
        rule = exp.rule

        # Re-ingest commands per vertical
        commands_by_vertical = {
            "financials":    f"uv run scripts/backfill_fmp.py {ticker}",
            "segments":      f"uv run scripts/ingest_segments.py {ticker}",
            "employees":     f"uv run scripts/ingest_employees.py {ticker}",
            "sec_qual":      f"uv run scripts/fetch_sec_filings.py {ticker}",
            "press_release": f"uv run scripts/ingest_company.py {ticker}",
            "transcript":    f"uv run scripts/ingest_transcripts.py {ticker}",
        }
        cmd = commands_by_vertical.get(v, f"# inspect {v} ingest path for {ticker}")

        if not cell.has_data:
            prose = (
                f"{ticker} has no current rows in the {v!r} vertical. The "
                f"standard expects this data to be present. Most likely "
                f"cause: ingest for this vertical hasn't run for {ticker}. "
                f"Run the suggested command to fetch and load it. If "
                f"{ticker} legitimately has no {v} data (recent IPO, vendor "
                f"doesn't cover it), suppress with a reason — the suppression "
                f"reason IS the acceptance criteria and lives in the audit "
                f"trail."
            )
        elif rule == "min_periods":
            prose = (
                f"{ticker} has {result.actual} period(s) of {v} data; the "
                f"standard expects at least {result.expected}. Likely cause: "
                f"backfill window was narrower than the standard. Re-run with "
                f"a wider --since date. If the vendor genuinely doesn't have "
                f"older data for this ticker (IPO date, spinoff date), "
                f"suppress with a reason and an expiry pointing at when the "
                f"ticker should have enough history to revisit."
            )
        elif rule == "recency":
            prose = (
                f"{ticker}'s most-recent {v} data is {result.actual} days old; "
                f"the standard expects refreshes within {result.expected} days. "
                f"Likely cause: scheduled refresh hasn't run, or the vendor "
                f"hasn't published a newer value yet. Re-run the suggested "
                f"command."
            )
        else:
            prose = f"{v} fails {rule}: {result.detail}"

        return {
            "kind": f"reingest_{v}",
            "params": {"ticker": ticker, "vertical": v, "rule": rule},
            "command": cmd,
            "prose": prose,
        }
