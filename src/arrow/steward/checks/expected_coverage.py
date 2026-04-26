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
(financials / segments / employees / sec_qual / press_release) so
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

            try:
                expectations = expectations_for(row.ticker, row.tier)
            except ValueError:
                # Unknown tier — schema CHECK should prevent this, but if it
                # ever happens, surface a finding rather than crashing the run.
                yield self._unknown_tier_finding(row)
                continue

            for exp in expectations:
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
            f"{row.ticker} ({row.tier}) — {exp.vertical} fails {exp.rule}: "
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
                "tier": row.tier,
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
        }
        cmd = commands_by_vertical.get(v, f"# inspect {v} ingest path for {ticker}")

        if not cell.has_data:
            prose = (
                f"{ticker} has no current rows in the {v!r} vertical. The "
                f"{row.tier!r} tier expects this data to be present. "
                f"Most likely cause: ingest for this vertical hasn't run for "
                f"{ticker}. Run the suggested command to fetch and load it. "
                f"If {ticker} legitimately has no {v} data (e.g. recent IPO, "
                f"vendor doesn't cover it), suppress with reason; if the "
                f"tier expectation itself is wrong for this ticker, edit "
                f"`PER_TICKER_OVERRIDES` in src/arrow/steward/expectations.py."
            )
        elif rule == "min_periods":
            prose = (
                f"{ticker} has {result.actual} period(s) of {v} data; the "
                f"{row.tier!r} tier expects at least {result.expected}. "
                f"Likely cause: backfill window was narrower than the "
                f"expectation. Re-run with a wider --since date. If the "
                f"vendor truly doesn't have older data for this ticker "
                f"(IPO date, spinoff date), add a per-ticker override in "
                f"expectations.py rather than suppressing repeatedly."
            )
        elif rule == "recency":
            prose = (
                f"{ticker}'s most-recent {v} data is {result.actual} days old; "
                f"the {row.tier!r} tier expects refreshes within {result.expected} "
                f"days. Likely cause: scheduled refresh hasn't run, or the "
                f"vendor hasn't published a newer value yet. Re-run the "
                f"suggested command."
            )
        else:
            prose = f"{v} fails {rule}: {result.detail}"

        return {
            "kind": f"reingest_{v}",
            "params": {"ticker": ticker, "vertical": v, "rule": rule},
            "command": cmd,
            "prose": prose,
        }

    def _unknown_tier_finding(self, row) -> FindingDraft:
        fp = fingerprint(
            self.name,
            scope={"ticker": row.ticker, "vertical": "_meta", "rule": "unknown_tier"},
            rule_params={"tier": row.tier},
        )
        return FindingDraft(
            fingerprint=fp,
            finding_type=self.name,
            severity="investigate",
            company_id=row.company_id,
            ticker=row.ticker,
            vertical=None,
            fiscal_period_key=None,
            evidence={"tier": row.tier},
            summary=f"{row.ticker} has unrecognized tier {row.tier!r} — no expectations defined.",
            suggested_action={
                "kind": "fix_membership",
                "params": {"ticker": row.ticker, "tier": row.tier},
                "command": f"# Update tier via the dashboard or:\nuv run python -c \"...\"",
                "prose": (
                    f"Tier {row.tier!r} is not in UNIVERSE_DEFAULTS. Either add "
                    f"the tier to expectations.py or change this membership to a "
                    f"known tier."
                ),
            },
        )
