"""Coverage expectations.

Single uniform standard applied to any ticker in `coverage_membership`.
Read by the ``expected_coverage`` check to compare expected vs. actual
and surface gaps as findings.

Design (post-V1.1 simplification, see commit history):
  Earlier V1 had two tiers (`core` / `extended`) and a
  `PER_TICKER_OVERRIDES` constant for legitimate exceptions (recent
  IPOs, spinoffs). Both were dropped because:

    1. Tiers prevented cross-ticker comparison — different depths in
       the same coverage universe meant analyses couldn't trust
       symmetric history. The right binary is "tracked or not,"
       not "tracked at what strictness."
    2. PER_TICKER_OVERRIDES encoded operator judgment in code,
       silently filtering findings before the operator could see
       them. Legitimate exceptions (CRWV recent IPO) ARE the
       operator's acceptance criteria — they belong in the suppress
       reason on a finding, not in a Python constant. That way
       every exception lives in the audit trail and becomes V2
       training data.

  When a tracked ticker can't meet the standard for legitimate
  reasons, the steward fires a finding. The operator suppresses it
  with a clear note. The note is the acceptance criteria; the
  audit trail records the decision.

Three rule kinds in V1:
  - ``present``     vertical has at least 1 current row
  - ``min_periods`` at least N distinct periods (period_count >= N)
  - ``recency``     latest period is no older than ``max_age_days``

Adding a rule kind: define it here, add an evaluator branch in
``evaluate_expectation`` below, and add a corresponding suggested-
action prose in the check.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Expectation:
    """One expectation for one (ticker, vertical) pair.

    The vertical is the one the rule applies to. The rule + params
    define the assertion.
    """

    vertical: str
    rule: str       # 'present' | 'min_periods' | 'recency'
    params: dict[str, Any]


#: The single uniform standard applied to any ticker in coverage.
#: 5 years of quarterly data (or whatever exists), 5 years of SEC
#: filings (or whatever exists), employees within the last fiscal
#: year, segments where the company reports them.
STANDARD: list[Expectation] = [
    # 5 years of quarterly financials (the dashboard's audit horizon).
    Expectation("financials", "min_periods", {"count": 20}),
    # Some segment data. Filers that don't report segments will
    # surface findings the operator can suppress with a clear note.
    Expectation("segments", "present", {}),
    # Employee count refreshed within the last ~14 months. FMP's
    # historical-employee-count is annual.
    Expectation("employees", "recency", {"max_age_days": 400}),
    # 5 years of qualitative SEC filings (≈20 distinct fiscal periods).
    Expectation("sec_qual", "min_periods", {"count": 20}),
    # 5 years of quarterly earnings-call transcripts, refreshed shortly
    # after each quarterly call. Recent IPO/spinoff exceptions are handled
    # through finding suppressions, not per-ticker code overrides.
    Expectation("transcript", "present", {}),
    Expectation("transcript", "min_periods", {"count": 20}),
    Expectation("transcript", "recency", {"max_age_days": 150}),
]


def expectations_for(ticker: str) -> list[Expectation]:
    """Resolve effective expectations for a ticker.

    Single uniform standard — `ticker` is currently unused but kept
    in the signature so per-ticker rules CAN be added later via the
    audit trail (suppression policies, dynamic adjustments) without
    rewriting callers.
    """
    return list(STANDARD)


@dataclass(frozen=True)
class EvaluationResult:
    """Outcome of comparing one expectation against actual state."""

    met: bool
    actual: Any        # the observed value (count, age_days, etc.)
    expected: Any      # the threshold expected
    detail: str        # short prose for the finding summary


def evaluate_expectation(
    exp: Expectation,
    *,
    has_data: bool,
    period_count: int,
    latest_age_days: float | None,
) -> EvaluationResult:
    """Evaluate one expectation against the observed state of a
    (ticker, vertical) pair.

    Inputs come from a ``VerticalCoverage`` instance produced by
    ``arrow.steward.coverage``:
      - has_data: row_count > 0
      - period_count: distinct periods present
      - latest_age_days: days since the latest period (None if no data)
    """
    if exp.rule == "present":
        if has_data:
            return EvaluationResult(True, actual=True, expected=True, detail="present")
        return EvaluationResult(False, actual=False, expected=True,
                                 detail="vertical has no current rows")

    if exp.rule == "min_periods":
        threshold = int(exp.params.get("count", 0))
        if period_count >= threshold:
            return EvaluationResult(True, actual=period_count, expected=threshold,
                                     detail=f"{period_count} ≥ {threshold} periods")
        return EvaluationResult(False, actual=period_count, expected=threshold,
                                 detail=f"{period_count} < {threshold} expected periods")

    if exp.rule == "recency":
        max_age = int(exp.params.get("max_age_days", 0))
        if not has_data:
            return EvaluationResult(False, actual=None, expected=max_age,
                                     detail="no data to evaluate recency against")
        if latest_age_days is None:
            return EvaluationResult(False, actual=None, expected=max_age,
                                     detail="latest period unknown (NULL date)")
        if latest_age_days <= max_age:
            return EvaluationResult(True, actual=int(latest_age_days), expected=max_age,
                                     detail=f"latest is {int(latest_age_days)}d old (≤ {max_age}d)")
        return EvaluationResult(False, actual=int(latest_age_days), expected=max_age,
                                 detail=f"latest is {int(latest_age_days)}d old (> {max_age}d allowed)")

    raise ValueError(f"unknown expectation rule: {exp.rule!r}")
