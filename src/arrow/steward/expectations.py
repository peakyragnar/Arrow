"""Coverage expectations.

Per-tier rules describing what each ticker SHOULD have, by vertical.
Read by the ``expected_coverage`` check to compare expected vs. actual
and surface gaps as findings.

Design note (lean default applied):
  Lives as a Python module in V1, NOT a database table. Promotes to
  a ``coverage_expectations`` table when one of these is true:
    - Rules grow past what one file holds comfortably (>~50 lines)
    - Operator wants to edit exceptions through the dashboard
    - Per-period or PIT expectations need real history
  Until then: edit this file, restart the dashboard / re-run the
  steward, the new rules take effect on the next sweep. Cheap and
  honest. See `docs/architecture/steward.md` § ExpectationSet for
  the rationale.

Three rule kinds in V1:
  - ``present``     vertical has at least 1 current row
  - ``min_periods`` at least N distinct periods (period_count >= N)
  - ``recency``     latest period is no older than ``max_age_days``

Adding a rule kind: define it here, add an evaluator branch in
``evaluate_expectation`` below, and add a corresponding suggested-
action prose in the check.

Per-ticker overrides exist for legitimate exceptions (recent IPOs,
spinoffs) where the tier-default would always fail. Permanent
suppression of a single finding is a separate lever (suppress that
finding via the dashboard); use overrides only when the tier rule
itself doesn't apply to the ticker.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class Expectation:
    """One expectation for one (tier-implied) (ticker, vertical) pair.

    The vertical is the one the rule applies to. The rule + params
    define the assertion.
    """

    vertical: str
    rule: str       # 'present' | 'min_periods' | 'recency'
    params: dict[str, Any]


#: Default expectations per coverage tier. Apply unless overridden in
#: PER_TICKER_OVERRIDES below.
UNIVERSE_DEFAULTS: dict[str, list[Expectation]] = {
    "core": [
        # 5 years of quarterly financials (the dashboard's audit horizon).
        Expectation("financials", "min_periods", {"count": 20}),
        # Some segment data. (Quarterly segments are expected; some filers
        # only report annually — handled via per-ticker overrides if needed.)
        Expectation("segments", "present", {}),
        # Employee count refreshed within the last ~14 months. FMP's
        # historical-employee-count is annual, so 400d covers the
        # last completed FY plus refresh slop.
        Expectation("employees", "recency", {"max_age_days": 400}),
        # 5 years of qualitative SEC filings (≈25: 5 10-K + 20 10-Q,
        # but distinct fiscal_period_key works out to ~20).
        Expectation("sec_qual", "min_periods", {"count": 20}),
    ],
    "extended": [
        # Lighter quality bar: 2 years of quarterly financials.
        Expectation("financials", "min_periods", {"count": 8}),
        # Some SEC qualitative present (recency check would also be
        # reasonable; keeping it 'present' to avoid noise on extended).
        Expectation("sec_qual", "present", {}),
    ],
}


#: Per-ticker overrides. Replaces the corresponding tier default's
#: ``params`` (NOT the rule). Use for tickers where the default rule
#: APPLIES but the threshold is wrong (recent IPO with short history,
#: spinoff with no pre-spin data).
#:
#: To remove a vertical entirely from a ticker's expectations, set
#: ``params={"count": 0}`` for ``min_periods`` or simply suppress
#: the resulting finding through the dashboard.
PER_TICKER_OVERRIDES: dict[str, dict[str, dict[str, Any]]] = {
    # CoreWeave IPO'd 2025-03-28 — only ~1 year of public quarterly history.
    "CRWV": {
        "financials": {"count": 4},   # don't expect 5y of quarters yet
        "sec_qual":   {"count": 4},
    },
    # GE Vernova spun off from GE on 2024-04-02 — 2 years of public history.
    "GEV": {
        "financials": {"count": 8},
        "sec_qual":   {"count": 8},
    },
}


def expectations_for(ticker: str, tier: str) -> list[Expectation]:
    """Resolve effective expectations for a ticker, applying any
    per-ticker overrides on top of the tier defaults.

    Returns a list of Expectation instances with effective params.
    """
    ticker = ticker.upper()
    if tier not in UNIVERSE_DEFAULTS:
        raise ValueError(f"unknown tier: {tier!r}")

    defaults = UNIVERSE_DEFAULTS[tier]
    overrides = PER_TICKER_OVERRIDES.get(ticker, {})

    out: list[Expectation] = []
    for exp in defaults:
        params = overrides.get(exp.vertical)
        if params is not None:
            out.append(Expectation(
                vertical=exp.vertical,
                rule=exp.rule,
                params={**exp.params, **params},
            ))
        else:
            out.append(exp)
    return out


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
