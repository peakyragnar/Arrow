"""Dry-run coverage sweep across the golden_eval ticker set.

Purpose: Surface the FULL set of FMP-mapper coverage gaps in one pass,
rather than discovering them one ticker at a time via ingest halts.

What this does:
  1. For each ticker, fetch FMP IS/BS/CF (all periods FMP returns).
  2. In-memory only — NO database writes, NO ingest_runs. Repeat-safe.
  3. For each filing row, record:
       - which FMP fields were populated (non-null)
       - which populated fields the current mapper does NOT reference
       - which Layer-1 ties failed (with filer/computed/delta)
  4. Produce two reports:
       - Per-ticker: gaps + failed ties
       - Aggregate: "this gap appears on N tickers" (priority ranking)

Usage:
    uv run scripts/sweep_fmp_coverage.py
    uv run scripts/sweep_fmp_coverage.py NVDA DELL MSFT   # subset

Output: text report to stdout. No side effects.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any

from arrow.ingest.fmp.client import FMPClient

INCOME_STATEMENT_ENDPOINT = "income-statement"
BALANCE_SHEET_ENDPOINT = "balance-sheet-statement"
CASH_FLOW_ENDPOINT = "cash-flow-statement"
from arrow.normalize.financials.fmp_bs_mapper import (
    _BS_BUCKETS,
    map_balance_sheet_row,
)
from arrow.normalize.financials.fmp_cf_mapper import (
    _CF_BUCKETS,
    map_cash_flow_row,
)
from arrow.normalize.financials.fmp_is_mapper import (
    _IS_BUCKETS,
    map_income_statement_row,
)
from arrow.normalize.financials.verify_bs import verify_bs_ties
from arrow.normalize.financials.verify_cf import verify_cf_ties
from arrow.normalize.financials.verify_is import verify_is_ties


GOLDEN_EVAL_TICKERS = [
    "NVDA", "LYB", "SYM", "DELL", "MSFT", "PANW", "UNP", "FCX", "NUE",
    "VLO", "ET", "TDG", "OKLO", "S", "PLTR", "CAT", "ADM", "GOOGL",
    "KOP", "HWKN",
]

# Match ingest default `since_date` — only evaluate filings whose period_end
# falls on or after this date. Older periods are pre-validation-era and not
# relevant to the current ingest scope.
SWEEP_SINCE_DATE = "2021-01-01"


# ----- helpers --------------------------------------------------------------

def _mapper_known_fields(buckets: list) -> set[str]:
    """Pull the set of FMP field names referenced by a mapper's bucket list.

    IS mapper: tuples are (concept, fmp_field, unit) — string field.
    BS/CF mappers: tuples are (concept, [fmp_fields], unit) — list of fields.
    """
    known: set[str] = set()
    for entry in buckets:
        field_spec = entry[1]
        if isinstance(field_spec, str):
            known.add(field_spec)
        else:
            known.update(field_spec)
    return known


IS_KNOWN_FIELDS = _mapper_known_fields(_IS_BUCKETS)
BS_KNOWN_FIELDS = _mapper_known_fields(_BS_BUCKETS)
CF_KNOWN_FIELDS = _mapper_known_fields(_CF_BUCKETS)

# FMP returns these as structural metadata, not filer-reported numbers.
# Exclude from gap analysis so we don't flag them as "unmapped fields."
FMP_METADATA_FIELDS = {
    # Filing metadata (non-numeric or identifiers)
    "date", "symbol", "reportedCurrency", "cik", "filingDate",
    "acceptedDate", "fiscalYear", "period", "link", "finalLink",
    # ---- IS derived/aggregate fields (not independent filer inputs) ----
    "ebitda", "ebit",
    "netInterestIncome",
    "nonOperatingIncomeExcludingInterest",
    "totalOtherIncomeExpensesNet",
    "costAndExpenses",                                     # = cogs + total_opex
    "depreciationAndAmortization",                         # IS duplicate of CF line
    "otherExpenses",                                       # umbrella
    "otherAdjustmentsToNetIncome",                         # already rolled into netIncome
    "bottomLineNetIncome",                                 # duplicate of netIncome
    "netIncomeDeductions",                                 # part of NI chain, typically 0
    "netIncomeFromContinuingOperationsOtherAdjustments",   # part of NI chain
    # ---- BS derived/aggregate fields ----
    "cashAndShortTermInvestments",     # = cash + STI
    "netReceivables",                  # = accountsReceivables + otherReceivables
    "totalPayables",                   # = accountPayables + otherPayables
    "goodwillAndIntangibleAssets",     # = goodwill + intangibles
    "totalInvestments",                # aggregate
    "totalDebt",                       # aggregate debt
    "netDebt",                         # totalDebt - cash
    "totalNonCurrentAssets",           # subtotal
    "totalNonCurrentLiabilities",      # subtotal
    "capitalLeaseObligations",         # aggregate current + noncurrent
    "totalStockholdersEquity",         # equity without NCI; totalEquity already handles
    # ---- CF derived/aggregate fields + aliases ----
    "operatingCashFlow",                    # alias of netCashProvidedByOperatingActivities
    "capitalExpenditure",                   # alias of investmentsInPropertyPlantAndEquipment
    "freeCashFlow",                         # = cfo - capex (derived)
    "changeInWorkingCapital",               # = sum of individual WC change fields
    # FMP's "net_*_issuance" are derived from gross fields we already map.
    "netCommonStockIssuance", "netStockIssuance", "netPreferredStockIssuance",
    "netDebtIssuance", "netDividendsPaid",
    # Supplemental disclosures — not part of primary CF sections.
    "interestPaid", "incomeTaxesPaid",
    # CFO subtotal alias (the canonical field is netCashProvidedByOperatingActivities)
    # Nothing else needed here.
}


# Any field in the raw FMP row that's not a number is considered "not a
# reported numeric line" and excluded from gap analysis.
def _numeric_nonzero_fields(row: dict[str, Any]) -> set[str]:
    """Return the set of FMP field names in this row that are non-zero
    numeric values. Zero-valued fields are excluded — FMP includes
    placeholder zeros for every possible line regardless of whether the
    filer actually reports it (e.g., NVDA's generalAndAdministrativeExpenses
    is always 0 because NVDA only reports combined SG&A). Those zeros
    aren't coverage gaps, they're structural noise."""
    out = set()
    for k, v in row.items():
        if k in FMP_METADATA_FIELDS:
            continue
        if isinstance(v, (int, float)) and v != 0:
            out.add(k)
    return out


# ----- per-ticker analysis --------------------------------------------------

@dataclass
class StatementResult:
    periods_fetched: int = 0
    periods_evaluated: int = 0      # Layer 1 actually ran (had components)
    periods_passed: int = 0
    # (fmp_field_name, observed_abs_max) — max magnitude of an unmapped field across periods
    unmapped_fields: dict[str, Decimal] = field(default_factory=dict)
    # list of (fiscal_label, period_end, list[TieFailure])
    tie_failures: list[tuple[str, str, list]] = field(default_factory=list)


@dataclass
class TickerResult:
    ticker: str
    is_: StatementResult = field(default_factory=StatementResult)
    bs: StatementResult = field(default_factory=StatementResult)
    cf: StatementResult = field(default_factory=StatementResult)
    fetch_errors: list[str] = field(default_factory=list)


def _period_label(row: dict[str, Any]) -> str:
    return f"FY{row.get('fiscalYear')} {row.get('period')} {row.get('date')}"


def _analyze_is(ticker: str, rows: list[dict[str, Any]]) -> StatementResult:
    res = StatementResult()
    res.periods_fetched = len(rows)
    for row in rows:
        # Unmapped-field gap
        observed = _numeric_nonzero_fields(row)
        unmapped = observed - IS_KNOWN_FIELDS
        for f in unmapped:
            v = Decimal(str(row[f]))
            res.unmapped_fields[f] = max(
                res.unmapped_fields.get(f, Decimal(0)), abs(v)
            )
        # Layer 1 verification
        mapped = map_income_statement_row(row)
        values_by_concept = {m.concept: m.value for m in mapped}
        failures = verify_is_ties(values_by_concept)
        res.periods_evaluated += 1
        if not failures:
            res.periods_passed += 1
        else:
            res.tie_failures.append(
                (_period_label(row), row.get("date", ""), failures)
            )
    return res


def _analyze_bs(ticker: str, rows: list[dict[str, Any]]) -> StatementResult:
    res = StatementResult()
    res.periods_fetched = len(rows)
    for row in rows:
        observed = _numeric_nonzero_fields(row)
        unmapped = observed - BS_KNOWN_FIELDS
        for f in unmapped:
            v = Decimal(str(row[f]))
            res.unmapped_fields[f] = max(
                res.unmapped_fields.get(f, Decimal(0)), abs(v)
            )
        mapped = map_balance_sheet_row(row)
        values_by_concept = {m.concept: m.value for m in mapped}
        failures = verify_bs_ties(values_by_concept)
        res.periods_evaluated += 1
        if not failures:
            res.periods_passed += 1
        else:
            res.tie_failures.append(
                (_period_label(row), row.get("date", ""), failures)
            )
    return res


def _analyze_cf(ticker: str, rows: list[dict[str, Any]]) -> StatementResult:
    res = StatementResult()
    res.periods_fetched = len(rows)
    for row in rows:
        observed = _numeric_nonzero_fields(row)
        unmapped = observed - CF_KNOWN_FIELDS
        for f in unmapped:
            v = Decimal(str(row[f]))
            res.unmapped_fields[f] = max(
                res.unmapped_fields.get(f, Decimal(0)), abs(v)
            )
        mapped = map_cash_flow_row(row)
        values_by_concept = {m.concept: m.value for m in mapped}
        failures = verify_cf_ties(values_by_concept)
        res.periods_evaluated += 1
        if not failures:
            res.periods_passed += 1
        else:
            res.tie_failures.append(
                (_period_label(row), row.get("date", ""), failures)
            )
    return res


def _fetch_all_periods(client: FMPClient, endpoint: str, ticker: str) -> list[dict[str, Any]]:
    """Fetch quarterly + annual periods from FMP, filtered to post-SWEEP_SINCE_DATE."""
    rows: list[dict[str, Any]] = []
    for period in ("Q1", "Q2", "Q3", "Q4", "annual"):
        resp = client.get(endpoint, symbol=ticker, period=period, limit=40)
        data = json.loads(resp.body)
        for row in data:
            d = row.get("date", "")
            if d >= SWEEP_SINCE_DATE:
                rows.append(row)
    return rows


def sweep_ticker(client: FMPClient, ticker: str) -> TickerResult:
    result = TickerResult(ticker=ticker)
    try:
        is_rows = _fetch_all_periods(client, INCOME_STATEMENT_ENDPOINT, ticker)
        result.is_ = _analyze_is(ticker, is_rows)
    except Exception as e:
        result.fetch_errors.append(f"IS: {type(e).__name__}: {e}")
    try:
        bs_rows = _fetch_all_periods(client, BALANCE_SHEET_ENDPOINT, ticker)
        result.bs = _analyze_bs(ticker, bs_rows)
    except Exception as e:
        result.fetch_errors.append(f"BS: {type(e).__name__}: {e}")
    try:
        cf_rows = _fetch_all_periods(client, CASH_FLOW_ENDPOINT, ticker)
        result.cf = _analyze_cf(ticker, cf_rows)
    except Exception as e:
        result.fetch_errors.append(f"CF: {type(e).__name__}: {e}")
    return result


# ----- reporting ------------------------------------------------------------

def _fmt_m(v: Decimal) -> str:
    """Format a Decimal amount as a compact magnitude with $M suffix."""
    abs_v = abs(v)
    if abs_v >= Decimal("1e9"):
        return f"${v/Decimal('1e9'):,.2f}B"
    if abs_v >= Decimal("1e6"):
        return f"${v/Decimal('1e6'):,.1f}M"
    if abs_v >= Decimal("1e3"):
        return f"${v/Decimal('1e3'):,.1f}K"
    return f"${v:,.0f}"


def _print_statement_block(stmt: str, sr: StatementResult) -> None:
    print(f"  {stmt}:")
    print(f"    periods fetched / evaluated / passed: "
          f"{sr.periods_fetched} / {sr.periods_evaluated} / {sr.periods_passed}")
    if sr.unmapped_fields:
        print(f"    UNMAPPED fields observed ({len(sr.unmapped_fields)}):")
        for f, mag in sorted(sr.unmapped_fields.items(),
                             key=lambda kv: kv[1], reverse=True):
            print(f"      - {f}:  max observed = {_fmt_m(mag)}")
    if sr.tie_failures:
        # Bucket tie failures by tie name
        by_tie = defaultdict(list)
        for label, _end, fails in sr.tie_failures:
            for f in fails:
                by_tie[f.tie].append((label, f.delta))
        print(f"    LAYER-1 tie failures ({sum(len(v) for v in by_tie.values())} total):")
        for tie, items in by_tie.items():
            print(f"      * {tie}  ({len(items)} period(s))")
            for label, delta in items[:3]:
                print(f"          - {label}  Δ={_fmt_m(delta)}")
            if len(items) > 3:
                print(f"          - ... +{len(items)-3} more")


def _print_per_ticker(results: list[TickerResult]) -> None:
    print("=" * 80)
    print("PER-TICKER RESULTS")
    print("=" * 80)
    for r in results:
        print(f"\n{r.ticker}")
        if r.fetch_errors:
            for e in r.fetch_errors:
                print(f"  FETCH ERROR — {e}")
        _print_statement_block("IS", r.is_)
        _print_statement_block("BS", r.bs)
        _print_statement_block("CF", r.cf)


def _print_aggregate(results: list[TickerResult]) -> None:
    # unmapped fields: field → tickers-affected
    is_unmapped: dict[str, set[str]] = defaultdict(set)
    bs_unmapped: dict[str, set[str]] = defaultdict(set)
    cf_unmapped: dict[str, set[str]] = defaultdict(set)
    is_tie_fail: dict[str, set[str]] = defaultdict(set)
    bs_tie_fail: dict[str, set[str]] = defaultdict(set)
    cf_tie_fail: dict[str, set[str]] = defaultdict(set)

    for r in results:
        for f in r.is_.unmapped_fields:
            is_unmapped[f].add(r.ticker)
        for f in r.bs.unmapped_fields:
            bs_unmapped[f].add(r.ticker)
        for f in r.cf.unmapped_fields:
            cf_unmapped[f].add(r.ticker)
        for (_, _, fails) in r.is_.tie_failures:
            for ff in fails:
                is_tie_fail[ff.tie].add(r.ticker)
        for (_, _, fails) in r.bs.tie_failures:
            for ff in fails:
                bs_tie_fail[ff.tie].add(r.ticker)
        for (_, _, fails) in r.cf.tie_failures:
            for ff in fails:
                cf_tie_fail[ff.tie].add(r.ticker)

    print("\n" + "=" * 80)
    print("AGGREGATE: UNMAPPED FMP FIELDS (ranked by tickers affected)")
    print("=" * 80)
    for label, bucket in [
        ("INCOME STATEMENT", is_unmapped),
        ("BALANCE SHEET", bs_unmapped),
        ("CASH FLOW", cf_unmapped),
    ]:
        print(f"\n{label}:")
        if not bucket:
            print("  (none — all observed FMP fields are mapped)")
            continue
        for f, tickers in sorted(bucket.items(),
                                 key=lambda kv: (-len(kv[1]), kv[0])):
            print(f"  [{len(tickers):>2}/{len(results)}] {f:<50}  tickers: {', '.join(sorted(tickers))}")

    print("\n" + "=" * 80)
    print("AGGREGATE: LAYER-1 TIE FAILURES (ranked by tickers affected)")
    print("=" * 80)
    for label, bucket in [
        ("INCOME STATEMENT", is_tie_fail),
        ("BALANCE SHEET", bs_tie_fail),
        ("CASH FLOW", cf_tie_fail),
    ]:
        print(f"\n{label}:")
        if not bucket:
            print("  (none — all Layer 1 ties pass)")
            continue
        for tie, tickers in sorted(bucket.items(),
                                   key=lambda kv: (-len(kv[1]), kv[0])):
            print(f"  [{len(tickers):>2}/{len(results)}] {tie}")
            print(f"        tickers: {', '.join(sorted(tickers))}")


# ----- main -----------------------------------------------------------------

def main() -> int:
    tickers = [t.upper() for t in sys.argv[1:]] or GOLDEN_EVAL_TICKERS
    client = FMPClient()
    results: list[TickerResult] = []
    for i, t in enumerate(tickers, 1):
        print(f"[{i}/{len(tickers)}] sweeping {t}...", file=sys.stderr)
        results.append(sweep_ticker(client, t))

    _print_per_ticker(results)
    _print_aggregate(results)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
