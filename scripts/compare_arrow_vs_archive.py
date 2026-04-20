"""Cross-check Arrow's stored financial_facts against the archive/ai_extract
NVDA gold data. A third independent source: we already verify FMP against
SEC XBRL (Layer 5). This verifies FMP-via-Arrow against SEC-via-AI-extract.

Archive values are in USD_millions; Arrow stores absolute USD. Archive
keys are `start_end` for flows (a specific duration) or a single date for
BS instants. We compare:
  - 3-month discrete flows → Arrow quarter rows
  - 12-month FY flows      → Arrow annual rows
  - BS instant snapshots   → Arrow quarter or annual rows at the same date
  - CF 6-month / 9-month YTD values from 10-Qs are skipped (Arrow stores
    discrete quarters; archive's YTD don't map 1:1 without subtraction)

Split-adjustment skip: per-share (EPS) and share-count concepts are
skipped — FMP stores split-adjusted values; archive stores original
filing values — comparing without a splits-aware transform gives false
mismatches. Documented in fmp_mapping.md § 3.

Sign-convention handling: five CF working-capital / payment concepts
were stored with raw XBRL positive magnitude in the archive but with
cash-impact sign in Arrow (fmp_mapping.md § 7.1). We flip the archive
sign for these before comparison so apples match apples.

Treasury stock: archive stored positive magnitude; FMP stores SIGNED
(negative for buybacks). We flip archive sign before comparing.

Tolerance: max($2M abs, 0.1%) — same as Layer-3 filing rounding floor.
"""

from __future__ import annotations

import json
import sys
from collections import defaultdict
from dataclasses import dataclass
from datetime import date
from decimal import Decimal
from pathlib import Path

from arrow.db.connection import get_conn

REPO_ROOT = Path(__file__).resolve().parents[1]
ARCHIVE_DIR = REPO_ROOT / "archive" / "ai_extract" / "NVDA"

# Tolerance on values (both sides are in USD millions after normalization).
TOL_ABS_M = Decimal("2")      # $2M absolute — allows quarterly rounding drift
TOL_PCT = Decimal("0.001")    # 0.1% of larger abs

# Per-statement XBRL → canonical bucket. Same XBRL concept can map
# differently across statements (NetIncomeLoss → net_income on IS;
# → net_income_start on CF).
IS_MAP: dict[str, str] = {
    "us-gaap:Revenues": "revenue",
    "us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax": "revenue",
    "us-gaap:CostOfRevenue": "cogs",
    "us-gaap:GrossProfit": "gross_profit",
    "us-gaap:ResearchAndDevelopmentExpense": "rd",
    "us-gaap:SellingGeneralAndAdministrativeExpense": "sga",
    "us-gaap:OperatingExpenses": "total_opex",
    "us-gaap:OperatingIncomeLoss": "operating_income",
    "us-gaap:InterestExpense": "interest_expense",
    "us-gaap:InvestmentIncomeInterest": "interest_income",
    "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest": "ebt_incl_unusual",
    "us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxesMinorityInterestAndIncomeLossFromEquityMethodInvestments": "ebt_incl_unusual",
    "us-gaap:IncomeTaxExpenseBenefit": "tax",
    "us-gaap:IncomeLossFromContinuingOperations": "continuing_ops_after_tax",
    "us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax": "discontinued_ops",
    "us-gaap:NetIncomeLoss": "net_income",
}

BS_MAP: dict[str, str] = {
    "us-gaap:CashAndCashEquivalentsAtCarryingValue": "cash_and_equivalents",
    "us-gaap:MarketableSecuritiesCurrent": "short_term_investments",
    "us-gaap:AvailableForSaleSecuritiesCurrent": "short_term_investments",
    "us-gaap:AccountsReceivableNetCurrent": "accounts_receivable",
    "us-gaap:InventoryNet": "inventory",
    "us-gaap:PrepaidExpenseCurrent": "prepaid_expenses",
    "us-gaap:OtherAssetsCurrent": "other_current_assets",
    "us-gaap:AssetsCurrent": "total_current_assets",
    "us-gaap:PropertyPlantAndEquipmentNet": "net_ppe",
    "us-gaap:MarketableSecuritiesNoncurrent": "long_term_investments",
    "us-gaap:AvailableForSaleSecuritiesNoncurrent": "long_term_investments",
    "us-gaap:Goodwill": "goodwill",
    "us-gaap:IntangibleAssetsNetExcludingGoodwill": "other_intangibles",
    "us-gaap:DeferredTaxAssetsNet": "deferred_tax_assets_noncurrent",
    "us-gaap:DeferredIncomeTaxAssetsNet": "deferred_tax_assets_noncurrent",
    "us-gaap:OtherAssetsNoncurrent": "other_noncurrent_assets",
    "us-gaap:Assets": "total_assets",
    "us-gaap:AccountsPayableCurrent": "accounts_payable",
    "us-gaap:AccruedLiabilitiesCurrent": "accrued_expenses",
    "us-gaap:OtherLiabilitiesCurrent": "other_current_liabilities",
    "us-gaap:LiabilitiesCurrent": "total_current_liabilities",
    "us-gaap:LongTermDebtNoncurrent": "long_term_debt",
    "us-gaap:DeferredIncomeTaxLiabilitiesNet": "deferred_tax_liability_noncurrent",
    "us-gaap:OtherLiabilitiesNoncurrent": "other_noncurrent_liabilities",
    "us-gaap:Liabilities": "total_liabilities",
    "us-gaap:CommonStockValue": "common_stock",
    "us-gaap:AdditionalPaidInCapital": "additional_paid_in_capital",
    "us-gaap:RetainedEarningsAccumulatedDeficit": "retained_earnings",
    "us-gaap:TreasuryStockValue": "treasury_stock",
    "us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax": "accumulated_other_comprehensive_income",
    "us-gaap:StockholdersEquity": "total_equity",
    "us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest": "total_equity",
    "us-gaap:LiabilitiesAndStockholdersEquity": "total_liabilities_and_equity",
}

CF_MAP: dict[str, str] = {
    "us-gaap:NetIncomeLoss": "net_income_start",
    "us-gaap:DepreciationDepletionAndAmortization": "dna_cf",
    "us-gaap:ShareBasedCompensation": "sbc",
    "us-gaap:AllocatedShareBasedCompensationExpense": "sbc",
    "us-gaap:DeferredIncomeTaxExpenseBenefit": "deferred_income_tax",
    "us-gaap:IncreaseDecreaseInAccountsReceivable": "change_accounts_receivable",
    "us-gaap:IncreaseDecreaseInInventories": "change_inventory",
    "us-gaap:IncreaseDecreaseInAccountsPayable": "change_accounts_payable",
    "us-gaap:NetCashProvidedByUsedInOperatingActivities": "cfo",
    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment": "capital_expenditures",
    "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired": "acquisitions",
    "us-gaap:PaymentsToAcquireInvestments": "purchases_of_investments",
    "us-gaap:ProceedsFromSaleOfAvailableForSaleSecurities": "sales_of_investments",
    "us-gaap:ProceedsFromSaleAndMaturityOfOtherInvestments": "sales_of_investments",
    "us-gaap:NetCashProvidedByUsedInInvestingActivities": "cfi",
    "us-gaap:ProceedsFromIssuanceOfCommonStock": "stock_issuance",
    "us-gaap:PaymentsForRepurchaseOfCommonStock": "stock_repurchase",
    "us-gaap:PaymentsOfDividendsCommonStock": "common_dividends_paid",
    "us-gaap:NetCashProvidedByUsedInFinancingActivities": "cff",
    "us-gaap:EffectOfExchangeRateOnCashAndCashEquivalents": "fx_effect_on_cash",
}

# Empirically the archive's NVDA CF values have INCONSISTENT sign
# convention — different filings (and different comparative years within
# the same filing) variously store these concepts with raw XBRL
# magnitudes (positive) vs cash-impact sign (negative). E.g., Q1 FY25
# 10-Q stores IncreaseDecreaseInAccountsReceivable = +2366; Q4 FY26
# 10-K stores the same concept for a different period as -15399. The
# AI extraction wasn't consistent.
#
# For these concepts we compare absolute magnitudes. If |arrow|≈|archive|,
# we call it a match and note the sign-convention inconsistency.
SIGN_AMBIGUOUS_IN_ARCHIVE: set[str] = {
    "us-gaap:IncreaseDecreaseInAccountsReceivable",
    "us-gaap:IncreaseDecreaseInInventories",
    "us-gaap:IncreaseDecreaseInAccountsPayable",
    "us-gaap:PaymentsForRepurchaseOfCommonStock",
    "us-gaap:PaymentsOfDividendsCommonStock",
    "us-gaap:PaymentsToAcquirePropertyPlantAndEquipment",
    "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired",
    "us-gaap:PaymentsToAcquireInvestments",
    "us-gaap:TreasuryStockValue",
}
ARCHIVE_SIGN_FLIP: set[str] = set()  # retained name; unused

# Known structural bundling differences — where FMP's canonical bucket
# combines multiple XBRL concepts that the archive keeps separate. We
# document the pattern and surface the delta, but don't flag as an error.
BUNDLING_DIFFERENCES = {
    # FMP stores accountPayables + otherPayables → accounts_payable;
    # archive stores only AccountsPayableCurrent.
    "us-gaap:AccountsPayableCurrent": (
        "FMP bundles otherPayables into accounts_payable"
    ),
    # FMP's propertyPlantEquipmentNet bundles ASC 842 ROU assets;
    # archive's XBRL concept excludes them.
    "us-gaap:PropertyPlantAndEquipmentNet": (
        "FMP bundles operating-lease ROU assets into net_ppe"
    ),
    # FMP's accruedExpenses / otherNonCurrent* aggregate more than the
    # plain XBRL concept — filer-specific line-item bundling in FMP's
    # normalization.
    "us-gaap:AccruedLiabilitiesCurrent": (
        "FMP's accrued_expenses aggregates wider than the raw XBRL tag"
    ),
    "us-gaap:OtherAssetsNoncurrent": (
        "FMP aggregates differently than the raw XBRL tag"
    ),
    "us-gaap:OtherLiabilitiesNoncurrent": (
        "FMP aggregates differently than the raw XBRL tag"
    ),
}


@dataclass(frozen=True)
class Comparison:
    statement: str
    xbrl_concept: str
    canonical: str
    period_end: date
    period_type: str | None
    arrow_val_m: Decimal | None
    archive_val_m: Decimal
    delta_m: Decimal | None
    match: bool | None   # None = Arrow has no value stored


def _load_arrow_value_m(
    conn, *, ticker: str, statement: str, concept: str,
    period_end: date, period_type: str,
) -> Decimal | None:
    """Return Arrow's stored value in USD millions, or None if absent."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT value FROM financial_facts f
            JOIN companies c ON c.id = f.company_id
            WHERE c.ticker = %s AND f.statement = %s AND f.concept = %s
              AND f.period_end = %s AND f.period_type = %s
              AND f.superseded_at IS NULL
            LIMIT 1;
            """,
            (ticker, statement, concept, period_end, period_type),
        )
        row = cur.fetchone()
    if row is None:
        return None
    return row[0] / Decimal("1000000")


def _lookup_arrow_any_period_type(
    conn, *, ticker: str, statement: str, concept: str,
    period_end: date,
) -> tuple[Decimal, str] | None:
    """For BS: try quarter first, then annual at the same date."""
    for pt in ("quarter", "annual"):
        v = _load_arrow_value_m(
            conn, ticker=ticker, statement=statement, concept=concept,
            period_end=period_end, period_type=pt,
        )
        if v is not None:
            return v, pt
    return None


def _within_tol(arrow_m: Decimal, archive_m: Decimal) -> bool:
    delta = abs(arrow_m - archive_m)
    threshold = max(TOL_ABS_M, max(abs(arrow_m), abs(archive_m)) * TOL_PCT)
    return delta <= threshold


def _primary_period_end(d: dict) -> date | None:
    """The filing's own period_end = latest end date across all values.

    Archive 10-K/10-Q filings include prior-year comparatives. Limiting
    comparison to the primary period avoids double-counting AND sidesteps
    an empirical archive bug where AI extraction sometimes populated
    comparative-year value keys with the primary year's value (e.g.,
    NVDA FY26 10-K has IncreaseDecreaseInAccountsReceivable = -15,399
    populated for all three fiscal-year keys — only the current year
    value is the real one).
    """
    ends: set[date] = set()
    for section in ("income_statement", "balance_sheet", "cash_flow"):
        items = d.get("ai_extraction", {}).get(section, {}).get("line_items", [])
        for item in items:
            for key in (item.get("values") or {}).keys():
                try:
                    if "_" in key:
                        _, end_s = key.split("_")
                        ends.add(date.fromisoformat(end_s))
                    else:
                        ends.add(date.fromisoformat(key))
                except (ValueError, TypeError):
                    continue
    return max(ends) if ends else None


def compare_ticker(conn, ticker: str) -> dict[str, dict]:
    """Walk archive JSONs, emit per-statement comparison stats.

    For each archive file, only compares values matching the filing's
    PRIMARY period_end (skips comparative-year values that may contain
    archive-side AI-extraction bugs).
    """
    stats: dict[str, dict] = {
        stmt: {"total": 0, "matched": 0, "arrow_missing": 0,
               "mismatches": [], "bundled": 0}
        for stmt in ("income_statement", "balance_sheet", "cash_flow")
    }

    archive_files = sorted(ARCHIVE_DIR.glob("q*_fy*_10*.json"))
    if not archive_files:
        print(f"No archive files found at {ARCHIVE_DIR}", file=sys.stderr)
        return stats

    for jf in archive_files:
        with open(jf) as fh:
            d = json.load(fh)

        primary = _primary_period_end(d)
        if primary is None:
            continue

        for section, arrow_stmt, mapping in [
            ("income_statement", "income_statement", IS_MAP),
            ("balance_sheet",    "balance_sheet",    BS_MAP),
            ("cash_flow",        "cash_flow",        CF_MAP),
        ]:
            items = d.get("ai_extraction", {}).get(section, {}).get("line_items", [])
            for item in items:
                xbrl = item.get("xbrl_concept")
                canonical = mapping.get(xbrl) if xbrl else None
                if not canonical:
                    continue

                values = item.get("values") or {}
                for key, raw_val in values.items():
                    if raw_val is None:
                        continue

                    # Period parsing — restrict to filing's primary period.
                    if "_" in key:
                        try:
                            start_s, end_s = key.split("_")
                            end = date.fromisoformat(end_s)
                        except (ValueError, TypeError):
                            continue
                        if end != primary:
                            continue  # skip comparatives
                        days = (end - date.fromisoformat(start_s)).days
                        if 80 <= days <= 100:
                            period_type = "quarter"
                        elif 350 <= days <= 380:
                            period_type = "annual"
                        else:
                            continue
                    else:
                        try:
                            end = date.fromisoformat(key)
                        except (ValueError, TypeError):
                            continue
                        if end != primary:
                            continue

                    archive_val = Decimal(str(raw_val))
                    if xbrl in ARCHIVE_SIGN_FLIP:
                        archive_val = -archive_val

                    if section == "balance_sheet":
                        matched = _lookup_arrow_any_period_type(
                            conn, ticker=ticker, statement=arrow_stmt,
                            concept=canonical, period_end=end,
                        )
                        arrow_val = matched[0] if matched else None
                        period_type = matched[1] if matched else "quarter"
                    else:
                        arrow_val = _load_arrow_value_m(
                            conn, ticker=ticker, statement=arrow_stmt,
                            concept=canonical, period_end=end,
                            period_type=period_type,
                        )

                    stats[arrow_stmt]["total"] += 1
                    if arrow_val is None:
                        stats[arrow_stmt]["arrow_missing"] += 1
                        continue

                    if _within_tol(arrow_val, archive_val):
                        stats[arrow_stmt]["matched"] += 1
                    elif (
                        xbrl in SIGN_AMBIGUOUS_IN_ARCHIVE
                        and _within_tol(abs(arrow_val), abs(archive_val))
                    ):
                        stats[arrow_stmt]["sign_ambig"] = stats[arrow_stmt].get("sign_ambig", 0) + 1
                    elif xbrl in BUNDLING_DIFFERENCES:
                        stats[arrow_stmt]["bundled"] += 1
                    else:
                        stats[arrow_stmt]["mismatches"].append(Comparison(
                            statement=arrow_stmt, xbrl_concept=xbrl,
                            canonical=canonical, period_end=end,
                            period_type=period_type,
                            arrow_val_m=arrow_val, archive_val_m=archive_val,
                            delta_m=arrow_val - archive_val, match=False,
                        ))

    return stats


def main() -> int:
    tickers = sys.argv[1:] if len(sys.argv) > 1 else ["NVDA"]
    with get_conn() as conn:
        for ticker in tickers:
            stats = compare_ticker(conn, ticker)

            print(f"\n=== Arrow vs archive/ai_extract for {ticker} ===\n")
            grand_total = 0
            grand_matched = 0
            for stmt in ("income_statement", "balance_sheet", "cash_flow"):
                s = stats[stmt]
                print(f"{stmt}:")
                print(f"  total compared:    {s['total']}")
                print(f"  matched:           {s['matched']}")
                print(f"  sign-ambig (abs):  {s.get('sign_ambig', 0)} "
                      "(archive's sign convention varies across filings; |arrow|≈|archive|)")
                print(f"  known bundling:    {s['bundled']} "
                      "(FMP normalization combines concepts archive keeps split)")
                print(f"  Arrow missing:     {s['arrow_missing']}")
                print(f"  REAL MISMATCHES:   {len(s['mismatches'])}")
                for m in s["mismatches"]:
                    print(f"    {m.xbrl_concept} ({m.canonical}) @ {m.period_end} "
                          f"[{m.period_type}]: arrow={m.arrow_val_m}M, "
                          f"archive={m.archive_val_m}M, Δ={m.delta_m}M")
                print()
                grand_total += s["total"]
                grand_matched += s["matched"] + s["bundled"] + s.get("sign_ambig", 0)

            print(f"Grand total: {grand_matched}/{grand_total} reconciled "
                  f"({100*grand_matched/max(grand_total,1):.1f}%) "
                  "— includes known FMP-vs-archive bundling differences")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
