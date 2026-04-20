"""Export validated financial_facts to analyst-friendly CSVs.

Usage:
    uv run scripts/export_financials.py NVDA [MSFT ...]

Writes three CSVs per ticker to data/exports/:
    {TICKER}_income_statement.csv
    {TICKER}_balance_sheet.csv
    {TICKER}_cash_flow.csv

Layout per file:
    Row 1  (header): concept | unit | FY2021 Q1 | FY2021 Q2 | FY2021 Q3 | FY2021 Q4 | FY2021 | FY2022 Q1 | ...
    Other rows: one per canonical bucket, in display order, values in chronological columns.

Values are the raw canonical USD magnitudes (or USD/share, or shares)
from financial_facts. Empty cells = bucket not reported for that period.
Only the currently-"live" rows (superseded_at IS NULL) are exported.

This is a read-only view: regenerate anytime by re-running.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from decimal import Decimal
from pathlib import Path

from arrow.db.connection import get_conn


# ---------------------------------------------------------------------------
# Display orders — concept rows follow the analyst-standard layout:
# top-line → margins → bottom-line for IS; current → noncurrent → balance
# for BS; operating → investing → financing → roll-forward for CF.
# ---------------------------------------------------------------------------

IS_DISPLAY_ORDER = [
    "revenue",
    "cogs",
    "gross_profit",
    "rd",
    "sga",
    "total_opex",
    "operating_income",
    "interest_income",
    "interest_expense",
    "ebt_incl_unusual",
    "tax",
    "continuing_ops_after_tax",
    "discontinued_ops",
    "net_income",
    "eps_basic",
    "eps_diluted",
    "shares_basic_weighted_avg",
    "shares_diluted_weighted_avg",
]

BS_DISPLAY_ORDER = [
    # --- Current assets ---
    "cash_and_equivalents",
    "short_term_investments",
    "accounts_receivable",
    "inventory",
    "prepaid_expenses",
    "other_current_assets",
    "total_current_assets",
    # --- Noncurrent assets ---
    "net_ppe",
    "long_term_investments",
    "goodwill",
    "other_intangibles",
    "deferred_tax_assets_noncurrent",
    "other_noncurrent_assets",
    "total_assets",
    # --- Current liabilities ---
    "accounts_payable",
    "accrued_expenses",
    "current_portion_lt_debt",
    "current_portion_leases_operating",
    "deferred_revenue_current",
    "other_current_liabilities",
    "total_current_liabilities",
    # --- Noncurrent liabilities ---
    "long_term_debt",
    "long_term_leases_operating",
    "deferred_revenue_noncurrent",
    "deferred_tax_liability_noncurrent",
    "other_noncurrent_liabilities",
    "total_liabilities",
    # --- Equity ---
    "preferred_stock",
    "common_stock",
    "additional_paid_in_capital",
    "retained_earnings",
    "treasury_stock",
    "accumulated_other_comprehensive_income",
    "noncontrolling_interest",
    "total_equity",
    "total_liabilities_and_equity",
]

CF_DISPLAY_ORDER = [
    "net_income_start",
    "dna_cf",
    "sbc",
    "deferred_income_tax",
    "other_noncash",
    "change_accounts_receivable",
    "change_inventory",
    "change_accounts_payable",
    "change_other_working_capital",
    "cfo",
    "capital_expenditures",
    "acquisitions",
    "purchases_of_investments",
    "sales_of_investments",
    "other_investing",
    "cfi",
    "short_term_debt_issuance",
    "long_term_debt_issuance",
    "stock_issuance",
    "stock_repurchase",
    "common_dividends_paid",
    "preferred_dividends_paid",
    "other_financing",
    "cff",
    "fx_effect_on_cash",
    "net_change_in_cash",
    "cash_begin_of_period",
    "cash_end_of_period",
]

STATEMENTS = [
    ("income_statement", "income_statement", IS_DISPLAY_ORDER),
    ("balance_sheet",    "balance_sheet",    BS_DISPLAY_ORDER),
    ("cash_flow",        "cash_flow",        CF_DISPLAY_ORDER),
]


REPO_ROOT = Path(__file__).resolve().parents[1]
EXPORT_DIR = REPO_ROOT / "data" / "exports"


def _format_value(v: Decimal | None) -> str:
    """Strip trailing zeros; emit bare empty string when absent."""
    if v is None:
        return ""
    s = str(v)
    if "." in s:
        s = s.rstrip("0").rstrip(".")
    return s


def _fetch_facts(conn, *, ticker: str, statement: str):
    """Return rows: (concept, fiscal_year, fiscal_quarter, period_type,
    fiscal_period_label, period_end, value, unit) for current facts."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT f.concept, f.fiscal_year, f.fiscal_quarter, f.period_type,
                   f.fiscal_period_label, f.period_end, f.value, f.unit
            FROM financial_facts f
            JOIN companies c ON c.id = f.company_id
            WHERE c.ticker = %s
              AND f.statement = %s
              AND f.superseded_at IS NULL
            ORDER BY f.fiscal_year, f.period_end, f.period_type, f.concept;
            """,
            (ticker.upper(), statement),
        )
        return cur.fetchall()


def _period_sort_key(fiscal_year: int, fiscal_quarter: int | None, period_type: str):
    """Order: within a fiscal year, Q1 → Q2 → Q3 → Q4 → FY (annual)."""
    if period_type == "annual":
        q_rank = 5
    else:
        q_rank = fiscal_quarter or 0
    return (fiscal_year, q_rank)


def _export_statement(
    conn, *, ticker: str, statement: str, display_order: list[str], out_path: Path,
) -> tuple[int, int]:
    """Write one CSV. Returns (rows_written, periods_included)."""
    rows = _fetch_facts(conn, ticker=ticker, statement=statement)

    # Build: column_keys in chronological order, values table, units
    column_key_set = set()
    column_labels: dict[tuple, str] = {}
    values: dict[str, dict[tuple, Decimal]] = defaultdict(dict)
    units: dict[str, str] = {}

    for concept, fy, fq, pt, label, period_end, value, unit in rows:
        key = (_period_sort_key(fy, fq, pt), label)
        column_key_set.add(key)
        column_labels[key] = label
        values[concept][key] = value
        units[concept] = unit

    columns = sorted(column_key_set)  # tuples sort lexicographically; first element is the sort key

    # Include buckets that appear in display order AND have at least one value,
    # preserving display order. Extra buckets (unknown to display order) get
    # appended after, so we never silently drop data.
    present_concepts = set(values.keys())
    ordered = [c for c in display_order if c in present_concepts]
    extras = sorted(present_concepts - set(display_order))

    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        header = ["concept", "unit"] + [column_labels[k] for k in columns]
        w.writerow(header)
        for concept in ordered + extras:
            row = [concept, units.get(concept, "")]
            for k in columns:
                row.append(_format_value(values[concept].get(k)))
            w.writerow(row)

    return len(ordered) + len(extras), len(columns)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: export_financials.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = [t.upper() for t in sys.argv[1:]]
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        for ticker in tickers:
            print(f"{ticker}:")
            for statement, suffix, display_order in STATEMENTS:
                out_path = EXPORT_DIR / f"{ticker}_{suffix}.csv"
                n_concepts, n_periods = _export_statement(
                    conn, ticker=ticker, statement=statement,
                    display_order=display_order, out_path=out_path,
                )
                rel = out_path.relative_to(REPO_ROOT)
                print(f"  {statement:18s} → {rel}  ({n_concepts} concepts × {n_periods} periods)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
