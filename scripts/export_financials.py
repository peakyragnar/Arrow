"""Export validated financial_facts to analyst-friendly CSVs.

Usage:
    uv run scripts/export_financials.py NVDA [MSFT ...]

Writes three CSVs per ticker to data/exports/:
    {TICKER}_income_statement.csv
    {TICKER}_balance_sheet.csv
    {TICKER}_cash_flow.csv

Layout per file:
    Columns: concept | unit | FY2021 Q1 | FY2021 Q2 | ... | FY2026 Q4 | FY2026
    Rows:
      - canonical concept rows (values), in display order
      - CHECK rows inline immediately after each subtotal they verify,
        prefixed "  Δ " — cell values are the computed delta
        (filer_subtotal − sum_of_components). A delta of 0 (or empty
        when a component is missing) means the math holds for that period.

CHECK rows render the full Layer-1 subtotal tie stack (gross_profit tie,
operating-income tie, net-income tie, BS balance identity, CF cash
roll-forward, etc.) plus one Layer-2 cross-statement tie in the CF sheet
(cf.net_income_start vs is.net_income). Watching Δ = 0 across every
period is the visual form of "the validation math ran and passed."

Values are raw canonical USD magnitudes (or USD/share, or shares) from
financial_facts. Empty cell on a concept row = bucket not reported for
that period; on a check row = one of the components was absent, so the
check was skipped for that period.

Only currently-live rows (superseded_at IS NULL) are exported.
Regenerate anytime by re-running.
"""

from __future__ import annotations

import csv
import sys
from collections import defaultdict
from dataclasses import dataclass
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

# ---------------------------------------------------------------------------
# Inline check rows — rendered immediately after their subtotal concept.
# Each check row shows: delta = filer[subtotal] − Σ(filer[component] × sign)
# Δ = 0 means the tie holds for that period.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class CheckRow:
    label: str                          # e.g. "  Δ revenue − cogs − gross_profit"
    insert_after: str                   # concept whose row this check follows
    subtotal: str                       # concept being checked
    components: tuple[tuple[str, int], ...]  # (concept, sign)
    # For cross-statement ties: if set, look up this concept from the
    # NAMED statement (e.g. "income_statement") instead of the current one.
    cross_statement: tuple[str, str] | None = None  # (concept_name, statement)


# Layer 1 IS ties
IS_CHECKS: tuple[CheckRow, ...] = (
    CheckRow("  Δ gross_profit − (revenue − cogs)",
             insert_after="gross_profit", subtotal="gross_profit",
             components=(("revenue", +1), ("cogs", -1))),
    CheckRow("  Δ operating_income − (gross_profit − total_opex)",
             insert_after="operating_income", subtotal="operating_income",
             components=(("gross_profit", +1), ("total_opex", -1))),
    CheckRow("  Δ continuing_ops_after_tax − (ebt_incl_unusual − tax)",
             insert_after="continuing_ops_after_tax",
             subtotal="continuing_ops_after_tax",
             components=(("ebt_incl_unusual", +1), ("tax", -1))),
    CheckRow("  Δ net_income − (continuing_ops_after_tax + discontinued_ops)",
             insert_after="net_income", subtotal="net_income",
             components=(("continuing_ops_after_tax", +1),
                         ("discontinued_ops", +1))),
)

# Layer 1 BS ties + balance identity
BS_CHECKS: tuple[CheckRow, ...] = (
    CheckRow("  Δ total_current_assets − Σ(current assets)",
             insert_after="total_current_assets",
             subtotal="total_current_assets",
             components=(("cash_and_equivalents", +1),
                         ("short_term_investments", +1),
                         ("accounts_receivable", +1),
                         ("inventory", +1),
                         ("prepaid_expenses", +1),
                         ("other_current_assets", +1))),
    CheckRow("  Δ total_assets − (current + noncurrent assets)",
             insert_after="total_assets", subtotal="total_assets",
             components=(("total_current_assets", +1),
                         ("net_ppe", +1),
                         ("long_term_investments", +1),
                         ("goodwill", +1),
                         ("other_intangibles", +1),
                         ("deferred_tax_assets_noncurrent", +1),
                         ("other_noncurrent_assets", +1))),
    CheckRow("  Δ total_current_liabilities − Σ(current liabilities)",
             insert_after="total_current_liabilities",
             subtotal="total_current_liabilities",
             components=(("accounts_payable", +1),
                         ("accrued_expenses", +1),
                         ("current_portion_lt_debt", +1),
                         ("current_portion_leases_operating", +1),
                         ("deferred_revenue_current", +1),
                         ("other_current_liabilities", +1))),
    CheckRow("  Δ total_liabilities − (current + noncurrent liabilities)",
             insert_after="total_liabilities", subtotal="total_liabilities",
             components=(("total_current_liabilities", +1),
                         ("long_term_debt", +1),
                         ("long_term_leases_operating", +1),
                         ("deferred_revenue_noncurrent", +1),
                         ("deferred_tax_liability_noncurrent", +1),
                         ("other_noncurrent_liabilities", +1))),
    CheckRow("  Δ total_equity − (preferred + common + APIC + retained + treasury + AOCI + NCI)",
             insert_after="total_equity", subtotal="total_equity",
             components=(("preferred_stock", +1),
                         ("common_stock", +1),
                         ("additional_paid_in_capital", +1),
                         ("retained_earnings", +1),
                         ("treasury_stock", +1),
                         ("accumulated_other_comprehensive_income", +1),
                         ("noncontrolling_interest", +1))),
    CheckRow("  Δ total_liabilities_and_equity − (total_liabilities + total_equity)",
             insert_after="total_liabilities_and_equity",
             subtotal="total_liabilities_and_equity",
             components=(("total_liabilities", +1), ("total_equity", +1))),
    # THE BALANCE
    CheckRow("  Δ total_assets − total_liabilities_and_equity  [BALANCE]",
             insert_after="total_liabilities_and_equity",
             subtotal="total_assets",
             components=(("total_liabilities_and_equity", +1),)),
)

# Layer 1 CF ties + cash roll-forward + Layer 2 cross-statement NI tie
CF_CHECKS: tuple[CheckRow, ...] = (
    CheckRow("  Δ cfo − Σ(non-cash adjustments + working capital)",
             insert_after="cfo", subtotal="cfo",
             components=(("net_income_start", +1),
                         ("dna_cf", +1), ("sbc", +1),
                         ("deferred_income_tax", +1),
                         ("other_noncash", +1),
                         ("change_accounts_receivable", +1),
                         ("change_inventory", +1),
                         ("change_accounts_payable", +1),
                         ("change_other_working_capital", +1))),
    CheckRow("  Δ cfi − Σ(investing components)",
             insert_after="cfi", subtotal="cfi",
             components=(("capital_expenditures", +1),
                         ("acquisitions", +1),
                         ("purchases_of_investments", +1),
                         ("sales_of_investments", +1),
                         ("other_investing", +1))),
    CheckRow("  Δ cff − Σ(financing components)",
             insert_after="cff", subtotal="cff",
             components=(("short_term_debt_issuance", +1),
                         ("long_term_debt_issuance", +1),
                         ("stock_issuance", +1),
                         ("stock_repurchase", +1),
                         ("common_dividends_paid", +1),
                         ("preferred_dividends_paid", +1),
                         ("other_financing", +1))),
    CheckRow("  Δ net_change_in_cash − (cfo + cfi + cff + fx)",
             insert_after="net_change_in_cash", subtotal="net_change_in_cash",
             components=(("cfo", +1), ("cfi", +1), ("cff", +1),
                         ("fx_effect_on_cash", +1))),
    CheckRow("  Δ net_change_in_cash − (cash_end − cash_begin)  [cash roll-forward]",
             insert_after="cash_end_of_period", subtotal="net_change_in_cash",
             components=(("cash_end_of_period", +1),
                         ("cash_begin_of_period", -1))),
    # Layer 2 cross-statement — references IS.
    CheckRow("  Δ net_income_start (CF) − net_income (IS)  [Layer 2 cross-statement]",
             insert_after="net_income_start", subtotal="net_income_start",
             components=(("net_income", +1),),
             cross_statement=("net_income", "income_statement")),
)


STATEMENTS = [
    ("income_statement", "income_statement", IS_DISPLAY_ORDER, IS_CHECKS),
    ("balance_sheet",    "balance_sheet",    BS_DISPLAY_ORDER, BS_CHECKS),
    ("cash_flow",        "cash_flow",        CF_DISPLAY_ORDER, CF_CHECKS),
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


def _col_letter(idx_1based: int) -> str:
    """1 → A, 2 → B, 27 → AA, ..."""
    result = ""
    n = idx_1based
    while n > 0:
        n, rem = divmod(n - 1, 26)
        result = chr(ord("A") + rem) + result
    return result


def _build_check_formula(
    excel_col: str,
    sub_row: int,
    components_with_rows: list[tuple[int, int]],
) -> str:
    """Build an Excel formula:  =<subtotal> − (<signed sum of components>)

    components_with_rows is a list of (excel_row, sign). Result cell
    evaluates to filer_subtotal − sum(components × signs), which is 0
    when the tie holds. Excel interprets the leading "=" as a formula;
    the user sees the math in the formula bar and the computed delta
    in the cell.
    """
    rhs_parts: list[str] = []
    for i, (row, sign) in enumerate(components_with_rows):
        cell = f"{excel_col}{row}"
        if i == 0:
            rhs_parts.append(cell if sign > 0 else f"-{cell}")
        else:
            rhs_parts.append(f"{'+' if sign > 0 else '-'}{cell}")
    rhs = "".join(rhs_parts)
    return f"={excel_col}{sub_row}-({rhs})"


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


def _load_statement_values(
    conn, *, ticker: str, statement: str,
) -> tuple[
    dict[str, dict[tuple, Decimal]],  # concept → period-key → value
    dict[tuple, str],                  # period-key → fiscal_period_label
    dict[str, str],                    # concept → unit
    list[tuple],                       # sorted column keys
]:
    rows = _fetch_facts(conn, ticker=ticker, statement=statement)
    values: dict[str, dict[tuple, Decimal]] = defaultdict(dict)
    column_labels: dict[tuple, str] = {}
    units: dict[str, str] = {}
    column_key_set = set()
    for concept, fy, fq, pt, label, period_end, value, unit in rows:
        key = (_period_sort_key(fy, fq, pt), label)
        column_key_set.add(key)
        column_labels[key] = label
        values[concept][key] = value
        units[concept] = unit
    columns = sorted(column_key_set)
    return values, column_labels, units, columns


def _compute_check_delta(
    check: CheckRow,
    own_values: dict[str, dict[tuple, Decimal]],
    other_values: dict[str, dict[tuple, Decimal]] | None,
    column_key: tuple,
) -> Decimal | None:
    """Compute delta = filer[subtotal] − Σ(component × sign) for one period.

    Returns None if any required value is absent (skip cell)."""
    # Locate subtotal value in own sheet
    sub = own_values.get(check.subtotal, {}).get(column_key)
    if sub is None:
        return None

    total = Decimal("0")
    for concept, sign in check.components:
        if check.cross_statement and concept == check.cross_statement[0]:
            # Look up from the other statement's values.
            v = (other_values or {}).get(concept, {}).get(column_key)
        else:
            v = own_values.get(concept, {}).get(column_key)
        if v is None:
            return None
        total += v * sign

    return sub - total


def _export_statement(
    conn,
    *,
    ticker: str,
    statement: str,
    display_order: list[str],
    checks: tuple[CheckRow, ...],
    out_path: Path,
    companion_values: dict[str, dict[str, dict[tuple, Decimal]]] | None = None,
) -> tuple[int, int]:
    """Write one CSV. Returns (rows_written, periods_included).

    Check rows emit Excel formulas (e.g., `=C4-(C2-C3)`) so the math is
    visible in Excel's formula bar when you click a check cell. The cell
    evaluates to the Δ (0 when the tie holds). Cross-statement checks
    (which would need a reference into another CSV file) fall back to
    pre-computed values since CSV can't cleanly reference between files.
    """
    values, column_labels, units, columns = _load_statement_values(
        conn, ticker=ticker, statement=statement,
    )

    present_concepts = set(values.keys())
    ordered = [c for c in display_order if c in present_concepts]
    extras = sorted(present_concepts - set(display_order))

    checks_by_after: dict[str, list[CheckRow]] = defaultdict(list)
    for c in checks:
        checks_by_after[c.insert_after].append(c)

    # --- Pass 1: plan rows and assign Excel row numbers ---
    # Excel rows are 1-indexed; row 1 is the header, so data rows start at 2.
    concept_to_row: dict[str, int] = {}
    row_plan: list[tuple[str, object]] = []  # ('concept', name) | ('check', CheckRow)
    excel_row = 1
    for concept in ordered + extras:
        excel_row += 1
        concept_to_row[concept] = excel_row
        row_plan.append(("concept", concept))
        for chk in checks_by_after.get(concept, ()):
            excel_row += 1
            row_plan.append(("check", chk))

    # --- Pass 2: emit CSV ---
    with out_path.open("w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["concept", "unit"] + [column_labels[k] for k in columns])

        for kind, content in row_plan:
            if kind == "concept":
                concept = content  # type: ignore[assignment]
                row = [concept, units.get(concept, "")]
                for k in columns:
                    row.append(_format_value(values[concept].get(k)))
                w.writerow(row)
            else:
                chk: CheckRow = content  # type: ignore[assignment]
                row = [chk.label, ""]

                if chk.cross_statement:
                    # Can't reference across CSV files cleanly — pre-compute.
                    other = (
                        companion_values.get(chk.cross_statement[1])
                        if companion_values else None
                    )
                    for k in columns:
                        delta = _compute_check_delta(chk, values, other, k)
                        row.append(_format_value(delta) if delta is not None else "")
                    w.writerow(row)
                    continue

                sub_row = concept_to_row.get(chk.subtotal)
                comp_rows_signs = [
                    (concept_to_row.get(c), sign) for c, sign in chk.components
                ]
                any_concept_absent = (
                    sub_row is None
                    or any(r is None for r, _ in comp_rows_signs)
                )

                for col_idx, k in enumerate(columns, start=3):
                    if any_concept_absent:
                        row.append("")
                        continue
                    # Also skip per-period if any component has no value this period.
                    sub_val = values.get(chk.subtotal, {}).get(k)
                    comp_vals = [
                        values.get(c, {}).get(k) for c, _ in chk.components
                    ]
                    if sub_val is None or any(v is None for v in comp_vals):
                        row.append("")
                        continue

                    excel_col = _col_letter(col_idx)
                    # Narrow types: we already ensured sub_row + all comp rows are non-None
                    comps = [(r, s) for r, s in comp_rows_signs if r is not None]  # type: ignore[misc]
                    formula = _build_check_formula(
                        excel_col, sub_row, comps,  # type: ignore[arg-type]
                    )
                    row.append(formula)
                w.writerow(row)

    return len(row_plan), len(columns)


def main() -> int:
    if len(sys.argv) < 2:
        print("Usage: export_financials.py TICKER [TICKER ...]", file=sys.stderr)
        return 2

    tickers = [t.upper() for t in sys.argv[1:]]
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    with get_conn() as conn:
        for ticker in tickers:
            print(f"{ticker}:")
            # Pre-load IS values so CF's cross-statement check can reference them.
            is_values, _, _, _ = _load_statement_values(
                conn, ticker=ticker, statement="income_statement",
            )
            companion = {"income_statement": is_values}

            for statement, suffix, display_order, checks in STATEMENTS:
                out_path = EXPORT_DIR / f"{ticker}_{suffix}.csv"
                n_rows, n_periods = _export_statement(
                    conn, ticker=ticker, statement=statement,
                    display_order=display_order, checks=checks,
                    out_path=out_path, companion_values=companion,
                )
                rel = out_path.relative_to(REPO_ROOT)
                print(f"  {statement:18s} → {rel}  ({n_rows} rows × {n_periods} periods)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
