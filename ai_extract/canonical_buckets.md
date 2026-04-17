# Canonical Buckets

The universal analytical bucket set. Applies to every company. Load-bearing:
the Stage 2 AI prompt assigns each as-reported row to one bucket, the
verification battery enforces bucket-level formula ties and cross-statement
invariants, `calculate.py` reads bucket values directly, and the CSV renders
one section per statement using these names.

Two kinds of buckets per statement:

- **detail** — populated by the AI, which assigns each as-reported row
  (including `xbrl_not_on_statement` note detail) to a single detail bucket.
  A company that doesn't report a bucket leaves it null. No fills. No zeros.
- **subtotal** — computed arithmetically from detail buckets (and other
  subtotals). Formulas below are canonical and must tie in every quarter.

Short names are chosen to be concise but unambiguous; statement is the
implicit namespace (so `dna` on IS and `dna` on CF are distinct buckets).

---

## Income Statement

```
REVENUES (detail)
  revenue
  finance_div_revenue
  insurance_div_revenue
  other_revenue

total_revenue            = revenue + finance_div_revenue + insurance_div_revenue + other_revenue

GROSS PROFIT
  cogs                   (negative sign in the subtotal)
gross_profit             = total_revenue - cogs

OPERATING EXPENSES (detail)
  sga
  rd
  dna                    (when reported as a separate line; else null)
  other_opex

total_opex               = sga + rd + dna + other_opex
operating_income         = gross_profit - total_opex

NET INTEREST (detail)
  interest_expense
  interest_income

net_interest_expense     = interest_expense - interest_income

EBT (detail — non-operating)
  equity_affiliates
  other_nonop

ebt_excl_unusual         = operating_income - net_interest_expense + equity_affiliates + other_nonop

UNUSUAL ITEMS (detail)
  restructuring
  goodwill_impairment
  gain_sale_assets
  gain_sale_investments
  other_unusual

ebt_incl_unusual         = ebt_excl_unusual + restructuring + goodwill_impairment
                           + gain_sale_assets + gain_sale_investments + other_unusual

NET INCOME
  tax                                (income tax expense; negative in subtotal)
continuing_ops           = ebt_incl_unusual - tax

  minority_interest                  (negative in subtotal)
net_income               = continuing_ops - minority_interest

  preferred_dividend                 (negative in subtotal)
ni_common_incl_extra     = net_income - preferred_dividend
ni_common_excl_extra     = ni_common_incl_extra - extraordinary items (if any)
```

---

## Balance Sheet

```
CURRENT ASSETS (detail)
  cash
  sti                    (short-term investments)
  trading_securities
total_cash_sti           = cash + sti + trading_securities

  accounts_receivable
  other_receivables
total_receivables        = accounts_receivable + other_receivables

  inventory
  restricted_cash
  prepaid_expenses
  other_current_assets

total_current_assets     = total_cash_sti + total_receivables + inventory
                           + restricted_cash + prepaid_expenses + other_current_assets

NONCURRENT ASSETS (detail)
  gross_ppe
  accumulated_depreciation           (negative)
net_ppe                  = gross_ppe + accumulated_depreciation

  long_term_investments
  goodwill
  other_intangibles
  loans_receivable_lt
  deferred_tax_assets_lt
  deferred_charges_lt
  other_lt_assets

total_assets             = total_current_assets + net_ppe + long_term_investments
                           + goodwill + other_intangibles + loans_receivable_lt
                           + deferred_tax_assets_lt + deferred_charges_lt + other_lt_assets

CURRENT LIABILITIES (detail)
  accounts_payable
  accrued_expenses
  current_portion_lt_debt
  current_portion_leases
  current_income_taxes_payable
  unearned_revenue_current
  other_current_liabilities

total_current_liabilities = sum(above)

NONCURRENT LIABILITIES (detail)
  long_term_debt
  long_term_leases
  unearned_revenue_nc
  deferred_tax_liability_nc
  other_nc_liabilities

total_liabilities        = total_current_liabilities + long_term_debt + long_term_leases
                           + unearned_revenue_nc + deferred_tax_liability_nc + other_nc_liabilities

EQUITY (detail)
  common_stock
  apic                   (additional paid-in capital)
  retained_earnings
  treasury_stock         (negative)
  comprehensive_income_other
common_equity            = common_stock + apic + retained_earnings + treasury_stock + comprehensive_income_other

  noncontrolling_interest
total_equity             = common_equity + noncontrolling_interest

total_liabilities_and_equity = total_liabilities + total_equity
```

---

## Cash Flow

```
CASH FROM OPERATIONS (detail)
  net_income_start       (reconciliation starting line — ties to IS.net_income)

  non-cash adjustments:
  dna                    (depreciation & amortization)
  gain_sale_asset
  gain_sale_investments
  amort_deferred_charges
  asset_writedown_restructuring
  sbc                    (stock-based compensation)
  other_operating

  working-capital changes:
  change_ar
  change_inventory
  change_ap
  change_unearned_revenue
  change_income_taxes
  change_other_operating

cfo                      = net_income_start
                           + dna + gain_sale_asset + gain_sale_investments
                           + amort_deferred_charges + asset_writedown_restructuring
                           + sbc + other_operating
                           + change_ar + change_inventory + change_ap
                           + change_unearned_revenue + change_income_taxes
                           + change_other_operating

CASH FROM INVESTING (detail)
  capex                  (negative)
  sale_ppe
  acquisitions           (negative)
  divestitures
  investment_securities
  loans_orig_sold
  other_investing

cfi                      = sum(above)

CASH FROM FINANCING (detail)
  short_term_debt_issued
  long_term_debt_issued
total_debt_issued        = short_term_debt_issued + long_term_debt_issued

  short_term_debt_repaid (negative)
  long_term_debt_repaid  (negative)
total_debt_repaid        = short_term_debt_repaid + long_term_debt_repaid

  stock_issuance
  stock_repurchase       (negative)

  common_dividends       (negative)
  preferred_dividends    (negative)
total_common_pref_dividends = common_dividends + preferred_dividends

  special_dividends      (negative)
  other_financing

cff                      = total_debt_issued + total_debt_repaid
                           + stock_issuance + stock_repurchase
                           + total_common_pref_dividends + special_dividends
                           + other_financing

NET CHANGE IN CASH (detail)
  fx_adjustments
  misc_cf_adjustments

net_change_in_cash       = cfo + cfi + cff + fx_adjustments + misc_cf_adjustments
```

---

## Cross-statement invariants

These MUST tie in every quarter. They are the final correctness signal — if
any invariant breaks, the extraction is wrong, not rounded.

1. **Balance sheet closes:** `total_assets == total_liabilities_and_equity`
2. **Cash roll-forward:** `net_change_in_cash == cash_end_of_period - cash_beginning_of_period`
   where the cash balances come from consecutive BS period-ends (`cash` bucket).
3. **Net income tie:** `income_statement.net_income == cash_flow.net_income_start`
4. **BS cash match CF start:** the `cash` bucket on the BS at period-end equals
   the CF "cash at end of period" for that filing.

---

## Period derivation (for flow concepts)

- **Q1 10-Q**: 3-month column = quarterly, pass through.
- **Q2/Q3 10-Q**: the 3-month column is the standalone quarterly value for IS.
  For CF, where filings only report YTD, compute quarterly = YTD − prior YTD.
- **10-K**: annual value. Q4 flows = annual − Q1 − Q2 − Q3 (linear derivation
  preserves formula ties).
- **Stocks (BS)**: snapshot at each period-end; never summed across quarters.
- **Q1+Q2+Q3+Q4 = annual**: applies to every flow bucket for every fiscal year
  where all five are present. Mismatch = hard failure.

---

## Null semantics

A bucket's value is null when:
1. The concept genuinely isn't reported in that period's XBRL (audited).
2. The AI has found no as-reported row that maps to it.

A bucket is never zero as a fill. A subtotal computed from buckets where some
components are null treats missing components as zero only if the underlying
concept is confirmed absent; otherwise the subtotal itself is null.

---

## Forward-fill rule

Some items (e.g., employee counts, some note-only disclosures) are reported
only in 10-Ks. For the Q1–Q3 of the following fiscal year they may be
forward-filled from the most recent 10-K.

A forward-fill is valid ONLY if the concept is genuinely absent from the
target period's raw `parsed_xbrl.json` under any naming variant. The
verification battery re-opens the raw XBRL and rejects any forward-fill that
shadowed an actual reported value.
