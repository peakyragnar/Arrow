# Concepts — Canonical Bucket Schema

The normalization contract. Every financial fact Arrow stores fits into a named bucket on this page. Every subtotal we compute is a deterministic formula over named buckets. Every sign is explicit. No bucket is a plug.

Derived from `archive/ai_extract/canonical_buckets.md` (the legacy pipeline's never-productionized draft) with the corrections listed in § 12.

This doc is **source-agnostic**. FMP-specific field mapping is in `fmp_mapping.md`. Verification architecture (subtotal ties, cross-statement invariants, tolerance levels, failure modes) is in `verification.md`.

If this doc conflicts with `docs/architecture/system.md` or `docs/reference/periods.md`, those win — open a PR updating both together.

---

## 1. Philosophy

Three rules, enforced by schema and the verification stack:

1. **Every bucket has a mechanical definition.** A bucket is populated by explicit assignment of reported line items, never by residual computation.
2. **Every reported subtotal must tie.** If a filer reports `total_opex = 2508`, our `sga + rd + dna + other_opex` must equal 2508 (within rounding tolerance, § 8). Mismatch = hard failure.
3. **Signs are stored, not derived.** The sign a bucket carries in the database is defined per bucket (§ 2). Formulas operate on stored signs without hidden transforms.

"Other" buckets are allowed. They are filled only by explicit assignment of reported line items the filer chose not to break out further. They are not silent residuals.

---

## 2. Sign Conventions (global)

Arrow uses **two sign conventions** depending on the statement.

### 2.1 Income Statement, Balance Sheet — reported-sign preserved

The sign stored in the database is the sign the filer reports, with one refinement: **buckets with structurally-determined sign** (always an expense, always a credit) are stored as positive magnitudes and subtracted in formulas.

| Bucket category | Stored sign |
|---|---|
| Revenue, income buckets | positive |
| Expense buckets with known sign (cogs, sga, rd, tax) | positive magnitude — formula subtracts |
| Balance-sheet asset/liability/equity line items | positive magnitude (except `accumulated_depreciation`, stored negative by accounting convention) |
| Items with filer-determined sign (unusual items, non-operating other, AOCI) | reported sign — formula adds directly |

Rationale: primary IS/BS items have structural signs (cogs is always an expense); storing magnitudes makes formulas readable. Items like restructuring or gain_on_sale_assets can be income or expense depending on the period, so we preserve the filer's sign.

### 2.2 Cash Flow — cash-impact sign

**All CF buckets are stored with the sign of their cash impact.**
- Cash out → negative
- Cash in → positive

Increase in accounts receivable → AR went up → cash went down → `change_accounts_receivable` stored **negative**.
Capital expenditures → cash went out → `capital_expenditures` stored **negative**.
Issuance of long-term debt → cash came in → `long_term_debt_issuance` stored **positive**.

CF formulas are straight sums. No per-item sign transforms in formulas.

```
cfo = net_income_start + dna_cf + sbc + deferred_income_tax - gain_on_sale_assets_cf
       + asset_writedown + other_noncash
       + change_accounts_receivable + change_inventory + change_accounts_payable
       + change_deferred_revenue + change_other_working_capital
```

Rationale: matches FMP's convention (empirically verified, see `fmp_mapping.md`); matches what analysts read (a negative number means cash went out); eliminates the double-sign bug from the legacy doc.

---

## 3. Per-Bucket Spec Format

Each bucket is documented with five facts:

```
name              — canonical bucket name (snake_case)
statement         — income_statement | balance_sheet | cash_flow
type              — detail | subtotal
stored_sign       — positive | negative | reported (depends on period)
null_policy       — when NULL is the correct value
formula           — (subtotal only) deterministic expression
```

§§ 4–6 apply this format statement by statement.

---

## 4. Income Statement

### 4.1 Revenue

| name | type | stored_sign | notes |
|---|---|---|---|
| `revenue` | detail | positive | consolidated IS top line; the filer's total revenue |
| `other_revenue` | detail | positive | non-primary revenue streams the filer breaks out (null if none) |
| `total_revenue` | subtotal | positive | `= revenue + other_revenue` |

**Segment revenue** (product/service, geographic, reportable segment) is **not** an IS bucket — it's out of scope here, handled in a future `segments` table.

### 4.2 Gross profit

| name | type | stored_sign | notes |
|---|---|---|---|
| `cogs` | detail | positive magnitude | cost of revenue / cost of goods sold |
| `gross_profit` | subtotal | positive | `= total_revenue - cogs` |

Tie: `gross_profit` must equal the filer's reported gross profit.

### 4.3 Operating expenses

| name | type | stored_sign | notes |
|---|---|---|---|
| `sga` | detail | positive magnitude | Combined S, G & A aggregate. Always populated when filer reports SG&A in any form. Ties reference this aggregate, not the split below. |
| `general_and_admin_expense` | detail | positive magnitude | G&A only, when filer reports it separately. Zero when filer reports combined SG&A. Tie relationship when populated: `sga = general_and_admin_expense + selling_and_marketing_expense` (verified empirically across MSFT, GOOGL, PANW, PLTR, TDG, OKLO, S, UNP, VLO, ET). |
| `selling_and_marketing_expense` | detail | positive magnitude | Selling + marketing only, split-reporters. Zero otherwise. See note on `general_and_admin_expense`. |
| `rd` | detail | positive magnitude | R&D expense (may be null if filer doesn't break out) |
| `dna_is` | detail | positive magnitude | D&A when reported as a separate operating line; else null (D&A is typically embedded in cogs and sga) |
| `other_opex` | detail | reported sign | operating expenses not fitting above (may be gain or loss, hence reported sign) |
| `total_opex` | subtotal | positive | `= sga + rd + dna_is + other_opex` (treating nulls as zero iff audited absent) |
| `operating_income` | subtotal | reported | `= gross_profit - total_opex` |

Tie: `operating_income` must equal the filer's reported operating income.

### 4.4 Non-operating / EBT

| name | type | stored_sign | notes |
|---|---|---|---|
| `interest_expense` | detail | positive magnitude | null if only net is reported |
| `interest_income` | detail | positive magnitude | null if only net is reported |
| `net_interest_expense` | subtotal | positive | `= interest_expense - interest_income` when both present; else the filer's reported net |
| `equity_affiliates` | detail | reported sign | income/loss from equity-method investments |
| `other_nonop` | detail | reported sign | other non-operating income/expense |
| `ebt_excl_unusual` | subtotal | reported | `= operating_income - net_interest_expense + equity_affiliates + other_nonop` |

### 4.5 Unusual items

| name | type | stored_sign | notes |
|---|---|---|---|
| `restructuring` | detail | reported sign | typically negative (expense); could be positive if reversal |
| `goodwill_impairment` | detail | reported sign | typically negative |
| `gain_sale_assets` | detail | reported sign | positive for gain, negative for loss |
| `gain_sale_investments` | detail | reported sign | positive for gain, negative for loss |
| `other_unusual` | detail | reported sign | unusual items not fitting above |
| `ebt_incl_unusual` | subtotal | reported | `= ebt_excl_unusual + restructuring + goodwill_impairment + gain_sale_assets + gain_sale_investments + other_unusual` |

Unusual items are stored with reported sign because they can be gains or losses. The formula is a straight sum.

### 4.6 Net income chain

| name | type | stored_sign | notes |
|---|---|---|---|
| `tax` | detail | positive magnitude | income tax expense (negative stored value = tax benefit) |
| `continuing_ops_after_tax` | subtotal | reported | `= ebt_incl_unusual - tax`. **Pre-NCI consolidated.** Maps to `us-gaap:IncomeLossFromContinuingOperations` / FMP `netIncomeFromContinuingOperations`. |
| `discontinued_ops` | detail | reported sign | gain/loss from discontinued operations, after-tax. Pre-NCI. |
| `net_income` | subtotal | reported | **Pre-NCI consolidated total.** `= continuing_ops_after_tax + discontinued_ops`. Maps to `us-gaap:ProfitLoss` (primary) / `us-gaap:NetIncomeLoss` (fallback for non-NCI filers). **Computed by the mapper** (not from FMP's IS-endpoint `netIncome`, which is post-NCI — see `fmp_mapping.md` § 5.4). |
| `minority_interest` | detail | reported sign | **NCI's share of consolidated net income, with sign.** Positive = NCI gained; negative = NCI took a loss. Maps to `us-gaap:NetIncomeLossAttributableToNoncontrollingInterest`. **Computed by the mapper** as `net_income - net_income_attributable_to_parent`. Zero for non-NCI filers. |
| `net_income_attributable_to_parent` | subtotal | reported | **Post-NCI parent-shareholder NI.** `= net_income - minority_interest`. Maps to `us-gaap:NetIncomeLoss` / FMP IS-endpoint `netIncome`. **This is the NI used in EPS, P/E, and analyst conventions for "net income."** For non-NCI filers equals `net_income`. |
| `preferred_dividends_is` | detail | positive magnitude | preferred dividends declared for the period (if reported on IS) |
| `ni_common` | subtotal | reported | `= net_income_attributable_to_parent - preferred_dividends_is` |

**Ties:**
- `net_income == continuing_ops_after_tax + discontinued_ops` (tautological by mapper's derivation, retained as contract guard)
- `net_income_attributable_to_parent == net_income - minority_interest` (tautological by mapper's derivation, retained as contract guard)
- `net_income == cash_flow.net_income_start` (both are pre-NCI consolidated; ties at Layer 2)

**Which NI to use when:**
- CF roll-forward tie (Layer 2): `net_income` (pre-NCI, matches what CF reports at the top)
- EPS, P/E, analyst "net income": `net_income_attributable_to_parent` (post-NCI)
- ROE using `common_equity`: `net_income_attributable_to_parent` (must pair parent NI with parent equity)
- ROE using `total_equity`: `net_income` (whole-entity consistency)

### 4.7 Per-share data

| name | type | stored_sign | notes |
|---|---|---|---|
| `eps_basic` | detail | reported | $/share, as reported |
| `eps_diluted` | detail | reported | $/share, as reported |
| `shares_basic_weighted_avg` | detail | positive | millions of shares |
| `shares_diluted_weighted_avg` | detail | positive | millions of shares |

Per-share values and share counts from FMP are **split-adjusted** (FMP back-applies stock splits to all historical periods). See `fmp_mapping.md` § split-adjustment policy.

---

## 5. Balance Sheet

Balance-sheet stocks are snapshots at period-end. Never summed across periods. Every filing emits one BS snapshot per filing date (the 10-Q/10-K reporting date).

### 5.1 Current assets

| name | type | stored_sign | notes |
|---|---|---|---|
| `cash_and_equivalents` | detail | positive | cash + cash equivalents |
| `short_term_investments` | detail | positive | marketable securities classified current |
| `restricted_cash_current` | detail | positive | null if not reported |
| `accounts_receivable` | detail | positive | trade receivables, net |
| `other_receivables` | detail | positive | non-trade receivables |
| `inventory` | detail | positive | |
| `prepaid_expenses` | detail | positive | |
| `income_taxes_receivable_current` | detail | positive | null if not separately reported |
| `other_current_assets` | detail | positive | current assets not fitting above |
| `total_current_assets` | subtotal | positive | sum of all above |

### 5.2 Noncurrent assets

| name | type | stored_sign | notes |
|---|---|---|---|
| `net_ppe` | detail | positive | primary PPE bucket, from BS face |
| `gross_ppe` | sub-detail | positive | null if only net reported |
| `accumulated_depreciation` | sub-detail | **negative** | by accounting convention; null if only net reported |
| `right_of_use_assets_operating` | detail | positive | ASC 842 operating lease ROU; null pre-2019 |
| `long_term_investments` | detail | positive | non-equity-method LT investments |
| `equity_method_investments` | detail | positive | null if bundled into long_term_investments |
| `goodwill` | detail | positive | |
| `other_intangibles` | detail | positive | intangibles other than goodwill, net |
| `deferred_tax_assets_noncurrent` | detail | positive | |
| `other_noncurrent_assets` | detail | positive | |
| `total_assets` | subtotal | positive | `= total_current_assets + net_ppe + right_of_use_assets_operating + long_term_investments + equity_method_investments + goodwill + other_intangibles + deferred_tax_assets_noncurrent + other_noncurrent_assets` |

Sub-detail rule: if both `gross_ppe` and `accumulated_depreciation` are reported, `net_ppe == gross_ppe + accumulated_depreciation` (tie).

### 5.3 Current liabilities

| name | type | stored_sign | notes |
|---|---|---|---|
| `accounts_payable` | detail | positive | |
| `accrued_expenses` | detail | positive | accrued liabilities (compensation, interest, etc., bundled) |
| `current_portion_lt_debt` | detail | positive | |
| `short_term_borrowings` | detail | positive | commercial paper, revolver draws, etc. |
| `current_portion_leases_operating` | detail | positive | ASC 842 current operating lease liability |
| `current_portion_leases_finance` | detail | positive | finance lease current portion |
| `income_taxes_payable_current` | detail | positive | null if not separately reported |
| `deferred_revenue_current` | detail | positive | contract liabilities due within one year |
| `other_current_liabilities` | detail | positive | |
| `total_current_liabilities` | subtotal | positive | sum of all above |

### 5.4 Noncurrent liabilities

| name | type | stored_sign | notes |
|---|---|---|---|
| `long_term_debt` | detail | positive | excludes current portion |
| `long_term_leases_operating` | detail | positive | ASC 842 noncurrent operating lease liability |
| `long_term_leases_finance` | detail | positive | |
| `deferred_revenue_noncurrent` | detail | positive | |
| `deferred_tax_liability_noncurrent` | detail | positive | |
| `other_noncurrent_liabilities` | detail | positive | |
| `total_liabilities` | subtotal | positive | `= total_current_liabilities + long_term_debt + long_term_leases_operating + long_term_leases_finance + deferred_revenue_noncurrent + deferred_tax_liability_noncurrent + other_noncurrent_liabilities` |

### 5.5 Equity

| name | type | stored_sign | notes |
|---|---|---|---|
| `preferred_stock` | detail | positive | null if company has no preferred |
| `common_stock` | detail | positive | par + no-par common stock (null if bundled with APIC) |
| `additional_paid_in_capital` | detail | positive | null if bundled |
| `common_stock_and_apic` | detail | positive | used when the filer reports common+APIC as a single XBRL concept; null if split into the two above |
| `retained_earnings` | detail | reported sign | negative if accumulated deficit |
| `treasury_stock` | detail | signed negative (for buybacks) | added in the formula; see `fmp_mapping.md` for FMP empirical convention |
| `accumulated_other_comprehensive_income` | detail | reported sign | AOCI, can be + or - |
| `other_equity` | detail | reported sign | filer-specific equity lines that don't fit the standard 6 buckets above (e.g., cumulative translation adjustment reported separately from AOCI, partners' capital for MLP-style filers, specific stock-based-compensation reserves). FMP surfaces these as `otherTotalStockholdersEquity`. Non-zero value = "look at the filer's 10-K equity section for semantic meaning." |
| `common_equity` | subtotal | reported | `= preferred_stock + (common_stock + additional_paid_in_capital OR common_stock_and_apic) + retained_earnings + treasury_stock + accumulated_other_comprehensive_income + other_equity` (treasury is added because it's stored with its signed value, which is negative for buybacks — see § 5.5 note) |
| `noncontrolling_interest` | detail | positive | |
| `total_equity` | subtotal | reported | `= common_equity + noncontrolling_interest` |
| `total_liabilities_and_equity` | subtotal | positive | `= total_liabilities + total_equity` |

Use either `(common_stock + additional_paid_in_capital)` OR `common_stock_and_apic`, not both. The one you populate depends on whether the filer reports them as two XBRL concepts or one.

### 5.6 Balance sheet invariant

`total_assets == total_liabilities_and_equity` (tolerance: ± $1M, see `verification.md`).

---

## 6. Cash Flow

**Every CF bucket is stored with cash-impact sign (§ 2.2).** All CF subtotals are straight sums.

### 6.1 Starting line

| name | type | stored_sign | notes |
|---|---|---|---|
| `net_income_start` | detail | reported | matches `is.net_income` exactly |

### 6.2 CFO — non-cash adjustments

| name | type | stored_sign | notes |
|---|---|---|---|
| `dna_cf` | detail | positive | D&A add-back (non-cash expense that reduced NI) |
| `sbc` | detail | positive | stock-based compensation add-back |
| `deferred_income_tax` | detail | reported | reported sign; can be + or - |
| `gain_on_sale_assets_cf` | detail | negative when a gain | gain was in NI but didn't produce cash; subtracted |
| `gain_on_sale_investments_cf` | detail | negative when a gain | same pattern |
| `asset_writedown` | detail | positive | non-cash charge add-back |
| `other_noncash` | detail | reported | other non-cash reconciling items |

### 6.3 CFO — working-capital changes (cash-impact sign)

| name | type | stored_sign | notes |
|---|---|---|---|
| `change_accounts_receivable` | detail | negative when AR increased | AR up → cash down → negative |
| `change_inventory` | detail | negative when inventory increased | |
| `change_accounts_payable` | detail | positive when AP increased | AP up → owed more → cash preserved → positive |
| `change_deferred_revenue` | detail | positive when deferred rev increased | customer paid upfront → cash in → positive |
| `change_income_taxes` | detail | cash-impact sign | increase in tax payable = cash in; increase in tax receivable = cash out |
| `change_other_working_capital` | detail | cash-impact sign | |
| `cfo` | subtotal | reported | straight sum: `= net_income_start + dna_cf + sbc + deferred_income_tax + gain_on_sale_assets_cf + gain_on_sale_investments_cf + asset_writedown + other_noncash + change_accounts_receivable + change_inventory + change_accounts_payable + change_deferred_revenue + change_income_taxes + change_other_working_capital` |

### 6.4 CFI (all cash-impact sign)

| name | type | stored_sign | notes |
|---|---|---|---|
| `capital_expenditures` | detail | negative | |
| `acquisitions` | detail | negative | cash paid for acquisitions, net of cash acquired |
| `divestitures` | detail | positive | proceeds from divestitures |
| `purchases_of_investments` | detail | negative | |
| `sales_of_investments` | detail | positive | includes maturities |
| `loans_originated` | detail | negative | |
| `loans_collected` | detail | positive | |
| `other_investing` | detail | cash-impact sign | |
| `cfi` | subtotal | reported | straight sum |

### 6.5 CFF (all cash-impact sign)

| name | type | stored_sign | notes |
|---|---|---|---|
| `short_term_debt_issuance` | detail | positive | |
| `short_term_debt_repayment` | detail | negative | |
| `long_term_debt_issuance` | detail | positive | |
| `long_term_debt_repayment` | detail | negative | |
| `stock_issuance` | detail | positive | proceeds from stock issuance |
| `stock_repurchase` | detail | negative | treasury stock purchases |
| `common_dividends_paid` | detail | negative | |
| `preferred_dividends_paid` | detail | negative | |
| `special_dividends_paid` | detail | negative | |
| `other_financing` | detail | cash-impact sign | |
| `cff` | subtotal | reported | straight sum |

### 6.6 FX / misc

| name | type | stored_sign | notes |
|---|---|---|---|
| `fx_effect_on_cash` | detail | reported | typically small; sign is net FX impact |
| `misc_cf_adjustments` | detail | reported | uncommon; catch-all for items not fitting above sections |

### 6.7 Cash roll-forward

| name | type | stored_sign | notes |
|---|---|---|---|
| `net_change_in_cash` | subtotal | reported | `= cfo + cfi + cff + fx_effect_on_cash + misc_cf_adjustments` |
| `cash_begin_of_period` | detail | positive | reported beginning cash, typically = `bs.cash_and_equivalents[t-1]` |
| `cash_end_of_period` | detail | positive | reported ending cash, typically = `bs.cash_and_equivalents[t]` |

### 6.8 Cash flow invariants

1. `net_change_in_cash == cash_end_of_period - cash_begin_of_period` (exact)
2. `cash_end_of_period == bs.cash_and_equivalents[t]` (exact, same filing)
3. `cash_begin_of_period == bs.cash_and_equivalents[t-1]` (exact, prior filing)
4. `cf.net_income_start == is.net_income` (exact, same filing)

---

## 7. Period derivation

Per `docs/reference/periods.md` § 7 and § 6. Summarizing here because it interacts with the buckets above:

- **Q1 10-Q**: 3-month column = quarterly, pass through for IS and CF (YTD = discrete for Q1).
- **Q2/Q3 10-Q**: IS reports both 3-month and YTD columns — use the 3-month as the discrete quarter. CF reports YTD only — compute discrete = YTD − prior YTD.
- **10-K (Q4)**: annual values. Q4 flow = annual − Q1 − Q2 − Q3 (for IS and CF). BS Q4 = reported 10-K balance sheet (snapshot).
- **BS stocks**: snapshot at period-end. Never summed across quarters.
- **TTM**: derived at query time as the sum of the most recent 4 discrete quarters. Not stored.

---

## 8. Null semantics

A bucket's value is NULL when **both** are true:
1. No as-reported row maps to it after the extraction pass.
2. The concept is confirmed absent in the filing's XBRL under any naming variant (audited).

**NEVER**:
- zero-fill an unreported bucket
- interpolate from adjacent periods
- substitute a default value

Subtotal behavior with nulls:
- If a detail component is NULL **and** audited-absent, treat as zero in the subtotal.
- If a detail component is NULL **and** not audited (extraction failed to classify), the subtotal is also NULL and flagged for review.

This enforces "no plug" at the bucket level. `verification.md` enforces it at the formula level via component-guards.

---

## 9. Forward-fill rule

Some concepts are reported only in 10-Ks (e.g., employee counts, certain note-detail disclosures). For Q1–Q3 of the following fiscal year, these may be forward-filled from the most recent 10-K value.

A forward-fill is valid **only** if the concept is genuinely absent from the target period's raw XBRL under any naming variant. A validation pass re-opens the raw XBRL and rejects any forward-fill that shadowed an actually-reported value.

Forward-filled rows are provenance-tagged (`source_artifact_id` points at the 10-K the value was carried from, with `extraction_version` annotated `forward_filled=true`).

---

## 10. Cross-statement invariants (summary)

Full spec with tolerances in `verification.md`. Summary:

1. `bs.total_assets == bs.total_liabilities_and_equity`
2. `is.net_income == cf.net_income_start`
3. `cf.net_change_in_cash == bs.cash_and_equivalents[t] - bs.cash_and_equivalents[t-1]`
4. `cf.cash_end_of_period == bs.cash_and_equivalents[t]`
5. `cf.cash_begin_of_period == bs.cash_and_equivalents[t-1]`
6. `sum(Q1, Q2, Q3, Q4) ≈ FY` for every flow bucket (IS flows, CF flows)

---

## 11. Bucket-level tie requirements (subtotal ties)

In addition to cross-statement invariants, these **intra-statement** ties must hold per filing:

Income statement:
- `gross_profit == total_revenue - cogs`
- `operating_income == gross_profit - total_opex`
- `ebt_excl_unusual == operating_income - net_interest_expense + equity_affiliates + other_nonop`
- `ebt_incl_unusual == ebt_excl_unusual + restructuring + goodwill_impairment + gain_sale_assets + gain_sale_investments + other_unusual`
- `continuing_ops_after_tax == ebt_incl_unusual - tax`
- `net_income == continuing_ops_after_tax + discontinued_ops`
- `net_income_attributable_to_parent == net_income - minority_interest`
- `ni_common == net_income_attributable_to_parent - preferred_dividends_is`

Balance sheet:
- `total_current_assets == cash + sti + restricted_cash_current + AR + other_receivables + inventory + prepaid + itax_receivable_current + other_current_assets`
- `total_assets == total_current_assets + net_ppe + ROU_operating + LT_investments + equity_method + goodwill + intangibles + DTA_noncurrent + other_noncurrent`
- `total_current_liabilities == AP + accrued_expenses + current_portion_lt_debt + ST_borrowings + operating_lease_current + finance_lease_current + itax_payable_current + deferred_rev_current + other_current_liabilities`
- `total_liabilities == total_current_liabilities + LT_debt + operating_lease_LT + finance_lease_LT + deferred_rev_noncurrent + DTL_noncurrent + other_noncurrent_liabilities`
- `common_equity == preferred + common_stock + APIC + retained_earnings + treasury + AOCI`  (treasury signed; negative for buybacks)
- `total_equity == common_equity + NCI`
- `total_liabilities_and_equity == total_liabilities + total_equity`

Cash flow: each subtotal (`cfo`, `cfi`, `cff`, `net_change_in_cash`) is the straight sum of its detail buckets (§ 6).

The filer's reported subtotal **must equal** our computed subtotal within tolerance. Mismatch = hard failure. Tolerances in `verification.md`.

---

## 12. Open items (future amendments)

Documented so nobody redundantly re-derives the decision:

- **Capitalized-interest policy.** How to handle interest costs capitalized into PP&E (subtracted from reported interest expense? stored as a component of `net_ppe`?). Revisit when we ingest a filer where this is material.
- **Accrued compensation** as a separate bucket from `accrued_expenses`. Often large and analytically interesting (SBC vs cash comp split), but most filers don't break it out on the BS face.
- **Deferred tax assets/liabilities current** (`deferred_tax_assets_current`, `deferred_tax_liability_current`). Rare but does occur — add buckets when a filer requires them.
- **Expanded discontinued-operations treatment.** Currently `discontinued_ops` is a single IS bucket. Full treatment requires separate CF line (cash impact of discontinued ops) and retained-earnings roll effects. Revisit when a major filer triggers it.
- **Segment data.** Product/service, geographic, and reportable-segment revenue belong in a future `segments` table keyed to `company_events` or a dedicated `segments` table. Explicitly out of scope for the IS buckets here.

---

## 13. What this doc unlocks

- `financial_facts.concept` values (snake_case bucket names from above)
- `financial_facts.statement` enum values (`income_statement`, `balance_sheet`, `cash_flow`)
- FMP mapper (`fmp_mapping.md`) — maps FMP JSON fields into these buckets
- Verification stack (`verification.md`) — enforces the ties and invariants above
- Formula definitions (`formulas.md`) — computed values reference these bucket names
- Future `concepts` table (Build Order ~step 10) — this doc becomes the seed data

When a migration or mapper conflicts with this doc, this doc wins — open a PR updating both together.
