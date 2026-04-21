# Verification — Correctness Stack

Arrow runs **five independent checks** on every ingest. Only **Layer 1** is a hard gate — its failure prevents ingest entirely. Layers 2, 3, and 5 are **soft gates**: they still run, they still catch problems, but they record findings to `data_quality_flags` rather than blocking ingest. Layer 4 is formula-level and advisory.

This design reflects the empirical reality that financial data has irreducible noise: filers restate prior periods in subsequent filings, vendors normalize inconsistently, and even within a single 10-K manual reconciliation doesn't always tie. The system faithfully represents this messiness instead of forcing it away, and surfaces every inconsistency alongside the data so an analyst can decide what matters for their specific question.

Complements:
- `concepts.md` — the normalization contract (what the checks enforce)
- `fmp_mapping.md` — how FMP-sourced values reach the buckets being checked
- `formulas.md` — the formulas that layer 5 guards
- `docs/research/amendment_phase_1_5_design.md` — detailed design of Layer 3 amendment handling + flag semantics
- `docs/architecture/system.md` § Build Order

---

## 1. The five layers

```
Layer 1 — Subtotal ties                  (intra-statement; HARD GATE)
Layer 2 — Cross-statement ties           (IS ↔ BS ↔ CF; SOFT — writes flags)
Layer 3 — Period arithmetic (Q sum = FY) (SOFT — amendment agent + flags)
Layer 4 — Formula component-guards       (advisory)
Layer 5 — Cross-source reconciliation    (FMP ↔ SEC XBRL anchors; SOFT — writes flags)
```

- **HARD GATE (Layer 1 only)** — violation raises and the whole ingest transaction rolls back. Nothing about the failing filer persists. Layer 1 catches genuine internal-math violations (gross_profit ≠ revenue - cogs within a single filing, balance identity broken, cash roll-forward internal to CF broken). These indicate the data itself is internally inconsistent at the source row, not just in relation to other data — so blocking is correct.

- **SOFT GATE (Layers 2, 3, 5)** — the check still runs, but failures write rows to the `data_quality_flags` table (see `db/schema/011_data_quality_flags.sql`). The data loads into `financial_facts` as the filer reported it. The flag records: what check fired, which concept and period, what values disagreed, by how much, a human-readable reason, and where to look for verification. Analyst queries the flag table to see all known issues; resolved flags retain a provenance record (`resolved_at`, `resolution`, `resolution_value`).

- **ADVISORY (Layer 4)** — formula-component guards. If a required input is missing, the dependent formula returns null rather than producing a misleading value. No flags currently written; may be added if downstream analysis needs them.

Why Layers 2/3/5 are soft: they catch cross-filing, cross-source, and cross-period inconsistencies that frequently reflect **legitimate filer or vendor behavior** rather than data errors:

- **Layer 2** often catches vendor-internal inconsistency (FMP's IS endpoint and CF endpoint reporting different pre-NCI net income for the same period). This is FMP's bug, not data corruption.
- **Layer 3** catches restatements — when a filer restates prior quarters in a later 10-K (or later 10-Q comparative) but the earlier filings' tagged values don't change. The data isn't wrong; it reflects two different snapshots in time.
- **Layer 5** catches FMP-vs-SEC XBRL divergence, usually because FMP hasn't picked up a comparative-period restatement. The SEC value is usually authoritative; the FMP value was correct at its filing date.

In all three cases, **blocking ingest would prevent loading legitimate filers**. Flagging preserves the data for use while surfacing the known caveat.

Severity of an individual flag: `informational` (<1% delta), `warning` (1-10% delta), `investigate` (≥10% or sanity-bound violation).

---

## 2. Layer 1 — Subtotal Ties

Every reported subtotal must equal our computed subtotal, per filing, within tolerance.

### 2.1 IS ties

```
gross_profit               == total_revenue - cogs
operating_income           == gross_profit - total_opex
ebt_excl_unusual           == operating_income - net_interest_expense + equity_affiliates + other_nonop
ebt_incl_unusual           == ebt_excl_unusual + restructuring + goodwill_impairment
                              + gain_sale_assets + gain_sale_investments + other_unusual
continuing_ops_after_tax   == ebt_incl_unusual - tax
net_income                 == continuing_ops_after_tax + discontinued_ops
net_income_attributable_to_parent == net_income - minority_interest
ni_common                  == net_income_attributable_to_parent - preferred_dividends_is
```

Plus: **our subtotal == filer-reported subtotal** for each. If FMP returns `operatingIncome = X` and our computed `gross_profit - total_opex = Y`, then `X == Y` required.

### 2.2 BS ties

```
total_current_assets       == cash_and_equivalents + short_term_investments + restricted_cash_current
                              + accounts_receivable + other_receivables + inventory + prepaid_expenses
                              + income_taxes_receivable_current + other_current_assets
total_assets               == total_current_assets + net_ppe + right_of_use_assets_operating
                              + long_term_investments + equity_method_investments + goodwill
                              + other_intangibles + deferred_tax_assets_noncurrent + other_noncurrent_assets
total_current_liabilities  == accounts_payable + accrued_expenses + current_portion_lt_debt
                              + short_term_borrowings + current_portion_leases_operating
                              + current_portion_leases_finance + income_taxes_payable_current
                              + deferred_revenue_current + other_current_liabilities
total_liabilities          == total_current_liabilities + long_term_debt + long_term_leases_operating
                              + long_term_leases_finance + deferred_revenue_noncurrent
                              + deferred_tax_liability_noncurrent + other_noncurrent_liabilities
common_equity              == preferred_stock + (common_stock + additional_paid_in_capital OR common_stock_and_apic)
                              + retained_earnings - treasury_stock + accumulated_other_comprehensive_income
total_equity               == common_equity + noncontrolling_interest
total_liabilities_and_equity == total_liabilities + total_equity
```

Sub-detail tie (when both reported):
```
net_ppe == gross_ppe + accumulated_depreciation
```
(where `accumulated_depreciation` is stored negative).

### 2.3 CF ties (cash-impact sign throughout)

```
cfo == net_income_start + dna_cf + sbc + deferred_income_tax
       + gain_on_sale_assets_cf + gain_on_sale_investments_cf + asset_writedown + other_noncash
       + change_accounts_receivable + change_inventory + change_accounts_payable
       + change_deferred_revenue + change_income_taxes + change_other_working_capital

cfi == capital_expenditures + acquisitions + divestitures
       + purchases_of_investments + sales_of_investments
       + loans_originated + loans_collected + other_investing

cff == short_term_debt_issuance + short_term_debt_repayment
       + long_term_debt_issuance + long_term_debt_repayment
       + stock_issuance + stock_repurchase
       + common_dividends_paid + preferred_dividends_paid + special_dividends_paid
       + other_financing

net_change_in_cash == cfo + cfi + cff + fx_effect_on_cash + misc_cf_adjustments
```

Plus filer-tie: our `cfo` must equal FMP's `netCashProvidedByOperatingActivities`; `cfi` vs FMP's `netCashProvidedByInvestingActivities`; `cff` vs `netCashProvidedByFinancingActivities`; `net_change_in_cash` vs FMP's `netChangeInCash`.

### 2.4 Tolerance

**Absolute: ±$1M (or ±0.1% of larger absolute, whichever is larger).**

Rationale: FMP values are USD absolute; filing rounding commonly goes to the nearest million, which introduces up to ±$0.5M of noise on each line × multiple lines. $1M absolute captures this. Larger filers with rounded-to-million values may show $1–3M differences legitimately; the 0.1% floor keeps this from false-flagging on multi-hundred-billion balance sheets.

EPS: ±$0.01 absolute (per-share rounding).
Shares: ±500K absolute (share-count rounding).

### 2.5 Failure

HARD BLOCK. The ingest run sets:
```
ingest_runs.status = 'partial'
ingest_runs.error_details = {
  "layer": "subtotal_tie",
  "statement": "balance_sheet",
  "filing": "artifacts[N]",
  "failed_ties": [
    {"tie": "total_assets == computed", "filer": 125456000000, "computed": 125123000000, "delta": 333000000, "pct": 0.00266}
  ]
}
```

No facts from the failing statement are written for that filing. Operator fixes the mapper or documents a known-delta case (e.g., filer has a line we don't bucket yet).

---

## 3. Layer 2 — Cross-Statement Ties

These span statements; they verify the three statements cohere.

### 3.1 Ties (per filing)

```
1. bs.total_assets == bs.total_liabilities_and_equity                                  (balance sheet closes)
2. cf.net_income_start == is.net_income                                                (CF begins from IS net income)
3. cf.net_change_in_cash == bs.cash_and_equivalents[t] - bs.cash_and_equivalents[t-1]  (cash rollforward)
4. cf.cash_end_of_period == bs.cash_and_equivalents[t]                                 (cash endpoint agreement)
5. cf.cash_begin_of_period == bs.cash_and_equivalents[t-1]                             (cash startpoint agreement)
```

Tie #3 is the one that catches almost every extraction bug — if any CF line is wrong, cash doesn't rollforward. It is mandatory.

### 3.2 Tolerance

- Ties #1, #3: ±$1M (same as layer 1)
- Ties #2, #4, #5: exact (zero tolerance — these are the SAME stored number appearing in two places; rounding shouldn't differ)

### 3.3 Failure

**SOFT — writes a `data_quality_flags` row of type `layer2_cross_statement` and continues.**

Layer 2 failures most commonly reflect **FMP's own internal inconsistency** — e.g., for DELL Q2 FY25, FMP's IS endpoint returns `netIncomeFromContinuingOperations = $841M` while FMP's CF endpoint returns `netIncome = $804M` for the same period. Both should be pre-NCI consolidated net income and they should match; FMP's normalization diverges from itself. Or the failure reflects cash/restricted-cash classification drift between the BS snapshot and the CF cash-flow statement for a period with material restricted cash. In either case, the data is loadable as-is; the flag surfaces the inconsistency for analyst review. The analyst can either (a) accept the disagreement if the specific metric isn't central to their analysis, (b) manually supersede with a verified value via the supersession mechanism, or (c) exclude the affected periods from their specific analysis.

The helper `_write_layer2_flags` in `src/arrow/agents/fmp_ingest.py` inserts one flag row per failed tie, with severity scaled by `|delta| / max(|LHS|, |RHS|)`.

---

## 4. Layer 3 — Period Arithmetic

For every flow bucket (IS flows, CF flows), across every fiscal year where all five values exist:

```
Q1_discrete + Q2_discrete + Q3_discrete + Q4_discrete  ≈  FY
```

### 4.1 Derivation

Per `periods.md` § 6, § 7:
- Q1 discrete = Q1 10-Q value
- Q2 discrete = Q2 10-Q 3-month column (IS) OR Q2_YTD - Q1_YTD (CF)
- Q3 discrete = Q3 10-Q 3-month column (IS) OR Q3_YTD - Q2_YTD (CF)
- Q4 discrete = FY - (Q1 + Q2 + Q3)

So Q1+Q2+Q3+Q4 ≡ FY by construction when Q4 is computed from the identity. This layer guards against:
- A filer reporting discrete Q4 (rare) that disagrees with the identity
- Restatement cases where Q4 = restated_FY - restated_Q1..Q3 (not pre-restatement)
- Cross-source divergence where FMP's quarterly sum doesn't match FMP's annual

### 4.2 Tolerance

Layer 3 sums five independently-rounded values (four quarterly discrete flows plus the filer's own reported annual). Each is typically rounded to the nearest $1M at filing time (~±$0.5M noise). The max expected rounding drift on the identity is therefore 5 × $0.5M = **±$2.5M**, wider than Layer 1's per-line $1M.

```
Layer 3 tolerance: max($2.5M absolute, 0.1% of larger abs)
```

This is filing-level, not implementation-level — verifiable directly from SEC XBRL. Example: NVDA FY2021 SG&A. SEC's own XBRL reports:

| source | value |
|---|---|
| Q1 (3-month) | $293M |
| Q2 (3-month) | $627M |
| Q3 (3-month) | $515M |
| Q4 = FY − 9M_YTD | $503M |
| **sum of quarters** | **$1,938M** |
| **FY (10-K)** | **$1,940M** |

The $2M delta is entirely NVDA's filing-level rounding — present in SEC XBRL before FMP ever touches it. The $2.5M floor absorbs this while keeping anything genuinely anomalous (beyond rounding scale) surfaced.

### 4.3 Stocks exempt

BS stocks are NOT subject to this check (they're snapshots, not flows).

### 4.3 Reclassification detection (CF only)

Per `periods.md` § 7.1: if later Q's YTD − discrete implies a prior Q1 that differs from the stored Q1 by > 0.5%, treat as reclassification candidate. Confirm by comparing prior-year comparatives. If confirmed, supersede the older row.

### 4.4 Tolerance

±$2M absolute (or ±0.1% of FY, whichever larger). Slightly looser than layer 1 because Q4 derivation compounds three subtractions of rounded values.

### 4.5 Failure

**SOFT — the amendment-detect agent attempts auto-resolution first; anything unresolved writes `data_quality_flags` rows of type `layer3_q_sum_vs_fy`.**

See `docs/research/amendment_phase_1_5_design.md` for the full design of the amendment-detect agent (`src/arrow/agents/amendment_detect.py`). High-level flow on Layer 3 failure:

1. Agent fetches SEC XBRL companyfacts for the filer.
2. For each failing (concept, fiscal_year), agent looks for latest-filed XBRL values that differ from FMP's stored values (indicating a comparative-period restatement in a later filing).
3. Agent applies supersessions atomically within a savepoint, then re-verifies Layer 1 holds post-supersession. If Layer 1 would break, savepoint rolls back and flags are written for the unresolved failures.
4. If supersessions stick, any remaining Layer 3 residuals (e.g., concepts where XBRL has no newer value) are written as flags.

Failure modes captured as flags:
- `layer3_q_sum_vs_fy` — Q1+Q2+Q3+Q4 ≠ FY for a concept
- `xbrl_sanity_bound` — an XBRL supersession candidate violated Rule B (sign flip, >50% delta)

Full-period atomicity is preserved: when one concept at a period gets superseded, all mappable concepts at that period are checked so intra-period Layer 1 ties stay intact.

---

## 5. Layer 4 — Formula Component Guards

Every formula in `formulas.md` declares:
- `requires`: the list of buckets (by canonical name) the formula consumes
- `on_missing`: what to do when any required bucket is NULL
- `on_out_of_range`: what to do if a computed intermediate is unreasonable (e.g., negative where economically impossible)

### 5.1 The rule

```
for each formula F with output bucket O:
    for each required component C in F.requires:
        if C is NULL in the required period:
            set O.value = NULL
            set O.provenance.reason = "missing component: C at period P"
            continue to next formula (do not compute)
```

**Never:**
- substitute 0 for a NULL component
- interpolate from adjacent periods
- use a prior period's value in place of the missing one
- compute a partial value and flag it

### 5.2 Example: R&D capitalization

```
formula: r_and_d_amortization_q(t)
requires: rd_q[t], rd_q[t-1], rd_q[t-2], ..., rd_q[t-19]   (20 consecutive quarters)
on_missing: suppress output, reason = "missing: rd_q[YYYY-Qn]"
```

If NVDA's Q1 FY20 `rd_q` is missing, no `r_and_d_amortization_q` value is produced for any period t where t-19 ≥ Q1 FY20. The first valid output appears when all 20 quarters of the window exist. Rolling forward, each new quarter fills a new window.

### 5.3 Tax rate

```
formula: tax_rate_q(t)
requires: tax[t], ebt_incl_unusual[t]
on_missing: suppress
on_out_of_range: suppress if |ebt_incl_unusual| < $1M (denominator near zero)
                 flag if tax_rate < -50% or > 50% (economically unreasonable)
```

### 5.4 Free cash flow

```
formula: fcf_q(t)
requires: cfo[t], capital_expenditures[t]
on_missing: suppress (either component missing → FCF undefined)
note: capital_expenditures is stored with cash-impact sign (negative);
       fcf = cfo + capital_expenditures  (NOT cfo - capital_expenditures)
```

### 5.5 Failure

SUPPRESS. Formula output bucket's value is NULL with `provenance.reason`. Downstream formulas that depend on it suppress transitively.

---

## 6. Layer 5 — Cross-Source Reconciliation

FMP-derived values are cross-checked against SEC XBRL-derived values. This is the layer that **earns trust in FMP empirically**, rather than asserting it.

### 6.1 Source of truth for cross-check: SEC XBRL

**Not HTML.** Rationale:
- XBRL is structured, machine-parseable; HTML requires fragile parsing heuristics.
- SEC's EDGAR requires XBRL numeric facts to match the HTML presentation values (regulatory).
- XBRL concept names are stable across filers and filings (`us-gaap:Revenues` means Revenues everywhere); HTML labels drift (Revenue, Net Sales, Total Revenues).
- The archive's `parse_xbrl.py` infrastructure already proved reliable in the legacy pipeline — per the postmortem, "XBRL face concepts … are reliable ground truth."

HTML is used only as a debugging aid when XBRL is ambiguous. Primary cross-check is XBRL.

### 6.2 Comparison mechanics

For every `(company_id, concept, period_end, period_type)` tuple where both:
- A `financial_facts` row with `source = 'fmp'` exists, AND
- A corresponding XBRL-derived value exists (from a separately-ingested SEC XBRL pass)

Compute:
```
divergence_pct = |fmp_value - xbrl_value| / max(|fmp_value|, |xbrl_value|)
```

### 6.3 Threshold

**0.5% of the larger absolute value.**

Rationale: per `periods.md` § 7.1, this is the tolerance the legacy pipeline used for CF reclassification detection. It's proven in practice — tight enough to catch real divergences, loose enough to absorb rounding/unit differences between vendors.

### 6.4 Failure

**SOFT — writes `data_quality_flags` rows of type `layer5_xbrl_anchor` and continues.**

The helper `_write_layer5_flags` in `src/arrow/agents/fmp_ingest.py` inserts one flag row per (concept, period) divergence. Each flag captures: FMP's stored value, SEC XBRL's latest-filed value, which XBRL tag matched, accession + filed date of the XBRL source, delta, tolerance, severity, and a human-readable reason. SEC XBRL is generally authoritative when they disagree (since it's the filer's own SEC record), but analyst confirms before overriding.

Persistent divergence on the same concept across many periods is a signal worth promoting to a per-company "data quality status" annotation for dashboard visibility.

### 6.5 Timing

Layer 5 runs **inline during every ingest**, alongside Layers 1/2/3. This is a change from the earlier design where Layer 5 was scheduled separately — inline execution means FMP-vs-SEC divergences are flagged at the time data is loaded, giving analysts immediate visibility without waiting for a scheduled reconciliation run.

---

## 7. Regression Tests (supporting layer)

Not one of the five runtime layers, but load-bearing for development.

### 7.1 Archive gold as CI truth

The 12 NVDA JSONs in `archive/ai_extract/NVDA/test/*.json` are the regression fixture. For every `(period, xbrl_concept)` with a matched canonical bucket, the mapper's output must equal the archive value within ±0.1% (tighter than runtime tolerances because these are regression tests, not production data).

Test file: `tests/regression/test_fmp_vs_archive_gold.py` (not yet written; part of the FMP ingest implementation).

### 7.2 Exception: known-mapping discrepancies

A small set of concepts have documented archive-vs-FMP differences that are not bugs (see `fmp_mapping.md` § 7):
- CF working-capital items where archive stored magnitude; FMP and canonical use cash-impact → expected delta, test asserts the cash-impact value
- `us-gaap:OtherNonoperatingIncomeExpense` concept drift → test skips or uses a wider tolerance with a TODO to improve mapping

### 7.3 Other companies

Archive has gold for NVDA, LYB, FCX. Starting with NVDA only; LYB/FCX can be wired up when their mapping is verified.

---

## 8. Failure-Mode Summary

| Layer | Failure | What user sees | Where it's recorded |
|---|---|---|---|
| 1: subtotal ties | HARD BLOCK | ingest_run = 'partial'; facts for failing statement NOT written | `ingest_runs.error_details.failed_ties` |
| 2: cross-statement | HARD BLOCK | same | same |
| 3: period arithmetic | HARD BLOCK | same | same |
| 4: formula guards | SUPPRESS | formula output NULL with reason | `financial_facts.value = NULL` (though NOT NULL constraint means row is not written for suppressed values; alternative: emit to a separate `suppressed_formula_outputs` log) |
| 5: cross-source | FLAG | row is written, also appears in divergence view | `view_fmp_sec_divergence` |

Note on layer 4: `financial_facts.value` is NOT NULL. Suppressed formulas do not produce a `financial_facts` row. The suppression reason is captured either (a) in ingest_run logs or (b) a separate `formula_suppressions` table if we decide volume warrants it — TBD at step 14 (analyst retrieval tools).

---

## 9. Tolerance Summary

| Check | Tolerance | Rationale |
|---|---|---|
| Layer 1 subtotal ties (most) | ±$1M or ±0.1% | rounding noise accumulation across multiple line items |
| Layer 1 EPS | ±$0.01 | per-share rounding |
| Layer 1 share counts | ±500K | share-count rounding |
| Layer 2 tie #1 (BS closes) | ±$1M or ±0.1% | same as layer 1 |
| Layer 2 ties #2, #4, #5 | exact | same stored value in two places |
| Layer 2 tie #3 (cash rollforward) | ±$1M or ±0.1% | same as layer 1 |
| Layer 3 period arithmetic | ±$2M or ±0.1% | Q4 derivation compounds rounding |
| Layer 4 component guards | N/A | presence check, not numeric |
| Layer 4 out-of-range (where defined) | formula-specific | see formulas.md |
| Layer 5 cross-source | 0.5% | proven legacy pipeline threshold |
| Regression tests (archive gold) | ±0.1% | tighter than runtime; development-time guardrail |

---

## 10. Where the Checks Live in Code

```
src/arrow/reconcile/
  invariants.py         — layers 1, 2, 3 (run at end of ingest batch)
  fmp_vs_sec.py         — layer 5 (scheduled job, step 9.5)

src/arrow/normalize/financials/
  formulas.py           — layer 4 (component guards applied per formula)

tests/regression/
  test_fmp_vs_archive_gold.py  — regression against archive JSONs

db/queries/
  view_fmp_sec_divergence.sql  — layer 5's output view
```

Planned creation timeline per Build Order:
- Layers 1, 2, 3 + regression tests: part of step 8 (FMP ingest) — land together.
- Layer 4 (formula guards): as formulas are implemented (step 8+).
- Layer 5 (cross-source reconciliation): step 9.5 (after step 8 + an SEC XBRL pass exists).

---

## 11. Revisions / audit trail

When a check tightens or loosens, open a PR with:
- the triggering case (a real filing that passed when it shouldn't have, or failed when it shouldn't have)
- the tolerance change
- an ADR if the change crosses philosophy (e.g., moving from HARD BLOCK to FLAG)

Tolerances are conservative by design. Loosening without specific justification is how silent failures accumulate.
