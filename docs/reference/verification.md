# Verification — Audit Reference

This document describes Arrow's audit/reconciliation framework.

Important:
- this is **not** the default baseline ingest path
- baseline `financial_facts` come from FMP without inline audit adjudication
- only Layer 1 runs during default ingest; Layers 2–5 are side-rail / scaffold / planned
- use this doc when working on the audit side rail, not when orienting to normal ingest

For normal operation first, read:
- `docs/architecture/normal_vs_audit.md`
- `docs/architecture/system.md`
- ADR-0010 (`docs/decisions/0010-fmp-baseline-truth-sec-documents-audit-side-rail.md`)

Arrow's validation stack has five conceptual layers. **Only Layer 1 runs during default FMP backfill; inside Layer 1, some checks are hard gates and some are inline soft flags.** The remaining layers exist as audit side-rail tooling (Layers 3, 5) or as scaffold/planned work (Layers 2, 4). When the side-rail audit pass runs, it can write to `data_quality_flags` and, under strict rules, supersede FMP facts via the amendment-detect agent. It does not run on every ingest.

This design reflects the ADR-0010 split: trust FMP as baseline; keep audit callable but off the critical path. Layer definitions below still describe the adjudication intent so that the side rail has a spec to implement and so analysts reading flag rows know what each `flag_type` means.

**Adjudication contract (when audit runs, not every ingest):**
- trust FMP when it remains the best usable normalized value
- supersede FMP when SEC/XBRL is clearly better (criteria in § 1.3)
- keep FMP and flag when the disagreement is real but not safe to auto-resolve

Complements:
- `concepts.md` — the normalization contract (what the checks enforce)
- `fmp_mapping.md` — how FMP-sourced values reach the buckets being checked
- `formulas.md` — the formulas that Layer 4 would guard (not yet implemented)
- `docs/research/amendment_phase_1_5_design.md` — detailed design of Layer 3 amendment handling + flag semantics (soft-flag refinement applied 2026-04-22)
- `docs/architecture/system.md` § Build Order (step 9.5 = audit rail)

---

## 1. The five layers

```
Layer 1 — Subtotal ties                  (intra-statement; inline on every ingest)
  1-HARD  BS balance identity,
          CF cash roll-forward            (HARD GATE)
  1-SOFT  IS subtotal drift,
          BS subtotal-component drift,
          CF top-level aggregation,
          CF cfo/cfi/cff subtotal ==
          sum of FMP's component fields   (inline flag, non-blocking)
Layer 2 — Cross-statement ties           (IS ↔ BS ↔ CF; scaffold present, not wired)
Layer 3 — Period arithmetic (Q sum = FY) (side rail; amendment agent + flags)
Layer 4 — Formula component-guards       (planned; not yet implemented)
Layer 5 — Cross-source reconciliation    (FMP ↔ SEC XBRL anchors; side rail; writes flags)
```

### Layer status at a glance

| Layer | Status | Invocation | Failure behavior |
|---|---|---|---|
| 1-HARD | **live / mainline** | inline during `backfill_fmp_statements` | **HARD BLOCK** — transaction rolls back, no facts persist |
| 1-SOFT | **live / mainline** | inline during `backfill_fmp_statements` | soft — writes `data_quality_flags` rows of type `is_subtotal_tie_drift` / `bs_subtotal_component_drift` / `cf_subtotal_component_drift`; row is loaded verbatim |
| 2 | **scaffold, not wired** | function exists (`verify_cross_statement_ties`) but no caller in mainline or side rail | n/a yet — when wired, will be side-rail soft-flag |
| 3 | **side rail** | `amendment_detect.detect_and_apply_amendments`, invoked from audit tooling, not default backfill | soft — attempts XBRL supersession; unresolved becomes `data_quality_flags` row |
| 4 | **planned** | formula layer not yet implemented; `formulas.py` does not exist | when built, will suppress formula output on missing components |
| 5 | **side rail** | `scripts/reconcile_fmp_vs_xbrl.py` → `arrow.agents.fmp_reconcile` → `arrow.reconcile.fmp_vs_xbrl` | soft — writes `data_quality_flags` rows |

Default `backfill_fmp_statements` runs Layer 1 (hard + soft) only. No other layer mutates `financial_facts` during default ingest. Soft-tie flags written during ingest do not change the stored fact values — they annotate them.

### Why Layer 1 has a soft class

Not every Layer-1 tie tests the same thing. Some test filer integrity: if the filer-reported value doesn't satisfy the tie, the filing itself is internally broken. Some test vendor bucketing consistency: the tie formula sums canonical buckets that FMP populated from their own normalization of the filing. When FMP's bucketing drops or misbuckets an item, the sum doesn't tie to FMP's own reported subtotal even though the filer's actual 10-Q is fine. These are different failure modes and should have different gates.

| Tie | Tests | Hard / Soft |
|---|---|---|
| IS `gross_profit == revenue − cogs` (and the IS chain) | FMP's normalized subtotal arithmetic in the income statement; older quarters can drift | SOFT |
| BS `total_assets == total_liabilities + total_equity` | filer's balance identity | HARD |
| BS subtotal ties (`total_current_assets == sum of components`, etc.) | FMP's bucketing of filer components into canonical BS buckets | SOFT |
| CF `net_change_in_cash == cash_end − cash_begin` | filer cash roll-forward (definitional) | HARD |
| CF `net_change_in_cash == cfo + cfi + cff + fx` | FMP's section-decomposition (fails on Q4-derivation artifacts) | SOFT |
| CF `cfo == sum(non-cash + working-capital components)` | **FMP's bundling of filer line-items into FMP buckets** | SOFT |
| CF `cfi == sum(investing components)` | same | SOFT |
| CF `cff == sum(financing components)` | same | SOFT |

The DELL FY2026 Q2, AMD FY2022 Q1/Q3, and VRT FY2023 Q4 cases are all SOFT failures — FMP's normalization leaked a bucket boundary (DELL: restricted cash / operating-lease-current; AMD: unknown ~$6M financing item; VRT: restricted cash double-count inside cash plus other-current-assets) inside an otherwise well-formed filing. Hard-blocking in those cases loses the entire ticker's history over a vendor bundling leak. Soft-flagging loads the row verbatim and makes the caveat queryable.

When in doubt: if the tie could fail because *FMP* mis-bucketed rather than because the *filer* reported inconsistent numbers, the tie is SOFT. If the filer genuinely can't have shipped a 10-Q in that state, the tie is HARD.

### What "HARD GATE", "inline soft-flag", and "side rail" actually mean

- **HARD GATE (Layer 1 hard ties)** — violation raises and the entire ingest transaction for that period rolls back. Nothing about the failing filer's period persists. Catches genuine filer-level integrity violations: BS balance identity, CF cash roll-forward. The filing itself would have to be internally broken to fail these.

- **INLINE SOFT-FLAG (Layer 1 soft ties; IS/BS/CF subtotal drift, today)** — runs during normal ingest, alongside hard ties. On failure, writes one row per failing tie to `data_quality_flags` with `flag_type = 'is_subtotal_tie_drift'`, `bs_subtotal_component_drift`, or `cf_subtotal_component_drift` and severity scaled by `|delta| / max(|filer|, |computed|)` (<1% → informational, 1–10% → warning, ≥10% → investigate). The `financial_facts` row is loaded verbatim — FMP's reported subtotal and FMP's component fields are both stored as shipped. The flag row is the analyst-visible record that they don't agree. Analyst reviews via `scripts/review_flags.py`; accept-as-is leaves the fact unchanged.

- **SIDE RAIL (Layers 3, 5)** — the check does not run on every ingest. It runs when an operator explicitly invokes the audit tooling (for Layer 5, `scripts/reconcile_fmp_vs_xbrl.py`; for Layer 3, the amendment-detect agent invoked via audit scripts). When it runs, failures write rows to `data_quality_flags` (see `db/schema/011_data_quality_flags.sql` + migration 012). Layer 3's amendment-detect agent may also supersede FMP facts via savepointed XBRL replacement, but only under the atomicity rules in `docs/research/amendment_phase_1_5_design.md` — never as a by-product of mainline ingest.

- **SCAFFOLD (Layer 2)** — the verification function exists in `src/arrow/normalize/financials/verify_cross_statement.py` but has no caller in mainline code or the audit side rail. It is documented here as a spec; when re-wired, it will run side-rail and soft-flag.

- **PLANNED (Layer 4)** — no implementation yet; `formulas.md` describes the contract.

### Flag lifecycle on re-ingest

When a mainline re-ingest supersedes `financial_facts` rows, flags raised by **earlier side-rail audit passes** against those facts no longer point at current data. Leaving them open would contaminate the "unresolved flags for this company" view with anomalies that may not reproduce against the fresh payload.

Rule, enforced by `backfill_fmp_statements`: before any fact is written for a ticker, unresolved flags in the re-ingested `(company_id, fiscal_year ∈ [min_fy, max_fy])` window are auto-resolved with `resolution = 'superseded_by_reingest'`. The next audit pass raises fresh flags for anything that still applies. Migration 012 adds the resolution code; it is distinct from the three analyst-action codes (`approve_suggestion`, `override`, `accept_as_is`) so audit queries can separate automated housekeeping from human decisions.

Flags with NULL `fiscal_year` are not auto-closed — they aren't FY-scoped, so a FY-window re-ingest cannot decide whether it subsumes them.

Severity of an individual flag: `informational` (<1% delta), `warning` (1–10% delta), `investigate` (≥10% or sanity-bound violation).

### 1.1 Decision contract (audit-time only)

When the audit side rail runs, each disputed fact or period must end in exactly one of three states:

1. `trusted` — FMP row remains current.
2. `superseded` — a later SEC-backed row becomes current with provenance, via the amendment-detect agent's savepointed protocol.
3. `flagged_unresolved` — FMP row remains current and a flag records why Arrow refused to guess.

A flagged case is therefore not a pipeline failure. It is often the correct outcome of adjudication.

### 1.2 What the layers mean

- **Layer 1**: is the statement internally usable at all? (enforced every ingest)
- **Layer 2**: do related statements agree strongly enough to trust together? (scaffold)
- **Layer 3**: did a later filing probably restate this period? (audit side rail)
- **Layer 4**: are downstream formulas allowed to compute from these facts? (planned)
- **Layer 5**: after comparing to SEC, should Arrow trust, supersede, or flag? (audit side rail)

### 1.3 When SEC is "clearly better"

Applies only when the audit side rail is running. SEC/XBRL does **not** win automatically whenever it differs from FMP. A SEC-backed value is eligible to supersede FMP only when **all** of the following hold:

1. **Later authority** — the SEC fact is later-filed than the filing context represented by the current FMP row.
2. **Same economic meaning** — the SEC tag and the Arrow canonical bucket mean the same thing, not a narrower/wider cousin.
3. **Same period geometry** — same `period_end`, same duration semantics for flows, same instant semantics for BS facts.
4. **Sanity bounds pass** — no forbidden sign flip, no implausible magnitude jump unless policy explicitly allows it.
5. **Package survives Layer 1** — replacing the value, together with any sibling concepts required for that period, still leaves the statement internally coherent. This is the atomicity rule enforced by the amendment-detect agent's savepoint protocol.

If any one fails:
- SEC is **not** "clearly better" for auto-supersession
- FMP remains current
- Arrow writes a flag instead of inventing certainty

### 1.4 Mapping-confidence classes

To make "same economic meaning" operational, Arrow should reason about concepts in these classes:

| class | meaning | auto-supersede? | example |
|---|---|---|---|
| `exact_equivalent` | SEC tag and FMP bucket are the same economic line | yes, if all other checks pass | `revenue`, `total_assets` |
| `equivalent_if_packaged` | same concept, but only safe if neighboring lines also move coherently | yes, but only as a period package | `total_equity`, some subtotals |
| `bundled_mismatch` | FMP bucket is wider/narrower than the SEC tag | no, flag/manual only | FMP `accounts_payable` vs SEC `AccountsPayableCurrent` when FMP bundles other payables |
| `derived_only` | Arrow can justify the value only by deriving it from other accepted SEC facts | only under explicit derivation rules | subtotal-support concepts |
| `unknown` | mapping confidence insufficient | no, flag/manual only | filer-specific residual buckets |

### 1.5 Decision table

| situation | Arrow action | current row |
|---|---|---|
| FMP passes the stack; no better SEC evidence | trust FMP | FMP row stays current |
| SEC is later, semantically equivalent, sane, and Layer 1 survives | supersede (audit side rail only) | XBRL-amendment row becomes current |
| SEC is later but narrower/wider than FMP | flag | FMP row stays current |
| SEC is later and looks plausible, but replacing it breaks Layer 1 | flag | FMP row stays current |
| SEC candidate fails sanity bounds | flag | FMP row stays current |
| SEC has no matching fact for the Arrow concept/period | trust FMP, possibly with informational note | FMP row stays current |

---

## 2. Layer 1 — Subtotal Ties (live / mainline)

Every reported subtotal must equal our computed subtotal, per filing, within tolerance. Enforced inline during `backfill_fmp_statements`:

- `verify_is_ties` — all IS ties are **HARD**.
- `verify_bs_ties` — all BS ties are **HARD**.
- `verify_cf_hard_ties` — the two filer-integrity CF ties (net_change_in_cash = cfo+cfi+cff+fx, cash roll-forward). **HARD**.
- `verify_cf_soft_ties` — the three vendor-bucketing CF ties (cfo/cfi/cff = sum of components). **SOFT / inline flag** (see § 2.3b).

All live in `src/arrow/normalize/financials/`.

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

#### 2.3a HARD ties (filer integrity)

The cash roll-forward is the only definitional-integrity tie on the CF: if it fails, the filing is literally broken. Blocks ingest.

```
net_change_in_cash == cash_end_of_period − cash_begin_of_period
```

Implementation: `verify_cf_hard_ties` in `src/arrow/normalize/financials/verify_cf.py`.

(Historically the system also HARD-tied `net_change_in_cash == cfo + cfi + cff + fx`. Empirical cases — AMD FY2017 Q4 — show this tie failing on vendor-derivation artifacts while the cash roll-forward ties perfectly, meaning the underlying filing is internally consistent and FMP's decomposition is where the ~$60M leak lives. That tie was moved to SOFT alongside the cfo/cfi/cff subtotal-component ties.)

#### 2.3b SOFT ties (vendor bucketing consistency; inline flag)

These fail when FMP's reported subtotal and FMP's own component fields inside the single shipped row disagree. The filer's 10-Q is typically internally consistent; the defect is FMP's normalization dropping or misbucketing an item (DELL FY26 Q2 restricted cash, AMD FY22 Q1/Q3 unknown ~$6M financing item, VRT FY23 Q4 restricted cash double-count). Load the row verbatim; write one `data_quality_flags` row per failing tie.

```
cfo == net_income_start + dna_cf + sbc + deferred_income_tax + other_noncash
       + change_accounts_receivable + change_inventory
       + change_accounts_payable + change_other_working_capital

cfi == capital_expenditures + acquisitions
       + purchases_of_investments + sales_of_investments + other_investing

cff == short_term_debt_issuance + long_term_debt_issuance
       + stock_issuance + stock_repurchase
       + common_dividends_paid + preferred_dividends_paid + other_financing
```

(Several concepts — gain_on_sale_*, asset_writedown, divestitures, loans_originated/collected, special_dividends_paid, misc_cf_adjustments, short_term/long_term debt repayments — are bundled by FMP into `otherNonCashItems`, `otherWorkingCapital`, `otherInvestingActivities`, or the net debt issuance figures. They therefore do not appear as separate addends above. The ties reflect FMP's data model.)

Implementation: `verify_cf_soft_ties` in `src/arrow/normalize/financials/verify_cf.py`. Flag write: `_write_cf_soft_tie_flag` in `src/arrow/normalize/financials/load.py`.

#### Filer-tie (via mapper, both hard and soft classes)

Our canonical subtotals (`cfo`, `cfi`, `cff`, `net_change_in_cash`) are mapped straight from FMP's reported fields (`netCashProvidedByOperatingActivities`, `netCashProvidedByInvestingActivities`, `netCashProvidedByFinancingActivities`, `netChangeInCash`). The ties above therefore reconstruct each subtotal from components and compare to FMP's reported subtotal.

### 2.4 Tolerance

**Absolute: ±$1M (or ±0.1% of larger absolute, whichever is larger).**

Rationale: FMP values are USD absolute; filing rounding commonly goes to the nearest million, which introduces up to ±$0.5M of noise on each line × multiple lines. $1M absolute captures this. Larger filers with rounded-to-million values may show $1–3M differences legitimately; the 0.1% floor keeps this from false-flagging on multi-hundred-billion balance sheets.

EPS: ±$0.01 absolute (per-share rounding).
Shares: ±500K absolute (share-count rounding).

### 2.5 Failure

**Hard-tie failure → HARD BLOCK.** `backfill_fmp_statements` raises one of `VerificationFailed` / `BSVerificationFailed` / `CFVerificationFailed`, the caller's `with conn.transaction()` rolls back, and the ingest run is marked failed:

```
ingest_runs.status = 'failed'
ingest_runs.error_details = {
  "kind": "bs_verification_failed",
  "period_label": "FY2024 Q3",
  "failed_ties": [
    {"tie": "total_assets == computed", "filer": "125456000000", "computed": "125123000000", "delta": "333000000", "tolerance": "125456000.0"}
  ]
}
```

No facts from the failing statement are written for that filing.

**Soft-tie failure → inline flag.** The `financial_facts` row is still loaded verbatim. In the same transaction, one `data_quality_flags` row is written per failing tie with:

- `flag_type = 'cf_subtotal_component_drift'`
- `severity` auto-assigned by drift pct (<1% informational, 1–10% warning, ≥10% investigate)
- `expected_value` = FMP's reported subtotal, `computed_value` = sum of FMP's components, plus `delta` and `tolerance`
- `reason` = human-readable explanation noting the row was loaded verbatim
- `context` = structured metadata including which tie fired

The analyst reviews pending flags and decides. Default resolution is `accept_as_is` — meaning "keep FMP's row as loaded; the disagreement is within my tolerance for this analysis." Other resolutions (`override`, `approve_suggestion`) apply only when the audit side rail proposes a replacement value via the amendment-detect agent.

Review workflow:

```
uv run scripts/review_flags.py               # list all unresolved
uv run scripts/review_flags.py NVDA AMD      # filter by ticker
uv run scripts/review_flags.py --show 42     # inspect one flag's detail
uv run scripts/review_flags.py --accept 42 --note "vendor rounding, within noise"
uv run scripts/review_flags.py --accept-all AMD --note "FY22 Q1/Q3 FMP drift"
```

Accepted flags stay in the DB forever with `resolved_at`, `resolution='accept_as_is'`, `resolution_note` populated — full audit trail.

The DELL FY2026 Q2 (`fmp_bug_report_dell_fy26_q2.md`) and AMD FY2022 Q1/Q3 cases are real SOFT-tie failures. Under this policy they would each produce one flag and load the row. Under the prior all-hard-ties policy they blocked the ticker entirely.

---

## 3. Layer 2 — Cross-Statement Ties (scaffold)

**Not currently wired to any runtime path.** The function `verify_cross_statement_ties` is implemented in `src/arrow/normalize/financials/verify_cross_statement.py`; nothing in mainline ingest or the audit side rail calls it. Documented here as the spec for when it's re-wired.

When wired, it will run as a side-rail audit layer (like Layer 5) and write `data_quality_flags` rows with `flag_type='layer2_cross_statement'`. It will not hard-block.

### 3.1 Ties (per filing)

```
1. bs.total_assets == bs.total_liabilities_and_equity                                  (balance sheet closes)
2. cf.net_income_start == is.net_income                                                (CF begins from IS net income)
3. cf.net_change_in_cash == bs.cash_and_equivalents[t] - bs.cash_and_equivalents[t-1]  (cash rollforward)
4. cf.cash_end_of_period == bs.cash_and_equivalents[t]                                 (cash endpoint agreement)
5. cf.cash_begin_of_period == bs.cash_and_equivalents[t-1]                             (cash startpoint agreement)
```

Tie #1 is already enforced by Layer 1 BS balance identity, so Layer 2 does not duplicate it.

Ties #3, #4, #5 need ASC 230 handling: post-2018 ASC 230 amendments require "cash + cash equivalents + restricted cash" on the CF endpoints, while BS reports only cash + cash equivalents. FMP doesn't expose restricted cash on its balance-sheet endpoint; the scaffold pulls it from the SEC XBRL companyfacts payload (the same one used by Layer 5). Tags consulted, in order: `RestrictedCashCurrent` + `RestrictedCashNoncurrent` (summed), then `RestrictedCashAndCashEquivalentsAtCarryingValue` (combined), then 0 (filer doesn't report any restricted cash).

### 3.2 Tolerance

- Tie #2 (CF net_income_start vs IS net_income): ±$1M or 0.1% (filing rounding between two independently-reported presentations of the same number).
- Ties #3, #4, #5: ±$1M or 0.1% (cash + restricted rollforward, ASC 230).

### 3.3 Failure

Will write one `data_quality_flags` row per failed tie with severity scaled by `|delta| / max(|LHS|, |RHS|)`. No mainline-blocking helper exists; the scaffold function returns a list of `CrossStatementFailure` records for the caller to persist.

---

## 4. Layer 3 — Period Arithmetic (side rail)

Runs via the amendment-detect agent (`src/arrow/agents/amendment_detect.py`), which is invoked from audit tooling, not from `backfill_fmp_statements`.

For every flow bucket (IS flows, CF flows), across every fiscal year where all five values exist:

```
Q1_discrete + Q2_discrete + Q3_discrete + Q4_discrete  ≈  FY
```

### 4.1 Derivation

Per `periods.md` § 6, § 7:
- Q1 discrete = Q1 10-Q value
- Q2 discrete = Q2 10-Q 3-month column (IS) OR Q2_YTD − Q1_YTD (CF)
- Q3 discrete = Q3 10-Q 3-month column (IS) OR Q3_YTD − Q2_YTD (CF)
- Q4 discrete = FY − (Q1 + Q2 + Q3)

So Q1+Q2+Q3+Q4 ≡ FY by construction when Q4 is computed from the identity. Layer 3 guards against:
- a filer reporting discrete Q4 (rare) that disagrees with the identity
- restatement cases where Q4 = restated_FY − restated_Q1..Q3 (not pre-restatement)
- cross-source divergence where FMP's quarterly sum doesn't match FMP's annual

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

### 4.4 Reclassification detection (CF only)

Per `periods.md` § 7.1: if a later Q's YTD − discrete implies a prior Q1 that differs from the stored Q1 by > 0.5%, treat as a reclassification candidate. This detection runs in the audit side rail, not in mainline ingest. Any supersession goes through the amendment-detect savepoint protocol.

### 4.5 Failure

**Side rail — the amendment-detect agent attempts auto-resolution first; anything unresolved writes `data_quality_flags` rows of type `layer3_q_sum_vs_fy`.** The agent **never raises**; it returns a categorical `AmendmentResult` (`clean` / `amended` / `unresolved_flagged`). See `docs/research/amendment_phase_1_5_design.md` for the full spec (soft-flag policy refined 2026-04-22).

High-level flow on Layer 3 failure:

1. Agent fetches SEC XBRL companyfacts for the filer.
2. For each failing (concept, fiscal_year), agent looks for latest-filed XBRL values that differ from FMP's stored values (indicating a comparative-period restatement in a later filing).
3. Agent applies supersessions atomically within a savepoint, then re-verifies Layer 1 holds post-supersession. If Layer 1 would break, savepoint rolls back and flags are written for the unresolved failures.
4. If supersessions stick, any remaining Layer 3 residuals (e.g., concepts where XBRL has no newer value) are written as flags.

Failure modes captured as flags:
- `layer3_q_sum_vs_fy` — Q1+Q2+Q3+Q4 ≠ FY for a concept
- `xbrl_sanity_bound` — an XBRL supersession candidate violated Rule B (sign flip, >50% delta)

Full-period atomicity is preserved: when one concept at a period gets superseded, all mappable concepts at that period are checked so intra-period Layer 1 ties stay intact.

---

## 5. Layer 4 — Formula Component Guards (planned)

**Not yet implemented.** `src/arrow/normalize/financials/formulas.py` does not exist. This section is the contract for when it's built (Build Order step 8+).

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

## 6. Layer 5 — Cross-Source Reconciliation (side rail)

FMP-derived values are cross-checked against SEC XBRL-derived values. This is the layer that **earns trust in FMP empirically**, rather than asserting it.

**Not inline.** Layer 5 runs via the audit side rail only: `scripts/reconcile_fmp_vs_xbrl.py` → `arrow.agents.fmp_reconcile.reconcile_fmp_vs_xbrl` → `arrow.reconcile.fmp_vs_xbrl.reconcile_top_line_anchors`. It is read-only with respect to `financial_facts` — it writes `data_quality_flags` but does not supersede facts. Amendment-detect is the only path that supersedes, and it operates under Layer 3.

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
- A corresponding XBRL-derived value exists (from the SEC XBRL companyfacts payload fetched at audit time)

Compute:
```
divergence_pct = |fmp_value - xbrl_value| / max(|fmp_value|, |xbrl_value|)
```

### 6.3 Threshold

**0.5% of the larger absolute value.**

Rationale: per `periods.md` § 7.1, this is the tolerance the legacy pipeline used for CF reclassification detection. It's proven in practice — tight enough to catch real divergences, loose enough to absorb rounding/unit differences between vendors.

### 6.4 Failure

**Side rail — writes `data_quality_flags` rows of type `layer5_xbrl_anchor` and continues.**

Each flag captures: FMP's stored value, SEC XBRL's latest-filed value, which XBRL tag matched, accession + filed date of the XBRL source, delta, tolerance, severity, and a human-readable reason.

Important: Layer 5 itself is an **audit layer**, not an override layer.

- Layer 5 never supersedes `financial_facts` directly. It only writes flags.
- If an analyst or subsequent audit pass determines SEC is genuinely later, semantically equivalent, sane, and the replacement package would survive Layer 1, supersession happens through the amendment-detect agent (Layer 3), never through Layer 5.
- If SEC is later but narrower/wider than the FMP bucket, fails sanity bounds, or breaks Layer 1 when applied, Arrow keeps FMP and the flag stands.
- If SEC has no matching fact, Arrow keeps FMP and records nothing.

So the practical rule is: **SEC disagreement triggers a flag, not automatic replacement. Replacement, if warranted, is a separate amendment-detect invocation.**

Persistent divergence on the same concept across many periods is a signal worth promoting to a per-company "data quality status" annotation for dashboard visibility.

### 6.5 Timing

Layer 5 runs **only when the audit side rail is explicitly invoked** — e.g., `scripts/reconcile_fmp_vs_xbrl.py NVDA`. It does not run during default `backfill_fmp_statements`. The pre-pivot design ran Layer 5 inline; ADR-0010 moved it off the critical path.

---

## 7. Regression Tests (supporting layer)

Not one of the five runtime layers, but load-bearing for development.

### 7.1 Archive gold as reference

The 12 NVDA JSONs in `archive/ai_extract/NVDA/test/*.json` are the historical reference fixture. They predate the FMP pivot and can be used as a reference corpus when wiring a regression test comparing FMP-mapped values against the archive's canonical numbers. No regression-gold test is currently committed.

### 7.2 Known-mapping discrepancies

A small set of concepts have documented archive-vs-FMP differences that are not bugs (see `fmp_mapping.md` § 7):
- CF working-capital items where archive stored magnitude; FMP and canonical use cash-impact → expected delta.
- `us-gaap:OtherNonoperatingIncomeExpense` concept drift.

### 7.3 Other companies

Archive has gold for NVDA, LYB, FCX. NVDA is the first candidate when wiring regression coverage.

---

## 8. Failure-Mode Summary

| Layer | Status | Failure | What user sees | Where it's recorded |
|---|---|---|---|---|
| 1-HARD: IS/BS subtotal ties, BS balance identity, CF aggregation + roll-forward | live / mainline | HARD GATE | ingest transaction rolls back; no data for the failing period persists | raised exception, `ingest_runs.error_details` |
| 1-SOFT: CF cfo/cfi/cff subtotal == sum of components | live / mainline | soft (inline flag) | row loaded verbatim; one flag row per failing tie | `data_quality_flags` with `flag_type='cf_subtotal_component_drift'` |
| 2: cross-statement | scaffold, not wired | n/a | nothing runs yet | — |
| 3: period arithmetic | side rail | soft (amendment agent + flags) | audit run: data stays loaded; agent resolves what it can via XBRL supersession; residuals become flags | `data_quality_flags` with `flag_type='layer3_q_sum_vs_fy'` (plus `xbrl_sanity_bound` for Rule B violations) |
| 4: formula guards | planned | — | not implemented | — |
| 5: cross-source | side rail | soft (flag) | audit run: one flag row per divergent (concept, period) | `data_quality_flags` with `flag_type='layer5_xbrl_anchor'` |

Note on layer 4 (planned): `financial_facts.value` is NOT NULL. When implemented, suppressed formulas will not produce a `financial_facts` row. Suppression reason capture (ingest-run logs vs. a separate `formula_suppressions` table) is TBD at step 14 (analyst retrieval tools).

Note on resolved flags: when an analyst manually verifies and corrects a flagged value, the supersession infrastructure in `financial_facts` (via `extraction_version='human-verified-v1'`) records the corrected value with full provenance, and the flag row gets `resolved_at` + `resolution` fields populated. Resolved flags are never deleted — they retain the audit trail of "we looked at this on date D, decided X."

---

## 9. Tolerance Summary

| Check | Tolerance | Rationale |
|---|---|---|
| Layer 1 subtotal ties (most) | ±$1M or ±0.1% | rounding noise accumulation across multiple line items |
| Layer 1 EPS | ±$0.01 | per-share rounding |
| Layer 1 share counts | ±500K | share-count rounding |
| Layer 2 tie #2 (CF NI start = IS NI) | ±$1M or ±0.1% | two presentations of the same number |
| Layer 2 ties #3, #4, #5 (cash rollforward, ASC 230) | ±$1M or ±0.1% | filing rounding |
| Layer 3 period arithmetic | ±$2.5M or ±0.1% | five-value identity compounds rounding |
| Layer 4 component guards | N/A | presence check, not numeric |
| Layer 4 out-of-range (where defined) | formula-specific | see formulas.md |
| Layer 5 cross-source | 0.5% | proven legacy pipeline threshold |

---

## 10. Where the Checks Live in Code

```
src/arrow/normalize/financials/
  verify_is.py                  — Layer 1 IS ties (SOFT; called by load_fmp_is_rows)
  verify_bs.py                  — Layer 1 BS ties: split functions
                                    verify_bs_hard_ties (HARD)
                                    verify_bs_soft_ties (SOFT / inline flag)
                                    verify_bs_ties       (combined; used by
                                                          audit callers/tests)
  verify_cf.py                  — Layer 1 CF ties: split functions
                                    verify_cf_hard_ties (HARD)
                                    verify_cf_soft_ties (SOFT / inline flag)
                                    verify_cf_ties       (combined; used by
                                                          audit side rail)
  verify_cross_statement.py     — Layer 2 scaffold; not called by anything live
  verify_period_arithmetic.py   — Layer 3 core; called by amendment_detect

src/arrow/normalize/financials/load.py
  load_fmp_bs_rows              — calls verify_bs_hard_ties (raise)
                                  then verify_bs_soft_ties (collect),
                                  inserts facts, then _write_bs_soft_tie_flag
                                  for each soft failure
  _write_bs_soft_tie_flag       — writes one data_quality_flags row per
                                  soft failure; severity auto-assigned
  load_fmp_cf_rows              — calls verify_cf_hard_ties (raise)
                                  then verify_cf_soft_ties (collect),
                                  inserts facts, then _write_cf_soft_tie_flag
                                  for each soft failure
  _write_cf_soft_tie_flag       — writes one data_quality_flags row per
                                  soft failure; severity auto-assigned

src/arrow/agents/
  fmp_ingest.py                 — mainline orchestrator (Layer 1 only)
  amendment_detect.py           — Layer 3 side rail (amendment agent)
  fmp_reconcile.py              — Layer 5 side rail entry point

src/arrow/reconcile/
  fmp_vs_xbrl.py                — Layer 5 comparison core
  xbrl_concepts.py              — FMP bucket ↔ XBRL tag mapping

scripts/
  backfill_fmp.py               — invokes mainline ingest (Layer 1 hard+soft)
  review_flags.py               — list / show / accept-as-is flag workflow
  reconcile_fmp_vs_xbrl.py      — invokes Layer 5 audit side rail
  review_restatements.py        — audit tooling around amendment review
```

Build Order alignment:
- Layer 1 + mainline ingest: step 8 (ongoing).
- Layer 3 / Layer 5 audit rail: step 9.5 (built 2026-04-21/22 per ADR-0010).
- Layer 4 (formula guards): step 8+ when formulas are implemented.
- Layer 2 (re-wire as side rail): no step assigned; wire when a use case justifies it.

---

## 11. Revisions / audit trail

When a check tightens or loosens, record the change with:
- the triggering case (a real filing that passed when it shouldn't have, or failed when it shouldn't have)
- the tolerance change
- an ADR if the change crosses philosophy (e.g., moving a layer between mainline and side rail)

Tolerances are conservative by design. Loosening without specific justification is how silent failures accumulate.
