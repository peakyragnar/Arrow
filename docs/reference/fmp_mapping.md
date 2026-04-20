# FMP Mapping — Canonical Concepts ↔ FMP Fields

The concrete mapping layer. For every canonical bucket in `concepts.md`, this doc specifies:
- the XBRL concept(s) that populate it (when sourced from SEC filings)
- the FMP field name (when sourced from FMP's REST endpoints)
- sign transform (if FMP's sign differs from our canonical)
- coverage notes
- open items (concepts we need to map but haven't yet)

This doc is the contract `src/arrow/normalize/financials/fmp_mapper.py` implements.

Empirically validated by [scripts/explore_fmp_vs_archive.py](../../scripts/explore_fmp_vs_archive.py) against 12 NVDA filings (FY24 Q1 → FY26 Q4). See `data/exports/fmp_vs_archive_NVDA.csv` for the reconciliation snapshot — 447 exact matches, 17 documented sign flips, 299 concepts still to be mapped.

If FMP's behavior changes and the mapper's output stops matching the regression tests (`tests/regression/test_fmp_vs_archive_gold.py`), **this doc is the source to update first**, then the mapper follows.

---

## 1. FMP API basics

- **Base**: `https://financialmodelingprep.com/stable`
- **Endpoints used**:
  - `/income-statement?symbol={TICKER}&period={quarter|annual}`
  - `/balance-sheet-statement?symbol={TICKER}&period={quarter|annual}`
  - `/cash-flow-statement?symbol={TICKER}&period={quarter|annual}`
- **Period parameter values** (verified empirically):
  - `period=quarter` → returns rows with `period ∈ {Q1, Q2, Q3, Q4}`
  - `period=annual` → returns rows with `period = FY`
  - Avoid: `period=Q` returns FY rows (counter-intuitive)
- **Authentication**: `?apikey=...` query param (key in env `FMP_API`)
- **Unit**: absolute USD (not millions). Archive gold is USD_millions. Mapper divides FMP values by 1,000,000 for canonical storage. EPS and share counts are NOT scaled.
- **Date**: FMP's `date` field matches the filing's `period_end` date exactly for NVDA's 52/53-week calendar (verified across 12 periods). No drift adjustment needed.
- **Published date**: FMP exposes `filingDate`. This is what we use for `financial_facts.published_at`, not `fetched_at`.

---

## 2. Sign Transform Policy (summary)

Derived from empirical reconciliation. See § 7 for per-bucket details.

| Statement | FMP convention | Our canonical | Transform |
|---|---|---|---|
| Income statement | positive magnitudes for expenses, reported sign for gain/loss items | same (§ 2.1 of `concepts.md`) | **none** on verified buckets |
| Balance sheet | positive magnitudes | same | **none** |
| Cash flow | cash-impact sign | cash-impact sign (§ 2.2 of `concepts.md`) | **none** — they agree |

The 17 sign flips in the reconciliation report resolve as follows:
- **8 rows (CF working capital and payment items)**: archive stored raw XBRL magnitude; FMP and our canonical both use cash-impact. Archive was inconsistent. **FMP wins; no transform.**
- **9 rows (`us-gaap:OtherNonoperatingIncomeExpense`)**: XBRL concept drift in the archive — the tag referred to different filing lines over time. Mapping issue, not a sign issue. Handled in § 7.

Net: **no sign transforms needed on any currently-mapped FMP field.** The mapper is a straight field-rename pass.

---

## 3. Stock-Split / Reverse-Split Handling

Splits and reverse splits both fall under the same mechanism — a reverse split is mathematically just an inverse forward split (1-for-10 reverse = multiply historical values by 0.1; 10-for-1 forward = multiply by 0.1 from the other direction; FMP applies both the same way).

### 3.1 What FMP does

FMP back-applies every stock split (forward or reverse) to all historical per-share values and share counts. Verified against NVDA's June 2024 10-for-1 forward split:
- NVDA's FY24 10-K reported diluted EPS = $11.93 at filing time (Feb 2024)
- After the June 2024 split, FMP returns diluted EPS = $1.19 for the same FY24 period
- All quarters and years prior to the split are uniformly divided by 10 in FMP's returned history
- FY25 and FY26 values are post-split natively and unchanged

Buckets affected (every per-share or share-count value):
- `eps_basic`, `eps_diluted`
- `shares_basic_weighted_avg`, `shares_diluted_weighted_avg`
- any future per-share metric (EPS TTM, revenue per share, etc.)

Buckets NOT affected (split-neutral):
- All revenue, expense, income, balance-sheet, and cash-flow values
- Any metric denominated in currency absolute (not per-share)

### 3.2 v1 policy: accept FMP's adjusted values as-is

**Arrow stores FMP's split-adjusted values directly.** One canonical representation per bucket. No separate `split_adjusted` flag, no dual storage of as-reported and adjusted, no split-factor column.

Rationale:
- ≥90% of analyst queries want split-adjusted values (comparability across time)
- Matches vendor convention (Bloomberg, S&P CapIQ, Refinitiv all adjust)
- Keeps schema simple (no new columns)
- As-reported values remain recoverable later via SEC XBRL direct (Build Order step 19)

### 3.3 When a new split occurs to a company we've already ingested

This is the case that needs explicit mechanics. FMP's historical values *change retroactively* when a new split is applied. Here's the flow Arrow uses:

1. **FMP updates its history silently** on the day a new split takes effect. Every prior per-share value returned by the relevant endpoint is now divided (or multiplied for reverse) by the split ratio.
2. **On the next scheduled FMP ingest for that company**, we refetch the endpoints. The raw payload differs from the prior fetch (values changed, bytes changed, hash changed).
3. **New `raw_responses` row is written** (append-only, new `raw_hash` + `canonical_hash`).
4. **Mapper processes the new raw payload** and prepares new `financial_facts` rows.
5. **Supersession cycle triggers** via the partial-unique index `financial_facts_one_current_idx`:
   - For each (company_id, concept, period_end, period_type, extraction_version), the prior row's `superseded_at` is stamped with the new ingest's `published_at`.
   - The new row is written with `superseded_at = NULL` — it becomes the current value.
6. **Old rows remain in the DB** with `superseded_at` set. They are preserved for audit, PIT queries, and post-hoc analysis.

The `financial_facts_one_current_idx` DB constraint makes this a database-enforced invariant: a split-adjustment re-ingest that fails to supersede old rows fails at INSERT time.

### 3.4 Point-in-time queries around splits

PIT correctness is preserved by the supersession chain:

| Query | Returns |
|---|---|
| "NVDA FY24 diluted EPS, as of 2024-05-01" | $11.93 (pre-split row; was current at that date) |
| "NVDA FY24 diluted EPS, today" | $1.19 (post-split row; current now) |
| "NVDA FY24 diluted EPS history — all values ever stored" | both rows, ordered by `published_at` |

This is exactly the behavior `system.md` § Time-Aware Model requires: "what was known as of date D" returns whichever row was current on D.

**Caveat (relevant today):** the pre-split row only exists in our DB if we ingested *before* the split occurred. If a company splits before we ever ingested them, Arrow has no pre-split row — FMP only shows the adjusted history, and we only ever see the adjusted history. For NVDA specifically, we have not yet begun production ingest, so the first ingest will capture post-June-2024 values across the full history. Pre-split rows for NVDA will only appear in Arrow if we explicitly later source them from SEC XBRL direct.

### 3.5 Split-event log (deferred)

Arrow does not currently have a `splits_events` table or dedicated split-adjustment log. The **supersession chain on `financial_facts` is the split-event audit trail** — each supersession carries a `published_at` that identifies when the re-adjustment was ingested, paired with the source `raw_responses.fetched_at`.

When `company_events` is built (Build Order step 13), stock splits will get first-class event rows:
```
company_events
  event_type = 'stock_split'  (or 'reverse_stock_split')
  event_datetime = effective_date
  linked_artifact_id = reference to the 8-K announcing the split
  metadata = { "ratio": "10:1", "type": "forward", "record_date": ..., "ex_date": ... }
```

Until then, splits are inferred post-hoc from a supersession cluster where per-share buckets changed in a constant-ratio way.

### 3.6 What's still an open issue

- **Silent re-adjustments mid-window.** If FMP re-adjusts between two of our scheduled ingests, we see only the post-re-adjustment history on the next fetch. The prior in-between state is lost (FMP doesn't expose a time-machine endpoint). Our PIT trail is accurate for the dates we actually ingested, not for arbitrary dates between ingests.
- **As-reported per-share values.** Until SEC XBRL direct ingest (step 19), Arrow cannot serve as-reported per-share values for historical dates. Analyst queries requiring the shareholders'-eye view at the original filing date are not currently supportable.
- **Splits announced but not yet effective.** If a split is announced in a 10-Q, effective the next quarter, FMP may or may not pre-adjust on announcement — behavior varies. The safe bet: re-ingest affected periods after the effective date to ensure we have the final adjusted values.

These are deferred issues, not breaking bugs. They're flagged here so we notice when the fix becomes necessary.

---

## 4. Unit-Conversion Policy

FMP returns absolute USD. Archive uses USD_millions. We store **absolute USD** in `financial_facts.value` with `financial_facts.unit = 'USD'`. The mapper converts FMP → our canonical by passing through (no division).

**Exception:** if users later want values in millions for display, derive at query time. Do not store millions.

Per-share and share-count buckets use their own units:
- `eps_basic`, `eps_diluted` → `unit = 'USD/share'`
- `shares_basic_weighted_avg`, `shares_diluted_weighted_avg` → `unit = 'shares'` (count, absolute — not millions)

---

## 5. Mapping Tables

### Conventions

- **canonical bucket**: from `concepts.md`
- **xbrl_concept**: the primary `us-gaap:` tag (when extracted from SEC XBRL)
- **fmp_field**: exact FMP JSON key
- **status**: `verified` (value-matched for NVDA across ≥1 period) · `seed` (mapping inferred from field names, unverified) · `needs_check` (reconciliation flagged for review)
- **notes**: sign transforms, coverage gaps, known issues

### 5.1 Income Statement

| canonical | xbrl_concept | fmp_field | status | notes |
|---|---|---|---|---|
| `revenue` | `us-gaap:Revenues` OR `us-gaap:RevenueFromContractWithCustomerExcludingAssessedTax` | `revenue` | verified | match in all 12 NVDA periods |
| `cogs` | `us-gaap:CostOfRevenue` | `costOfRevenue` | verified | |
| `gross_profit` | `us-gaap:GrossProfit` | `grossProfit` | verified | subtotal; tie to `revenue - cogs` |
| `rd` | `us-gaap:ResearchAndDevelopmentExpense` | `researchAndDevelopmentExpenses` | verified | |
| `sga` | `us-gaap:SellingGeneralAndAdministrativeExpense` | `sellingGeneralAndAdministrativeExpenses` | verified | |
| `dna_is` | `us-gaap:DepreciationAndAmortization` (IS face line) | `depreciationAndAmortization` | needs_check | FMP exposes this on IS; whether NVDA reports it on IS face is filer-dependent |
| `other_opex` | (varies) | — | seed | no clean FMP mapping; compute as `operatingExpenses − rd − sga` when needed, or populate from XBRL direct |
| `total_opex` | `us-gaap:OperatingExpenses` | `operatingExpenses` | verified | |
| `operating_income` | `us-gaap:OperatingIncomeLoss` | `operatingIncome` | verified | subtotal; tie required |
| `interest_expense` | `us-gaap:InterestExpense` | `interestExpense` | verified | FMP stores positive magnitude, matches our canonical |
| `interest_income` | `us-gaap:InvestmentIncomeInterest` | `interestIncome` | verified | positive magnitude |
| `other_nonop` | `us-gaap:OtherNonoperatingIncomeExpense` or filer-specific | `nonOperatingIncomeExcludingInterest` | needs_check | XBRL concept drifts across NVDA filings; FMP may aggregate differently than the archive line. Low priority — small magnitudes in most periods. |
| `ebt_excl_unusual` / `ebt_incl_unusual` | `us-gaap:IncomeLossFromContinuingOperationsBeforeIncomeTaxes...` | `incomeBeforeTax` | verified | Arrow doesn't separate unusual items in FMP-sourced data (FMP doesn't break them out); `ebt_incl_unusual` maps directly to FMP's `incomeBeforeTax`. Unusual-item buckets may remain null when sourcing from FMP. |
| `tax` | `us-gaap:IncomeTaxExpenseBenefit` | `incomeTaxExpense` | verified | positive magnitude (tax benefit = negative) |
| `continuing_ops_after_tax` | `us-gaap:IncomeLossFromContinuingOperations` | `netIncomeFromContinuingOperations` | verified | |
| `discontinued_ops` | `us-gaap:IncomeLossFromDiscontinuedOperationsNetOfTax` | `netIncomeFromDiscontinuedOperations` | verified | zero for NVDA; value tested on filers with discontinued ops |
| `net_income` | `us-gaap:NetIncomeLoss` | `netIncome` | verified | subtotal; tie: `== cf.net_income_start` |
| `minority_interest` | `us-gaap:NetIncomeLossAttributableToNoncontrollingInterest` | (implicit) | seed | FMP typically returns consolidated NI; NCI may not be separately exposed. For NCI-bearing filers, cross-source from XBRL. |
| `ni_common` | `us-gaap:NetIncomeLossAvailableToCommonStockholdersBasic` | `bottomLineNetIncome` | needs_check | FMP's `bottomLineNetIncome` purpose unclear; verify against NCI/preferred filers |
| `eps_basic` | `us-gaap:EarningsPerShareBasic` | `eps` | verified (split-adjusted) | FMP back-applies splits |
| `eps_diluted` | `us-gaap:EarningsPerShareDiluted` | `epsDiluted` | verified (split-adjusted) | |
| `shares_basic_weighted_avg` | `us-gaap:WeightedAverageNumberOfSharesOutstandingBasic` | `weightedAverageShsOut` | verified (split-adjusted) | absolute shares, not millions |
| `shares_diluted_weighted_avg` | `us-gaap:WeightedAverageNumberOfDilutedSharesOutstanding` | `weightedAverageShsOutDil` | verified (split-adjusted) | |

**IS unmapped (at-verified-level) — needs further work:**
- Unusual items (restructuring, goodwill_impairment, gain_sale_assets, gain_sale_investments): FMP does not expose these individually on the IS endpoint. For filers with unusual items, cross-source from SEC XBRL or leave null with an audit flag.
- `equity_affiliates`: not explicitly exposed by FMP income-statement; check XBRL direct.

### 5.2 Balance Sheet

| canonical | xbrl_concept | fmp_field | status | notes |
|---|---|---|---|---|
| `cash_and_equivalents` | `us-gaap:CashAndCashEquivalentsAtCarryingValue` | `cashAndCashEquivalents` | verified | |
| `short_term_investments` | `us-gaap:MarketableSecuritiesCurrent` | `shortTermInvestments` | verified | |
| `restricted_cash_current` | `us-gaap:RestrictedCashCurrent` | — | seed | FMP doesn't expose separately; null from FMP sourcing |
| `accounts_receivable` | `us-gaap:AccountsReceivableNetCurrent` | `accountsReceivables` | verified | |
| `other_receivables` | `us-gaap:OtherReceivablesNetCurrent` | `otherReceivables` | needs_check | |
| `inventory` | `us-gaap:InventoryNet` | `inventory` | verified | |
| `prepaid_expenses` | `us-gaap:PrepaidExpenseCurrent` | `prepaids` | verified | |
| `income_taxes_receivable_current` | `us-gaap:IncomeTaxesReceivableCurrent` | — | seed | not separately in FMP BS response |
| `other_current_assets` | `us-gaap:OtherAssetsCurrent` | `otherCurrentAssets` | verified | aggregation may differ from archive |
| `total_current_assets` | `us-gaap:AssetsCurrent` | `totalCurrentAssets` | verified | |
| `net_ppe` | `us-gaap:PropertyPlantAndEquipmentNet` | `propertyPlantEquipmentNet` | verified | |
| `gross_ppe` | `us-gaap:PropertyPlantAndEquipmentGross` | — | seed | not on FMP BS response; source from XBRL direct when needed |
| `accumulated_depreciation` | `us-gaap:AccumulatedDepreciationDepletionAndAmortizationPropertyPlantAndEquipment` | — | seed | same |
| `right_of_use_assets_operating` | `us-gaap:OperatingLeaseRightOfUseAsset` | — | seed | not in FMP BS response; source from XBRL direct |
| `long_term_investments` | `us-gaap:MarketableSecuritiesNoncurrent` | `longTermInvestments` | verified | |
| `equity_method_investments` | `us-gaap:EquityMethodInvestments` | — | seed | not in FMP BS response |
| `goodwill` | `us-gaap:Goodwill` | `goodwill` | verified | |
| `other_intangibles` | `us-gaap:IntangibleAssetsNetExcludingGoodwill` | `intangibleAssets` | verified | |
| `deferred_tax_assets_noncurrent` | `us-gaap:DeferredTaxAssetsNet` | `taxAssets` | needs_check | FMP field name ambiguous |
| `other_noncurrent_assets` | `us-gaap:OtherAssetsNoncurrent` | `otherNonCurrentAssets` | verified | aggregation may differ |
| `total_assets` | `us-gaap:Assets` | `totalAssets` | verified | subtotal; tie: `== total_liabilities + total_equity` |
| `accounts_payable` | `us-gaap:AccountsPayableCurrent` | `accountPayables` | verified | |
| `accrued_expenses` | `us-gaap:AccruedLiabilitiesCurrent` | `accruedExpenses` | needs_check | FMP aggregation wider than the archive's "accrued expenses" — may include items archive broke out separately |
| `current_portion_lt_debt` | `us-gaap:LongTermDebtCurrent` | `shortTermDebt` | needs_check | FMP bundles current-portion-of-LT-debt with short-term-borrowings into `shortTermDebt` |
| `short_term_borrowings` | `us-gaap:ShortTermBorrowings` | (same as above) | needs_check | |
| `current_portion_leases_operating` | `us-gaap:OperatingLeaseLiabilityCurrent` | `capitalLeaseObligationsCurrent` | needs_check | FMP's field name is `capitalLease...` (stale term); verify it actually maps to operating lease |
| `income_taxes_payable_current` | `us-gaap:AccruedIncomeTaxesCurrent` | `taxPayables` | needs_check | |
| `deferred_revenue_current` | `us-gaap:ContractWithCustomerLiabilityCurrent` | `deferredRevenue` | needs_check | FMP's `deferredRevenue` may not split current/noncurrent |
| `other_current_liabilities` | `us-gaap:OtherLiabilitiesCurrent` | `otherCurrentLiabilities` | verified | |
| `total_current_liabilities` | `us-gaap:LiabilitiesCurrent` | `totalCurrentLiabilities` | verified | |
| `long_term_debt` | `us-gaap:LongTermDebtNoncurrent` | `longTermDebt` | verified | |
| `long_term_leases_operating` | `us-gaap:OperatingLeaseLiabilityNoncurrent` | `capitalLeaseObligationsNonCurrent` | needs_check | same naming issue as current-portion |
| `deferred_revenue_noncurrent` | `us-gaap:ContractWithCustomerLiabilityNoncurrent` | `deferredRevenueNonCurrent` | verified | |
| `deferred_tax_liability_noncurrent` | `us-gaap:DeferredIncomeTaxLiabilitiesNet` | `deferredTaxLiabilitiesNonCurrent` | verified | |
| `other_noncurrent_liabilities` | `us-gaap:OtherLiabilitiesNoncurrent` | `otherNonCurrentLiabilities` | verified | aggregation differs from archive |
| `total_liabilities` | `us-gaap:Liabilities` | `totalLiabilities` | verified | |
| `preferred_stock` | `us-gaap:PreferredStockValue` | `preferredStock` | seed | zero for NVDA; verify on filers with preferred |
| `common_stock` | `us-gaap:CommonStockValue` | `commonStock` | needs_check | may be bundled with APIC in FMP |
| `additional_paid_in_capital` | `us-gaap:AdditionalPaidInCapital` | `additionalPaidInCapital` | verified | |
| `common_stock_and_apic` | `us-gaap:CommonStocksIncludingAdditionalPaidInCapital` | — | seed | alternative XBRL concept; populate when filer uses this |
| `retained_earnings` | `us-gaap:RetainedEarningsAccumulatedDeficit` | `retainedEarnings` | verified | |
| `treasury_stock` | `us-gaap:TreasuryStockValue` | `treasuryStock` | verified | **FMP stores SIGNED NEGATIVE** for buybacks (e.g. NVDA FY2022 Q3 returns -12,038,000,000). Empirically confirmed during BS ingest live smoke — filer-reported totalEquity only balances when treasuryStock is ADDED with its FMP-returned sign, not subtracted. Store as-is; BS equity tie adds it. Earlier doc claim of "signed positive magnitude" was incorrect. |
| `accumulated_other_comprehensive_income` | `us-gaap:AccumulatedOtherComprehensiveIncomeLossNetOfTax` | `accumulatedOtherComprehensiveIncomeLoss` | verified | |
| `common_equity` | — | — | derived | subtotal computed in the mapper |
| `noncontrolling_interest` | `us-gaap:MinorityInterest` | `minorityInterest` | verified | |
| `total_equity` | `us-gaap:StockholdersEquityIncludingPortionAttributableToNoncontrollingInterest` | `totalEquity` | verified | |
| `total_liabilities_and_equity` | `us-gaap:LiabilitiesAndStockholdersEquity` | `totalLiabilitiesAndTotalEquity` | verified | |

### 5.3 Cash Flow (all cash-impact sign; no transforms)

| canonical | xbrl_concept | fmp_field | status | notes |
|---|---|---|---|---|
| `net_income_start` | `us-gaap:NetIncomeLoss` (same row as IS) | `netIncome` | verified | tie: `== is.net_income` |
| `dna_cf` | `us-gaap:DepreciationDepletionAndAmortization` | `depreciationAndAmortization` | verified | |
| `sbc` | `us-gaap:ShareBasedCompensation` OR `us-gaap:AllocatedShareBasedCompensationExpense` | `stockBasedCompensation` | verified | |
| `deferred_income_tax` | `us-gaap:DeferredIncomeTaxExpenseBenefit` | `deferredIncomeTax` | verified | |
| `gain_on_sale_assets_cf` | `us-gaap:GainLossOnSaleOfPropertyPlantEquipment` | — | seed | not separately in FMP CF response |
| `gain_on_sale_investments_cf` | `us-gaap:GainLossOnSalesOfAssetsAndAssetImpairmentCharges` | — | seed | same |
| `asset_writedown` | `us-gaap:AssetImpairmentCharges` | — | seed | not separately in FMP CF |
| `other_noncash` | (varies) | `otherNonCashItems` | verified | aggregation differs |
| `change_accounts_receivable` | `us-gaap:IncreaseDecreaseInAccountsReceivable` | `accountsReceivables` (in CF section) | verified | FMP uses cash-impact sign (confirmed on NVDA FY25 10-K and Q1) |
| `change_inventory` | `us-gaap:IncreaseDecreaseInInventories` | `inventory` (in CF section) | verified | |
| `change_accounts_payable` | `us-gaap:IncreaseDecreaseInAccountsPayable` | `accountsPayables` (in CF section) | verified | |
| `change_deferred_revenue` | `us-gaap:IncreaseDecreaseInContractWithCustomerLiability` | — | seed | may be bundled into `changeInWorkingCapital` |
| `change_other_working_capital` | — | `otherWorkingCapital` | verified | |
| `cfo` | `us-gaap:NetCashProvidedByUsedInOperatingActivities` | `netCashProvidedByOperatingActivities` | verified | alt alias `operatingCashFlow` in FMP; identical value |
| `capital_expenditures` | `us-gaap:PaymentsToAcquirePropertyPlantAndEquipment` | `investmentsInPropertyPlantAndEquipment` | verified | alt alias `capitalExpenditure`; cash-impact negative |
| `acquisitions` | `us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired` | `acquisitionsNet` | verified | |
| `purchases_of_investments` | `us-gaap:PaymentsToAcquireInvestments` | `purchasesOfInvestments` | verified | |
| `sales_of_investments` | `us-gaap:ProceedsFromSaleOfAvailableForSaleSecurities` OR `us-gaap:ProceedsFromSaleAndMaturityOfOtherInvestments` | `salesMaturitiesOfInvestments` | verified | |
| `other_investing` | — | `otherInvestingActivities` | verified | |
| `cfi` | `us-gaap:NetCashProvidedByUsedInInvestingActivities` | `netCashProvidedByInvestingActivities` | verified | |
| `short_term_debt_issuance` | `us-gaap:ProceedsFromShortTermDebt` | `shortTermNetDebtIssuance` | needs_check | FMP exposes net issuance; may bundle issuance and repayment |
| `long_term_debt_issuance` | `us-gaap:ProceedsFromIssuanceOfLongTermDebt` | `longTermNetDebtIssuance` | needs_check | same net-vs-gross ambiguity |
| `stock_issuance` | `us-gaap:ProceedsFromIssuanceOfCommonStock` | `commonStockIssuance` | verified | |
| `stock_repurchase` | `us-gaap:PaymentsForRepurchaseOfCommonStock` | `commonStockRepurchased` | verified | cash-impact negative |
| `common_dividends_paid` | `us-gaap:PaymentsOfDividendsCommonStock` | `commonDividendsPaid` | verified | cash-impact negative |
| `preferred_dividends_paid` | `us-gaap:PaymentsOfDividendsPreferredStockAndPreferenceStock` | `preferredDividendsPaid` | seed | zero for NVDA |
| `other_financing` | — | `otherFinancingActivities` | verified | |
| `cff` | `us-gaap:NetCashProvidedByUsedInFinancingActivities` | `netCashProvidedByFinancingActivities` | verified | |
| `fx_effect_on_cash` | `us-gaap:EffectOfExchangeRateOnCashAndCashEquivalents` | `effectOfForexChangesOnCash` | verified | |
| `misc_cf_adjustments` | — | — | seed | rare |
| `net_change_in_cash` | — | `netChangeInCash` | verified | subtotal |
| `cash_begin_of_period` | — | `cashAtBeginningOfPeriod` | verified | tie: `== bs.cash_and_equivalents[t-1]` |
| `cash_end_of_period` | — | `cashAtEndOfPeriod` | verified | tie: `== bs.cash_and_equivalents[t]` |

Additional FMP CF fields not mapped (disclosure-level):
- `incomeTaxesPaid`, `interestPaid`: supplementary disclosure, not part of CFO/CFI/CFF composition. Can be ingested as separate metric buckets later.
- `freeCashFlow`: FMP-computed = `operatingCashFlow + capitalExpenditure`. Do NOT store — our `formulas.md` computes FCF canonically.

---

## 6. Coverage Gaps (TO-DO)

Concepts in `concepts.md` that have **no reliable FMP source**, so must be left null from FMP ingest or sourced from SEC XBRL direct:

**Balance sheet:**
- `gross_ppe`, `accumulated_depreciation` (FMP only exposes net)
- `right_of_use_assets_operating` (ROU assets — not in FMP BS response as of the stable API)
- `equity_method_investments` (bundled into long_term_investments on FMP)
- `restricted_cash_current`
- `income_taxes_receivable_current`

**Income statement:**
- Unusual items (restructuring, goodwill_impairment, gain_sale_assets, gain_sale_investments) — FMP does not break out
- `equity_affiliates` — not explicitly in FMP IS response

**Cash flow:**
- `gain_on_sale_assets_cf`, `gain_on_sale_investments_cf`, `asset_writedown` — not separately in FMP CF; bundled into `otherNonCashItems`
- `change_deferred_revenue` — may be bundled into `changeInWorkingCapital`

These gaps become relevant when (a) we ingest a filer that has those items, or (b) a formula in `formulas.md` requires one of them. Either case triggers adding an SEC XBRL direct source (Build Order step ~19) to fill the gap.

For v1 FMP-only ingest, unmapped concepts are stored as NULL with `financial_facts.value = NULL` disallowed by the NOT NULL constraint — meaning the row is simply not written. The absence is auditable via the set of buckets the filer reports (XBRL) minus the set we wrote (FMP).

---

## 7. Empirical Sign Findings

From `data/exports/fmp_vs_archive_NVDA.csv` (17 sign flips out of 448 comparisons):

### 7.1 Cash flow working-capital / payment items (8 rows, 4 concepts × 2 periods)

| concept | archive | FMP | our canonical |
|---|---|---|---|
| `us-gaap:IncreaseDecreaseInAccountsReceivable` | positive magnitude | cash-impact (negative when AR↑) | cash-impact → **agrees with FMP** |
| `us-gaap:IncreaseDecreaseInInventories` | positive magnitude | cash-impact | cash-impact → **agrees with FMP** |
| `us-gaap:PaymentsForRepurchaseOfCommonStock` | positive magnitude | cash-impact (negative) | cash-impact → **agrees with FMP** |
| `us-gaap:PaymentsOfDividends` | positive magnitude | cash-impact (negative) | cash-impact → **agrees with FMP** |

**Resolution**: archive was storing raw XBRL magnitude without applying cash-impact sign. Our canonical matches FMP. **No transform needed on FMP; the archive gold CSV must be re-read with this in mind** when checking regression tests (see `verification.md` § regression tests).

### 7.2 `us-gaap:OtherNonoperatingIncomeExpense` (9 rows)

Not a sign issue per se — the XBRL concept refers to different filing lines in different NVDA periods. In older filings it tagged the small "Other, net" line (~-$15M); in recent filings it appears to tag a broader non-operating bucket (~-$1B+). Our mapping to FMP's `nonOperatingIncomeExcludingInterest` may be wrong.

**Resolution**: low-priority concept; values are small in most periods. Flag as `needs_check` in the mapping table. Revisit when we ingest filers where this item is material.

---

## 8. How to Extend This Doc

When a new canonical concept is added (via a concepts.md amendment or a new filer exposes something we don't cover):

1. Identify the XBRL concept(s) that populate it — use SEC's XBRL viewer or the archive JSONs.
2. Find the corresponding FMP field (if one exists) — search the response keys shown by `scripts/explore_fmp_vs_archive.py`.
3. Run the reconciliation on one or more filers; confirm values match across ≥1 period.
4. Add the row to the appropriate table in § 5 with `status = verified` (or `seed` / `needs_check` per confidence level).
5. Update the mapper in `src/arrow/normalize/financials/fmp_mapper.py` to emit the new bucket.
6. Add regression coverage — the archive JSONs are one source; extend to other tickers as gold data becomes available.

When FMP changes behavior:

1. Regression tests (`tests/regression/test_fmp_vs_archive_gold.py`) will break if values shift.
2. Update this doc first, then the mapper, then ensure the mapper passes regression tests against the new FMP output.
3. If the change is a sign flip, pause — sign-convention changes are load-bearing and warrant ADR.

---

## 9. What this doc does NOT do

- It does not normalize **across** filers. FMP is one vendor. Filer-specific quirks (e.g., NUE Q2 FY23 fiscal-period mis-tagging, Dell FY24 Q1/Q2 DEI errors, spin-offs) are handled at the periods layer (`periods.md`) and filer overrides in the normalize layer, not here.
- It does not specify **metric formulas**. Formulas are in `formulas.md` and compute on the canonical buckets.
- It does not specify **verification logic**. Subtotal ties, cross-statement invariants, tolerances, and failure modes live in `verification.md`.
- It does not specify **period logic**. YTD→discrete conversion, Q4 derivation, 52/53-week handling are all in `periods.md`.
