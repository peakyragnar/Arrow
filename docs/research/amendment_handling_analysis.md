# Amendment handling analysis — DELL in-10-K restatement case

**Date:** 2026-04-21
**Trigger:** DELL backfill fails Layer 3 (Q1+Q2+Q3+Q4 = FY) on FY2024 and FY2025 due to unannounced restatement in FY25 10-K (accn `0001571996-25-000034`, filed 2025-03-25).
**Purpose:** Document the exact behavior, what FMP serves, what XBRL serves, and what Arrow does today. This is the foundational reference for designing "amendment detect + XBRL supersede" (Phase 1.5).

## 1. The restatement

DELL's FY25 10-K contained restated values for **all 8 quarters** covering FY24 + FY25, WITHOUT filing formal 10-Q/A amendments. Per `docs/benchmarks/golden_eval.xlsx` restatements sheet:

| Period (period_end) | Original | Restated | Δ cogs |
|---|---|---|---|
| Q1 FY24 (2023-05-05) | cogs 15,904 / NI 578 | cogs 15,842 / NI 632 | −62 |
| Q2 FY24 (2023-08-04) | cogs 17,547 / NI 455 | cogs 17,518 / NI 482 | −29 |
| Q3 FY24 (2023-11-03) | cogs 17,103 / NI 1,004 | cogs 17,050 / NI 1,050 | −53 |
| Q4 FY24 (2024-02-02) | cogs 17,002 / NI 1,158 | cogs 16,946 / NI 1,208 | −56 |
| Q1 FY25 (2024-05-03) | cogs 17,438 / NI 955 | cogs 17,393 / NI 992 | −45 |
| Q2 FY25 (2024-08-02) | cogs 19,715 / NI 841 | cogs 19,665 / NI 882 | −50 |
| Q3 FY25 (2024-11-01) | cogs 19,059 / NI 1,127 | cogs 19,006 / NI 1,170 | −53 |
| Q4 FY25 (2025-01-31) | cogs 18,105 / NI 1,653 | cogs 18,253 / NI 1,532 | +148 |

Total FY24 cogs restatement: −$200M. Total FY25 cogs restatement: ~+$0M net (Q4 counteracts Q1-Q3).

**The restatement form matters:** It's embedded in a regular 10-K, NOT a 10-K/A or 10-Q/A. Filers can do this legally if the amendments are within GAAP materiality / MDA disclosure thresholds.

## 2. What FMP serves us

FMP's IS endpoint for DELL returns:
- Q1/Q2/Q3 FY24 at the ORIGINAL 10-Q values (not restated)
- Q4 FY24 at the FY24 10-K value (original at time of that filing)
- FY24 annual total at the FY25-10-K-RESTATED value

Same pattern for FY25. FMP never backpropagated the restatement to the quarterly rows.

Empirical verification (DELL Q3 FY25 2024-11-01):
- FMP IS endpoint `netIncome` = **$1,132M** (original, pre-restatement)
- XBRL `NetIncomeLoss` from 10-Q filed 2024-12-10 = $1,132M (original)
- XBRL `NetIncomeLoss` from 10-K filed 2025-03-25 (accn `0001571996-25-000034`) = **$1,175M** (restated)

FMP serves the 10-Q value, not the 10-K restated value.

## 3. What SEC XBRL companyfacts actually contains

Both sets of values, tagged with different accession numbers. Example:

```
CostOfRevenue @ end=2023-05-05 (DELL Q1 FY24):
  val=$15,904M  accn=0001571996-23-000019  form=10-Q  fp=Q1  filed=2023-06-12  ORIGINAL
  val=$15,842M  accn=0001571996-25-000034  form=10-K  fp=FY  filed=2025-03-25  RESTATED
     (start=2023-02-04, end=2023-05-05, 90-day span — same window, later filing)
```

The restated values are filed as `form=10-K`, `fp=FY` (because the filing form is annual), but with start/end dates defining the 3-month window. Layer 5's `_find_xbrl_fact` matches on (end, duration) with "latest-filed wins" — it would pick the restated value correctly.

## 4. What Arrow's ingest does today

1. **Layer 1 (subtotal ties):** Passes. Each period's internal math is consistent with itself.
2. **Layer 1 BS, CF:** Passes.
3. **Layer 3 (Q1+Q2+Q3+Q4 = FY):** **Fails** by the restatement delta. For DELL FY24 cogs: Q-sum (all originals) = $67,556M vs FY (restated) = $67,356M, delta = $200M.
4. **Outcome:** `PeriodArithmeticViolation` raised, transaction rolls back, **nothing is stored for DELL**.

Arrow correctly identifies that a restatement exists and refuses to load inconsistent data. No partial state is persisted.

## 5. What's NOT happening (the gap)

The supersession infrastructure in the schema (`superseded_at` column + `financial_facts_one_current_idx` partial unique index) exists and is designed for exactly this scenario — mark old values superseded when newer authoritative values arrive. But:

- **No producer writes supersession events from XBRL evidence.** The Layer 5 XBRL fetch happens, but only for top-line anchors (revenue, net_income, etc.) and only as a cross-check — not as a source of overriding values.
- **FMP is the only data source feeding `financial_facts`.** When FMP's Q1 FY24 value doesn't match its own FY24 total, nothing reaches into XBRL to say "the restated Q1 is $15,842M, use that instead."
- **The system has no concept of "this quarter was amended."** No `amended_by_accn` column, no amendment log.

The architectural scaffolding for amendment awareness simply isn't wired.

## 6. Three-way distinction Layer 3 currently conflates

Layer 3 catches three fundamentally different kinds of Q-sum ≠ FY inconsistency, and can't distinguish them:

| Kind | Cause | Resolution path |
|---|---|---|
| **Amendment restatement** | Filer restated prior quarters in a later 10-K. Restated values exist in XBRL. | Fetch restated values from XBRL, supersede FMP quarterly rows. After supersession, Q-sum should tie FY. |
| **Spinoff / discontinued-ops reclassification** | Filer spun off a subsidiary; earlier quarters include the spinoff, FY reports continuing ops only. No "restated quarterly" values exist because the original quarters correctly included the discontinued ops. | No arithmetic reconciliation possible. Needs explicit flag per (filer, fiscal_year) to downgrade Layer 3 from hard-fail to warning. |
| **Genuine FMP data bug** | FMP normalization failed for a specific period (like DELL Q2 FY26 $146M delta on restricted cash). | Per-period exclusion or wait for FMP to republish. |

Without distinguishing these, Layer 3 is a correct but blunt gate.

## 7. Proposed resolution (Phase 1.5 — Amendment Detect + Supersede)

A new agent `src/arrow/agents/amendment_detect.py` that, on Layer 3 failure:

1. For each failing `(statement, concept, fiscal_year)` triple, identify which quarters contributed.
2. Fetch SEC XBRL companyfacts (already done in Layer 5 — reuse).
3. For each stored FMP quarterly value, look for an XBRL fact at the same `period_end` with:
   - Same canonical concept (via `xbrl_concepts.py` mapping)
   - 3-month duration (80-100 day span)
   - A later `filed` date than the original FMP source's filing (i.e., a restatement)
4. If a later-filed XBRL value exists and differs from FMP's stored value:
   - Insert new `financial_facts` row with `extraction_version='xbrl-amendment-v1'` and `source_raw_response_id` pointing to the XBRL fetch
   - Partial unique index automatically supersedes the FMP original (sets `superseded_at`)
5. Re-run Layer 3 for the fiscal year.
6. If tie now holds → amendment fully resolved → continue ingest.
7. If tie still fails → not an amendment (spinoff or bug) → raise `NonReconcilableViolation` with diagnostics.

## 8. Test cases for Phase 1.5 validation

Against golden_eval restatements sheet:

**DELL (in-10-K restatement, no 10-Q/A):**
- 8 quarters × multiple concepts = ~40 individual restatement events
- Post-supersession, FY24 and FY25 Layer 3 should tie
- Cross-check: each superseded value should equal the golden_eval restated row

**SYM (formal 10-Q/A amendments):**
- Q1-Q3 FY24 each filed a formal 10-Q/A in Dec 2024 with restated values
- Test: does FMP pick up 10-Q/A amendments more automatically than in-10-K restatements? (Hypothesis: maybe — to be tested.)

## 9. What we are deliberately not doing

- **Not writing per-filer Python**. If Phase 1.5 needs per-filer tuning (e.g., "for DELL, always prefer 10-K values over 10-Q for quarters"), that's a declarative rule in a config, not a `companies/dell.py` file.
- **Not trusting FMP for amendments.** When Layer 3 fails and supersession is attempted, XBRL is the authority for the corrected values.
- **Not suppressing Layer 3.** The hard-fail-on-inconsistency behavior is load-bearing — it's what caught the DELL restatement in the first place. The amendment agent runs when Layer 3 fails and RESOLVES the failure; it doesn't replace Layer 3.

## 10. Empirical FMP behavior observations

### 10.1 FMP handles formal 10-Q/A automatically; in-10-K restatements it does not

This is the central empirical finding. FMP's amendment-pickup behavior depends on **HOW** the filer issued the restatement:

| Filer | Amendment form | Original Q value in FMP | Restated Q value in FMP | Layer 3 outcome |
|---|---|---|---|---|
| DELL FY24 Q1 (cogs) | In-10-K restatement (FY25 10-K, no 10-Q/A) | $15,904M served | — (never served) | **FAIL** — Q-sum(original) ≠ FY(restated) |
| SYM FY24 Q1 (revenue) | Formal 10-Q/A filed 2024-12-04 | — (replaced) | $360M served (= 10-Q/A) | **PASS** — Q-sum and FY both reflect amended values |

Verified directly on Apr 2026 FMP API call:
- SYM FY24 Q1: FMP revenue = $360M = 10-Q/A value (original was $368M)
- SYM FY24 Q2: FMP revenue = $393M = 10-Q/A (original $424M)
- SYM FY24 Q3: FMP revenue = $470M = 10-Q/A (original $492M)
- All three match the amended 10-Q/A values exactly.

Meanwhile DELL (in-10-K only):
- DELL FY24 Q1: FMP cogs = $15,904M = original 10-Q value (restated was $15,842M)
- All 8 DELL-FY24/FY25 restated quarters served original, not restated.

### 10.2 Why the difference

FMP's data pipeline likely monitors EDGAR for new filings. Detecting a `10-Q/A` is trivial — the form type suffix explicitly announces an amendment. Detecting an in-10-K restatement requires:
1. Parsing the 10-K's prior-period comparatives
2. Matching them to the original 10-Q period_ends
3. Comparing values
4. Updating the stored quarterly records

This is materially more work for FMP's pipeline and evidently isn't part of their current coverage.

### 10.3 Practical implications for Arrow's amendment handling

The two cases need different handling:

**Formal 10-Q/A (the SYM case):**
- FMP already handles it upstream
- Arrow just stores FMP's current output
- Layer 3 passes
- **No Arrow code needed.**

**In-10-K restatement (the DELL case):**
- FMP does not handle it
- Arrow must detect (Layer 3 catches) and RESOLVE (via XBRL supersede)
- Phase 1.5 amendment-detect agent is needed
- Without this, DELL and any similar filer cannot be ingested

This means the "amendment detect + XBRL supersede" feature is specifically for the in-10-K restatement pattern. It's a narrower feature than initially framed — we don't need to handle formal 10-Q/A at all (FMP does it), only the in-10-K case.

### 10.4 SYM FY2025 residuals — SAME pattern as DELL

Initial hypothesis (SYM FY25 residual was unrelated to amendments) was WRONG. The Q1 FY26 10-Q filed 2026-02-04 (accn `0001837240-26-000009`) contains restated comparative Q1 FY25 values:

| Concept | Q1 FY25 original (10-Q accn `...25-000044`) | Q1 FY25 restated (Q1 FY26 10-Q comparative) | FMP serves |
|---|---|---|---|
| cogs | $406.7M | $405.7M | $406.7M (original) |
| operating_income | −$24.6M | −$23.0M | −$24.6M (original) |
| rd | $43.6M | $43.3M | $43.6M (original) |

Back-solving from the FY25 10-K total ($1,824.3M cogs): if Q1 restated is $405.7M, then Q2+Q3+Q4 restated must sum to $1,418.6M. FMP's Q2+Q3+Q4 = $1,427.3M. So Q2/Q3/Q4 were ALSO restated by a combined $8.7M (values not yet visible in XBRL — will surface when Q2/Q3 FY26 10-Qs are filed).

Total restatement = $9.7M ≈ Layer 3's observed $9.6M delta. Full match.

### 10.5 Unified FMP amendment-handling rule

The correct generalized statement of FMP's behavior:

| Restatement mechanism | FMP picks up? |
|---|---|
| Formal `10-Q/A` filing | **Yes** — values replaced automatically |
| In-10-K comparative period restatement (no 10-Q/A) | **No** |
| In-later-10-Q comparative period restatement | **No** |

**FMP only picks up formal `/A` amendment filings.** Any comparative-period restatement embedded in a subsequent non-`/A` filing (whether 10-K or subsequent 10-Q) is invisible to FMP's ingest pipeline.

This collapses the two cases I originally thought were distinct (DELL "in-10-K" vs SYM "in-later-10-Q") into one pattern. The Phase 1.5 fix handles both: detect Layer 3 failure → fetch XBRL → find later-filed values for the failing quarters → supersede.

### 10.6 Implications

- **Phase 1.5 scope is slightly broader than "in-10-K restatement"**: it's "any comparative-period restatement in a later filing." Mechanically identical — look for later-filed XBRL facts at the same (concept, period_end) with 3-month span.
- **Test cases expand**: DELL gives us ~40 concept-period restatement pairs across FY24 + FY25 + FY26. SYM gives us ~5-10 more across FY25 Q1 (Q2/Q3 will surface in future 10-Qs). Both filers' Layer 3 failures should resolve to zero after supersession.
- **An amendment-detect agent that catches this will proactively protect us** against any filer doing comparative restatements in future filings.

### 10.7 Remaining investigation questions

- Do all 20 filers in golden_eval with formal 10-Q/A amendments pass Layer 3? (Need to scan EDGAR for 10-Q/A filings across the set — SYM FY24 Q1-Q3 was the explicit test.)
- How many of the other 18 tickers have comparative-period restatements (the broader Phase 1.5 target)?
- Does FMP occasionally miss a formal 10-Q/A? Edge case worth checking on one ticker manually.

### 10.8 Update (2026-04-22): further finding — Layer 2 / Layer 5 inconsistencies too

Empirical DELL testing revealed that amendment detection at Layer 3 is not sufficient to cleanly ingest DELL. Three additional classes of disagreement surface that are NOT amendments and can't be resolved by supersession:

1. **FMP internal inconsistency (Layer 2)**: FMP's IS endpoint and CF endpoint return different values for what should be the same concept at the same period. For DELL Q2 FY25, FMP's IS returns `netIncomeFromContinuingOperations = $841M` while FMP's CF returns `netIncome = $804M`. Both should be pre-NCI consolidated NI. Neither matches XBRL cleanly — this is FMP's bug, not a filer restatement.

2. **Cash/restricted-cash classification drift (Layer 2)**: `cf.cash_end_of_period ≠ bs.cash + xbrl.restricted_cash` by $141M-$287M on several DELL periods. Real-world reason: FMP's BS cash figure, CF cash_end figure, and SEC XBRL's restricted cash tag don't line up cleanly for DELL's specific filings.

3. **FMP-vs-SEC divergence at anchor level (Layer 5)**: for ~30 of DELL's stored values, FMP disagrees with SEC XBRL's latest-filed value. Usually because FMP didn't propagate a comparative-period restatement from a later filing.

None of these are recoverable via XBRL supersession (the supersession would break Layer 1 or the underlying data is genuinely inconsistent). The response chosen: **soften Layers 2, 3, and 5 to flag-and-continue rather than hard-block**. Layer 1 remains hard (catches genuine per-filing internal math violations). See `docs/reference/verification.md` § 1 for the updated policy.

Outcome on DELL FY23-FY25: 1,335 facts loaded into `financial_facts`; 74 flags written to `data_quality_flags` (24 Layer 3, 17 Layer 2, 30 Layer 5, 3 sanity). All anomalies visible, provenance complete, analyst can filter/resolve as their specific analysis requires.
