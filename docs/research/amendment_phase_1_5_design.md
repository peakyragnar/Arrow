# Phase 1.5: Amendment-within-regular-filing handling — design spec

**Status:** Implemented (original design approved 2026-04-21, policy refined to soft-flag on 2026-04-22)

**Context:** `docs/research/amendment_handling_analysis.md` identified that FMP picks up formal `10-Q/A` amendments but **not** comparative-period restatements embedded in subsequent regular filings (10-K or later 10-Q). DELL FY24/FY25 and SYM FY25 Q1 both hit this pattern.

**Purpose:** Specify a deterministic, auditable mechanism that detects these restatements, supersedes FMP's stale values with authoritative SEC XBRL values when a clean resolution is possible, and **records a `data_quality_flags` row with full provenance for any unresolved anomaly** when it isn't.

**Important policy note (added 2026-04-22):** The original design had the agent raise `UnresolvableAmendment` on any resolution failure, rolling back the entire ingest. Empirical testing on DELL revealed that many real filers have legitimate filing-over-time inconsistencies where even manual human reconciliation doesn't tie — restatements documented at annual level only, vendor-internal inconsistencies that XBRL doesn't arbitrate, cash/restricted-cash classification drift. Blocking ingest on these makes the system unusable for the real filer universe. The agent was converted to write `data_quality_flags` rows rather than raise, so the data loads with visible annotations instead of being withheld. This matches the broader policy change documented in `docs/reference/verification.md`: Layers 2/3/5 are soft gates that flag-and-continue rather than hard gates that block.

---

## 1. The pattern being handled

Some filers restate prior-period values in a later regular filing (not a `/A` amendment):
- 10-K with restated comparative quarters (DELL FY25 10-K restating FY24 Q1-Q4)
- Subsequent 10-Q with restated prior-year-same-quarter comparative (SYM Q1 FY26 10-Q restating Q1 FY25)

The restated values appear in SEC's XBRL companyfacts as additional facts for the same `(concept, period_end, duration)` with a later `filed` date. FMP's pipeline does not detect these because they lack the `/A` suffix on the filing form.

The **tell-tale symptom** is Layer 3 failure: Q1+Q2+Q3+Q4 ≠ FY for the restated fiscal year. The Q values are FMP's originals; the FY value reflects the restated total from the later filing.

## 2. The analyst's mental process (the rigor we must match)

When an analyst sees Layer 3 fail:

1. **Diagnose the failure mode.** Amendment? Spinoff? FMP bug? Real error?
2. **Find the authoritative source** for any restated values (EDGAR search for later filings citing this period).
3. **Validate it's a genuine restatement** (not misread of a prior-year comparative, not a tagging error).
4. **Apply the restated values and check the math holds EVERYWHERE.** Not just Q+Q+Q+Q=FY, but also every intra-statement subtotal tie (`gross_profit == revenue - cogs`, balance sheet identity, CF roll-forward). If restating cogs means gross_profit must change too, both must be consistent.
5. **Verify I can point to the exact line in the exact SEC filing** showing each restated value.
6. **Document** the change and its source.

The automated system must do all six steps deterministically, with refusal-to-proceed whenever any step cannot be completed with certainty.

## 3. Core acceptance rules (the analyst's "I checked my work")

A supersession is accepted if and ONLY if **all** of these hold simultaneously:

### Rule A: XBRL provenance is explicit and authoritative

Every candidate superseding value must come from an SEC XBRL companyfacts entry matching:
- Canonical concept → XBRL `us-gaap:` tag (via `xbrl_concepts.py` mapping)
- Same `period_end` as the FMP row being superseded
- Same `duration` (3-month for quarter; 12-month for annual)
- `filed` date **strictly after** the FMP source's original filing date
- **Latest-filed** such value (if multiple restatements exist, only the most recent wins)

If no candidate meets all criteria → **no supersession attempted** for this concept-period.

### Rule B: Sanity bounds

A candidate fails admissibility (and is rejected, not applied) if:
- Sign flip without filer-reported sign-convention change (e.g., cogs flips positive to negative)
- Absolute delta > 50% of original value (suggests tagging error, not a real restatement)
- Value magnitude is outside the filer's recent range by >10× (explicit tagging/unit error sanity)

If any single candidate fails Rule B → **the entire fiscal-year supersession is rejected** (don't partially trust a source with one sanity-violated value).

### Rule C: Post-supersession verification (Layer 1 only; savepoint rollback on failure)

After applying all admissible candidates for a fiscal year, the agent checks **Layer 1 subtotal ties** on every superseded period (gross_profit, operating_income, continuing_ops_after_tax, net_income, parent-NI, BS balance identity, CF subtotal + cash roll-forward).

If Layer 1 post-supersession fails → **the savepoint containing the supersessions is rolled back**. FMP's original values remain authoritative, and the agent writes `data_quality_flags` rows (one per Layer 3 failure) documenting what was attempted and why it couldn't be applied. Ingest continues past the amendment phase.

If Layer 1 post-supersession passes → savepoint commits. If any Layer 3 residuals remain (concepts where XBRL had no newer value, or where the filer restated annual-only without re-tagging quarters), flags are written for those residuals.

**Policy note:** The original design also required post-supersession Layer 2 and Layer 3 to pass; failures raised `UnresolvableAmendment` and rolled back the entire ingest. Empirical testing (DELL FY24 SG&A where the 10-K restated annual but not quarters; FMP IS-vs-CF vendor-internal inconsistencies that XBRL doesn't arbitrate) showed that many real filers can never satisfy this standard. The policy was relaxed to Layer-1-only post-check — Layer 1 catches genuinely-inconsistent data within a single filing period, while Layer 2/3 residuals are written as flags rather than blocking. See the revised Rule F below.

### Rule D: Full provenance

Every superseded row persisted to `financial_facts` carries:
- `source_raw_response_id` → the XBRL companyfacts fetch (already stored)
- `extraction_version = 'xbrl-amendment-v1'`
- `supersedes_fact_id` → FK to the superseded row (new column, see § 7)
- `supersession_reason` → human-readable (e.g., `"XBRL fact from accn 0001571996-25-000034 10-K filed 2025-03-25 reports $15,842M; supersedes FMP value $15,904M from 10-Q accn 0001571996-23-000019 filed 2023-06-12"`)

The original row remains in the table with `superseded_at` timestamp set. Historical queries return both rows.

### Rule E: Deterministic reproducibility

Given the same FMP raw_response + same SEC XBRL companyfacts response, the supersession output is **byte-identical** across runs. Specifically:
- No random tie-breakers (lexicographic ordering when needed)
- No time-of-day behavior (the only "time" used is XBRL's `filed` field)
- Regression tests assert identical JSON output on replay

### Rule F: Categorical outcomes (REVISED — soft-flag policy)

The agent returns an `AmendmentResult` with one of four statuses:

| Status | Meaning | DB state after |
|---|---|---|
| **clean** | No Layer 3 failures; agent not invoked or nothing to do | FMP values stored as loaded |
| **amended** | Agent found candidates, applied all, Layer 1 post-check passed, no Layer 3 residuals | Original + restated rows persisted with full provenance; no flags |
| **partially_amended** | Supersessions applied and committed, but some Layer 3 residuals remain (XBRL didn't have values for every failing concept) | Supersessions persisted for resolved concepts; flags written for residuals |
| **unresolved_flagged** | No candidates found, OR supersession would break Layer 1 (savepoint rolled back) | FMP values retained as-is; flags written for every Layer 3 failure |

The agent **never raises** under this policy. Ingest always continues past the amendment phase. Data loads.

**Layer 1 remains a hard gate** — but it runs inline during `load_fmp_*_rows`, not inside the amendment agent. If FMP's own per-filing subtotal math is broken, that raises during the mapping step before the amendment agent is ever invoked.

**At no point does data get silently corrupted.** Every supersession has full provenance (Rule D). Every unresolved anomaly has a `data_quality_flags` row with the failing values, the expected values, and a human-readable reason. The analyst sees everything.

## 4. Algorithm

```python
def detect_and_apply_amendments(conn, company, fiscal_year, xbrl_facts):
    # 1. Identify Layer 3 failures for this (company, fiscal_year)
    failures = run_layer3(conn, company, fiscal_year)
    if not failures:
        return AmendmentResult.CLEAN  # nothing to do

    # 2. For each (statement, concept, quarter) in a failing fiscal year,
    #    look for a later-filed XBRL value with the same (concept, period_end, duration).
    candidates = []
    for (statement, concept, quarter_period_end) in affected_quarters(failures):
        fmp_row = fetch_stored_fmp_fact(conn, company, concept, quarter_period_end)
        xbrl_candidate = find_xbrl_supersession_candidate(
            xbrl_facts,
            concept=concept,
            period_end=quarter_period_end,
            duration=(80, 100),  # 3-month
            must_be_filed_after=fmp_row.source_filed_date,
        )
        if xbrl_candidate is None:
            continue  # no superseding value — leave FMP row alone
        if xbrl_candidate.val == fmp_row.value:
            continue  # identical value, no restatement, skip
        candidates.append((fmp_row, xbrl_candidate))

    # 3. Rule B sanity checks on every candidate
    for fmp_row, xbrl in candidates:
        if not passes_sanity_bounds(fmp_row.value, xbrl.val):
            return AmendmentResult.UNRESOLVABLE(
                reason=f"sanity bound violated for {concept} @ {period_end}: "
                       f"FMP={fmp_row.value}, XBRL={xbrl.val}"
            )

    # 4. Apply all supersessions atomically in a SAVEPOINT
    with conn.transaction() as sp:
        for fmp_row, xbrl in candidates:
            insert_superseding_fact(conn, fmp_row, xbrl, reason=...)
            # Partial unique index automatically sets superseded_at on fmp_row

        # 5. Rule C: re-run ALL layers on all affected periods
        layer1_ok = re_verify_layer_1(conn, company, fiscal_year)
        layer2_ok = re_verify_layer_2(conn, company, fiscal_year)
        layer3_ok = re_verify_layer_3(conn, company, fiscal_year)
        if not (layer1_ok and layer2_ok and layer3_ok):
            sp.rollback()
            return AmendmentResult.UNRESOLVABLE(
                reason="post-supersession Layer 1/2/3 still fails — partial restatement"
            )
        return AmendmentResult.AMENDED(supersessions=candidates)
```

## 5. Schema changes required

Two new optional columns on `financial_facts`:

```sql
ALTER TABLE financial_facts ADD COLUMN supersedes_fact_id BIGINT
    REFERENCES financial_facts(id) ON DELETE RESTRICT;
ALTER TABLE financial_facts ADD COLUMN supersession_reason TEXT;
```

Both are NULL on normal ingest rows; populated only on XBRL-amendment rows. The FK preserves audit history (can't delete a row that a supersession points to).

Existing `superseded_at` timestamp already exists and is automatically set by the partial unique index `financial_facts_one_current_idx` when the superseding row is inserted.

## 6. What this agent does NOT do

- **Does not handle spinoffs / discontinued-ops reclassifications.** These fail Layer 3 but have no XBRL supersession candidates. Rule A correctly finds none. The system raises `UnresolvableAmendment`, and a separate declarative mechanism (per-filer × fiscal-year exemption list in a config, not code) handles those.
- **Does not modify FMP's original row.** FMP's value stays exactly as FMP returned it. We add a new row that supersedes it. The supersession is visible; it doesn't overwrite history.
- **Does not guess.** If any of Rules A-E fails, the entire supersession is rolled back.
- **Does not rely on statistical inference or fuzzy matching.** Every supersession has a specific XBRL fact provenance traceable to an exact accession number.

## 7. Test acceptance suite (the validation gate)

Phase 1.5 is not considered complete until every test below passes. These are regression tests that run on every change to the amendment agent.

### T1: DELL-FY24-restatement
- **Given:** DELL seeded + FMP ingest run (Layer 3 fails on FY24).
- **When:** amendment agent runs.
- **Then:**
  - Exactly 16 supersessions applied across 4 quarters × 4 IS concepts (cogs, gross_profit, operating_income, net_income) — or however many concepts the 10-K actually restated.
  - Each superseded value asserts **==** the golden_eval restatements-sheet restated value (exact, no tolerance).
  - Each superseded row has `source_accn = 0001571996-25-000034` (the FY25 10-K).
  - Post-supersession: Layer 1, Layer 2, Layer 3 all pass.
  - `financial_facts` contains both original (superseded_at IS NOT NULL) and amended (current) rows.

### T2: SYM-FY24-10QA-passthrough
- **Given:** SYM seeded + FMP ingest run (10-Q/A amendments already in FMP data).
- **When:** amendment agent runs.
- **Then:**
  - Layer 3 on FY24 passed without amendment agent intervention. Zero supersessions.
  - Demonstrates the agent correctly does nothing when FMP already handled it.

### T3: SYM-FY25-partial-unresolvable
- **Given:** SYM seeded + FMP ingest run (FY25 Q1 has XBRL restatement; Q2/Q3 don't yet have comparative restatements in XBRL).
- **When:** amendment agent runs.
- **Then:**
  - Q1 FY25 has a supersession candidate.
  - Q2/Q3 FY25 do not.
  - Post-hypothetical-supersession of Q1 only, Layer 3 on FY25 **still fails** (because Q2/Q3 deltas remain).
  - Rule C fires: supersession rolled back.
  - `UnresolvableAmendment` raised with clear diagnostic.
  - DB state: nothing persisted for FY25.

### T4: NVDA-clean
- **Given:** NVDA seeded + FMP ingest (Layer 3 passes without intervention).
- **When:** amendment agent runs.
- **Then:** Zero supersessions. No `financial_facts` rows with `extraction_version='xbrl-amendment-v1'`.

### T5: Sanity-bound-violation (synthetic)
- **Given:** A candidate with a 90% change (obviously a tagging error).
- **When:** amendment agent runs.
- **Then:** Rule B fires. `UnresolvableAmendment` raised. No supersession committed.

### T6: Determinism
- **Given:** Same DB state + same cached XBRL response.
- **When:** amendment agent run twice in succession.
- **Then:** Second run is a no-op (everything already superseded). Byte-identical result set.

### T7: Post-Layer-1-break (synthetic)
- **Given:** An amendment that would supersede cogs but NOT gross_profit for the same period, creating an intra-statement inconsistency.
- **When:** amendment agent runs.
- **Then:** Layer 1 re-verify fails. Supersession rolled back. `UnresolvableAmendment` raised.

Each test has fixtures with exact expected values from golden_eval. Not written until the agent is built, but the outline is locked.

## 8. Deployment sequence

1. Add schema columns (`supersedes_fact_id`, `supersession_reason`) via migration.
2. Build `src/arrow/agents/amendment_detect.py`.
3. Write tests T1-T7 as `tests/integration/test_amendment_detect.py`.
4. Wire into `backfill_fmp_statements`: on `PeriodArithmeticViolation` (Layer 3), invoke amendment agent; on success continue; on `UnresolvableAmendment` raise as before.
5. Run DELL end-to-end: expect `Amended` outcome.
6. Run SYM end-to-end: expect `Amended` on FY24, `UnresolvableAmendment` on FY25 (correct per rule C — until Q2/Q3 FY26 10-Qs are filed with comparatives).
7. Run NVDA regression: unchanged outcome.

## 9. Ongoing maintenance

- Every new filer that hits `UnresolvableAmendment` is a flag for human review. Over time, a small declarative "known reclassifications" list (YAML) handles the spinoff/discontinued-ops cases.
- The amendment agent runs on every backfill. If a filer adds a new comparative-period restatement in a later 10-Q, the agent picks it up automatically on the next ingest.
- The golden_eval test suite is the regression harness. Any change to supersession logic that breaks golden_eval values fails CI before merging.

## 10. Open questions to resolve during implementation

- **Q:** Should we also supersede BS instant facts (not just IS/CF flow facts)? BS restatements are rarer but possible.
  **A:** Yes — add BS handling in the same pattern. The `_find_xbrl_instant_fact` machinery already exists in `reconcile/fmp_vs_xbrl.py`.

- **Q:** What if multiple restatements chain (e.g., Q1 restated in a 10-K, then re-restated in a later 10-K)?
  **A:** Rule A's "latest-filed" criterion handles this. The supersession always picks the most recent authoritative value.

- **Q:** What if FMP itself later picks up the restated values (FMP's pipeline updates)?
  **A:** The partial unique index handles it automatically. FMP's new value supersedes the XBRL-amendment value (via a new normal ingest), and the chain is visible in history: original FMP → XBRL amendment → updated FMP. Each with provenance.
