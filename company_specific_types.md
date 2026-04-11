# Company-Specific Extraction Issues

When adding a new company, the master extraction script (`extract.py`) will typically get 80-90% of components correct. The remaining issues require per-company overrides in `companies/{ticker}.py`.

This document catalogs the types of issues we have encountered across companies. When a new company has missing or incorrect values, check this list first — the fix pattern likely already exists.

## Issue Types

### 1. Alternate Concept Names

**Symptom:** Component returns null. The value exists in the filing but under a different XBRL concept than the master script expects.

**Example:** NVIDIA uses `PaymentsToAcquireProductiveAssets` for CapEx instead of `PaymentsToAcquirePropertyPlantAndEquipment`. The concept changed between fiscal years.

**Fix:** Add alternate concepts to the company override's `get_components()`. If the alternate is common across many companies, promote it to the master script's concept list.

### 2. Dimensioned Contexts

**Symptom:** Component returns 0 (default for balance sheet) but the golden expects a non-zero value. The value is in the XBRL but tagged with a dimension (segment), not as a non-dimensioned consolidated total.

**Example:** Dell FY2024 Q2-Q3 tags `AccountsReceivableNetCurrent` and `AccountsPayableCurrent` with `RelatedPartyTransactionsByRelatedPartyAxis`, splitting into `NonrelatedPartyMember` (trade) and `RelatedPartyMember` (related party). There is no non-dimensioned total. The master script only reads non-dimensioned contexts, so it finds nothing.

**Fix:** In the company override's `post_process()`, re-parse the XBRL and extract the value from the specific dimensioned context (e.g., `NonrelatedPartyMember`).

### 3. Multiple Line Items Requiring Summation

**Symptom:** Component value is too low. The filing has the amount split across multiple XBRL concepts that need to be summed.

**Example:** NVIDIA FY2026 Q4 has two acquisition lines — the standard `PaymentsToAcquireBusinessesNetOfCashAcquired` plus a separate Groq acquisition under `PaymentsToAcquireBusinessTwoNetOfCashAcquired`. Only the first is captured by the master script.

**Fix:** In the company override's `post_process()`, find and sum the additional concepts.

### 4. Concept Reclassification Between Periods

**Symptom:** Component works for some quarters but not others. The company changed how they tag a line item across fiscal years.

**Example:** NVIDIA changed CapEx concept from `PurchasesOfPropertyAndEquipmentAndIntangibleAssets` (FY2024) to `PaymentsToAcquireProductiveAssets` (FY2025+). Dell has `AccountsReceivableNetCurrent` as non-dimensioned in Q1 but dimensioned in Q2-Q4 of the same fiscal year.

**Fix:** Provide multiple concepts in priority order in `get_components()`, or handle per-period in `post_process()`.

### 5. Incorrect DEI Fiscal Period Tagging

**Symptom:** Quarters are mislabeled (e.g., Q1 and Q2 both show as Q4). The extraction log shows duplicate fiscal period assignments or impossible labels like a 10-Q mapped to Q4. Downstream, Q4 derivation breaks because the fiscal year grouping can't find the correct prior quarters.

**Example:** Dell FY2024 Q1 and Q2 (10-Qs for 2023-05-05 and 2023-08-04) have `DocumentFiscalPeriodFocus = FY` instead of `Q1`/`Q2`. The master script converts `FY` → `Q4` (correct for 10-Ks), so both filings get labeled Q4.

**Detection:** After extraction, check the log output for duplicate period assignments within a fiscal year, or 10-Q filings labeled as Q4. The DEI values can be inspected directly with `parse_dei()`.

**Fix:** In the company override, implement `fix_dei(dei, meta)` to correct the `DocumentFiscalPeriodFocus` value. Derive the correct quarter from the report date and `CurrentFiscalYearEndDate`. The master script calls `fix_dei` before using DEI values, so the correction flows through to all downstream logic.

### 6. Q4 Derivation with Restatements

**Symptom:** Q4 values are wrong by a consistent amount that matches the restatement delta. Q4 is derived as FY (from 10-K) minus 9M YTD (from Q3 10-Q). If the 10-K restated Q1-Q3 values but the 9M YTD comes from the original Q3 10-Q, the subtraction mixes restated and pre-restatement figures.

**Example:** Dell FY2025 Q4 — the 10-K (with `DocumentFinStmtErrorCorrectionFlag=true`) restated FY2024 and FY2025 Q1-Q3. The FY total in the 10-K reflects restated figures, but 9M YTD from the Q3 10-Q does not. Q4 = restated FY - original 9M = wrong.

**Fix:** In the company override's `post_process()`, compute Q4 using the restated quarterly values (FY - restated Q1 - restated Q2 - restated Q3) instead of the standard FY - 9M YTD derivation.

### 7. Spurious XBRL Tags (One-Time Tagging Artifacts)

**Symptom:** A component suddenly appears with a non-zero value for one or two periods when the company has never historically reported that line item, and subsequent filings revert to not tagging it.

**Example:** Palo Alto Networks (PANW) FY2025 Q4 10-K tags `InventoryNet` ($113.4M) for the first and only time across all filings. PANW is a software/services company with no inventory. No prior 10-Q or 10-K includes this concept, and the following FY2026 Q1 and Q2 10-Qs do not include it either. This is an XBRL tagging artifact, not a real balance.

**Detection:** A component jumps from 0 to a material value in a single period with no corresponding change in business model, then disappears. Check adjacent filings to confirm it's isolated.

**Fix:** In the company override's `post_process()`, zero out the value for the specific period(s). Scope the override narrowly (exact fiscal_year + fiscal_period) so that if future filings legitimately start reporting the line item, it flows through and gets flagged for review.

### 8. CF Line Items Broken Out vs Rolled Up

**Symptom:** A component (typically D&A) is too low because the company reports sub-components as separate cash flow reconciliation line items with their own XBRL concepts, rather than rolling them into a single line. The master script captures only the primary concept.

**Example:** Palo Alto Networks (PANW) breaks D&A into four separate CF lines: `DepreciationDepletionAndAmortization` (property/equipment), `CapitalizedContractCostAmortization` (deferred contract costs), `AmortizationOfFinancingCostsAndDiscounts` (debt issuance costs), and `AccretionAmortizationOfDiscountsAndPremiumsInvestments` (investment premiums, negative). The master script only captures the first, missing ~60% of total D&A.

**Important:** The same XBRL concept can mean different things depending on the company. Dell tags `CapitalizedContractCostAmortization` in a footnote disclosure, but rolls it into "Other, net" on the CF face — summing it would double-count. Always check the CF statement to confirm whether a concept is a separate line item or a footnote disclosure before adding it to a company's `get_components()`.

**Detection:** D&A (or another flow component) is materially lower than expected. Pull up the company's cash flow statement and count the D&A-related line items. Then search the XBRL for concepts containing "depreci" or "amortiz" on discrete/YTD duration contexts to find the matching concept names.

**Fix:** In the company override's `get_components()`, override the component with `sum_concepts: True` and list all concepts to sum. Use `negate_in_sum` for concepts with reversed sign conventions (e.g., investment premium amortization where positive XBRL = CF reduction). This is a company-specific fix, not a master fix, because the same XBRL concept may be a separate CF line for one company and a footnote disclosure for another.

### 9. Incorrect DEI Fiscal Year Tagging

**Symptom:** A quarter is missing from output while another quarter has wrong values. The extraction log shows a 10-K mapping to the wrong fiscal year (e.g., a 2024-12-31 10-K appearing as FY2023 Q4). The misassigned filing overwrites the correct quarter's data.

**Example:** FCX FY2024 10-K (accession `0000831259-25-000006`, period end 2024-12-31) has `DocumentFiscalYearFocus = 2023` instead of `2024`. The master script trusts this tag, so the filing maps to FY2023 Q4, overwriting the real FY2023 Q4 data from the actual 2023 10-K. FY2024 Q4 is missing entirely.

**Detection:** After extraction, check for missing quarters in the output. Cross-reference the extraction log — if a 10-K's `DocumentPeriodEndDate` doesn't match the fiscal year it was assigned to, the DEI tag is wrong. Also look for Q4 values that are wildly different from adjacent quarters.

**Fix:** In the company override, implement `fix_dei(dei, meta)` to correct `DocumentFiscalYearFocus` for the specific accession. Derive the correct year from `DocumentPeriodEndDate`.

### 10. Opposite Sign Convention on Alternate Concept

**Symptom:** A component extracts as 0 because the company uses an XBRL concept with the opposite sign convention from the master script's concepts. The master script's `negate` flag would double-negate the value if the alternate concept were simply added as a fallback.

**Example:** FCX uses `InterestIncomeExpenseNet` (negative in XBRL, -70M) instead of `InterestExpense` (positive in XBRL, 70M). The master script has `negate: True` on interest expense to flip positive XBRL values to negative. Adding `InterestIncomeExpenseNet` as a fallback would negate it to +70M — wrong sign. The override removes `negate` since the concept is already in the correct sign convention.

**Detection:** Component is 0 across all quarters. Search the XBRL for related concepts — if found under a different name, check whether its XBRL sign matches the golden convention or is opposite.

**Fix:** In the company override's `get_components()`, override the component definition with the alternate concept and adjust/remove the `negate` flag to match the concept's native sign convention.

### 11. 10-K vs 10-Q Concept Availability (Balance Sheet)

**Symptom:** A balance sheet component is correct in Q4 (from 10-K) but 0 in Q1-Q3 (from 10-Qs), or vice versa. The 10-K breaks out a sub-concept that the master script prefers, while the 10-Qs only report a combined concept.

**Example:** FCX 10-Ks tag both `AccountsPayableCurrent` (trade AP, 2,948M) and `AccountsPayableAndAccruedLiabilitiesCurrent` (combined, 4,565M). The 10-Qs only tag the combined concept. The master script picks `AccountsPayableCurrent` first (more specific), causing Q4 to use the narrower value while Q1-Q3 use the combined value. Similarly, FCX only discloses `OperatingLeaseLiabilityCurrent`/`OperatingLeaseLiabilityNoncurrent` in 10-Ks, not 10-Qs.

**Detection:** A balance sheet value drops or jumps sharply in Q4 relative to Q1-Q3, or is 0 in Q1-Q3 but non-zero in Q4. Check whether the 10-K has a more granular concept breakdown than the 10-Qs.

**Fix:** For concept preference issues, override `get_components()` in the company script to use the concept that's consistent across all filing types. For missing-in-10-Q issues, forward-fill in `calculate.py` (carries the most recent non-zero value into subsequent quarters).

### 12. Bad R&D Quarters (Spin-offs, IPOs, Reclassifications)

**Symptom:** R&D capitalization values are wrong because one or more quarterly R&D expense values in the 20-quarter amortization window are bad — negative, missing, or include divested business units.

**Example:** Dell FY2022 Q1-Q3 10-Qs include VMware R&D ($1.3-1.5B/quarter), but the 10-K FY2022 annual is post-VMware-spin ($2.577B). Q4 derivation (FY - 9M YTD) produces -$1.631B. Palantir FY2021 Q1 is missing entirely because the IPO was Sep 2020 and the 10-Q isn't in the fetch window.

**Detection:** R&D capitalization mismatches in eval. Check the quarterly R&D series in the output — look for negative values, sudden jumps/drops that don't match the filing narrative, or missing quarters.

**Fix:** In the company override, implement `fix_rd_series(quarterly_rd, records)`. This receives the R&D list and returns a corrected version. Can replace bad values with annual/4 estimates (Dell) or prepend missing quarters (Palantir). The fix runs before the amortization calculation in `compute.py`.
