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

### 5. Q4 Derivation with Restatements

**Symptom:** Q4 values are wrong by a consistent amount that matches the restatement delta. Q4 is derived as FY (from 10-K) minus 9M YTD (from Q3 10-Q). If the 10-K restated Q1-Q3 values but the 9M YTD comes from the original Q3 10-Q, the subtraction mixes restated and pre-restatement figures.

**Example:** Dell FY2025 Q4 — the 10-K (with `DocumentFinStmtErrorCorrectionFlag=true`) restated FY2024 and FY2025 Q1-Q3. The FY total in the 10-K reflects restated figures, but 9M YTD from the Q3 10-Q does not. Q4 = restated FY - original 9M = wrong.

**Fix:** In the company override's `post_process()`, compute Q4 using the restated quarterly values (FY - restated Q1 - restated Q2 - restated Q3) instead of the standard FY - 9M YTD derivation.
