# Layer 2 Prompt — Calculation Component Verification

This is the second layer of the extraction prompt. After extracting and verifying the three financial statements, the AI must search the ENTIRE filing to ensure all components needed for downstream calculations are captured completely.

---

## LAYER 2 — CALCULATION COMPONENT VERIFICATION

After extracting the three financial statements, you must now verify that all components needed for the following calculations are captured. For each component below, search the statement face, notes, supplemental disclosures, and dimensioned XBRL facts. Do not assume a component is absent — actively look for it.

Add a "calculation_components" section to your JSON output:

### 1. Operating Lease Liabilities (for Invested Capital, Net Debt)
Search for BOTH current and non-current operating lease liabilities.
- They may appear as separate balance sheet line items (easy case).
- Current portion is often HIDDEN inside "Accrued and other current liabilities" or "Other current liabilities." Check the notes for the breakout.
- Some companies only disclose operating leases in the 10-K, not 10-Qs. If this is a 10-Q and you cannot find them, flag it.
- Some companies tag a total `OperatingLeaseLiability` in addition to the split. If the total exists, use it. If only the split exists, sum current + non-current.
- Check for the XBRL extensible list tag `OperatingLeaseLiabilityCurrentStatementOfFinancialPositionExtensibleList` — this tells you WHERE the current portion is classified on the balance sheet.
```json
"operating_leases": {
  "current": 300,
  "noncurrent": 1521,
  "total": 1821,
  "current_location": "Included in Accrued and other current liabilities per Note 8",
  "found_in_10q": true
}
```

### 2. Depreciation and Amortization (for EBITDA, Reinvestment Rate)
D&A may be ONE line on the cash flow statement, or it may be SPLIT across multiple lines:
- `Depreciation and amortization` (PP&E)
- `Amortization of intangible assets` (acquired intangibles — often a separate CF line)
- `Amortization of debt issuance costs` (financing-related)
- `Capitalized contract cost amortization` (deferred contract costs)
- `Depletion` (mining/resource companies — included in D&A for these industries)
- `Accretion/amortization of investment premiums` (may be negative)

CRITICAL: Check the cash flow statement for ALL lines containing "depreci", "amortiz", or "deplet". Report each separately and provide the total. Also check the notes for D&A breakdowns that may differ from the CF presentation.

Do NOT assume the single CF line is the complete D&A. Companies routinely break it out.
```json
"depreciation_amortization": {
  "cf_depreciation_and_amortization": 611,
  "cf_amortization_intangibles": null,
  "cf_amortization_debt_costs": null,
  "cf_other_amortization": null,
  "cf_depletion": null,
  "total_da": 611,
  "note_breakdown": "Note 7: intangible amortization $159M (subset of CF D&A line)",
  "is_single_line": true
}
```

### 3. Accounts Payable (for DPO, Working Capital, CCC)
AP must be PURE trade accounts payable, not combined with accrued liabilities.
- If the balance sheet shows "Accounts payable" as a separate line — use it.
- If the balance sheet shows "Accounts payable and accrued liabilities" as a COMBINED line — you MUST find the pure AP in the notes. Check Note 8 or similar balance sheet component notes.
- Some companies use dimensioned contexts: AP split by `RelatedPartyMember` and `NonrelatedPartyMember`. If so, use the non-related-party value for trade AP.
- Flag whether AP is pure or combined so downstream knows.
```json
"accounts_payable": {
  "value": 7331,
  "is_pure": true,
  "combined_with": null,
  "note_breakout": null
}
```

### 4. Accounts Receivable (for DSO, Working Capital, CCC)
Same issue as AP — must be pure trade AR.
- If combined with other receivables, find the pure AR in the notes.
- Check for dimensioned contexts (related vs non-related party splits).
```json
"accounts_receivable": {
  "value": 22132,
  "is_pure": true,
  "combined_with": null,
  "note_breakout": null
}
```

### 5. CapEx (for FCF, Reinvestment Rate)
Capital expenditures on property, plant, equipment, and intangible assets.
- Usually "Purchases of property and equipment" on investing activities.
- Some companies use different concept names between fiscal years (e.g., `PaymentsToAcquirePropertyPlantAndEquipment` vs `PaymentsToAcquireProductiveAssets`).
- Check supplemental disclosures for "Capital expenditures incurred but not yet paid" — this is non-cash capex committed but not in the CF number.
- Some companies include intangible asset purchases in the same line as PP&E; others separate them.
```json
"capex": {
  "cf_value": 1227,
  "supplemental_not_yet_paid": 408,
  "includes_intangibles": true,
  "xbrl_concept_used": "us-gaap:PaymentsToAcquireProductiveAssets"
}
```

### 6. Acquisitions (for Reinvestment Rate, Organic Growth)
Cash paid for acquisitions net of cash acquired.
- Usually one line in investing activities.
- Some companies have MULTIPLE acquisition lines in the same period (e.g., two different deals tagged with different concepts). Sum all acquisition-related payments.
- Some companies use extension concepts (company namespace, not us-gaap). Search for any concept containing "acquire" or "acquisition" in both us-gaap and company namespaces.
- Report total and list individual items if multiple.
```json
"acquisitions": {
  "total": 383,
  "items": [
    {"concept": "us-gaap:PaymentsToAcquireBusinessesNetOfCashAcquired", "value": 383}
  ]
}
```

### 7. Short-Term Debt (for Invested Capital, Net Debt)
May be zero for many companies. But verify:
- Check for "Current portion of long-term debt", "Short-term borrowings", "Commercial paper", "Notes payable"
- If none found, confirm it is truly zero — don't silently skip.
```json
"short_term_debt": {
  "value": 0,
  "components": [],
  "confirmed_zero": true
}
```

### 8. SBC (for SBC % Revenue)
Stock-based compensation expense.
- Primary source: cash flow statement addback line.
- Also disclosed in notes by function (cost of revenue, R&D, SGA). Report the CF total as the canonical value.
- Check if the XBRL concept is `ShareBasedCompensation` or `AllocatedShareBasedCompensationExpense` — they may have different values if one includes capitalized SBC.
```json
"sbc": {
  "cf_value": 1474,
  "note_by_function": {"cost_of_revenue": 233, "rd": 891, "sga": 350},
  "note_total": 1474
}
```

### 9. Interest Expense (for Interest Coverage)
Gross interest expense, NOT net of interest income.
- If the income statement shows "Interest expense" as a separate line — use it.
- If the income statement shows "Interest expense, net" or "Interest income, net" — find gross interest expense in the notes (typically a debt footnote).
- Report both gross and net so downstream can choose.
```json
"interest_expense": {
  "gross": 63,
  "income": 515,
  "net": 452,
  "source": "IS shows Interest income and Interest expense as separate lines"
}
```

### 10. Tax Rate Components (for NOPAT)
- Income tax expense and pretax income from the income statement.
- Compute effective rate.
- FLAG if pretax income is negative (use 21% fallback).
- FLAG if tax expense is negative (refund — unusual).
```json
"tax_rate": {
  "tax_expense": 3135,
  "pretax_income": 21910,
  "effective_rate": 0.143,
  "flags": []
}
```

### 11. Inventory (for DIO, Working Capital)
- If company has no inventory (software/services), confirm it is truly zero.
- If inventory is broken out (raw materials, WIP, finished goods) in the notes, report the breakdown — useful for analysis even though calculation uses the total.
```json
"inventory": {
  "total": 11333,
  "raw_materials": 2525,
  "wip": 5339,
  "finished_goods": 3469,
  "source": "BS face total; breakdown from Note 8"
}
```
