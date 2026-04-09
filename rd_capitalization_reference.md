# R&D Capitalization: Formula Reference

## Purpose

When calculating ROIC and other return metrics, R&D must be treated as a capital expenditure rather than an operating expense. This requires:

1. **R&D Asset** — the unamortized balance of all R&D spending still "in service"
2. **Amortization Expense** — the portion of past R&D that expires each quarter
3. **Operating Income Adjustment** — the difference between reported R&D expense and amortization

## Assumptions

- **Amortization method:** Straight-line
- **Useful life:** 5 years = 20 quarters
- **Frequency:** Quarterly

## Definitions

Let `R&D(t)` = R&D expense in quarter `t` (the current quarter).
Let `R&D(t-j)` = R&D expense `j` quarters ago.
Let `N` = amortization life in quarters (default 20).

## R&D Asset

The R&D Asset at quarter `t` is the sum of all unamortized R&D from the current quarter and the prior 19 quarters. Each vintage is weighted by its remaining life fraction:

```
Asset(t) = Σ  R&D(t-j) × (N - j) / N     for j = 0 to N-1
```

Expanded:

```
Asset(t) = R&D(t)   × 20/20
         + R&D(t-1)  × 19/20
         + R&D(t-2)  × 18/20
         + ...
         + R&D(t-19) × 1/20
```

**Intuition:** The most recent quarter's R&D is fully capitalized (20/20). Each older quarter has one more period amortized away. R&D from 20+ quarters ago is fully amortized and drops out.

## Amortization Expense

The quarterly amortization is the sum of each vintage's per-quarter amortization charge:

```
Amort(t) = Σ  R&D(t-j) / N     for j = 0 to N-1
```

Expanded:

```
Amort(t) = R&D(t)/20 + R&D(t-1)/20 + R&D(t-2)/20 + ... + R&D(t-19)/20
         = SUM(R&D(t) through R&D(t-19)) / 20
```

**Intuition:** Every quarter of R&D in the 20-quarter window contributes exactly 1/20th of itself as amortization. This is simply the average of the last 20 quarters' R&D.

## Operating Income Adjustment

```
OI_Adjustment(t) = R&D(t) - Amort(t)
```

- **Positive** when current R&D > amortization → company is investing more than it consumes → capitalization increases operating income vs. reported
- **Negative** when current R&D < amortization → R&D is declining → capitalization decreases operating income
- **Near zero** when R&D is stable over time

## Adjusted Operating Income

```
Adjusted_OI(t) = Reported_OI(t) + R&D(t) - Amort(t)
```

This adds back the full R&D expense (which was deducted on the income statement) and subtracts only the amortization charge.

## Adjusted Invested Capital

```
Adjusted_IC(t) = Reported_IC(t) + R&D_Asset(t)
```

The R&D Asset is added to invested capital because it represents a capitalized investment that generates future returns.

## Data Requirements

A full calculation requires 20 quarters of R&D history. When only 12 quarters of actual data are available:

- Use 3 prior fiscal years of annual R&D (from 10-K filings)
- Divide each annual figure by 4 to estimate quarterly R&D for those years
- This provides 12 estimated quarters + 12 actual quarters = 24 quarters total
- Quarters with 20+ periods of lookback history have exact coverage
- The annual/4 approximation introduces minimal error because the oldest vintages carry the smallest weights (1/20 to 8/20)

## Worked Example: Constant R&D

If R&D is constant at $100M per quarter:

| Metric | Value | Derivation |
|--------|-------|------------|
| Amortization | $100M | 20 vintages × $100M/20 = $100M |
| R&D Asset | $1,050M | $100M × (20+19+18+...+1)/20 = $100M × 10.5 |
| OI Adjustment | $0 | $100M - $100M = $0 |

When R&D is perfectly stable, capitalization has no effect on operating income — only on the balance sheet (invested capital increases by the R&D Asset).

## Spreadsheet Layout

The template uses a single-column vertical layout per company:

| Row | Column A | Column B (Input) | Column C (Output) | Column D (Output) |
|-----|----------|-------------------|--------------------|--------------------|
| 7 | FY Year 1 (oldest annual) | Annual R&D | — | — |
| 8 | FY Year 2 | Annual R&D | — | — |
| 9 | FY Year 3 | Annual R&D | — | — |
| 10 | Q1 (oldest quarterly) | Quarterly R&D | Amortization | R&D Asset |
| 11 | Q2 | Quarterly R&D | Amortization | R&D Asset |
| ... | ... | ... | ... | ... |
| 21 | Q12 (most recent) | Quarterly R&D | Amortization | R&D Asset |

A hidden helper column constructs the full 24-quarter series by dividing each annual input by 4 for the first 12 quarters, then using actuals for the remaining 12.
