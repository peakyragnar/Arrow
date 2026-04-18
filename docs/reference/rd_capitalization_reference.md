# R&D Capitalization: Formula Reference

## Purpose

When calculating ROIC and other return metrics, R&D is treated as a capital
expenditure rather than an operating expense. This requires:

1. **R&D Asset** — unamortized balance of all R&D spending still "in service"
2. **R&D Amortization** — portion of past R&D that expires each quarter
3. **Operating-income adjustment** — difference between reported R&D expense
   and amortization

## Assumptions

- **Method:** straight-line
- **Useful life:** 5 years = 20 quarters
- **Frequency:** quarterly

## Definitions

Let `R&D(t)` = R&D expense in quarter `t`, `R&D(t-j)` = R&D expense `j`
quarters ago, `N` = amortization life in quarters (default 20).

## R&D Asset

```
Asset(t) = Σ  R&D(t-j) × (N - j) / N     for j = 0 to N-1
```

Expanded:

```
Asset(t) = R&D(t)    × 20/20
         + R&D(t-1)  × 19/20
         + R&D(t-2)  × 18/20
         + ...
         + R&D(t-19) × 1/20
```

The most recent quarter's R&D is fully capitalized (20/20). Each older
vintage has one more period amortized away. R&D from 20+ quarters ago is
fully amortized and drops out.

## R&D Amortization

```
Amort(t) = Σ  R&D(t-j) / N     for j = 0 to N-1
```

Expanded:

```
Amort(t) = R&D(t)/20 + R&D(t-1)/20 + ... + R&D(t-19)/20
         = SUM(R&D(t) through R&D(t-19)) / 20
```

Every quarter of R&D in the 20-quarter window contributes exactly 1/20th of
itself as amortization. This is the average of the last 20 quarters' R&D.

## Operating-Income Adjustment

```
OI_Adjustment(t) = R&D(t) - Amort(t)
Adjusted_OI(t)   = Reported_OI(t) + R&D(t) - Amort(t)
Adjusted_IC(t)   = Reported_IC(t) + R&D_Asset(t)
```

## Data Requirements — 20 real quarters

The calculation uses **20 actual quarterly R&D values**. No synthesis from
annuals, no division of annual figures by 4, no approximation.

Quarterly R&D comes from two sources, in priority order:

1. **`archive/ai_extract/{TICKER}/quarterly.json`** — the authoritative legacy Stage 2
   output for periods that have been run through the AI pipeline.
2. **`archive/ai_extract/{TICKER}/rd_history.json`** — deterministic supplement for
   historical periods not run through Stage 2. Built by a standalone script
   (`archive/ai_extract/extract_rd_history.py`) that reads the XBRL instance doc of
   each on-disk filing, extracts `us-gaap:ResearchAndDevelopmentExpense` with
   period-type filtering (3-month for 10-Qs, 12-month for 10-Ks, Q4 derived
   from annual − Q1 − Q2 − Q3), and writes one record per quarter. No AI.

`archive/legacy-root/calculate.py` reads `quarterly.json` first. If fewer than 20 quarters are
present, it fills the gap from `rd_history.json` for the older periods
below the earliest Stage 2 quarter. No overlap — each period sourced from
exactly one file.

## Worked Example: Constant R&D

If R&D is constant at $100M per quarter:

| Metric | Value | Derivation |
|--------|-------|------------|
| Amortization | $100M | 20 vintages × $100M/20 = $100M |
| R&D Asset | $1,050M | $100M × (20+19+18+...+1)/20 = $100M × 10.5 |
| OI Adjustment | $0 | $100M - $100M = $0 |

When R&D is stable, capitalization has no effect on operating income — only
on the balance sheet (invested capital increases by the R&D Asset).

## Benchmark

Every computed value for NVDA (amortization, asset, OI adjustment per
quarter) must match `docs/benchmarks/golden_eval.xlsx`. Zero drift tolerated.
