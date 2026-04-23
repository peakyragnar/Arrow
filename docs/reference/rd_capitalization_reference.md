# R&D Capitalization Reference

## Purpose

Arrow capitalizes R&D when computing return metrics such as:

- `roic`
- `roiic`
- `reinvestment_rate`
- `adjusted_nopat_ttm`
- `adjusted_ic_q`

This creates three derived values per quarter:

1. `rd_amortization_q`
2. `rd_asset_q`
3. `oi_adjustment_q = rd_q - rd_amortization_q`

Authoritative implementation:

- spec: [formulas.md](formulas.md)
- live SQL: [`db/queries/03_v_rd_derived.sql`](../../db/queries/03_v_rd_derived.sql)
- downstream consumers:
  - [`db/queries/07_v_adjusted_nopat_ttm.sql`](../../db/queries/07_v_adjusted_nopat_ttm.sql)
  - [`db/queries/08_v_adjusted_ic_q.sql`](../../db/queries/08_v_adjusted_ic_q.sql)
  - [`db/queries/12_v_metrics_roic.sql`](../../db/queries/12_v_metrics_roic.sql)

## Core formula

Assumptions:

- straight-line amortization
- useful life: `20` quarters
- grain: quarterly only

Let:

- `R&D(t)` = reported quarterly R&D expense at quarter `t`
- `N = 20`

Then:

```text
R&D Amortization(t) = Σ R&D(t-j) / 20          for j = 0..19
R&D Asset(t)        = Σ R&D(t-j) × (20-j) / 20 for j = 0..19
OI Adjustment(t)    = R&D(t) - R&D Amortization(t)
```

Expanded:

```text
R&D Asset(t) =
    R&D(t)    × 20/20
  + R&D(t-1)  × 19/20
  + ...
  + R&D(t-19) ×  1/20
```

```text
R&D Amortization(t) =
    (R&D(t) + R&D(t-1) + ... + R&D(t-19)) / 20
```

## Live production behavior

Arrow production uses the full quarterly history currently present in
`financial_facts`.

Rules:

- source input = canonical quarterly `rd` facts from FMP-backed `financial_facts`
- max window = last `20` actual quarters present at or before `period_end`
- no annual synthesis
- no annual `/ 4` approximation
- no archive `ai_extract` supplement in the live system

This is implemented in `v_rd_derived` with a LATERAL pull of the latest
`20` quarterly `rd` rows per anchor quarter.

## Partial-history policy

Arrow intentionally computes the schedule even when a ticker has fewer than
`20` prior quarters in the database.

Meaning:

- missing older quarters are treated as absent history, not as hard failure
- amortization / asset are computed from the rows that do exist
- `rd_coverage_quarters` records how many quarters contributed

So if only `9` quarters exist:

- `rd_amortization_q = sum(last 9 quarters) / 20`
- `rd_asset_q = weighted sum(last 9 quarters) / 20`
- `rd_coverage_quarters = 9`

This is a deliberate Arrow policy choice. It keeps `roic`, `roiic`, and
`reinvestment_rate` available in early-window periods while clearly marking
the calculation as partial-history.

## Bounded benchmark mode vs production mode

This distinction matters.

`docs/benchmarks/golden_eval.xlsx` contains a useful **bounded fixture**
for the R&D schedule. It is not a universal statement of live production
output for long-history companies.

Two valid modes exist:

### 1. Bounded fixture mode

Use case:

- formula-spec examples
- human-auditable worksheets
- controlled regression fixtures

Behavior:

- the schedule starts at the first quarter shown in the fixture
- earlier quarters are outside the fixture and therefore contribute nothing

Example:

- if a worksheet begins at `FY2022 Q1`, then `FY2022 Q1` amortization/asset
  only reflect quarters shown in that worksheet window

### 2. Full-history production mode

Use case:

- Arrow database views
- dashboard metrics
- company analysis from live `financial_facts`

Behavior:

- all earlier quarterly `rd` facts already present in the DB are eligible
- a company like `NVDA` can legitimately use quarters older than the visible
  benchmark sheet window

Result:

- a bounded worksheet and live Arrow can disagree on `rd_amortization_q` and
  `rd_asset_q` for the same visible quarter
- that does **not** imply a bug
- it usually means the two calculations are using different history windows

## Benchmark role

`golden_eval.xlsx` is still useful, but its role is narrower:

- good for bounded/windowed formula examples
- good for human audit of the arithmetic shape
- not authoritative for full-history production outputs unless the workbook
  explicitly includes the same quarter history Arrow is using

So the correct test question is:

- **bounded fixture**: did Arrow reproduce the intended bounded arithmetic?
- **production history**: did Arrow reproduce the intended full-history SQL behavior?

Those are separate checks.

## Worked example: constant R&D

If quarterly R&D is constant at `$100M` for at least `20` quarters:

| Metric | Value | Derivation |
|---|---:|---|
| `rd_amortization_q` | `$100M` | `20 × 100 / 20` |
| `rd_asset_q` | `$1,050M` | `100 × (20+19+...+1) / 20` |
| `oi_adjustment_q` | `$0` | `100 - 100` |

If only `8` quarters are present, Arrow still computes:

| Metric | Value |
|---|---:|
| `rd_amortization_q` | `$40M` |
| `rd_asset_q` | `$660M` |
| `rd_coverage_quarters` | `8` |

That lower amortization is not a bug. It is the explicit partial-history rule.

## Testing guidance

Use two test families:

1. **bounded fixture tests**
   - controlled worksheet-style inputs
   - prove the arithmetic of the schedule

2. **full-history integration tests**
   - seed quarterly `rd` into Postgres
   - query `v_rd_derived`
   - confirm older real quarters change the output when they should

Do not compare a bounded worksheet and full-history production output as if
they are the same mode.
