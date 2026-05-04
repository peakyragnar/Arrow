"""Universe-screenable metric registry.

One source of truth for what metrics the analyst agent can rank companies
by, across the financials / estimates / valuation verticals. Each entry
maps a metric name to (view, value_expr, period grain, kind, description).

Adding a new metric is a one-entry change here — no new tool, no new
SQL elsewhere. The screener (`arrow.retrieval.screener.screen`) consumes
this registry and generates the rank query.

Verticals:
- ``financials`` — period-grain metrics from the v_metrics_* views.
- ``estimates``  — forward consensus estimates from analyst_estimates.
- ``valuation``  — daily valuation ratios from v_valuation_ratios_ttm.

Period grains:
- ``annual``   — one row per (company, fiscal_year), keyed by fy_end / fiscal_year.
- ``quarter``  — one row per (company, period_end), keyed by period_end.
- ``daily``    — one row per (security, date), keyed by date.

YoY-growth metrics carry ``yoy_pair`` referencing another metric in the
same vertical/grain; the screener handles the self-join automatically.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class MetricSpec:
    name: str
    vertical: str           # 'financials' | 'estimates' | 'valuation'
    view: str               # SQL view or table
    value_expr: str         # SQL expression yielding numeric value (uses view aliases)
    period_grain: str       # 'annual' | 'quarter' | 'daily'
    kind: str               # 'money' | 'ratio' | 'count' | 'price'
    description: str        # short, planner-facing
    yoy_pair: str | None = None     # for *_growth metrics: reference another metric for YoY denominator


# --------------------------------------------------------------------------- #
# Financials
# --------------------------------------------------------------------------- #

# v_metrics_fy is the default annual view. Quarterly counterparts live in
# v_metrics_q / v_metrics_ttm / v_metrics_roic. We expose annual-grain
# metrics for screening to keep the period-spec interface predictable;
# trajectory windows over annual rows still capture cycle behavior.

_FINANCIALS = [
    MetricSpec(
        name="revenue",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="revenue_fy",
        period_grain="annual",
        kind="money",
        description="Annual revenue (FY).",
    ),
    MetricSpec(
        name="gross_margin",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="gross_margin_fy",
        period_grain="annual",
        kind="ratio",
        description="Gross margin (FY).",
    ),
    MetricSpec(
        name="operating_margin",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="operating_margin_fy",
        period_grain="annual",
        kind="ratio",
        description="Operating margin (FY).",
    ),
    MetricSpec(
        name="net_margin",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="net_margin_fy",
        period_grain="annual",
        kind="ratio",
        description="Net margin (FY).",
    ),
    MetricSpec(
        name="fcf",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="(cfo_fy + capital_expenditures_fy)",
        period_grain="annual",
        kind="money",
        description="Free cash flow (CFO + CapEx; CapEx is negative) (FY).",
    ),
    MetricSpec(
        name="cfo",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="cfo_fy",
        period_grain="annual",
        kind="money",
        description="Cash from operations (FY).",
    ),
    MetricSpec(
        name="net_income",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="net_income_fy",
        period_grain="annual",
        kind="money",
        description="Net income (FY).",
    ),
    MetricSpec(
        name="operating_income",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="operating_income_fy",
        period_grain="annual",
        kind="money",
        description="Operating income / EBIT (FY).",
    ),
    MetricSpec(
        name="rd",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="rd_fy",
        period_grain="annual",
        kind="money",
        description="R&D expense (FY).",
    ),
    MetricSpec(
        name="sbc_pct_revenue",
        vertical="financials",
        view="v_metrics_fy",
        value_expr="sbc_pct_revenue_fy",
        period_grain="annual",
        kind="ratio",
        description="Stock-based comp as % of revenue (FY).",
    ),
    MetricSpec(
        name="roic",
        vertical="financials",
        view="v_metrics_roic",
        value_expr="roic",
        period_grain="quarter",
        kind="ratio",
        description="Return on invested capital (TTM grain, quarterly cadence).",
    ),
    MetricSpec(
        name="roiic",
        vertical="financials",
        view="v_metrics_roic",
        value_expr="roiic",
        period_grain="quarter",
        kind="ratio",
        description="Return on incremental invested capital (quarterly cadence).",
    ),
    MetricSpec(
        name="revenue_growth_yoy",
        vertical="financials",
        view="v_metrics_ttm_yoy",
        value_expr="revenue_yoy_ttm",
        period_grain="quarter",
        kind="ratio",
        description="TTM revenue YoY growth (computed by view).",
    ),
    MetricSpec(
        name="gross_profit_growth_yoy",
        vertical="financials",
        view="v_metrics_ttm_yoy",
        value_expr="gross_profit_yoy_ttm",
        period_grain="quarter",
        kind="ratio",
        description="TTM gross profit YoY growth.",
    ),
    MetricSpec(
        name="incremental_gross_margin",
        vertical="financials",
        view="v_metrics_ttm_yoy",
        value_expr="incremental_gross_margin",
        period_grain="quarter",
        kind="ratio",
        description="ΔGross profit ÷ ΔRevenue (TTM).",
    ),
    MetricSpec(
        name="incremental_operating_margin",
        vertical="financials",
        view="v_metrics_ttm_yoy",
        value_expr="incremental_operating_margin",
        period_grain="quarter",
        kind="ratio",
        description="ΔOperating income ÷ ΔRevenue (TTM).",
    ),
    MetricSpec(
        name="accruals_ratio",
        vertical="financials",
        view="v_metrics_ttm",
        value_expr="accruals_ratio",
        period_grain="quarter",
        kind="ratio",
        description="(NI − CFO − Investing CF) ÷ avg total assets (TTM).",
    ),
    MetricSpec(
        name="net_debt_to_ebitda",
        vertical="financials",
        view="v_metrics_q",
        value_expr="net_debt_to_ebitda",
        period_grain="quarter",
        kind="ratio",
        description="Net debt ÷ TTM EBITDA (quarter-end).",
    ),
    MetricSpec(
        name="interest_coverage_ttm",
        vertical="financials",
        view="v_metrics_ttm",
        value_expr="interest_coverage_ttm",
        period_grain="quarter",
        kind="ratio",
        description="Operating income ÷ interest expense (TTM).",
    ),
    MetricSpec(
        name="reinvestment_rate",
        vertical="financials",
        view="v_metrics_ttm",
        value_expr="reinvestment_rate",
        period_grain="quarter",
        kind="ratio",
        description="(CapEx + ΔWorking capital) ÷ NOPAT (TTM).",
    ),
]


# --------------------------------------------------------------------------- #
# Estimates (forward consensus)
# --------------------------------------------------------------------------- #

# Estimates live in a single table — analyst_estimates — keyed by
# (security_id, period_kind, period_end). The screener treats period_kind
# as a separate parameter (annual vs quarter), and period spec selects
# which forward periods to aggregate (or which past period to look up
# for the YoY denominator).

_ESTIMATES = [
    MetricSpec(
        name="revenue_avg",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="revenue_avg",
        period_grain="quarter",       # period_kind controlled at call time
        kind="money",
        description="Forward consensus revenue (avg).",
    ),
    MetricSpec(
        name="eps_avg",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="eps_avg",
        period_grain="quarter",
        kind="ratio",                 # EPS is a per-share ratio; not 'money' since not summable
        description="Forward consensus EPS (avg).",
    ),
    MetricSpec(
        name="ebitda_avg",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="ebitda_avg",
        period_grain="quarter",
        kind="money",
        description="Forward consensus EBITDA (avg). Steward flags some periods as unreliable — see read_consensus warnings.",
    ),
    MetricSpec(
        name="ebit_avg",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="ebit_avg",
        period_grain="quarter",
        kind="money",
        description="Forward consensus operating income / EBIT (avg). Often flagged unreliable; check read_consensus warnings.",
    ),
    MetricSpec(
        name="net_income_avg",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="net_income_avg",
        period_grain="quarter",
        kind="money",
        description="Forward consensus net income (avg).",
    ),
    # YoY growth metrics — pair with the same metric one year earlier.
    MetricSpec(
        name="revenue_growth",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="revenue_avg",
        period_grain="quarter",
        kind="ratio",
        description="Forward consensus revenue YoY growth (% change vs same period 1y prior).",
        yoy_pair="revenue_avg",
    ),
    MetricSpec(
        name="eps_growth",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="eps_avg",
        period_grain="quarter",
        kind="ratio",
        description="Forward consensus EPS YoY growth.",
        yoy_pair="eps_avg",
    ),
    MetricSpec(
        name="ebitda_growth",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="ebitda_avg",
        period_grain="quarter",
        kind="ratio",
        description="Forward consensus EBITDA YoY growth.",
        yoy_pair="ebitda_avg",
    ),
    MetricSpec(
        name="ebit_growth",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="ebit_avg",
        period_grain="quarter",
        kind="ratio",
        description="Forward consensus EBIT YoY growth. Use caution — underlying EBIT estimates often steward-flagged.",
        yoy_pair="ebit_avg",
    ),
    MetricSpec(
        name="net_income_growth",
        vertical="estimates",
        view="analyst_estimates",
        value_expr="net_income_avg",
        period_grain="quarter",
        kind="ratio",
        description="Forward consensus net income YoY growth.",
        yoy_pair="net_income_avg",
    ),
]


# --------------------------------------------------------------------------- #
# Valuation
# --------------------------------------------------------------------------- #

_VALUATION = [
    MetricSpec(
        name="pe_ttm",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="pe_ttm",
        period_grain="daily",
        kind="ratio",
        description="P/E ratio (TTM net income).",
    ),
    MetricSpec(
        name="ps_ttm",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="ps_ttm",
        period_grain="daily",
        kind="ratio",
        description="P/S ratio (TTM revenue).",
    ),
    MetricSpec(
        name="ev_ebitda_ttm",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="ev_ebitda_ttm",
        period_grain="daily",
        kind="ratio",
        description="EV/EBITDA (TTM).",
    ),
    MetricSpec(
        name="fcf_yield_ttm",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="fcf_yield_ttm",
        period_grain="daily",
        kind="ratio",
        description="FCF yield (TTM FCF ÷ market cap). Returned as a fraction.",
    ),
    MetricSpec(
        name="market_cap",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="market_cap",
        period_grain="daily",
        kind="money",
        description="Market capitalization (USD).",
    ),
    MetricSpec(
        name="ev",
        vertical="valuation",
        view="v_valuation_ratios_ttm",
        value_expr="ev",
        period_grain="daily",
        kind="money",
        description="Enterprise value (mkt cap + total debt + NCI − cash − ST investments).",
    ),
]


# --------------------------------------------------------------------------- #
# Combined registry
# --------------------------------------------------------------------------- #

METRICS: dict[str, MetricSpec] = {m.name: m for m in (_FINANCIALS + _ESTIMATES + _VALUATION)}


def list_metrics(vertical: str | None = None) -> list[MetricSpec]:
    """Return all metric specs, optionally filtered by vertical."""
    if vertical is None:
        return list(METRICS.values())
    return [m for m in METRICS.values() if m.vertical == vertical]


def metric_names(vertical: str) -> list[str]:
    """Names of metrics in a vertical, sorted. Useful for tool input enums."""
    return sorted(m.name for m in METRICS.values() if m.vertical == vertical)


def get_metric(name: str) -> MetricSpec:
    if name not in METRICS:
        raise ValueError(
            f"unknown metric '{name}'. Use list_metrics() to see registered metrics."
        )
    return METRICS[name]
