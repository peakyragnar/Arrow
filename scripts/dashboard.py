"""Arrow analyst dashboard — thin FastAPI surface over the metrics platform.

Start:
    uv run uvicorn scripts.dashboard:app --reload
Then visit: http://127.0.0.1:8000/

Design: docs/architecture/dashboard.md.
Reads: v_metrics_q, v_metrics_cy, v_metrics_ttm, v_metrics_ttm_yoy,
       v_metrics_roic, companies, data_quality_flags.
Writes: nothing.
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, Form, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from arrow.db.connection import get_conn
from arrow.steward.actions import (
    StewardActionError,
    dismiss_finding,
    resolve_finding,
    suppress_finding,
)
from arrow.steward.coverage import (
    VERTICALS,
    compute_coverage_matrix,
    compute_ticker_coverage,
)

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES = Jinja2Templates(directory=BASE_DIR / "templates")

logger = logging.getLogger("arrow.dashboard")


# ---------------------------------------------------------------------------
# Per-ticker TTL cache
#
# /t/{ticker} reads four metric views that recompute aggregates over ALL
# companies before filtering by ticker — the planner can't push the
# WHERE filter through the GroupAggregate, so a single render is ~6s on
# the dev DB. The proper fix is materializing the v_metrics_* stack or
# rewriting the views to be ticker-parameterizable; both are larger
# changes than V1 step 6 should absorb.
#
# In the meantime: cache the assembled per-ticker context dict for
# CACHE_TTL_S seconds. First click is slow, subsequent clicks are
# instant. On any data ingest the operator waits at most CACHE_TTL_S
# for fresh values to surface — acceptable for V1.
#
# Recorded as a Known Limitation in docs/architecture/steward.md.
# ---------------------------------------------------------------------------

import threading
import time as _time

CACHE_TTL_S = 60.0
_TICKER_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_TICKER_CACHE_LOCK = threading.Lock()


def _cache_get(key: str) -> dict[str, Any] | None:
    with _TICKER_CACHE_LOCK:
        entry = _TICKER_CACHE.get(key)
        if entry is None:
            return None
        ts, value = entry
        if _time.time() - ts > CACHE_TTL_S:
            del _TICKER_CACHE[key]
            return None
        return value


def _cache_put(key: str, value: dict[str, Any]) -> None:
    with _TICKER_CACHE_LOCK:
        _TICKER_CACHE[key] = (_time.time(), value)


def _cache_invalidate(key: str) -> None:
    with _TICKER_CACHE_LOCK:
        _TICKER_CACHE.pop(key, None)


def _ensure_views() -> None:
    """Apply the metrics-platform view stack idempotently on startup.

    The test suite's schema teardown DROPs tables CASCADE, which takes
    views with them. Without this hook, the first dashboard request
    after `uv run pytest` would 500 with `relation "v_metrics_q" does
    not exist`. Reapplying is fast (one DROP VIEW IF EXISTS + CREATE
    per view, subsecond) and harmless when views are already current.
    """
    # Import here to keep the top-level import light and to avoid a
    # cycle if apply_views ever grows to import from this module.
    from scripts.apply_views import main as apply_views_main

    try:
        rc = apply_views_main()
        if rc == 0:
            logger.info("dashboard: view stack applied on startup")
        else:
            logger.warning("dashboard: apply_views returned %s on startup", rc)
    except Exception:
        # Don't block the dashboard from starting if something is wrong;
        # the first request will surface a clearer error.
        logger.exception("dashboard: apply_views failed on startup")


@asynccontextmanager
async def lifespan(app: FastAPI):
    _ensure_views()
    yield


app = FastAPI(title="Arrow Dashboard", lifespan=lifespan)
app.mount("/static", StaticFiles(directory=BASE_DIR / "static"), name="static")

# Annual columns: the ticker's 5 most recent fiscal years (one per 10-K).
# Per-ticker because AMD's FY2025 ends Dec 2025 while NVDA's FY2026 ends
# Jan 2026. The fiscal-year axis is audit-aligned: each FY column ties
# to a single 10-K filing.
FY_COUNT = 5
Q_COUNT = 8          # rolling fiscal quarters displayed
Q_FETCH_COUNT = 12   # fetch 4 extra priors so each displayed quarter has a
                     # same-quarter-prior-year lookback for YoY deltas


# ---------------------------------------------------------------------------
# Data access
# ---------------------------------------------------------------------------

def _rows_as_dicts(cur: psycopg.Cursor) -> list[dict[str, Any]]:
    cols = [d[0] for d in cur.description] if cur.description else []
    return [dict(zip(cols, r)) for r in cur.fetchall()]


def fetch_tickers(conn: psycopg.Connection) -> list[str]:
    with conn.cursor() as cur:
        cur.execute("SELECT ticker FROM companies ORDER BY ticker;")
        return [r[0] for r in cur.fetchall()]


def fetch_quarterly(conn: psycopg.Connection, ticker: str, n: int = Q_FETCH_COUNT) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT q.*,
                   ttm.revenue_ttm, ttm.gross_profit_ttm, ttm.operating_income_ttm,
                   ttm.net_income_ttm, ttm.cfo_ttm, ttm.capital_expenditures_ttm,
                   ttm.adjusted_nopat_ttm, ttm.nopat_margin, ttm.cfo_to_nopat,
                   ttm.fcf_to_nopat, ttm.accruals_ratio, ttm.sbc_pct_revenue,
                   ttm.interest_coverage_ttm, ttm.revenue_per_employee,
                   ttm.unlevered_fcf_ttm, ttm.reinvestment_rate,
                   ttm.rd_coverage_quarters,
                   roic.roic, roic.roiic,
                   yoy.revenue_yoy_ttm, yoy.gross_profit_yoy_ttm,
                   yoy.incremental_gross_margin, yoy.incremental_operating_margin,
                   yoy.diluted_share_count_growth
            FROM v_metrics_q q
            LEFT JOIN v_metrics_ttm ttm
              ON ttm.company_id = q.company_id AND ttm.period_end = q.period_end
            LEFT JOIN v_metrics_roic roic
              ON roic.company_id = q.company_id AND roic.period_end = q.period_end
            LEFT JOIN v_metrics_ttm_yoy yoy
              ON yoy.company_id = q.company_id AND yoy.period_end = q.period_end
            WHERE q.ticker = %s
            ORDER BY q.period_end DESC
            LIMIT %s;
            """,
            (ticker, n),
        )
        rows = _rows_as_dicts(cur)
    rows.reverse()  # oldest → newest (Q-7 … Last Q)
    return rows


def fetch_fiscal_years(
    conn: psycopg.Connection, ticker: str, n: int = FY_COUNT
) -> list[dict]:
    """Return the ticker's `n` most recent fiscal-year annual rows
    (period_type = 'annual'). Oldest → newest."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT * FROM v_metrics_fy
            WHERE ticker = %s
            ORDER BY fy_end DESC
            LIMIT %s;
            """,
            (ticker, n),
        )
        rows = _rows_as_dicts(cur)
    rows.reverse()
    return rows


def fetch_fy_ttm_metrics(
    conn: psycopg.Connection, ticker: str, fy_end_dates: list[Any]
) -> dict[Any, dict]:
    """For each fiscal year-end date, return TTM / ROIC / DSO/DIO/DPO / etc.
    anchored at that fiscal_quarter=4 period_end. Each FY-end corresponds
    to the Q4 quarterly row with the same period_end.
    """
    if not fy_end_dates:
        return {}
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                q.period_end AS anchor_period_end,
                ttm.nopat_margin,
                ttm.cfo_to_nopat,
                ttm.fcf_to_nopat,
                ttm.accruals_ratio,
                ttm.sbc_pct_revenue,
                ttm.interest_coverage_ttm,
                ttm.revenue_per_employee,
                ttm.unlevered_fcf_ttm,
                ttm.reinvestment_rate,
                roic.roic,
                roic.roiic,
                yoy.diluted_share_count_growth,
                yoy.revenue_yoy_ttm,
                yoy.gross_profit_yoy_ttm,
                yoy.incremental_gross_margin,
                yoy.incremental_operating_margin,
                qm.dso,
                qm.dio,
                qm.dpo,
                qm.ccc,
                qm.net_debt_to_ebitda,
                qm.working_capital_intensity,
                qm.interest_coverage_q
            FROM v_metrics_q q
            LEFT JOIN v_metrics_ttm ttm
              ON ttm.company_id = q.company_id AND ttm.period_end = q.period_end
            LEFT JOIN v_metrics_roic roic
              ON roic.company_id = q.company_id AND roic.period_end = q.period_end
            LEFT JOIN v_metrics_ttm_yoy yoy
              ON yoy.company_id = q.company_id AND yoy.period_end = q.period_end
            LEFT JOIN v_metrics_q qm
              ON qm.company_id = q.company_id AND qm.period_end = q.period_end
            WHERE q.ticker = %s
              AND q.period_end = ANY(%s)
              AND q.fiscal_quarter = 4;
            """,
            (ticker, fy_end_dates),
        )
        rows = _rows_as_dicts(cur)
    return {r["anchor_period_end"]: r for r in rows}


def fetch_latest_ttm(conn: psycopg.Connection, ticker: str) -> dict | None:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ttm.*,
                   roic.roic, roic.roiic,
                   yoy.revenue_yoy_ttm, yoy.gross_profit_yoy_ttm,
                   yoy.incremental_gross_margin, yoy.incremental_operating_margin,
                   yoy.diluted_share_count_growth
            FROM v_metrics_ttm ttm
            LEFT JOIN v_metrics_roic roic
              ON roic.company_id = ttm.company_id AND roic.period_end = ttm.period_end
            LEFT JOIN v_metrics_ttm_yoy yoy
              ON yoy.company_id = ttm.company_id AND yoy.period_end = ttm.period_end
            WHERE ttm.ticker = %s
            ORDER BY ttm.period_end DESC
            LIMIT 1;
            """,
            (ticker,),
        )
        rows = _rows_as_dicts(cur)
    return rows[0] if rows else None


def fetch_flag_counts(conn: psycopg.Connection, ticker: str) -> dict[str, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE resolved_at IS NULL) AS unresolved,
                COUNT(*) FILTER (WHERE resolution = 'accept_as_is') AS accepted,
                COUNT(*) AS total
            FROM data_quality_flags f JOIN companies c ON c.id = f.company_id
            WHERE c.ticker = %s;
            """,
            (ticker,),
        )
        row = cur.fetchone()
    return {"unresolved": row[0], "accepted": row[1], "total": row[2]}


# ---------------------------------------------------------------------------
# Panel builder
# ---------------------------------------------------------------------------

@dataclass
class PanelRow:
    name: str
    format: str  # 'money' | 'pct' | 'bps' | 'days' | 'x' | 'count'
    values: list[Any]  # one per column (len = 5 CY + 1 TTM + Q_COUNT)
    is_change_row: bool = False
    tooltip: str | None = None


def _to_float(v: Any) -> float | None:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return float(v)
    return float(v)


def _pct_change(curr: Any, prev: Any) -> float | None:
    c, p = _to_float(curr), _to_float(prev)
    if c is None or p is None or p == 0:
        return None
    return (c - p) / abs(p)


def _bps_change(curr: Any, prev: Any) -> float | None:
    c, p = _to_float(curr), _to_float(prev)
    if c is None or p is None:
        return None
    return (c - p) * 10000


def _safe_div(n: Any, d: Any) -> float | None:
    n, d = _to_float(n), _to_float(d)
    if n is None or d is None or d == 0:
        return None
    return n / d


class _Columns:
    """Column accessors for the FY + TTM + Q layout.

    The FY columns are the ticker's N most recent fiscal-year annual rows
    (one per 10-K). Each FY maps 1:1 to a single filing for audit.
    """

    def __init__(
        self,
        fy_rows: list[dict],
        ttm: dict | None,
        quarterly: list[dict],
        fy_ttm_by_anchor: dict[Any, dict] | None = None,
        *,
        quarterly_full: list[dict] | None = None,
    ):
        """fy_rows / quarterly are the displayed sets (oldest → newest).

        `quarterly_full` holds the extended 12-quarter window so that
        each displayed quarter can look up its same-quarter-prior-year
        value for YoY deltas. quarterly_full is oldest → newest and
        has `quarterly` as its last len(quarterly) entries.
        """
        self.fy = fy_rows
        self.fy_ttm = fy_ttm_by_anchor or {}
        self.ttm = ttm or {}
        self.q = quarterly
        self.q_full = quarterly_full if quarterly_full is not None else quarterly

    @property
    def n_fy(self) -> int:
        return len(self.fy)

    def fy_series(self, key: str) -> list[Any]:
        return [row.get(key) for row in self.fy]

    def fy_ttm_series(self, key: str) -> list[Any]:
        """FY values for TTM-grain / quarter-end metrics, anchored at the
        FY-end period_end."""
        out = []
        for row in self.fy:
            anchor = row.get("fy_end")
            out.append(self.fy_ttm.get(anchor, {}).get(key))
        return out

    def ttm_value(self, key: str) -> Any:
        return self.ttm.get(key)

    def q_series(self, key: str) -> list[Any]:
        return [row.get(key) for row in self.q]

    def q_prior_year_series(self, key: str) -> list[Any]:
        """Return, for each displayed quarter, the value of `key` at the
        same quarter one fiscal year earlier (i.e. 4 quarters back).

        Uses q_full as the extended lookup window. If fewer than 4 prior
        quarters exist for a given displayed quarter, returns None in
        that slot so the caller suppresses the delta rather than computing
        against nothing.
        """
        # q_full is chronological oldest → newest. displayed quarters are
        # the last n of q_full. For displayed index i (0..n-1), its
        # position in q_full is offset + i, where offset = len(q_full) − n.
        # Its prior-year counterpart is at q_full[offset + i − 4].
        offset = len(self.q_full) - len(self.q)
        out: list[Any] = []
        for i in range(len(self.q)):
            src_idx = offset + i - 4
            out.append(self.q_full[src_idx].get(key) if src_idx >= 0 else None)
        return out

    def all_null_fy(self) -> list[Any]:
        return [None] * len(self.fy)

    def all_null_q(self) -> list[Any]:
        return [None] * len(self.q)


def _abs_row_with_yoy(name: str, c: _Columns, fy_key: str, ttm_key: str, q_key: str) -> list[PanelRow]:
    fy_vals = c.fy_series(fy_key)
    ttm_val = c.ttm_value(ttm_key)
    q_vals = c.q_series(q_key)
    values = fy_vals + [ttm_val] + q_vals

    # All deltas are YoY (year-over-year) for consistency:
    #   FY cell  → this FY vs prior FY        (already YoY)
    #   TTM cell → this TTM vs prior FY total (TTM-over-last-FY approximation)
    #   Q cell   → this quarter vs same quarter one year earlier (4 quarters back)
    # Quarterly QoQ was seasonality-dominated; YoY matches how analysts
    # actually talk about growth and the FY/TTM columns' semantics.
    fy_deltas = [None] + [_pct_change(fy_vals[i], fy_vals[i - 1]) for i in range(1, len(fy_vals))]
    ttm_delta = _pct_change(ttm_val, fy_vals[-1]) if fy_vals else None
    q_prior_year = c.q_prior_year_series(q_key)
    q_deltas = [_pct_change(q_vals[i], q_prior_year[i]) for i in range(len(q_vals))]
    deltas = fy_deltas + [ttm_delta] + q_deltas

    return [
        PanelRow(name=name, format="money", values=values),
        PanelRow(name=f"  Δ YoY", format="pct", values=deltas, is_change_row=True),
    ]


def _margin_row_with_bps(
    name: str,
    c: _Columns,
    fy_key: str,
    ttm_num_key: str,
    ttm_denom_key: str,
    q_key: str,
) -> list[PanelRow]:
    fy_vals = c.fy_series(fy_key)
    ttm_val = _safe_div(c.ttm_value(ttm_num_key), c.ttm_value(ttm_denom_key))
    q_vals = c.q_series(q_key)
    values = fy_vals + [ttm_val] + q_vals

    # BPS deltas mirror the abs-row YoY contract: quarter vs same-quarter-prior-year.
    fy_bps = [None] + [_bps_change(fy_vals[i], fy_vals[i - 1]) for i in range(1, len(fy_vals))]
    ttm_bps = _bps_change(ttm_val, fy_vals[-1]) if fy_vals else None
    q_prior_year = c.q_prior_year_series(q_key)
    q_bps = [_bps_change(q_vals[i], q_prior_year[i]) for i in range(len(q_vals))]
    bps = fy_bps + [ttm_bps] + q_bps

    return [
        PanelRow(name=name, format="pct", values=values),
        PanelRow(name="  Δbps YoY", format="bps", values=bps, is_change_row=True),
    ]


def _ttm_only_row(name: str, fmt: str, c: _Columns, ttm_key: str, q_key: str | None = None, tooltip: str | None = None) -> PanelRow:
    """Row for a TTM-grain metric.

    FY columns: populated from c.fy_ttm_series (TTM at each FY-end anchor).
    TTM column: latest TTM value.
    Quarter columns: rolling TTM at each quarter-end.
    """
    fy_vals = c.fy_ttm_series(ttm_key) if ttm_key in _FY_TTM_KEYS else c.all_null_fy()
    q_vals = c.q_series(q_key) if q_key else c.all_null_q()
    values = fy_vals + [c.ttm_value(ttm_key)] + q_vals
    return PanelRow(name=name, format=fmt, values=values, tooltip=tooltip)


# Keys returned by fetch_fy_ttm_metrics (TTM / ROIC / quarter-end metrics
# anchored at each FY-end).
_FY_TTM_KEYS = {
    "nopat_margin", "cfo_to_nopat", "fcf_to_nopat", "accruals_ratio",
    "sbc_pct_revenue", "interest_coverage_ttm", "revenue_per_employee",
    "unlevered_fcf_ttm", "reinvestment_rate",
    "roic", "roiic", "diluted_share_count_growth",
    "revenue_yoy_ttm", "gross_profit_yoy_ttm",
    "incremental_gross_margin", "incremental_operating_margin",
    "dso", "dio", "dpo", "ccc",
    "net_debt_to_ebitda", "working_capital_intensity", "interest_coverage_q",
}


def _quarter_only_row(name: str, fmt: str, c: _Columns, q_key: str) -> PanelRow:
    values = c.all_null_fy() + [None] + c.q_series(q_key)
    return PanelRow(name=name, format=fmt, values=values)


def build_panel(
    quarterly: list[dict],
    fy_rows: list[dict],
    ttm: dict | None,
    fy_ttm_by_anchor: dict[Any, dict] | None = None,
    quarterly_full: list[dict] | None = None,
) -> tuple[list[str], list[PanelRow]]:
    """Compose metric rows aligned with (FY + TTM + Q) columns.

    `quarterly` is the 8 displayed quarters. `quarterly_full` is the
    extended 12-quarter window used to supply each displayed quarter's
    prior-year lookback for YoY deltas.
    """
    c = _Columns(
        fy_rows, ttm, quarterly, fy_ttm_by_anchor,
        quarterly_full=quarterly_full,
    )

    # Column headers are structured as {date, main, sub?} so the
    # template can style each piece. date goes on top (dim, small),
    # main is the primary label, sub (optional) is a small position
    # indicator below on quarterly columns.
    def month_year(pe) -> str:
        return pe.strftime("%b %Y") if pe else ""

    fy_headers: list[dict[str, str]] = []
    for row in fy_rows:
        fy_headers.append({
            "date": month_year(row.get("fy_end")),
            "main": f"FY{row['fiscal_year']}",
            "sub": "",
        })

    q_headers: list[dict[str, str]] = []
    n = len(quarterly)
    for i in range(n):
        rank = n - i  # 1 = most recent
        position = "Last Q" if rank == 1 else f"Q-{rank - 1}"
        q_headers.append({
            "date": month_year(quarterly[i].get("period_end")),
            "main": quarterly[i]["fiscal_period_label"],
            "sub": position,
        })

    ttm_pe = quarterly[-1].get("period_end") if quarterly else None
    ttm_header = {
        "date": f"ending {month_year(ttm_pe)}" if ttm_pe else "",
        "main": "TTM",
        "sub": "",
    }

    headers = fy_headers + [ttm_header] + q_headers

    rows: list[PanelRow] = []

    # ----- Absolute levels + YoY% -----
    rows.extend(_abs_row_with_yoy("Revenue", c, "revenue_fy", "revenue_ttm", "revenue"))
    rows.extend(_abs_row_with_yoy("Gross Profit", c, "gross_profit_fy", "gross_profit_ttm", "gross_profit"))
    rows.extend(_abs_row_with_yoy("Operating Income", c, "operating_income_fy", "operating_income_ttm", "operating_income"))
    rows.extend(_abs_row_with_yoy("Net Income", c, "net_income_fy", "net_income_ttm", "net_income"))
    rows.extend(_abs_row_with_yoy("CFO", c, "cfo_fy", "cfo_ttm", "cfo"))

    # ----- Margins + BPS deltas -----
    rows.extend(_margin_row_with_bps("Gross Margin", c, "gross_margin_fy", "gross_profit_ttm", "revenue_ttm", "gross_margin"))
    rows.extend(_margin_row_with_bps("Operating Margin", c, "operating_margin_fy", "operating_income_ttm", "revenue_ttm", "operating_margin"))
    rows.extend(_margin_row_with_bps("Net Margin", c, "net_margin_fy", "net_income_ttm", "revenue_ttm", "net_margin"))

    # ----- Return-on-capital metrics (TTM grain, anchored at FY-end) -----
    coverage = (ttm or {}).get("rd_coverage_quarters")
    coverage_note = f"R&D coverage: {coverage}/20 quarters" if coverage else None
    rows.append(PanelRow(
        name="NOPAT Margin", format="pct",
        values=c.fy_ttm_series("nopat_margin") + [c.ttm_value("nopat_margin")] + c.q_series("nopat_margin"),
        tooltip=coverage_note,
    ))
    rows.append(PanelRow(
        name="Adjusted ROIC", format="pct",
        values=c.fy_ttm_series("roic") + [c.ttm_value("roic")] + c.q_series("roic"),
        tooltip=coverage_note,
    ))
    rows.append(PanelRow(
        name="ROIIC", format="pct",
        values=c.fy_ttm_series("roiic") + [c.ttm_value("roiic")] + c.q_series("roiic"),
        tooltip=coverage_note,
    ))
    rows.append(_ttm_only_row("Reinvestment Rate", "pct", c, "reinvestment_rate", "reinvestment_rate"))

    # ----- Cash-quality metrics -----
    rows.append(_ttm_only_row("CFO / NOPAT", "x", c, "cfo_to_nopat", "cfo_to_nopat"))
    rows.append(_ttm_only_row("FCF / NOPAT", "x", c, "fcf_to_nopat", "fcf_to_nopat"))
    rows.append(_ttm_only_row("Accruals Ratio", "pct", c, "accruals_ratio", "accruals_ratio"))
    rows.append(_ttm_only_row("Unlevered FCF", "money", c, "unlevered_fcf_ttm", "unlevered_fcf_ttm"))

    # ----- SBC / Revenue per Employee -----
    rows.append(PanelRow(
        name="SBC % Revenue", format="pct",
        values=c.fy_series("sbc_pct_revenue_fy") + [c.ttm_value("sbc_pct_revenue")] + c.q_series("sbc_pct_revenue"),
    ))
    rows.append(_ttm_only_row("Rev / Employee", "money", c, "revenue_per_employee", "revenue_per_employee"))

    # ----- Working capital days (FY cells = quarter-end at FY-end) -----
    rows.append(PanelRow(name="CCC", format="days",
        values=c.fy_ttm_series("ccc") + [None] + c.q_series("ccc")))
    rows.append(PanelRow(name="DSO", format="days",
        values=c.fy_ttm_series("dso") + [None] + c.q_series("dso")))
    rows.append(PanelRow(name="DIO", format="days",
        values=c.fy_ttm_series("dio") + [None] + c.q_series("dio")))
    rows.append(PanelRow(name="DPO", format="days",
        values=c.fy_ttm_series("dpo") + [None] + c.q_series("dpo")))

    # ----- Balance-sheet stocks (FY-end snapshots) -----
    rows.append(PanelRow(
        name="Net Debt", format="money",
        values=c.fy_series("net_debt_fy_end") + [None] + c.q_series("net_debt"),
    ))
    rows.append(PanelRow(
        name="Net Debt / EBITDA", format="x",
        values=c.fy_ttm_series("net_debt_to_ebitda") + [None] + c.q_series("net_debt_to_ebitda"),
    ))

    # ----- Share count growth -----
    rows.append(PanelRow(
        name="Diluted Shares YoY", format="pct",
        values=c.fy_ttm_series("diluted_share_count_growth")
              + [c.ttm_value("diluted_share_count_growth")]
              + c.q_series("diluted_share_count_growth"),
    ))

    return headers, rows


# ---------------------------------------------------------------------------
# Cell formatting
# ---------------------------------------------------------------------------

def fmt_cell(value: Any, fmt: str, is_change: bool) -> tuple[str, str]:
    v = _to_float(value)
    if v is None:
        return "—", "null"

    if fmt == "money":
        if abs(v) >= 1e9:
            text = f"${v / 1e9:.1f}B"
        elif abs(v) >= 1e6:
            text = f"${v / 1e6:,.0f}M"
        else:
            text = f"${v:,.0f}"
    elif fmt == "pct":
        text = f"{v * 100:+.1f}%" if is_change else f"{v * 100:.1f}%"
    elif fmt == "bps":
        text = f"{int(round(v)):+d}"
    elif fmt == "days":
        text = f"{v:.0f}d"
    elif fmt == "x":
        text = f"{v:.2f}x"
    elif fmt == "count":
        text = f"{int(v):,}"
    else:
        text = str(v)

    cls = ""
    if is_change and v != 0:
        cls = "pos" if v > 0 else "neg"
    return text, cls


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@app.get("/", response_class=HTMLResponse)
def index() -> Any:
    with get_conn() as conn:
        tickers = fetch_tickers(conn)
    if not tickers:
        # Even the no-data landing should keep the topbar so the
        # operator can navigate to /findings or /health without
        # editing the URL bar.
        return _no_data_response(
            heading="No companies seeded.",
            body_html="<p>Run <code>uv run scripts/ingest_company.py TICKER</code> first, "
                      "then refresh.</p>",
            tickers=[],
            current_ticker=None,
        )
    return RedirectResponse(url=f"/t/{tickers[0]}", status_code=307)


def _no_data_response(
    *,
    heading: str,
    body_html: str,
    tickers: list[str],
    current_ticker: str | None,
) -> HTMLResponse:
    """Render an empty-state page that still carries the topbar.

    Pre-V1-step-6 behavior was a bare ``<html><body><h1>...`` snippet
    with no nav, which left operators stranded if they clicked into
    a ticker that had no facts ingested. Now they keep the topbar
    (Findings link, ticker dropdown) and a clear in-pane message.
    """
    options = "".join(
        f'<option value="{t}"{" selected" if t == current_ticker else ""}>{t}</option>'
        for t in tickers
    )
    select_html = (
        '<form class="ticker-select" method="get" action="/">'
        '<label for="ticker-dropdown">Ticker:</label>'
        '<select id="ticker-dropdown" '
        'onchange="window.location.href = \'/t/\' + this.value;">'
        '<option value="">— pick —</option>' + options +
        '</select></form>'
    )
    return HTMLResponse(
        f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Arrow</title>
  <link rel="stylesheet" href="/static/dashboard.css">
</head>
<body>
<header class="topbar">
  <div class="brand">Arrow</div>
  <nav class="topnav">
    <a href="/findings?status=open" class="navlink">Findings</a>
    <a href="/coverage" class="navlink">Coverage</a>
  </nav>
  {select_html}
</header>
<main class="empty-state">
  <h1>{heading}</h1>
  {body_html}
</main>
<footer class="bottom">
  <a href="/findings?status=open">findings</a> &middot;
  <a href="/health">health</a>
</footer>
</body>
</html>"""
    )


def _build_ticker_context(ticker: str) -> dict[str, Any] | None:
    """Compute the per-ticker context dict (everything except `tickers`).

    Returns None if the ticker has no facts loaded — caller renders the
    no-data page in that case. Pulled out of the route handler so it
    can be cached in `_TICKER_CACHE` keyed on ticker.

    Excludes `tickers` (the companies-table dropdown) — that's cheap
    and changes when companies are added; it stays fresh on every
    request.
    """
    with get_conn() as conn:
        quarterly_full = fetch_quarterly(conn, ticker, n=Q_FETCH_COUNT)
        fy_rows = fetch_fiscal_years(conn, ticker, n=FY_COUNT)
        fy_end_dates = [r["fy_end"] for r in fy_rows]
        fy_ttm_by_anchor = fetch_fy_ttm_metrics(conn, ticker, fy_end_dates)
        ttm = fetch_latest_ttm(conn, ticker)
        flag_counts = fetch_flag_counts(conn, ticker)

    if not quarterly_full:
        return None

    quarterly = quarterly_full[-Q_COUNT:]

    headers, rows = build_panel(
        quarterly, fy_rows, ttm, fy_ttm_by_anchor,
        quarterly_full=quarterly_full,
    )

    col_period_ends: list[str] = []
    col_labels: list[str] = []
    for fr in fy_rows:
        col_period_ends.append(fr["fy_end"].isoformat() if fr.get("fy_end") else "")
        col_labels.append(f"FY{fr['fiscal_year']}")
    col_period_ends.append(
        quarterly[-1]["period_end"].isoformat() if quarterly else ""
    )
    col_labels.append("TTM")
    for q in quarterly:
        col_period_ends.append(q["period_end"].isoformat() if q.get("period_end") else "")
        col_labels.append(q["fiscal_period_label"])

    rendered_rows = []
    for row in rows:
        cells = []
        for i, v in enumerate(row.values):
            text, cls = fmt_cell(v, row.format, row.is_change_row)
            cell_tooltip = (
                f"{row.name} · {col_labels[i]} · {col_period_ends[i]}"
                if col_period_ends[i]
                else row.name
            )
            if row.tooltip:
                cell_tooltip += f" · {row.tooltip}"
            cells.append((text, cls, cell_tooltip))
        rendered_rows.append(
            {"name": row.name, "cells": cells, "is_change": row.is_change_row, "tooltip": row.tooltip}
        )

    return {
        "ticker": ticker,
        "headers": headers,
        "rows": rendered_rows,
        "flag_counts": flag_counts,
        "latest_period": quarterly[-1]["fiscal_period_label"] if quarterly else "",
        "n_fy": len(fy_rows),
        "n_q": len(quarterly),
    }


@app.get("/t/{ticker}", response_class=HTMLResponse)
def dashboard(request: Request, ticker: str) -> Any:
    ticker = ticker.upper()
    with get_conn() as conn:
        tickers = fetch_tickers(conn)
    if ticker not in tickers:
        raise HTTPException(404, f"{ticker} not in companies")

    cache_key = f"ticker:{ticker}"
    ctx = _cache_get(cache_key)
    if ctx is None:
        ctx = _build_ticker_context(ticker)
        if ctx is None:
            return _no_data_response(
                heading=f"{ticker}: no facts loaded yet.",
                body_html=(
                    f"<p>Run <code>uv run scripts/ingest_company.py {ticker}</code>"
                    f" first, then refresh.</p>"
                ),
                tickers=tickers,
                current_ticker=ticker,
            )
        _cache_put(cache_key, ctx)

    return TEMPLATES.TemplateResponse(
        request=request,
        name="dashboard.html.j2",
        context={**ctx, "tickers": tickers},
    )


@app.get("/t/{ticker}/raw")
def dashboard_raw(ticker: str) -> Any:
    ticker = ticker.upper()
    with get_conn() as conn:
        quarterly = fetch_quarterly(conn, ticker, n=Q_FETCH_COUNT)[-Q_COUNT:]
        fy_rows = fetch_fiscal_years(conn, ticker, n=FY_COUNT)
        ttm = fetch_latest_ttm(conn, ticker)
    return JSONResponse(
        {
            "ticker": ticker,
            "quarterly": [_serialize_row(r) for r in quarterly],
            "fiscal_years": [_serialize_row(r) for r in fy_rows],
            "ttm_latest": _serialize_row(ttm) if ttm else None,
        }
    )


# ---------------------------------------------------------------------------
# Steward findings pane
# ---------------------------------------------------------------------------


def _operator_actor() -> str:
    """Actor recorded for dashboard-initiated state changes.

    Reads $USER (the operator's OS account) so the audit trail captures
    who actually clicked. Falls back to 'human:dashboard' when $USER is
    unset. Avoids the prior cheat of hardcoding a specific operator
    name in shipped code. The ':dashboard' suffix distinguishes
    dashboard clicks from CLI invocations of `scripts/run_steward.py`
    (which use 'human:$USER' without the suffix).
    """
    user = os.environ.get("USER", "").strip()
    return f"human:{user}:dashboard" if user else "human:dashboard"


_VALID_SEVERITIES = ("informational", "warning", "investigate")
_VALID_STATUSES = ("open", "closed", "all")


def _build_note_template(
    finding: dict[str, Any],
    *,
    action_kind: str,
) -> str:
    """Pre-fill text for the note input on a lifecycle action form.

    Three labeled lines — Action / Cause / Expected — derived from the
    finding's existing ``suggested_action``. The format is structured
    enough that V2's RAG can key on it; the operator can edit any line
    or accept as-is. Either way the audit trail captures structured,
    consistent training data instead of hand-waved free text.

    action_kind: one of 'resolve' | 'suppress' | 'dismiss'.
    """
    sa = finding.get("suggested_action") or {}
    command = sa.get("command", "").strip()
    finding_type = finding.get("finding_type", "")
    ticker = finding.get("ticker") or "—"
    summary = finding.get("summary", "")

    # Compress the suggested-action prose to a one-line cause hint.
    short_cause = summary if summary else f"{finding_type} fired on {ticker}"

    if action_kind == "resolve":
        first_command_line = command.split("\n")[0].strip() if command else "[describe what was done]"
        return (
            f"Action: ran `{first_command_line}`\n"
            f"Cause: {short_cause}\n"
            f"Expected: finding auto-resolves on next sweep when fingerprint stops surfacing"
        )

    if action_kind == "suppress":
        return (
            f"Action: suppressed\n"
            f"Cause: [operator: explain the legitimate exception "
            f"— recent IPO, vendor gap, known taxonomy change, etc.]\n"
            f"Expected: revisit when [operator: name the condition that would change this]"
        )

    if action_kind == "dismiss":
        return (
            f"Action: dismissed (false positive)\n"
            f"Cause: [operator: explain what was wrong about the check or evidence]\n"
            f"Expected: tune check threshold or evidence collection if pattern recurs"
        )

    return ""


@app.get("/findings", response_class=HTMLResponse)
def findings_list(
    request: Request,
    ticker: str | None = None,
    severity: str | None = None,
    vertical: str | None = None,
    status: str = "open",
) -> Any:
    """List steward findings with optional filters.

    Status filter defaults to 'open' (the operator inbox). 'closed'
    shows the historical / resolved trail. 'all' shows both.

    All filters are validated against allow-lists before reaching SQL
    (no string-formatting of user input into queries). Unknown values
    are rejected with 400.
    """
    if status not in _VALID_STATUSES:
        raise HTTPException(400, f"invalid status: {status!r}")
    if severity is not None and severity not in _VALID_SEVERITIES:
        raise HTTPException(400, f"invalid severity: {severity!r}")

    where_clauses: list[str] = []
    params: list[Any] = []
    if status != "all":
        where_clauses.append("status = %s")
        params.append(status)
    if ticker:
        where_clauses.append("ticker = %s")
        params.append(ticker.upper())
    if severity:
        where_clauses.append("severity = %s")
        params.append(severity)
    if vertical:
        where_clauses.append("vertical = %s")
        params.append(vertical)

    where_sql = ("WHERE " + " AND ".join(where_clauses)) if where_clauses else ""
    sql = f"""
        SELECT id, fingerprint, finding_type, severity, ticker, vertical,
               fiscal_period_key, summary, status, closed_reason, closed_at,
               created_at, last_seen_at,
               EXTRACT(EPOCH FROM (now() - created_at))/86400 AS age_days
        FROM data_quality_findings
        {where_sql}
        ORDER BY
            CASE severity
              WHEN 'investigate' THEN 0
              WHEN 'warning' THEN 1
              WHEN 'informational' THEN 2
              ELSE 3
            END,
            created_at DESC
        LIMIT 500;
    """

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(sql, params)
        rows = _rows_as_dicts(cur)
        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE status = 'open') AS open_count,
                COUNT(*) FILTER (WHERE status = 'open' AND severity = 'investigate') AS investigate_count,
                COUNT(*) FILTER (WHERE status = 'open' AND severity = 'warning') AS warning_count,
                COUNT(*) FILTER (WHERE status = 'open' AND severity = 'informational') AS info_count,
                COUNT(*) AS total_count
            FROM data_quality_findings;
            """
        )
        counts = dict(zip([d[0] for d in cur.description], cur.fetchone()))
        tickers = fetch_tickers(conn)

    return TEMPLATES.TemplateResponse(
        request=request,
        name="findings_list.html.j2",
        context={
            "rows": rows,
            "counts": counts,
            "tickers": tickers,
            "filters": {
                "ticker": ticker.upper() if ticker else None,
                "severity": severity,
                "vertical": vertical,
                "status": status,
            },
            "valid_severities": _VALID_SEVERITIES,
            "valid_statuses": _VALID_STATUSES,
        },
    )


@app.get("/findings/{finding_id}", response_class=HTMLResponse)
def finding_detail(request: Request, finding_id: int) -> Any:
    """Per-finding detail page: full evidence, suggested action, history,
    and lifecycle action buttons.
    """
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT id, fingerprint, finding_type, severity,
                   company_id, ticker, vertical, fiscal_period_key,
                   source_check, evidence, summary, suggested_action,
                   status, closed_reason, closed_at, closed_by, closed_note,
                   suppressed_until, history,
                   created_at, created_by, last_seen_at
            FROM data_quality_findings
            WHERE id = %s;
            """,
            (finding_id,),
        )
        row = cur.fetchone()
        if row is None:
            raise HTTPException(404, f"finding #{finding_id} not found")
        finding = dict(zip([d[0] for d in cur.description], row))
        tickers = fetch_tickers(conn)

    return TEMPLATES.TemplateResponse(
        request=request,
        name="finding_detail.html.j2",
        context={
            "f": finding,
            "tickers": tickers,
            "note_prefill": {
                "resolve": _build_note_template(finding, action_kind="resolve"),
                "suppress": _build_note_template(finding, action_kind="suppress"),
                "dismiss": _build_note_template(finding, action_kind="dismiss"),
            },
        },
    )


def _lifecycle_action(
    finding_id: int,
    fn,
    *,
    redirect_to: str = "/findings",
    **kwargs: Any,
) -> RedirectResponse:
    """Shared wrapper for POST lifecycle handlers.

    Calls the action callable with operator actor; surfaces
    StewardActionError as 400; redirects with 303 (Post/Redirect/Get)
    so refresh doesn't re-submit.
    """
    actor = _operator_actor()
    try:
        with get_conn() as conn:
            fn(conn, finding_id, actor=actor, **kwargs)
    except StewardActionError as e:
        raise HTTPException(400, str(e))
    return RedirectResponse(url=redirect_to, status_code=303)


@app.post("/findings/{finding_id}/resolve")
def http_resolve_finding(
    finding_id: int,
    note: str = Form(""),
) -> Any:
    return _lifecycle_action(
        finding_id,
        resolve_finding,
        redirect_to=f"/findings/{finding_id}",
        note=note.strip() or None,
    )


@app.post("/findings/{finding_id}/suppress")
def http_suppress_finding(
    finding_id: int,
    reason: str = Form(...),
    expires: str = Form(""),
) -> Any:
    """Suppress a finding with a required reason and optional expiry date.

    Reason is required (suppressions without reasons rot the inbox).
    Expires is YYYY-MM-DD; if blank, the suppression has no expiry
    (permanent until manually reopened).
    """
    if not reason.strip():
        raise HTTPException(400, "suppress requires a non-empty reason")

    expires_date: date | None = None
    if expires.strip():
        try:
            expires_date = date.fromisoformat(expires.strip())
        except ValueError:
            raise HTTPException(400, f"invalid expires date: {expires!r} (want YYYY-MM-DD)")

    actor = _operator_actor()
    try:
        with get_conn() as conn:
            suppress_finding(
                conn,
                finding_id,
                actor=actor,
                reason=reason.strip(),
                expires=expires_date,
            )
    except StewardActionError as e:
        raise HTTPException(400, str(e))
    return RedirectResponse(url=f"/findings/{finding_id}", status_code=303)


@app.post("/findings/{finding_id}/dismiss")
def http_dismiss_finding(
    finding_id: int,
    note: str = Form(""),
) -> Any:
    return _lifecycle_action(
        finding_id,
        dismiss_finding,
        redirect_to=f"/findings/{finding_id}",
        note=note.strip() or None,
    )


# ---------------------------------------------------------------------------
# Coverage matrix + membership management
# ---------------------------------------------------------------------------


@app.get("/coverage", response_class=HTMLResponse)
def coverage_matrix(request: Request) -> Any:
    """Coverage matrix: every ticker in `companies` × verticals.

    Shows what data Arrow has per (ticker, vertical) — presence + row
    count + period count. Every company is automatically tracked by
    the steward — there is no separate membership step. To add a
    ticker, run `uv run scripts/ingest_company.py TICKER`.
    """
    with get_conn() as conn:
        matrix = compute_coverage_matrix(conn)
        tickers = fetch_tickers(conn)

    return TEMPLATES.TemplateResponse(
        request=request,
        name="coverage_matrix.html.j2",
        context={
            "matrix": matrix,
            "verticals": VERTICALS,
            "tickers": tickers,
        },
    )


@app.get("/coverage/{ticker}", response_class=HTMLResponse)
def coverage_ticker(request: Request, ticker: str) -> Any:
    """Per-ticker coverage detail: per-vertical period breakdown."""
    ticker = ticker.upper()
    with get_conn() as conn:
        result = compute_ticker_coverage(conn, ticker)
        tickers = fetch_tickers(conn)

    if result is None:
        raise HTTPException(
            404,
            f"{ticker} is not in companies. Run "
            f"`uv run scripts/ingest_company.py {ticker}` to seed it.",
        )
    summary, per_vertical_periods = result

    return TEMPLATES.TemplateResponse(
        request=request,
        name="coverage_ticker.html.j2",
        context={
            "summary": summary,
            "verticals": VERTICALS,
            "per_vertical": per_vertical_periods,
            "tickers": tickers,
        },
    )


@app.get("/health")
def health() -> Any:
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM companies;")
            ncompanies = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM financial_facts WHERE superseded_at IS NULL;")
            nfacts = cur.fetchone()[0]
            cur.execute("SELECT MAX(finished_at) FROM ingest_runs WHERE status = 'succeeded';")
            last_run = cur.fetchone()[0]
    return {
        "companies": ncompanies,
        "current_facts": nfacts,
        "last_succeeded_run": str(last_run) if last_run else None,
    }


def _serialize_row(row: dict | None) -> dict | None:
    if row is None:
        return None
    out: dict[str, Any] = {}
    for k, v in row.items():
        if isinstance(v, Decimal):
            out[k] = float(v)
        elif hasattr(v, "isoformat"):
            out[k] = v.isoformat()
        else:
            out[k] = v
    return out
