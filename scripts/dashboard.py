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
from contextlib import asynccontextmanager
from dataclasses import dataclass
from decimal import Decimal
from pathlib import Path
from typing import Any

import psycopg
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from starlette.requests import Request

from arrow.db.connection import get_conn

BASE_DIR = Path(__file__).resolve().parents[1]
TEMPLATES = Jinja2Templates(directory=BASE_DIR / "templates")

logger = logging.getLogger("arrow.dashboard")


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
Q_COUNT = 8  # rolling fiscal quarters


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


def fetch_quarterly(conn: psycopg.Connection, ticker: str, n: int = Q_COUNT) -> list[dict]:
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
    ):
        self.fy = fy_rows  # oldest → newest
        self.fy_ttm = fy_ttm_by_anchor or {}
        self.ttm = ttm or {}
        self.q = quarterly

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

    def all_null_fy(self) -> list[Any]:
        return [None] * len(self.fy)

    def all_null_q(self) -> list[Any]:
        return [None] * len(self.q)


def _abs_row_with_yoy(name: str, c: _Columns, fy_key: str, ttm_key: str, q_key: str) -> list[PanelRow]:
    fy_vals = c.fy_series(fy_key)
    ttm_val = c.ttm_value(ttm_key)
    q_vals = c.q_series(q_key)
    values = fy_vals + [ttm_val] + q_vals

    # Deltas: FY-over-FY, TTM-over-last-FY, Q-over-prior-Q
    fy_deltas = [None] + [_pct_change(fy_vals[i], fy_vals[i - 1]) for i in range(1, len(fy_vals))]
    ttm_delta = _pct_change(ttm_val, fy_vals[-1]) if fy_vals else None
    q_deltas = [None] + [_pct_change(q_vals[i], q_vals[i - 1]) for i in range(1, len(q_vals))]
    deltas = fy_deltas + [ttm_delta] + q_deltas

    return [
        PanelRow(name=name, format="money", values=values),
        PanelRow(name=f"  Δ%", format="pct", values=deltas, is_change_row=True),
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

    fy_bps = [None] + [_bps_change(fy_vals[i], fy_vals[i - 1]) for i in range(1, len(fy_vals))]
    ttm_bps = _bps_change(ttm_val, fy_vals[-1]) if fy_vals else None
    q_bps = [None] + [_bps_change(q_vals[i], q_vals[i - 1]) for i in range(1, len(q_vals))]
    bps = fy_bps + [ttm_bps] + q_bps

    return [
        PanelRow(name=name, format="pct", values=values),
        PanelRow(name="  Δbps", format="bps", values=bps, is_change_row=True),
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
) -> tuple[list[str], list[PanelRow]]:
    """Compose metric rows aligned with (FY + TTM + Q) columns.

    Returns (headers, rows, col_period_ends). col_period_ends gives the
    period_end date (or None) for each column — used for cell-level
    audit tooltips.
    """
    c = _Columns(fy_rows, ttm, quarterly, fy_ttm_by_anchor)

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
        return HTMLResponse(
            "<html><body><h1>No companies seeded.</h1>"
            "<p>Run <code>uv run scripts/seed_companies.py TICKER</code> first.</p></body></html>"
        )
    return RedirectResponse(url=f"/t/{tickers[0]}", status_code=307)


@app.get("/t/{ticker}", response_class=HTMLResponse)
def dashboard(request: Request, ticker: str) -> Any:
    ticker = ticker.upper()
    with get_conn() as conn:
        tickers = fetch_tickers(conn)
        if ticker not in tickers:
            raise HTTPException(404, f"{ticker} not in companies")
        quarterly = fetch_quarterly(conn, ticker, n=Q_COUNT)
        fy_rows = fetch_fiscal_years(conn, ticker, n=FY_COUNT)
        fy_end_dates = [r["fy_end"] for r in fy_rows]
        fy_ttm_by_anchor = fetch_fy_ttm_metrics(conn, ticker, fy_end_dates)
        ttm = fetch_latest_ttm(conn, ticker)
        flag_counts = fetch_flag_counts(conn, ticker)

    if not quarterly:
        return HTMLResponse(
            f"<html><body><h1>{ticker}: no facts loaded yet.</h1>"
            f"<p>Run <code>uv run scripts/backfill_fmp.py {ticker}</code> first.</p>"
            "</body></html>"
        )

    headers, rows = build_panel(quarterly, fy_rows, ttm, fy_ttm_by_anchor)

    # Column period-ends for cell-level audit tooltips:
    #   FY columns → fy_rows[i].fy_end
    #   TTM column → quarterly[-1].period_end
    #   Q columns  → quarterly[i].period_end
    col_period_ends: list[str] = []
    col_labels: list[str] = []  # human label used in tooltips ("FY2024", "TTM", "FY2026 Q3")
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

    return TEMPLATES.TemplateResponse(
        request=request,
        name="dashboard.html.j2",
        context={
            "ticker": ticker,
            "tickers": tickers,
            "headers": headers,
            "rows": rendered_rows,
            "flag_counts": flag_counts,
            "latest_period": quarterly[-1]["fiscal_period_label"] if quarterly else "",
            "n_fy": len(fy_rows),
            "n_q": len(quarterly),
        },
    )


@app.get("/t/{ticker}/raw")
def dashboard_raw(ticker: str) -> Any:
    ticker = ticker.upper()
    with get_conn() as conn:
        quarterly = fetch_quarterly(conn, ticker, n=Q_COUNT)
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
