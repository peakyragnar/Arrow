"""Integration tests for the registry-driven multi-metric retrieval.

Covers ``arrow.retrieval.multi_metric.get_metric_values`` and the agent
tool wrapper ``arrow.analysis.agent._tool_get_metrics``. The point of
the new tool is that any metric in ``arrow.retrieval.registry`` is
queryable through ONE call, including ROIIC at a specific quarter —
the case that exposed the prior hand-bundled tool's coverage gap.

These tests seed the simplest financial_facts shapes the metric views
need and exercise the dispatch / period-spec / error paths.
"""

from __future__ import annotations

from datetime import date

import pytest

from arrow.analysis.agent import _tool_get_metrics
from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from arrow.retrieval.multi_metric import get_metric_values


# Local seed helpers — kept minimal. test_ask_arrow.py has the elaborate
# happy-path seeders; this module only needs FY-grain facts to exercise the
# v_metrics_fy view.

H32 = b"\x00" * 32


def _reset(conn) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    from scripts.apply_views import main as apply_views_main
    apply_views_main()


def _seed_company(conn, *, ticker: str = "ARRM", cik: int = 9990001) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO companies (cik, ticker, name, fiscal_year_end_md) "
            "VALUES (%s, %s, %s, '12-31') RETURNING id;",
            (cik, ticker, f"{ticker} Test Co"),
        )
        return cur.fetchone()[0]


def _seed_run_raw(conn) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO ingest_runs (run_kind, vendor, status, finished_at) "
            "VALUES ('manual', 'fmp', 'succeeded', now()) RETURNING id;"
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params_hash,
                http_status, content_type, body_jsonb,
                raw_hash, canonical_hash
            ) VALUES (%s, 'fmp', 'income-statement', %s, 200,
                      'application/json', '{}'::jsonb, %s, %s)
            RETURNING id;
            """,
            (run_id, H32, H32, H32),
        )
        raw_id = cur.fetchone()[0]
    return run_id, raw_id


def _seed_fact(
    conn,
    *,
    company_id: int,
    concept: str,
    statement: str,
    value: int,
    fiscal_year: int,
    period_end: date,
    run_id: int,
    raw_id: int,
) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO financial_facts (
                company_id, statement, concept, value, unit,
                fiscal_year, fiscal_quarter, fiscal_period_label,
                period_end, period_type,
                calendar_year, calendar_quarter, calendar_period_label,
                published_at, source_raw_response_id, extraction_version,
                ingest_run_id
            ) VALUES (
                %s, %s, %s, %s, 'USD',
                %s, NULL, %s,
                %s, 'annual',
                %s, %s, %s,
                now(), %s, 'fmp-test-v1', %s
            );
            """,
            (
                company_id, statement, concept, value,
                fiscal_year, f"FY{fiscal_year}",
                period_end,
                period_end.year,
                (period_end.month - 1) // 3 + 1,
                f"CY{period_end.year} Q{(period_end.month - 1) // 3 + 1}",
                raw_id, run_id,
            ),
        )


def _seed_two_year_company(conn) -> int:
    company_id = _seed_company(conn)
    run_id, raw_id = _seed_run_raw(conn)
    for fy, period_end, mult in (
        (2023, date(2023, 12, 31), 1.0),
        (2024, date(2024, 12, 31), 1.30),
    ):
        _seed_fact(conn, company_id=company_id, concept="revenue",
                   statement="income_statement", value=int(2_200_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="gross_profit",
                   statement="income_statement", value=int(1_760_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="cogs",
                   statement="income_statement", value=int(440_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="operating_income",
                   statement="income_statement", value=int(120_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="net_income",
                   statement="income_statement", value=int(80_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="cfo",
                   statement="cash_flow", value=int(700_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
        _seed_fact(conn, company_id=company_id, concept="capital_expenditures",
                   statement="cash_flow", value=int(-50_000_000 * mult),
                   fiscal_year=fy, period_end=period_end, run_id=run_id, raw_id=raw_id)
    return company_id


# ---------------------------------------------------------------------------
# Multi-metric retrieval — happy paths
# ---------------------------------------------------------------------------


def test_single_fy_returns_one_row_with_multiple_metrics() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        rows = get_metric_values(
            conn, ticker="ARRM", metric_names=["revenue", "gross_margin"], period="FY2024"
        )
    assert len(rows) == 1
    r = rows[0]
    assert r.period_label == "FY2024"
    assert r.period_end == "2024-12-31"
    assert r.values["revenue"] is not None
    assert r.values["gross_margin"] is not None
    assert pytest.approx(float(r.values["revenue"])) == 2_200_000_000 * 1.30
    assert pytest.approx(float(r.values["gross_margin"]), rel=1e-6) == 0.80
    assert r.citations["revenue"] == r.citations["gross_margin"]
    assert r.citations["revenue"].startswith("M:v_metrics_fy:")
    assert r.citations["revenue"].endswith(":FY2024")


def test_latest_returns_most_recent_period() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        rows = get_metric_values(
            conn, ticker="ARRM", metric_names=["revenue"], period="latest"
        )
    assert len(rows) == 1
    assert rows[0].period_label == "FY2024"
    assert rows[0].period_end == "2024-12-31"


def test_window_returns_n_rows_period_end_desc() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        rows = get_metric_values(
            conn, ticker="ARRM", metric_names=["revenue"], period="last_2y"
        )
    assert [r.period_end for r in rows] == ["2024-12-31", "2023-12-31"]
    # Second row's revenue is the FY2023 multiplier (1.0)
    assert pytest.approx(float(rows[1].values["revenue"])) == 2_200_000_000.0


def test_unknown_ticker_returns_empty_list() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        rows = get_metric_values(
            conn, ticker="GHOST", metric_names=["revenue"], period="latest"
        )
    assert rows == []


# ---------------------------------------------------------------------------
# Multi-metric retrieval — error paths
# ---------------------------------------------------------------------------


def test_mixed_vertical_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        with pytest.raises(ValueError, match="multiple verticals"):
            get_metric_values(
                conn, ticker="ARRM", metric_names=["revenue", "pe_ttm"], period="latest"
            )


def test_unknown_metric_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        with pytest.raises(ValueError, match="unknown metric"):
            get_metric_values(
                conn, ticker="ARRM", metric_names=["not_a_real_metric"], period="latest"
            )


def test_annual_metric_with_quarterly_period_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        with pytest.raises(ValueError, match="annual-grain"):
            get_metric_values(
                conn, ticker="ARRM", metric_names=["revenue"], period="2024-Q4"
            )


def test_empty_metrics_list_raises() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        with pytest.raises(ValueError, match="non-empty"):
            get_metric_values(
                conn, ticker="ARRM", metric_names=[], period="latest"
            )


def test_estimates_growth_metric_rejected() -> None:
    """Forward-growth metrics live in the registry but don't compose with the
    single-ticker tool — they require a same-period-prior-year self-join the
    screener owns. Get the same data via screen_estimates / read_consensus."""
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        with pytest.raises(ValueError, match="forward-growth metrics"):
            get_metric_values(
                conn, ticker="ARRM", metric_names=["revenue_growth"], period="forward_4q_avg"
            )


# ---------------------------------------------------------------------------
# Agent wrapper
# ---------------------------------------------------------------------------


def test_agent_tool_wrapper_returns_tool_result_shape() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        result = _tool_get_metrics(
            conn,
            {
                "ticker": "ARRM",
                "metrics": ["revenue", "gross_margin"],
                "period": "FY2024",
            },
        )
    assert result.error is None
    assert len(result.rows) == 1
    row = result.rows[0]
    assert row["values"]["revenue"] is not None
    assert row["values"]["gross_margin"] is not None
    assert row["citations"]["revenue"].startswith("M:v_metrics_fy:")
    assert "M:v_metrics_fy:" in result.evidence_ids[0]


def test_agent_tool_wrapper_surfaces_errors_as_summaries() -> None:
    """Tool-level errors (mixed vertical, unknown metric) shouldn't raise out
    of the tool boundary — they should appear as ToolResult.error so the
    planner can adjust without a hard failure."""
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        result = _tool_get_metrics(
            conn,
            {"ticker": "ARRM", "metrics": ["revenue", "pe_ttm"], "period": "latest"},
        )
    assert result.error is not None
    assert "multiple verticals" in result.error
    assert result.rows == []


def test_agent_tool_wrapper_requires_metrics() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_two_year_company(conn)
        result = _tool_get_metrics(
            conn,
            {"ticker": "ARRM", "period": "latest"},
        )
    assert result.error == "metrics required"
