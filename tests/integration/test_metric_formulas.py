"""Integration tests for the live metric view stack.

These tests seed deterministic quarterly + annual facts into Postgres,
query the real SQL views, and compare them to hand-computed expectations.

Scope:
  - v_metrics_q
  - v_metrics_ttm
  - v_metrics_ttm_yoy
  - v_metrics_roic
  - v_metrics_cy
  - v_metrics_fy
  - tax-rate fallback

Warning: DROPs and recreates the `public` schema.
"""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import psycopg
from psycopg.rows import dict_row

from arrow.db.connection import get_conn
from arrow.db.migrations import apply
from scripts.apply_views import main as apply_views_main


CONCEPT_STATEMENT = {
    "revenue": "income_statement",
    "cogs": "income_statement",
    "gross_profit": "income_statement",
    "rd": "income_statement",
    "total_opex": "income_statement",
    "operating_income": "income_statement",
    "interest_expense": "income_statement",
    "ebt_incl_unusual": "income_statement",
    "tax": "income_statement",
    "net_income": "income_statement",
    "shares_diluted_weighted_avg": "income_statement",
    "accounts_receivable": "balance_sheet",
    "inventory": "balance_sheet",
    "accounts_payable": "balance_sheet",
    "total_assets": "balance_sheet",
    "total_equity": "balance_sheet",
    "current_portion_lt_debt": "balance_sheet",
    "long_term_debt": "balance_sheet",
    "current_portion_leases_operating": "balance_sheet",
    "long_term_leases_operating": "balance_sheet",
    "cash_and_equivalents": "balance_sheet",
    "short_term_investments": "balance_sheet",
    "cfo": "cash_flow",
    "capital_expenditures": "cash_flow",
    "dna_cf": "cash_flow",
    "sbc": "cash_flow",
    "cash_paid_for_interest": "cash_flow",
    "acquisitions": "cash_flow",
    "total_employees": "metrics",
}

UNSCALED_CONCEPTS = {
    "shares_diluted_weighted_avg",
    "total_employees",
}

MONEY_SCALE = Decimal("1000000")


def _d(value: str | int | float) -> Decimal:
    return Decimal(str(value))


def _quarter_end(year: int, quarter: int) -> date:
    month_day = {1: (3, 31), 2: (6, 30), 3: (9, 30), 4: (12, 31)}
    month, day = month_day[quarter]
    return date(year, month, day)


def _reset(conn: psycopg.Connection) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE;")
        cur.execute("CREATE SCHEMA public;")
    apply(conn)
    conn.autocommit = False
    apply_views_main()


def _seed_company(conn: psycopg.Connection, *, cik: int, ticker: str) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO companies (cik, ticker, name, fiscal_year_end_md)
            VALUES (%s, %s, %s, '12-31')
            RETURNING id;
            """,
            (cik, ticker, f"{ticker} INC"),
        )
        company_id = cur.fetchone()[0]
    conn.commit()
    return company_id


def _seed_run_and_raw(conn: psycopg.Connection) -> tuple[int, int]:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ingest_runs (run_kind, vendor, status)
            VALUES ('manual', 'test', 'started')
            RETURNING id;
            """,
        )
        run_id = cur.fetchone()[0]
        cur.execute(
            """
            INSERT INTO raw_responses (
                ingest_run_id, vendor, endpoint, params, params_hash,
                request_url, http_status, content_type,
                body_jsonb, raw_hash, canonical_hash
            ) VALUES (
                %s, 'test', '/metrics', '{}'::jsonb, decode(repeat('00',32),'hex'),
                'https://test/metrics', 200, 'application/json',
                '{}'::jsonb, decode(repeat('00',32),'hex'), decode(repeat('00',32),'hex')
            )
            RETURNING id;
            """,
            (run_id,),
        )
        raw_id = cur.fetchone()[0]
    conn.commit()
    return run_id, raw_id


def _insert_fact(
    conn: psycopg.Connection,
    *,
    company_id: int,
    ingest_run_id: int,
    raw_response_id: int,
    concept: str,
    value: Decimal,
    fiscal_year: int,
    period_type: str,
    fiscal_quarter: int | None = None,
) -> None:
    if period_type == "quarter":
        assert fiscal_quarter is not None
        period_end = _quarter_end(fiscal_year, fiscal_quarter)
        fiscal_label = f"FY{fiscal_year} Q{fiscal_quarter}"
    else:
        period_end = date(fiscal_year, 12, 31)
        fiscal_label = f"FY{fiscal_year}"
    calendar_quarter = (period_end.month - 1) // 3 + 1
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
                %s, %s, %s,
                %s, %s,
                %s, %s, %s,
                %s, %s, 'test-metrics-v1',
                %s
            );
            """,
            (
                company_id,
                CONCEPT_STATEMENT[concept],
                concept,
                value,
                fiscal_year,
                fiscal_quarter,
                fiscal_label,
                period_end,
                period_type,
                period_end.year,
                calendar_quarter,
                f"CY{period_end.year} Q{calendar_quarter}",
                datetime(period_end.year, period_end.month, min(period_end.day, 28), tzinfo=timezone.utc),
                raw_response_id,
                ingest_run_id,
            ),
        )
    conn.commit()


def _fetch_one(conn: psycopg.Connection, query: str, params: tuple) -> dict:
    with conn.cursor(row_factory=dict_row) as cur:
        cur.execute(query, params)
        row = cur.fetchone()
    assert row is not None
    return row


def _assert_close(actual: Decimal | None, expected: Decimal | None, tol: str = "0.0000001") -> None:
    if expected is None:
        assert actual is None
        return
    assert actual is not None
    assert abs(actual - expected) <= Decimal(tol)


def _weighted_rd_asset(values: list[Decimal]) -> Decimal:
    start_weight = 21 - len(values)
    total = Decimal("0")
    for idx, value in enumerate(values):
        total += value * Decimal(start_weight + idx) / Decimal("20")
    return total


def _seed_formula_company(conn: psycopg.Connection, *, ticker: str, cik: int) -> None:
    company_id = _seed_company(conn, cik=cik, ticker=ticker)
    run_id, raw_id = _seed_run_and_raw(conn)

    quarters = [
        (2021, 1, {"revenue": 100, "cogs": 40, "gross_profit": 60, "rd": 8, "total_opex": 40, "operating_income": 20, "interest_expense": 2, "ebt_incl_unusual": 18, "tax": 4.5, "net_income": 13.5, "shares_diluted_weighted_avg": 50, "accounts_receivable": 10, "inventory": 20, "accounts_payable": 15, "total_assets": 200, "total_equity": 100, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 15, "capital_expenditures": -3, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2021, 2, {"revenue": 110, "cogs": 44, "gross_profit": 66, "rd": 9, "total_opex": 44, "operating_income": 22, "interest_expense": 2, "ebt_incl_unusual": 20, "tax": 5, "net_income": 15, "shares_diluted_weighted_avg": 51, "accounts_receivable": 11, "inventory": 21, "accounts_payable": 16, "total_assets": 210, "total_equity": 105, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 16, "capital_expenditures": -3, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2021, 3, {"revenue": 120, "cogs": 48, "gross_profit": 72, "rd": 10, "total_opex": 48, "operating_income": 24, "interest_expense": 2, "ebt_incl_unusual": 22, "tax": 5.5, "net_income": 16.5, "shares_diluted_weighted_avg": 52, "accounts_receivable": 12, "inventory": 22, "accounts_payable": 17, "total_assets": 220, "total_equity": 110, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 17, "capital_expenditures": -4, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2021, 4, {"revenue": 130, "cogs": 52, "gross_profit": 78, "rd": 11, "total_opex": 52, "operating_income": 26, "interest_expense": 2, "ebt_incl_unusual": 24, "tax": 6, "net_income": 18, "shares_diluted_weighted_avg": 53, "accounts_receivable": 13, "inventory": 23, "accounts_payable": 18, "total_assets": 230, "total_equity": 115, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 18, "capital_expenditures": -4, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2022, 1, {"revenue": 140, "cogs": 56, "gross_profit": 84, "rd": 12, "total_opex": 56, "operating_income": 28, "interest_expense": 2, "ebt_incl_unusual": 26, "tax": 6.5, "net_income": 19.5, "shares_diluted_weighted_avg": 54, "accounts_receivable": 14, "inventory": 24, "accounts_payable": 19, "total_assets": 240, "total_equity": 120, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 19, "capital_expenditures": -5, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2022, 2, {"revenue": 150, "cogs": 60, "gross_profit": 90, "rd": 13, "total_opex": 60, "operating_income": 30, "interest_expense": 2, "ebt_incl_unusual": 28, "tax": 7, "net_income": 21, "shares_diluted_weighted_avg": 55, "accounts_receivable": 15, "inventory": 25, "accounts_payable": 20, "total_assets": 250, "total_equity": 125, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 20, "capital_expenditures": -5, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2022, 3, {"revenue": 160, "cogs": 64, "gross_profit": 96, "rd": 14, "total_opex": 64, "operating_income": 32, "interest_expense": 2, "ebt_incl_unusual": 30, "tax": 7.5, "net_income": 22.5, "shares_diluted_weighted_avg": 56, "accounts_receivable": 16, "inventory": 26, "accounts_payable": 21, "total_assets": 260, "total_equity": 130, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 21, "capital_expenditures": -6, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
        (2022, 4, {"revenue": 170, "cogs": 68, "gross_profit": 102, "rd": 15, "total_opex": 68, "operating_income": 34, "interest_expense": 2, "ebt_incl_unusual": 32, "tax": 8, "net_income": 24, "shares_diluted_weighted_avg": 57, "accounts_receivable": 17, "inventory": 27, "accounts_payable": 22, "total_assets": 270, "total_equity": 135, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "cash_and_equivalents": 20, "short_term_investments": 10, "cfo": 22, "capital_expenditures": -6, "dna_cf": 3, "sbc": 2, "cash_paid_for_interest": 2, "acquisitions": -1}),
    ]

    for fiscal_year, fiscal_quarter, row in quarters:
        for concept, value in row.items():
            _insert_fact(
                conn,
                company_id=company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                concept=concept,
                value=_d(value) if concept in UNSCALED_CONCEPTS else _d(value) * MONEY_SCALE,
                fiscal_year=fiscal_year,
                fiscal_quarter=fiscal_quarter,
                period_type="quarter",
            )

    annuals = {
        2021: {"revenue": 460, "cogs": 184, "gross_profit": 276, "operating_income": 92, "net_income": 63, "rd": 38, "sbc": 8, "cfo": 66, "capital_expenditures": -14, "dna_cf": 12, "interest_expense": 8, "cash_paid_for_interest": 8, "acquisitions": -4, "tax": 21, "ebt_incl_unusual": 84, "total_assets": 230, "total_equity": 115, "cash_and_equivalents": 20, "short_term_investments": 10, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "accounts_receivable": 13, "inventory": 23, "accounts_payable": 18, "total_employees": 100},
        2022: {"revenue": 620, "cogs": 248, "gross_profit": 372, "operating_income": 124, "net_income": 87, "rd": 54, "sbc": 8, "cfo": 82, "capital_expenditures": -22, "dna_cf": 12, "interest_expense": 8, "cash_paid_for_interest": 8, "acquisitions": -4, "tax": 29, "ebt_incl_unusual": 116, "total_assets": 270, "total_equity": 135, "cash_and_equivalents": 20, "short_term_investments": 10, "current_portion_lt_debt": 5, "long_term_debt": 30, "current_portion_leases_operating": 2, "long_term_leases_operating": 8, "accounts_receivable": 17, "inventory": 27, "accounts_payable": 22, "total_employees": 110},
    }

    for fiscal_year, row in annuals.items():
        for concept, value in row.items():
            _insert_fact(
                conn,
                company_id=company_id,
                ingest_run_id=run_id,
                raw_response_id=raw_id,
                concept=concept,
                value=_d(value) if concept in UNSCALED_CONCEPTS else _d(value) * MONEY_SCALE,
                fiscal_year=fiscal_year,
                period_type="annual",
            )


def test_metric_views_match_hand_computed_formulas() -> None:
    with get_conn() as conn:
        _reset(conn)
        _seed_formula_company(conn, ticker="FORM", cik=2001)

        period_end = date(2022, 12, 31)
        m = lambda x: _d(x) * MONEY_SCALE

        rd_values = [m(v) for v in [8, 9, 10, 11, 12, 13, 14, 15]]
        rd_asset_q = _weighted_rd_asset(rd_values)
        rd_asset_q_prior = _weighted_rd_asset(rd_values[:-1])
        rd_asset_prior_year = _weighted_rd_asset(rd_values[:4])
        rd_amort_qs = [m("2.5"), m("3.15"), m("3.85"), m("4.6")]
        rd_amort_ttm = sum(rd_amort_qs, m(0))

        tax_rate_ttm = m("29") / m("116")
        adjusted_oi_ttm = m("124") + m("54") - rd_amort_ttm
        adjusted_nopat_ttm = adjusted_oi_ttm * (Decimal("1") - tax_rate_ttm)
        reported_ic_q = m("135") + m("5") + m("30") + m("2") + m("8") - m("20") - m("10")
        adjusted_ic_q = reported_ic_q + rd_asset_q
        adjusted_ic_prior_q = (m("130") + m("5") + m("30") + m("2") + m("8") - m("20") - m("10")) + rd_asset_q_prior
        adjusted_ic_prior_year = (m("115") + m("5") + m("30") + m("2") + m("8") - m("20") - m("10")) + rd_asset_prior_year
        adjusted_nopat_ttm_prior_year = (m("92") + m("38") - (m("0.4") + m("0.85") + m("1.35") + m("1.9"))) * Decimal("0.75")

        q_row = _fetch_one(
            conn,
            """
            SELECT gross_margin, operating_margin, net_margin, revenue_qoq_annualized,
                   dso, dio, dpo, ccc, working_capital_intensity,
                   net_debt, net_debt_to_ebitda, interest_coverage_q
            FROM v_metrics_q
            WHERE ticker = 'FORM' AND period_end = %s;
            """,
            (period_end,),
        )
        _assert_close(q_row["gross_margin"], _d("102") / _d("170"))
        _assert_close(q_row["operating_margin"], _d("34") / _d("170"))
        _assert_close(q_row["net_margin"], _d("24") / _d("170"))
        _assert_close(q_row["revenue_qoq_annualized"], (Decimal("170") / Decimal("160")) ** Decimal("4") - Decimal("1"))
        _assert_close(q_row["dso"], m("17") / m("620") * _d("365"))
        _assert_close(q_row["dio"], m("27") / m("248") * _d("365"))
        _assert_close(q_row["dpo"], m("22") / m("248") * _d("365"))
        _assert_close(q_row["ccc"], (m("17") / m("620") * _d("365")) + (m("27") / m("248") * _d("365")) - (m("22") / m("248") * _d("365")))
        _assert_close(q_row["working_capital_intensity"], (m("17") + m("27") - m("22")) / m("620"))
        _assert_close(q_row["net_debt"], m("15"))
        _assert_close(q_row["net_debt_to_ebitda"], m("15") / m("136"))
        _assert_close(q_row["interest_coverage_q"], _d("34") / _d("2"))

        ttm_row = _fetch_one(
            conn,
            """
            SELECT revenue_ttm, gross_profit_ttm, adjusted_oi_ttm, adjusted_nopat_ttm,
                   nopat_margin, cfo_to_nopat, fcf_to_nopat, accruals_ratio,
                   sbc_pct_revenue, interest_coverage_ttm, revenue_per_employee,
                   unlevered_fcf_ttm, reinvestment_rate, rd_coverage_quarters
            FROM v_metrics_ttm
            WHERE ticker = 'FORM' AND period_end = %s;
            """,
            (period_end,),
        )
        _assert_close(ttm_row["revenue_ttm"], m("620"))
        _assert_close(ttm_row["gross_profit_ttm"], m("372"))
        _assert_close(ttm_row["adjusted_oi_ttm"], adjusted_oi_ttm)
        _assert_close(ttm_row["adjusted_nopat_ttm"], adjusted_nopat_ttm)
        _assert_close(ttm_row["nopat_margin"], adjusted_nopat_ttm / m("620"))
        _assert_close(ttm_row["cfo_to_nopat"], m("82") / adjusted_nopat_ttm)
        _assert_close(ttm_row["fcf_to_nopat"], (m("82") + m("-22")) / adjusted_nopat_ttm)
        _assert_close(ttm_row["accruals_ratio"], (m("87") - m("82")) / ((m("260") + m("270")) / _d("2")))
        _assert_close(ttm_row["sbc_pct_revenue"], m("8") / m("620"))
        _assert_close(ttm_row["interest_coverage_ttm"], m("124") / m("8"))
        _assert_close(ttm_row["revenue_per_employee"], m("620") / _d("110"))
        _assert_close(ttm_row["unlevered_fcf_ttm"], m("82") + m("8") * (Decimal("1") - tax_rate_ttm) + m("-22"))
        _assert_close(
            ttm_row["reinvestment_rate"],
            (
                m("22")
                + ((m("17") - m("13")) + (m("27") - m("23")) - (m("22") - m("18")))
                + m("4")
                - m("12")
                + (rd_asset_q - rd_asset_prior_year)
            ) / adjusted_nopat_ttm,
        )
        assert ttm_row["rd_coverage_quarters"] == 5

        yoy_row = _fetch_one(
            conn,
            """
            SELECT revenue_yoy_ttm, gross_profit_yoy_ttm,
                   incremental_gross_margin, incremental_operating_margin,
                   diluted_share_count_growth
            FROM v_metrics_ttm_yoy
            WHERE ticker = 'FORM' AND period_end = %s;
            """,
            (period_end,),
        )
        _assert_close(yoy_row["revenue_yoy_ttm"], (m("620") - m("460")) / m("460"))
        _assert_close(yoy_row["gross_profit_yoy_ttm"], (m("372") - m("276")) / m("276"))
        _assert_close(yoy_row["incremental_gross_margin"], (m("372") - m("276")) / (m("620") - m("460")))
        _assert_close(yoy_row["incremental_operating_margin"], (m("124") - m("92")) / (m("620") - m("460")))
        _assert_close(yoy_row["diluted_share_count_growth"], (_d("57") - _d("53")) / _d("53"))

        roic_row = _fetch_one(
            conn,
            """
            SELECT roic, roiic, adjusted_nopat_ttm, adjusted_nopat_ttm_prior_year,
                   adjusted_ic_q, ic_prior_q, ic_prior_year, rd_coverage_quarters
            FROM v_metrics_roic
            WHERE ticker = 'FORM' AND period_end = %s;
            """,
            (period_end,),
        )
        _assert_close(roic_row["adjusted_nopat_ttm"], adjusted_nopat_ttm)
        _assert_close(roic_row["adjusted_nopat_ttm_prior_year"], adjusted_nopat_ttm_prior_year)
        _assert_close(roic_row["adjusted_ic_q"], adjusted_ic_q)
        _assert_close(roic_row["ic_prior_q"], adjusted_ic_prior_q)
        _assert_close(roic_row["ic_prior_year"], adjusted_ic_prior_year)
        _assert_close(roic_row["roic"], adjusted_nopat_ttm / ((adjusted_ic_q + adjusted_ic_prior_q) / _d("2")))
        _assert_close(
            roic_row["roiic"],
            (adjusted_nopat_ttm - adjusted_nopat_ttm_prior_year) / (adjusted_ic_q - adjusted_ic_prior_year),
        )
        assert roic_row["rd_coverage_quarters"] == 8

        cy_row = _fetch_one(
            conn,
            """
            SELECT quarters_in_year, revenue_cy, gross_margin_cy, operating_margin_cy,
                   net_margin_cy, sbc_pct_revenue_cy, net_debt_cy_end
            FROM v_metrics_cy
            WHERE ticker = 'FORM' AND calendar_year = 2022;
            """,
            (),
        )
        assert cy_row["quarters_in_year"] == 4
        _assert_close(cy_row["revenue_cy"], m("620"))
        _assert_close(cy_row["gross_margin_cy"], m("372") / m("620"))
        _assert_close(cy_row["operating_margin_cy"], m("124") / m("620"))
        _assert_close(cy_row["net_margin_cy"], m("87") / m("620"))
        _assert_close(cy_row["sbc_pct_revenue_cy"], m("8") / m("620"))
        _assert_close(cy_row["net_debt_cy_end"], m("15"))

        fy_row = _fetch_one(
            conn,
            """
            SELECT revenue_fy, gross_margin_fy, operating_margin_fy, net_margin_fy,
                   sbc_pct_revenue_fy, net_debt_fy_end, total_employees_fy
            FROM v_metrics_fy
            WHERE ticker = 'FORM' AND fiscal_year = 2022;
            """,
            (),
        )
        _assert_close(fy_row["revenue_fy"], m("620"))
        _assert_close(fy_row["gross_margin_fy"], m("372") / m("620"))
        _assert_close(fy_row["operating_margin_fy"], m("124") / m("620"))
        _assert_close(fy_row["net_margin_fy"], m("87") / m("620"))
        _assert_close(fy_row["sbc_pct_revenue_fy"], m("8") / m("620"))
        _assert_close(fy_row["net_debt_fy_end"], m("15"))
        _assert_close(fy_row["total_employees_fy"], _d("110"))


def test_tax_rate_ttm_falls_back_to_15_percent_when_pretax_ttm_nonpositive() -> None:
    with get_conn() as conn:
        _reset(conn)
        company_id = _seed_company(conn, cik=2002, ticker="TAXF")
        run_id, raw_id = _seed_run_and_raw(conn)

        for quarter in range(1, 5):
            for concept, value in {
                "revenue": 10,
                "gross_profit": 5,
                "operating_income": 1,
                "rd": 0,
                "interest_expense": 3,
                "ebt_incl_unusual": -2,
                "tax": 0,
                "net_income": -2,
            }.items():
                _insert_fact(
                    conn,
                    company_id=company_id,
                    ingest_run_id=run_id,
                    raw_response_id=raw_id,
                    concept=concept,
                    value=_d(value),
                    fiscal_year=2022,
                    fiscal_quarter=quarter,
                    period_type="quarter",
                )

        row = _fetch_one(
            conn,
            """
            SELECT tax_ttm, pretax_ttm, tax_rate_ttm
            FROM v_tax_rate_ttm t
            JOIN companies c ON c.id = t.company_id
            WHERE c.ticker = 'TAXF' AND t.period_end = %s;
            """,
            (date(2022, 12, 31),),
        )
        _assert_close(row["tax_ttm"], _d("0"))
        _assert_close(row["pretax_ttm"], _d("-8"))
        _assert_close(row["tax_rate_ttm"], _d("0.15"))
