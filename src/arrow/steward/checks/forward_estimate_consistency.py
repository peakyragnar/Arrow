"""forward_estimate_consistency: forward analyst estimates that are
internally inconsistent — operating income deeply negative while net
income is materially positive, in amounts too large for normal non-
operating items to explain.

Pathology (discovered 2026-04-30 against BE forward consensus):
  FMP aggregates analyst estimates per concept (revenue, ebit, ebitda,
  net_income, eps) without normalizing across analyst methodology. For
  most companies this is fine — GAAP and non-GAAP roll up to the same
  ballpark. For companies where the distinction is material (clean-
  energy names with Investment Tax Credit monetization, biotech with
  R&D credits, etc.), the EBIT field can come from one set of GAAP-
  publishing analysts while the EPS field comes from a different set
  of non-GAAP publishers — producing a forward consensus where EBIT
  is deeply negative AND net income is robustly positive simultaneously.

  This check fires on rows where:
    - net_income_avg > 0 AND ebit_avg < 0  (sign divergence)
    - |net_income_avg - ebit_avg| > revenue_avg × 20%  (magnitude
      large enough that normal interest + tax + small non-operating
      items can't bridge it)
    - revenue_avg > 0  (avoid divide-by-zero on weird rows)

Severity: warning. The data isn't corrupted; the EBIT line just isn't
trustworthy for these tickers. The rest of the row (revenue, NI, EPS)
remains usable. Suppression with a per-ticker reason captures the
explanation in the audit trail.

Threshold rationale (calibrated against the BE / CRWV / LITE divergences
2026-04-30): normal companies show a NI-vs-EBIT gap of 5-15% of revenue
(interest + tax). 20% is solidly outside that range. The 12 flagged
rows in our universe at 20% are all real concerns; lowering to 10%
would surface false positives on companies with legitimately large
non-operating items.
"""

from __future__ import annotations

from typing import Iterable

import psycopg

from arrow.steward.fingerprint import fingerprint
from arrow.steward.registry import Check, FindingDraft, Scope, register


# Threshold: |NI - EBIT| / revenue > 0.20 = 20%. See module docstring
# for calibration rationale.
NI_EBIT_GAP_THRESHOLD = 0.20


@register
class ForwardEstimateConsistency(Check):
    name = "forward_estimate_consistency"
    severity = "warning"
    vertical = "estimates"

    def run(self, conn: psycopg.Connection, *, scope: Scope) -> Iterable[FindingDraft]:
        sql = """
        SELECT ae.security_id, s.ticker, s.company_id, ae.period_end,
               ae.period_kind,
               ae.revenue_avg, ae.ebit_avg, ae.ebitda_avg,
               ae.net_income_avg,
               ae.num_analysts_eps, ae.num_analysts_revenue,
               (ae.net_income_avg - ae.ebit_avg) AS gap,
               CASE WHEN ae.revenue_avg > 0
                    THEN ABS(ae.net_income_avg - ae.ebit_avg) / ae.revenue_avg
                    ELSE NULL END AS gap_share
        FROM analyst_estimates ae
        JOIN securities s ON s.id = ae.security_id
        WHERE s.status = 'active'
          AND ae.net_income_avg IS NOT NULL
          AND ae.ebit_avg IS NOT NULL
          AND ae.revenue_avg IS NOT NULL
          AND ae.revenue_avg > 0
          AND ae.net_income_avg > 0
          AND ae.ebit_avg < 0
          AND ABS(ae.net_income_avg - ae.ebit_avg) / ae.revenue_avg > %s
          AND ae.period_end >= CURRENT_DATE
        ORDER BY s.ticker, ae.period_end;
        """
        with conn.cursor() as cur:
            cur.execute(sql, (NI_EBIT_GAP_THRESHOLD,))
            rows = cur.fetchall()

        scope_tickers: set[str] | None = (
            {t.upper() for t in scope.tickers} if scope.tickers is not None else None
        )

        for row in rows:
            (security_id, ticker, company_id, period_end, period_kind,
             revenue_avg, ebit_avg, ebitda_avg, net_income_avg,
             n_eps, n_rev, gap, gap_share) = row

            if scope_tickers is not None and ticker.upper() not in scope_tickers:
                continue

            fp = fingerprint(
                self.name,
                scope={
                    "security_id": security_id,
                    "period_kind": period_kind,
                    "period_end": period_end.isoformat(),
                },
                rule_params={"gap_threshold": NI_EBIT_GAP_THRESHOLD},
            )
            yield FindingDraft(
                fingerprint=fp,
                finding_type=self.name,
                severity=self.severity,
                company_id=company_id,
                ticker=ticker,
                vertical=self.vertical,
                fiscal_period_key=None,
                evidence={
                    "security_id": security_id,
                    "period_kind": period_kind,
                    "period_end": period_end.isoformat(),
                    "revenue_avg": float(revenue_avg),
                    "ebit_avg": float(ebit_avg),
                    "ebitda_avg": float(ebitda_avg) if ebitda_avg is not None else None,
                    "net_income_avg": float(net_income_avg),
                    "ni_minus_ebit": float(gap),
                    "gap_share_of_revenue": float(gap_share),
                    "num_analysts_eps": n_eps,
                    "num_analysts_revenue": n_rev,
                    "gap_threshold": NI_EBIT_GAP_THRESHOLD,
                },
                summary=(
                    f"{ticker} {period_end} ({period_kind}): forward consensus "
                    f"is internally inconsistent — net income +${float(net_income_avg)/1e6:,.0f}M "
                    f"while operating income -${abs(float(ebit_avg))/1e6:,.0f}M. "
                    f"Implied non-operating bridge ${float(gap)/1e6:,.0f}M = "
                    f"{float(gap_share) * 100:.0f}% of forward revenue. "
                    f"Likely a GAAP-vs-non-GAAP methodology mismatch in FMP's "
                    f"aggregation; treat the EBIT/EBITDA estimates for this "
                    f"period as unreliable."
                ),
                suggested_action={
                    "kind": "investigate_forward_estimate_consistency",
                    "params": {"ticker": ticker, "period_end": period_end.isoformat()},
                    "command": (
                        f"# Inspect: SELECT * FROM analyst_estimates ae "
                        f"JOIN securities s ON s.id=ae.security_id "
                        f"WHERE s.ticker='{ticker}' AND ae.period_end='{period_end}';"
                    ),
                    "prose": (
                        f"FMP's analyst-estimate aggregation does not normalize "
                        f"across GAAP / non-GAAP methodology. For tickers with "
                        f"large tax credits, R&D credits, or otherwise material "
                        f"non-operating items, the forward EBIT/EBITDA fields "
                        f"can be sourced from a different (GAAP) pool of "
                        f"analysts than the forward NI/EPS fields (non-GAAP), "
                        f"producing an internally-inconsistent row. The "
                        f"revenue, NI, and EPS lines remain usable; the EBIT "
                        f"line should not be used for forward valuation on "
                        f"this ticker. Suppress with the per-ticker reason "
                        f"explaining the underlying driver (e.g. 'BE: ITC "
                        f"monetization pushes non-GAAP NI well above GAAP "
                        f"EBIT; FMP aggregation does not normalize')."
                    ),
                },
            )
