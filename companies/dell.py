"""
Dell-specific extraction overrides.

Known quirks:
- FY2024 Q1-Q2 (10-Qs for 2023-05-05 and 2023-08-04): DEI DocumentFiscalPeriodFocus
  is incorrectly tagged as "FY" instead of "Q1"/"Q2". Fixed by fix_dei() based on
  report_date month.
- FY2024 Q2-Q3: AccountsReceivableNetCurrent and AccountsPayableCurrent are tagged
  with RelatedPartyTransactionsByRelatedPartyAxis dimension only (no non-dimensioned
  total). Need to extract from NonrelatedPartyMember context.
- FY2025 10-K has DocumentFinStmtErrorCorrectionFlag=true, restating FY2024 and
  FY2025 Q1-Q3. Q4 derivation (FY - 9M YTD) mixes restated FY with original 9M,
  so Q4 must be re-derived from restated discrete quarterly values.
- Acquisitions concept present but sparse (zero when no acquisitions) — CF default
  handled by master script.
- Fiscal year ends in late January/early February.
"""

import json
import os
from datetime import datetime
from extract import parse_xbrl, DATA_DIR


def fix_dei(dei: dict, meta: dict) -> dict:
    """Fix incorrect DEI fiscal period on Dell 10-Q filings tagged as 'FY'."""
    if meta["form"] != "10-Q" or dei["DocumentFiscalPeriodFocus"] != "FY":
        return dei

    # Derive quarter from months elapsed since FY start
    # Dell FY ends early Feb (e.g. --02-02), so FY starts early Feb prior year
    fy_end_mmdd = dei["CurrentFiscalYearEndDate"]  # e.g. "--02-02"
    fy_end_month = int(fy_end_mmdd[2:4])
    report_dt = datetime.strptime(meta["report_date"], "%Y-%m-%d")
    # Months from FY start (month after FY end) to report date
    fy_start_month = fy_end_month % 12 + 1  # Feb end → Mar start
    months_in = (report_dt.month - fy_start_month) % 12
    quarter = months_in // 3 + 1
    dei["DocumentFiscalPeriodFocus"] = f"Q{quarter}"
    return dei


def _get_dimensioned_bs_value(filing_dir: str, meta: dict, concept: str,
                               member: str) -> int | None:
    """Extract a balance sheet value from a specific dimension member context."""
    xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
    if not os.path.exists(xbrl_path):
        return None

    contexts, facts, nsmap = parse_xbrl(xbrl_path)

    # Find instant contexts with the target dimension member
    import xml.etree.ElementTree as ET
    tree = ET.parse(xbrl_path)
    root = tree.getroot()

    xbrli_ns = nsmap.get("", "http://www.xbrl.org/2003/instance")

    # Map context IDs to their dimension members
    dim_contexts = {}
    for ctx in root.findall(f"{{{xbrli_ns}}}context"):
        ctx_id = ctx.get("id")
        period = ctx.find(f"{{{xbrli_ns}}}period")
        instant = period.find(f"{{{xbrli_ns}}}instant") if period is not None else None
        if instant is None:
            continue

        segment = ctx.find(f".//{{{xbrli_ns}}}segment")
        if segment is None:
            continue

        for dim_elem in segment:
            if dim_elem.text and member in dim_elem.text:
                dim_contexts[ctx_id] = instant.text

    if not dim_contexts:
        return None

    # Find the value in the matching dimensioned context at report date
    report_date = meta["report_date"]
    for ctx_id, inst_date in dim_contexts.items():
        if abs((int(inst_date[:4]) - int(report_date[:4])) * 400 +
               (int(inst_date[5:7]) - int(report_date[5:7])) * 31 +
               (int(inst_date[8:10]) - int(report_date[8:10]))) > 5:
            continue

        for elem in root:
            if concept in elem.tag and elem.get("contextRef") == ctx_id:
                try:
                    return int(float(elem.text))
                except (TypeError, ValueError):
                    pass

    return None


def fix_rd_series(quarterly_rd: list, records: list) -> list:
    """Replace FY2022 Q1-Q4 R&D with annual/4 estimates.

    Dell spun off VMware in Nov 2021 (FY2022 Q3). The original 10-Q filings
    for Q1-Q3 include VMware R&D, but the 10-K FY annual is post-spin.
    Q4 derivation (FY - 9M YTD) produces a negative value. Replace all 4
    quarters with annual/4 = $2,577,000,000 / 4 = $644,250,000.
    """
    fixed = list(quarterly_rd)
    for i, r in enumerate(records):
        if r["fiscal_year"] == 2022:
            fixed[i] = 644250000
    return fixed


def post_process(record: dict, extractions: list) -> dict:
    """Post-process extracted record for Dell-specific fixes."""
    fy = record["fiscal_year"]
    fp = record["fiscal_period"]

    # Fix AR/AP for FY2024 Q2-Q3 (dimensioned contexts only)
    if fy == 2024 and fp in ("Q2", "Q3"):
        _fix_dimensioned_ar_ap(record)

    # Fix FY2025 Q4 derivation (restated FY vs original 9M YTD)
    if fy == 2025 and fp == "Q4":
        _fix_restated_q4(record, extractions)

    return record


def _fix_dimensioned_ar_ap(record: dict):
    """Fix AR and AP for quarters where Dell only tags them with dimensions."""
    ticker_dir = os.path.join(DATA_DIR, "DELL")
    accession = record.get("accession")
    if not accession:
        return

    filing_dir = os.path.join(ticker_dir, accession)
    meta_path = os.path.join(filing_dir, "filing_meta.json")
    if not os.path.exists(meta_path):
        return

    with open(meta_path) as f:
        meta = json.load(f)

    member = "NonrelatedPartyMember"

    if record.get("accounts_receivable_q") == 0:
        val = _get_dimensioned_bs_value(filing_dir, meta,
                                         "AccountsReceivableNetCurrent", member)
        if val is not None:
            record["accounts_receivable_q"] = val

    if record.get("accounts_payable_q") == 0:
        val = _get_dimensioned_bs_value(filing_dir, meta,
                                         "AccountsPayableCurrent", member)
        if val is not None:
            record["accounts_payable_q"] = val


def _fix_restated_q4(record: dict, extractions: list):
    """
    Re-derive FY2025 Q4 from restated discrete quarterly values in the 10-K.
    The standard derivation (FY - 9M YTD) fails because the 10-K restated
    Q1-Q3 but the 9M YTD comes from the original Q3 10-Q.

    Q4 = FY - (restated Q1 + restated Q2 + restated Q3)
    The restated Q1-Q3 values are already in the output (applied by
    restatement overrides). FY comes from the 10-K.
    """
    ticker_dir = os.path.join(DATA_DIR, "DELL")

    # Find the FY2025 10-K
    fy2025_10k = None
    for dirname in sorted(os.listdir(ticker_dir)):
        meta_path = os.path.join(ticker_dir, dirname, "filing_meta.json")
        if not os.path.exists(meta_path):
            continue
        with open(meta_path) as f:
            meta = json.load(f)
        if meta["form"] == "10-K" and "2025-01" in meta["report_date"]:
            fy2025_10k = (os.path.join(ticker_dir, dirname), meta)
            break

    if not fy2025_10k:
        return

    filing_dir, meta = fy2025_10k
    xbrl_path = os.path.join(filing_dir, meta["xbrl_filename"])
    contexts, facts, nsmap = parse_xbrl(xbrl_path)
    from extract import parse_date, classify_contexts
    classified = classify_contexts(contexts, meta["report_date"])

    # Get FY values from the 10-K
    fy_ctx = classified.get("current_fy")
    if not fy_ctx:
        return

    # Components to fix — IS flow items affected by the restatement
    fix_components = {
        "cogs_q": ("CostOfRevenue", False),
        "operating_income_q": ("OperatingIncomeLoss", False),
        "income_tax_expense_q": ("IncomeTaxExpenseBenefit", False),
        "pretax_income_q": ("IncomeLossFromContinuingOperationsBeforeIncomeTaxesExtraordinaryItemsNoncontrollingInterest", False),
        "net_income_q": ("ProfitLoss", False),
    }

    # Find restated Q1-Q3 from the already-processed extraction results
    q1 = q2 = q3 = None
    for ext in extractions:
        if ext.get("fiscal_year") == 2025:
            if ext.get("fiscal_period") == "Q1":
                q1 = ext
            elif ext.get("fiscal_period") == "Q2":
                q2 = ext
            elif ext.get("fiscal_period") == "Q3":
                q3 = ext

    if not (q1 and q2 and q3):
        return

    for comp_name, (concept, negate) in fix_components.items():
        # Get FY value from 10-K
        fy_val = None
        entries = facts.get(concept, [])
        for cref, val in entries:
            if cref == fy_ctx:
                fy_val = val
                break

        if fy_val is None:
            continue

        # Get restated Q1-Q3 from the output records (post-restatement override)
        # We need to find these from the results list, not extractions
        # Actually, the restated values are already applied to the extractions'
        # derived output. But we're in post_process which runs on the result records.
        # We need the Q1-Q3 result records.
        # Since post_process runs per-record, we don't have direct access to other
        # result records. Use the 10-K's own restated quarterly values instead.

        # Find discrete quarterly contexts for current FY Q1-Q3 in the 10-K
        # Must be within the current fiscal year (after fy_start, before report_date)
        fy_start = classified.get("fy_start")
        if not fy_start:
            continue

        from extract import parse_date as pd
        fy_start_dt = pd(fy_start)
        report_dt = pd(meta["report_date"])

        q_entries = []  # (end_date, value)
        for ctx_id, ctx in contexts.items():
            if ctx.get("has_dimensions") or ctx.get("type") != "duration":
                continue
            days = ctx.get("days", 0)
            if not (60 <= days <= 120):
                continue
            start_dt = pd(ctx["start"])
            end_dt = pd(ctx["end"])
            # Must start on or after FY start and end before FY end
            if start_dt < fy_start_dt - __import__("datetime").timedelta(days=3):
                continue
            if abs((end_dt - report_dt).days) <= 3:
                continue  # Q4's own context

            for cref, val in entries:
                if cref == ctx_id:
                    q_entries.append((ctx["end"], val))
                    break

        if len(q_entries) != 3:
            continue

        # Sort by end date to get Q1, Q2, Q3 in order
        q_entries.sort()
        q_vals = [v for _, v in q_entries]

        # Q4 = FY - sum(Q1+Q2+Q3)
        q4_val = fy_val - sum(q_vals)

        record[comp_name] = q4_val
