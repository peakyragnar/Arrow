# Ask Arrow Question Log

Status: seed list for deterministic revenue-driver MVP

Use this file to run the first grounded analyst CLI over real questions and
record failure modes before adding LLM synthesis or transcripts.

Command shape:

```bash
uv run scripts/ask_arrow.py "What drove PLTR revenue growth in FY2024?"
```

Statuses:

- `good`: answer is grounded and useful
- `partial`: answer is grounded but misses important evidence
- `failed`: runtime could not answer

Failure modes:

- `period_failed`
- `retrieval_failed`
- `segment_gap`
- `evidence_gap`
- `template_bug`
- `needs_transcript`

## Tier 1: Known Packet Cases

### 1. PLTR FY2024 Revenue Drivers

Question: What drove PLTR revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer. Segment facts identify U.S., Government, and Commercial growth; ranked evidence now avoids intro and forward-looking boilerplate.

### 2. NVDA FY2025 Revenue Drivers

Question: What drove NVDA revenue growth in FY2025?
Status: good
Failure mode:
Notes: Verified deterministic answer. Segment facts identify Data Center growth; earnings-release evidence surfaces full-year and Data Center revenue commentary.

## Tier 2: Same Recipe, Broader Companies

### 3. AMD FY2024 Revenue Drivers

Question: What drove AMD revenue growth in FY2024?
Status: partial
Failure mode: evidence_gap
Notes: Verified deterministic answer. Segment facts identify Data Center and geography growth; MD&A evidence is useful. Earnings-release chunks exist, but none cleared the revenue-driver quality threshold, so the CLI correctly reports a soft gap instead of citing weak PR/highlight/table text.

### 4. MSFT FY2025 Revenue Drivers

Question: What drove MSFT revenue growth in FY2025?
Status: good
Failure mode:
Notes: Verified deterministic answer. MD&A evidence identifies Azure, Microsoft 365 Commercial cloud, Gaming, and Search/news advertising as drivers. Structured product facts are still coarser than management commentary for Azure, which is a future segment taxonomy improvement rather than a runtime failure.

### 5. GOOGL FY2024 Revenue Drivers

Question: What drove GOOGL revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer after ranking improvements. Segment facts identify Search, Cloud, subscriptions/platforms/devices, YouTube ads, and geography; MD&A and earnings-release evidence now surface driver text instead of non-GAAP or cost/tax table chunks.

### 6. AMZN FY2024 Revenue Drivers

Question: What drove AMZN revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer. Segment facts identify North America, International, AWS, online stores, and third-party seller services; MD&A and earnings-release evidence are period-aligned and driver-relevant.

### 7. INTC FY2024 Revenue Drivers

Question: What drove INTC revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer. Segment facts identify CCG, DCAI, NEX, and Foundry breakdown introduced in 2024. MD&A surfaces foundry-model commentary, earnings release surfaces Data Center recovery context. Useful test of declining-revenue case (revenue down YoY).

### 8. AVGO FY2024 Revenue Drivers

Question: What drove AVGO revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer. Period model handles AVGO's November fiscal year-end correctly; segment facts identify Semiconductor Solutions and Infrastructure Software (post-VMware) growth; MD&A surfaces VMware contribution; earnings-release surfaces Q4 revenue and AI Networking commentary.

### 9. TSLA FY2024 Revenue Drivers

Question: What drove TSLA revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer. Segment facts identify Automotive (sales/regulatory credits/leasing), Energy Generation and Storage, and Services. MD&A and earnings release surface auto pricing pressure plus energy storage record deployments.

### 10. VRT FY2024 Revenue Drivers

Question: What drove VRT revenue growth in FY2024?
Status: partial
Failure mode: evidence_gap
Notes: Verified deterministic answer. Segment facts identify Americas, EMEA, Asia Pacific, Product, and Service growth; earnings-release surfaces Q4 revenue and adjusted operating profit. MD&A chunks exist but none cleared revenue-driver quality threshold (best_score=10, candidates=6) — same pattern as AMD FY2024. Recurring failure mode in mid-cap MD&A tone.

## Tier 3: Multi-Year Sweep

Same recipe across multiple fiscal years per ticker to expose history-dependent failures.

### 11. NVDA FY2024 Revenue Drivers

Question: What drove NVDA revenue growth in FY2024?
Status: good
Failure mode:
Notes: Verified deterministic answer covering the AI inflection year. Segment facts and MD&A identify Data Center as the primary driver; earnings-release captures full-year revenue framing.

### 12. NVDA FY2023 Revenue Drivers

Question: What drove NVDA revenue growth in FY2023?
Status: good
Failure mode:
Notes: Verified deterministic answer for the pre-AI-boom comparison year. Confirms recipe works across regime changes — gaming weakness vs data center mix is captured.

### 13. PLTR FY2023 Revenue Drivers

Question: What drove PLTR revenue growth in FY2023?
Status: good
Failure mode:
Notes: Verified deterministic answer. Earlier-history check; segment facts and MD&A still period-aligned for FY before 2024 baseline.

### 14. PLTR FY2022 Revenue Drivers

Question: What drove PLTR revenue growth in FY2022?
Status: good
Failure mode:
Notes: Verified deterministic answer. Earliest year with full segment coverage; press-release period backfill (recent SEC ingestion work) holds.

### 15. AMD FY2022 Revenue Drivers

Question: What drove AMD revenue growth in FY2022?
Status: good
Failure mode:
Notes: Verified deterministic answer for AMD's strong-growth year (Xilinx integration). Segment facts capture Data Center and Embedded growth. Notable: AMD FY2024 produced an MD&A evidence_gap but FY2022 and FY2025 do not — suggests the gap is year-specific, not ticker-systemic.

### 16. AMD FY2025 Revenue Drivers

Question: What drove AMD revenue growth in FY2025?
Status: good
Failure mode:
Notes: Verified deterministic answer for the most recent fiscal year. Confirms recipe runs cleanly on most-recent-year evidence (no period-truncation bugs).

### 17. MSFT FY2024 Revenue Drivers

Question: What drove MSFT revenue growth in FY2024?
Status: partial
Failure mode: evidence_gap
Notes: Verified deterministic answer. MD&A surfaces Azure / Microsoft 365 / Gaming / Search drivers. Earnings-release chunks exist but none cleared the revenue-driver quality threshold (best_score=3, candidates=6) — MSFT press releases lean heavily on bullet-list revenue numbers without driver vocabulary the ranker rewards. Recurring weakness in earnings-release ranking for some mega-caps.

### 18. AMZN FY2023 Revenue Drivers

Question: What drove AMZN revenue growth in FY2023?
Status: good
Failure mode:
Notes: Verified deterministic answer. Confirms FY2024 success was not year-specific — recipe holds across multiple AMZN fiscal years.

## Tier 4: Edge Cases

Recent IPOs, spinoffs, and other history-thin companies to test the gap-reporting honesty.

### 19. CRWV FY2025 Revenue Drivers

Question: What drove CRWV revenue growth in FY2025?
Status: good
Failure mode: segment_gap (expected)
Notes: Verified deterministic answer. CRWV is a recent IPO; no prior-year segment data exists. Runtime correctly reports the missing prior-year segment matches as a structured gap rather than silently producing partial YoY analysis. Confirms gap reporting works on history-thin tickers.

### 20. GEV FY2024 Revenue Drivers

Question: What drove GEV revenue growth in FY2024?
Status: good
Failure mode: segment_gap (expected)
Notes: Verified deterministic answer. GEV is a recent GE spinoff; FY2024 has segment facts but FY2023 does not (pre-spinoff). Runtime correctly reports the missing prior-year segment matches as a gap.

## Summary: 20-Question Pass

- 17 good, 3 partial (AMD FY2024, VRT FY2024, MSFT FY2024), 0 failed.
- Period model holds across calendar (PLTR/AMZN/GOOGL/TSLA/VRT/CRWV/GEV), Jan (NVDA), June (MSFT), Nov (AVGO), Dec-27 (INTC, AMD).
- No HARD_FAIL across the set; no period_failed; no retrieval_failed; no template_bug.
- Recurring failure mode: `evidence_gap` from chunk-quality threshold (3/20). Pattern is mid-cap MD&A on VRT and earnings-release on MSFT/AMD — chunks exist but use vocabulary the revenue-signal ranker doesn't reward enough.
- Expected gaps on history-thin tickers (CRWV, GEV) are reported as structured gaps, not silent partial answers.
- No question required transcript evidence to be answered at the deterministic level. Transcript value will be visible on questions about guidance tone, walk-backs, and management voice — none of which this benchmark covers.
