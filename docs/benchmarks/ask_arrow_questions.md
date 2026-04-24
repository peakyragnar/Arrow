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

## Tier 3: Repeat For Coverage

Add more explicit revenue-driver questions here after the first six expose the
initial failure modes.
