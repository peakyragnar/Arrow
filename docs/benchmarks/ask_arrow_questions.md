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
Status:
Failure mode:
Notes:

### 2. NVDA FY2025 Revenue Drivers

Question: What drove NVDA revenue growth in FY2025?
Status:
Failure mode:
Notes:

## Tier 2: Same Recipe, Broader Companies

### 3. AMD FY2024 Revenue Drivers

Question: What drove AMD revenue growth in FY2024?
Status:
Failure mode:
Notes:

### 4. MSFT FY2025 Revenue Drivers

Question: What drove MSFT revenue growth in FY2025?
Status:
Failure mode:
Notes:

### 5. GOOGL FY2024 Revenue Drivers

Question: What drove GOOGL revenue growth in FY2024?
Status:
Failure mode:
Notes:

### 6. AMZN FY2024 Revenue Drivers

Question: What drove AMZN revenue growth in FY2024?
Status:
Failure mode:
Notes:

## Tier 3: Repeat For Coverage

Add more explicit revenue-driver questions here after the first six expose the
initial failure modes.
