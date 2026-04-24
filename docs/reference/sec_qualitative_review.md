# SEC Qualitative Review Guide

This guide explains how to judge whether Arrow's SEC qualitative corpus is
complete enough, clean enough, and readable enough to trust for AI-assisted
analysis.

It is written for operator review, not implementation detail.

## What The Qualitative Layer Is

The SEC qualitative layer is deterministic extraction from filings into
AI-ready retrieval tables:

```text
artifacts
  source filing records: 10-K, 10-Q, 8-K

artifact_sections
  full extracted filing sections: MD&A, Risk Factors, Business, etc.

artifact_section_chunks
  smaller retrieval units built from those sections
```

This is not yet an AI judgment layer. It is the evidence layer an AI will read
later.

## The Main Audit Command

Generate a readable HTML report:

```bash
uv run scripts/audit_sec_qualitative.py --html outputs/nvda_qual_audit.html NVDA
open outputs/nvda_qual_audit.html
```

Terminal-only audit:

```bash
uv run scripts/audit_sec_qualitative.py NVDA
```

Offline / database-only audit:

```bash
uv run scripts/audit_sec_qualitative.py --db-only NVDA
```

## What Good Looks Like

For a healthy ticker, the audit summary should show:

```text
missing expected filings:  0
unexpected stored filings: 0
weak/missing extractions:  0
```

Warnings can still exist. They are review targets, not automatic failures.

For NVDA, the intended current SEC qualitative shape is:

```text
10-K: 5 artifacts, FY2022-FY2026
10-Q: 15 artifacts, FY2022 Q1-FY2026 Q3
8-K: 21 earnings artifacts in the 5-year filing window
```

Five complete fiscal years of core 10-K / 10-Q evidence means:

```text
5 annual 10-Ks + 15 quarterly 10-Qs = 20 core filings
```

There is no Q4 10-Q. Q4 is covered by the annual 10-K.

## Reading The Report

### Status Cards

The top cards answer:

```text
Do we have the expected filings?
Did any extraction fail?
How much chunked evidence exists?
Are there warnings worth reviewing?
```

Possible statuses:

```text
PASS
  No missing filings, no unexpected filings, no weak extraction, no warnings.

PASS_WITH_WARNINGS
  Corpus and extraction are usable, but there are review notes such as optional
  sections missing or short tail chunks.

FAIL
  Missing expected filings, unexpected out-of-window filings, or weak/missing
  extraction.
```

### Filing Coverage

This proves the corpus boundary.

For 10-K / 10-Q, Arrow uses fiscal-year retention:

```text
last 5 complete fiscal years
plus any pre-window quarters needed to complete the first included fiscal year
```

For earnings 8-Ks, retention is filing-date based over the same calendar
window because 8-Ks do not carry the 10-K / 10-Q fiscal section contract.

Interpretation:

```text
expected == stored
  Good. The live SEC feed and database agree.

expected > stored
  Bad. We are missing filings.

stored > expected
  Bad or stale. We retained filings outside the current policy window.
```

### Section Extraction Health

This answers whether filings produced structured sections.

Important fields:

```text
artifacts
  Number of stored filings of that type.

with_sections
  Number of filings that produced at least one extracted section.

sections
  Number of extracted full sections.

chunks
  Number of retrieval chunks derived from those sections.

min_conf
  Lowest parser confidence in the group.

repairs
  Count of lower-confidence repaired sections.

fallbacks
  Count of full-body fallback sections.
```

Amended filings are partial in v1. A `10-K/A` or `10-Q/A` is not expected to
repeat the complete base filing section inventory. If an amendment produces
usable repaired text, the report lists it as an amendment note rather than a hard
failure.

### What `min_conf = 1.0` Means

`min_conf` is parser confidence, not AI confidence.

```text
1.0
  Clean deterministic heading match.

0.85 to 1.0
  Deterministic extraction still considered strong.

0.0 to 0.85
  Repair path or lower-confidence extraction.

0.0 with unparsed_fallback
  The extractor could not identify sections and stored the normalized filing
  body as one fallback section.
```

Read `1.0` as:

```text
The extractor found this section using the normal deterministic filing-heading
rules.
```

Do not read it as:

```text
The extracted text is guaranteed perfect.
```

### Section Matrix

The section matrix is the fastest way to inspect structure.

Green cells mean:

```text
This section was extracted for this filing.
```

Gray / missing cells mean:

```text
This standard section key was not extracted from this filing.
```

Some missing sections are normal.

For 10-Ks:

```text
Item 1C Cybersecurity
  Expected to be missing in older filings because the requirement is newer.
```

For 10-Qs:

```text
Part I Item 4 Controls
Part II Item 1A Risk Factors
Part II Item 5 Other Information
  Often omitted, not applicable, or phrased differently.
```

Critical sections to care about first:

```text
10-K Item 1 Business
10-K Item 1A Risk Factors
10-K Item 7 MD&A
10-Q Part I Item 2 MD&A
```

If those are missing, treat it as serious.

### Missing Standard Sections

This table is a review queue.

It does not mean Arrow failed. It means:

```text
This filing did not produce one of the section keys Arrow knows how to extract.
```

Interpret missing sections by importance:

```text
Critical section missing
  Investigate immediately.

Optional section missing
  Usually acceptable.

New-regulatory section missing from older filings
  Usually expected.
```

The report does not yet label critical vs optional sections. Until it does,
review missing sections using the critical list above.

Amendments are excluded from this full-standard-section queue. Use the
Amendments section of the HTML report to see what a partial amendment contributed.

### Chunk Shape

Chunks are the units retrieval will hand to an AI.

Good chunks:

```text
stay inside one filing section
preserve nearby headings
start and end at readable boundaries
are large enough to carry context
are small enough to retrieve precisely
```

Chunk statistics:

```text
p05 / p50 / p95
  Size distribution. Useful for spotting very small or very large chunks.

max / min
  Extreme chunk sizes.
```

### Chunk Outliers

Outliers are chunks with unusual size.

Short outliers are often harmless:

```text
signature blocks
legal tail text
controls boilerplate
end-of-section leftovers
```

More important warning signs:

```text
chunk starts in one section and ends in another
chunk includes the next filing heading
chunk is table noise with little prose
chunk is too large to inspect comfortably
```

If a chunk ends with text like:

```text
PART II. OTHER INFORMATION
```

that suggests the section boundary may run slightly long. It is usually not
catastrophic, but it is a good future cleanup target.

### Retrieval Smoke Tests

Retrieval smoke tests ask:

```text
If I search for an analyst-style topic, do plausible chunks come back?
```

They are not final AI quality. They are sanity checks.

Good retrieval snippets:

```text
come from plausible filings
come from plausible sections
mention the searched topic or nearby relevant context
are readable
```

Weak retrieval snippets:

```text
come from unrelated legal boilerplate
match only generic words
pull risk-factor language when MD&A would be better
```

Current retrieval is simple Postgres full-text search. It is useful, but broad.
Future ranking should use section filters, period filters, and query intent.

## Manual Review Workflow

Use this checklist when reviewing a ticker:

1. Generate the HTML report.
2. Confirm filing coverage is green.
3. Confirm no weak / fallback extractions.
4. Check the section matrix for critical missing sections.
5. Skim missing section warnings and decide whether they are expected.
6. Review chunk outliers for boundary problems.
7. Run retrieval smoke tests for topics you care about.
8. If retrieval snippets look plausible, the ticker is ready for first-pass AI
   analysis.

## How This Supports AI Accuracy

The AI layer should never answer from memory or from an invisible blob.

Every AI answer should eventually provide:

```text
claim
supporting filing
supporting section
supporting chunk id
short evidence snippet
caveat / missing evidence note
```

The audit report is the pre-flight check that those chunks exist and are
reasonable.

## Current Limitations

Known limitations:

```text
8-Ks are stored as artifacts but not sectionized.
Tables inside filing text are not parsed structurally.
Missing sections are not yet classified as critical vs optional.
Retrieval is FTS-only and can be noisy.
Chunk boundary cleanup is still needed for some filing tails.
```

These are normal next-step improvements, not reasons to reject the current
NVDA qualitative corpus.

## Practical Interpretation For NVDA

If NVDA reports:

```text
PASS_WITH_WARNINGS
missing expected filings: 0
unexpected stored filings: 0
weak/missing extractions: 0
```

then the corpus is usable for first-pass qualitative analysis.

Warnings should be read as:

```text
optional or newer sections may be absent
some chunk tails are low-value signature/legal text
retrieval needs ranking improvements before final analyst-grade synthesis
```

The most important evidence sections are present and chunked.
