# Transcript Ingest Build Plan

Status: final implementation plan as of 2026-04-27.

## Decision Anchor

**Goal.** Make earnings-call transcripts durable, searchable analyst evidence on the same `artifacts` / `artifact_text_units` substrate as press releases.

**User value.** The analyst runtime can search management commentary, compare calls across quarters, and ground deep-dive answers in primary call text.

**Constraints.**

- Raw first.
- PostgreSQL is the source of record.
- Fiscal truth is preserved via the two-clock model.
- Transcripts ship with steward coverage, expectations, and checks.
- No foreign-filer expansion.

**Evidence.**

- Live FMP stable API test returned transcript-date coverage for all 14 active company rows.
- FMP `earning-call-transcript-dates` returns compact `(quarter, fiscalYear, date)` rows.
- FMP `earning-call-transcript` returns a one-element list for present periods with keys: `symbol`, `period`, `year`, `date`, `content`.
- Missing periods return HTTP 200 with an empty list.
- `artifacts.artifact_type` already permits `transcript`.
- `artifact_text_units.unit_type` currently permits only `press_release`, so the schema change is there.

**Defer.**

- Speaker role/title/affiliation classifier.
- Prepared remarks vs. Q&A boundary detection.
- Sidecar transcript tables.
- Foreign-filer support.
- Realtime/live-call ingestion.
- Automation jobs.

**Universe.** Active US filers in `companies`. Backfill all available periods once; missing-only thereafter.

## Scope

In:

- FMP earnings-call transcripts via stable endpoints:
  - `earning-call-transcript-dates`
  - `earning-call-transcript`
- One transcript artifact per `(ticker, fiscal_year, fiscal_quarter)` call.
- One `artifact_text_units` row per parsed speaker turn.
- One fallback text-unit row when parsing fails or coverage is too low.
- Coverage matrix entry.
- `expected_coverage` rules.
- Transcript orphan check.

Out:

- `video_transcript`.
- Investor-day, conference, and guidance-update calls.
- Speaker role/title/affiliation enrichment.
- Q&A vs. prepared-remarks classification.
- Realtime/live-call ingestion.
- New transcript sidecar table.
- Per-ticker expectation overrides in code.

## Live API Shape

Dates endpoint:

```text
GET /stable/earning-call-transcript-dates?symbol=NVDA
```

Example row:

```json
{
  "quarter": 4,
  "fiscalYear": 2026,
  "date": "2026-02-25"
}
```

Transcript endpoint:

```text
GET /stable/earning-call-transcript?symbol=NVDA&year=2025&quarter=2
```

Example row shape:

```json
{
  "symbol": "NVDA",
  "period": "Q2",
  "year": 2025,
  "date": "2024-08-28",
  "content": "Operator: Good afternoon..."
}
```

Important API semantics:

- `year` is fiscal year, not calendar year.
- `period` is fiscal quarter label, e.g. `Q2`.
- Missing transcripts return `[]`, not a 404.
- Stable API does not return an FMP transcript id.

## Schema Migration

Add one migration:

```text
db/schema/020_artifact_text_units_transcript_unit_type.sql
```

Migration body:

```sql
ALTER TABLE artifact_text_units
    DROP CONSTRAINT artifact_text_units_type_check;

ALTER TABLE artifact_text_units
    ADD CONSTRAINT artifact_text_units_type_check
    CHECK (unit_type IN ('press_release', 'transcript'));
```

No new tables.

No changes to `artifacts`; `artifact_type='transcript'` is already permitted.

Regenerate `arrow_db_schema.html` in the same commit:

```bash
uv run scripts/gen_schema_viz.py
```

## Metadata Doc Cleanup

Update `docs/reference/artifact_metadata.md`, `transcript` section:

- Remove `fmp_transcript_id`; FMP stable API does not return it.
- Remove `speakers` from V1 metadata; speaker names live in `artifact_text_units.unit_title`.
- Keep `call_type` and `runtime_minutes` as optional keys.
- Omit `call_type='earnings'` in V1 because source + endpoint already imply it.
- Add supersession policy paragraph:

```text
FMP may republish corrected transcripts under the same source_document_id.
Transcript ingest writes artifacts through write_artifact. If the current
(source, source_document_id) row has the same raw_hash and canonical_hash,
the re-fetch is a no-op. If hashes differ, write_artifact inserts a new
artifact with supersedes set to the prior current artifact and stamps the
prior row's superseded_at. Old text units remain historical evidence; current
queries filter artifacts.superseded_at IS NULL.
```

## Cache Paths

Endpoint-mirrored layout:

```text
data/raw/fmp/earning-call-transcript-dates/{TICKER}.json
data/raw/fmp/earning-call-transcript/{TICKER}/FY{year}-Q{quarter}.json
```

Add helpers in `src/arrow/ingest/fmp/paths.py`:

- `fmp_transcript_dates_path(ticker: str) -> Path`
- `fmp_transcript_path(ticker: str, fiscal_year: int, fiscal_quarter: int) -> Path`

## Fetchers

New file:

```text
src/arrow/ingest/fmp/transcripts.py
```

Fetchers should match the existing FMP fetcher pattern: they accept `conn`, `ingest_run_id`, and `client`, write a `raw_responses` row, mirror bytes to filesystem cache, and return parsed data plus raw response identity.

Types:

```python
@dataclass(frozen=True)
class TranscriptDate:
    fiscal_year: int
    fiscal_quarter: int
    call_date: date


@dataclass(frozen=True)
class Transcript:
    ticker: str
    fiscal_year: int
    fiscal_quarter: int
    call_date: date
    content: str
    raw_row: dict[str, Any]


@dataclass(frozen=True)
class TranscriptDatesFetch:
    raw_response_id: int
    dates: list[TranscriptDate]


@dataclass(frozen=True)
class TranscriptFetch:
    raw_response_id: int
    transcript: Transcript | None
```

Functions:

```python
def fetch_transcript_dates(
    conn: psycopg.Connection,
    *,
    ticker: str,
    ingest_run_id: int,
    client: FMPClient,
) -> TranscriptDatesFetch:
    ...


def fetch_earning_call_transcript(
    conn: psycopg.Connection,
    *,
    ticker: str,
    fiscal_year: int,
    fiscal_quarter: int,
    ingest_run_id: int,
    client: FMPClient,
) -> TranscriptFetch:
    ...
```

Missing period behavior:

- HTTP 200 with `[]` returns `TranscriptFetch(..., transcript=None)`.
- Still write the raw response.

Cache semantics:

- Fetchers do not read from cache.
- Each fetch appends a raw response row.
- Missing-only ingest is driven by current transcript artifacts in PostgreSQL, not by filesystem cache.
- `--refresh` means re-fetch even when a current artifact exists.

## Parser

Parser can live in:

```text
src/arrow/ingest/fmp/transcript_parse.py
```

or in `src/arrow/ingest/fmp/transcripts.py` until it grows.

Boundary regex:

```python
SPEAKER_RE = re.compile(
    r"^(?P<speaker>[A-Z][\\w .,'\\-]{1,80}):\\s+(?P<text>.+)$"
)
```

Rules:

- Apply at start of line.
- Speaker marker starts a turn.
- Turn extends until the next speaker marker.
- Character offsets are against canonical content, matching existing text-unit extraction style.
- Keep raw `Speaker: utterance` span in `TextUnit.text`.
- Name length cap prevents obvious false positives.
- Do not classify roles, affiliation, prepared remarks, or Q&A in V1.

Output:

```python
@dataclass(frozen=True)
class ParsedTurn:
    ordinal: int
    speaker: str
    text: str
    start_offset: int
    end_offset: int
```

Coverage gate:

- Parsed turns must cover at least 80% of source characters.
- If zero turns or coverage below threshold, treat parse as failed.
- Failed parse emits one `unparsed` fallback row.

Extraction methods:

- Deterministic turns: `extraction_method='deterministic'`, `confidence=0.9`.
- Fallback: `extraction_method='unparsed_fallback'`, `confidence=0.0`.

## Orchestration

New file:

```text
src/arrow/agents/fmp_transcripts.py
```

Public callable:

```python
def ingest_transcripts(
    conn: psycopg.Connection,
    tickers: list[str],
    *,
    refresh: bool = False,
    actor: str = "operator",
) -> dict[str, Any]:
    ...
```

`actor` is accepted for the shared human/agent action path. In V1, do not add schema solely for actor. If useful, include it in run counts/details rather than adding a migration.

Flow:

1. Open `ingest_runs` row with `vendor='fmp'`, `run_kind='manual'`, and ticker scope.
2. For each ticker, look up company row.
3. Fetch transcript dates.
4. Select candidates:
   - default: candidates with no current transcript artifact for the same `source_document_id`
   - `refresh=True`: all candidates
5. Fetch each selected transcript.
6. Skip `None` transcripts but count them.
7. Normalize each transcript into artifact + text units + chunks.
8. Close `ingest_runs` with counts.
9. On failure, close run as failed with structured error details.

Counts:

- `transcript_dates_fetched`
- `transcripts_requested`
- `transcripts_missing`
- `transcripts_fetched`
- `artifacts_inserted`
- `artifacts_existing`
- `artifacts_superseded`
- `text_units_inserted`
- `text_chunks_inserted`

Successful one-period ingest should have non-zero counts so `zero_row_runs` stays quiet.

## Fiscal Anchoring

This is the load-bearing rule.

Use FMP fields verbatim:

- `year` -> `artifacts.fiscal_year`
- `period='Q2'` -> `artifacts.fiscal_quarter=2`
- `fiscal_period_label='FY2025 Q2'`

Resolve `period_end` by querying current `financial_facts`:

```sql
SELECT period_end
FROM financial_facts
WHERE company_id = %s
  AND fiscal_year = %s
  AND fiscal_quarter = %s
  AND period_type = 'quarter'
  AND superseded_at IS NULL
ORDER BY period_end DESC
LIMIT 1;
```

If no row exists, raise:

```python
class MissingFiscalAnchor(RuntimeError):
    ...
```

Message:

```text
Run `uv run scripts/backfill_fmp.py {ticker}` before ingesting transcripts for {ticker} FY{year} Q{quarter}.
```

No nominal quarter-end fallback.

No NULL `period_end`.

Derive calendar fields from resolved `period_end` using `derive_calendar_period`.

Use consistent period keys:

- `fiscal_period_label='FY2025 Q2'`
- `fiscal_period_key='FY2025 Q2'`
- Keep ticker identity in `source_document_id`, not in `fiscal_period_key`.

## Artifact Write

Use `write_artifact` only. Do not custom-insert transcript artifacts.

Source document id:

```python
source_document_id = (
    f"fmp:earning-call-transcript:{ticker}:FY{year}-Q{quarter}"
)
```

Write shape:

```python
write_artifact(
    conn,
    ingest_run_id=ingest_run_id,
    artifact_type="transcript",
    source="fmp",
    source_document_id=source_document_id,
    body=raw_json_bytes,
    company_id=company.id,
    ticker=ticker,
    fiscal_period_key=f"FY{year} Q{quarter}",
    fiscal_year=year,
    fiscal_quarter=quarter,
    fiscal_period_label=f"FY{year} Q{quarter}",
    period_end=resolved_period_end,
    period_type="quarter",
    calendar_year=calendar.calendar_year,
    calendar_quarter=calendar.calendar_quarter,
    calendar_period_label=calendar.calendar_period_label,
    title=f"{ticker} earnings call FY{year} Q{quarter}",
    content_type="application/json",
    language="en",
    published_at=fmp_call_date_as_timestamptz,
    artifact_metadata={},
)
```

Supersession policy:

- Same `(source, source_document_id)` and same hashes -> no-op.
- Same `(source, source_document_id)` and different hashes -> insert new artifact with `supersedes=old.id`; stamp old `superseded_at`.
- Old text units remain as historical evidence.
- Current queries filter `artifacts.superseded_at IS NULL`.

## Text Units

For each parsed turn:

- `unit_type='transcript'`
- `unit_key=f'turn:{ordinal:03d}'`
- `unit_title=speaker`
- `text=raw "Speaker: utterance" span`
- `start_offset`, `end_offset` against canonical content
- `unit_ordinal=ordinal`
- `extractor_version='fmp_transcript_units_v1'`
- `extraction_method='deterministic'`
- `confidence=0.9`
- `fiscal_period_key='FY2025 Q2'`

Fallback row:

- `unit_type='transcript'`
- `unit_key='unparsed'`
- `unit_title=f'{ticker} transcript (unparsed)'`
- `text=full canonical content`
- `start_offset=0`
- `end_offset=len(content)`
- `unit_ordinal=1`
- `extractor_version='fmp_transcript_units_v1'`
- `extraction_method='unparsed_fallback'`
- `confidence=0.0`
- `fiscal_period_key='FY2025 Q2'`

## Chunks

Reuse the existing text chunk shape:

- One or more `artifact_text_chunks` per text unit.
- `heading_path=[speaker]` for parsed turns.
- `heading_path=['Unparsed Transcript']` for fallback.
- `search_text` from existing `search_text_from_text` helper.
- Chunk sizing should mirror existing press-release chunking unless a helper extraction requires small refactoring.

## CLI

New script:

```text
scripts/ingest_transcripts.py
```

Usage:

```bash
uv run scripts/ingest_transcripts.py NVDA [AMD ...]
uv run scripts/ingest_transcripts.py --refresh NVDA
```

Thin wrapper around `ingest_transcripts(...)`.

Modify:

```text
scripts/ingest_company.py
```

Add transcript step after:

1. FMP financials
2. FMP segments
3. FMP employees

and before SEC qualitative.

The order matters because transcript normalization requires existing financial facts for fiscal anchoring.

## Steward Coverage

Update:

```text
src/arrow/steward/coverage.py
```

Add `transcript` to `VERTICALS`:

```python
VERTICALS = (
    "financials",
    "segments",
    "employees",
    "sec_qual",
    "press_release",
    "transcript",
)
```

Add aggregate branch:

```sql
SELECT company_id, COUNT(*),
       COUNT(DISTINCT fiscal_period_key),
       MIN(published_at), MAX(published_at)
FROM artifacts
WHERE company_id = ANY(%s)
  AND artifact_type = 'transcript'
  AND superseded_at IS NULL
GROUP BY company_id;
```

Add detail query in `compute_ticker_coverage`.

Update module docstring vertical list.

## Steward Expectations

Update:

```text
src/arrow/steward/expectations.py
```

Add:

```python
Expectation("transcript", "present", {}),
Expectation("transcript", "min_periods", {"count": 20}),
Expectation("transcript", "recency", {"max_age_days": 150}),
```

Known first-day exceptions:

- CRWV has 4 available transcript periods.
- GEV has 9 available transcript periods.

Keep the 20-period standard. Do not add per-ticker overrides. Suppress legitimate short-history findings in the audit trail with reason:

```text
IPO/spinoff history shorter than standard
```

Set expiry to the fiscal quarter when each is projected to cross 20 available periods.

Update `commands_by_vertical` in `expected_coverage.py`:

```python
"transcript": f"uv run scripts/ingest_transcripts.py {ticker}",
```

## Steward Orphan Check

New file:

```text
src/arrow/steward/checks/transcript_artifact_orphans.py
```

Parallel to `sec_artifact_orphans.py`.

Flags current transcript artifacts with zero `artifact_text_units` rows:

```sql
SELECT a.id, a.ticker, a.company_id, a.fiscal_period_key, a.published_at
FROM artifacts a
WHERE a.artifact_type = 'transcript'
  AND a.superseded_at IS NULL
  AND NOT EXISTS (
        SELECT 1
        FROM artifact_text_units u
        WHERE u.artifact_id = a.id
  );
```

Do not refactor to generic `artifact_orphans` yet.

## Test Bar

Required tests before commit:

1. Migration round-trip:
   - `unit_type='transcript'` accepted.
   - unknown value rejected.

2. Fiscal anchor required:
   - `_normalize_one` raises `MissingFiscalAnchor` when no current `financial_facts` row exists.
   - succeeds when one does.
   - resolved `period_end` matches facts row exactly.

3. Two-clock truth:
   - NVDA FY2025 Q2 artifact has:
     - `fiscal_year=2025`
     - `fiscal_quarter=2`
     - `period_end=2024-07-28`
     - `calendar_year=2024`
     - `calendar_quarter=3`
   - Verify against the actual facts row.

4. Parser deterministic path:
   - Real CRWV / GEV / MSFT JSON fixtures from live API tests.
   - coverage >= 80%.
   - expected turn counts within +/- 2.

5. Parser fallback path:
   - synthetic content with no speaker markers.
   - one `unparsed` row.
   - `extraction_method='unparsed_fallback'`.
   - `confidence=0.0`.

6. Supersession:
   - write v1 -> one artifact, N units, `superseded_at IS NULL`.
   - write v2 with different raw hash and same `source_document_id` -> two artifact rows.
   - old row: `superseded_at = v2.published_at`, `supersedes IS NULL`.
   - new row: `superseded_at IS NULL`, `supersedes = old.id`.
   - both sets of text units still exist.
   - `compute_coverage_matrix` reports `row_count=1`, `period_count=1`.
   - current filtered search returns only v2 turns.

7. Identical re-fetch no-op:
   - same raw hash -> `write_artifact` returns `created=False`.
   - no new artifact.
   - no new units.

8. Coverage + expectations:
   - synthetic ticker with 0 periods creates present/min-period/recency findings as expected.
   - synthetic ticker with 4 periods creates min-period finding.
   - synthetic ticker with 20 recent periods passes.

9. `zero_row_runs` quiet:
   - successful one-period ingest writes non-zero counts.

10. CLI smoke:
   - `uv run scripts/ingest_transcripts.py NVDA --limit 1` or equivalent dev-scoped option succeeds.
   - cache files are written under expected paths.
   - raw response rows are written.

## Build Sequence

All commits should be independently revertable.

1. Schema + docs:
   - migration `020`
   - regenerate `arrow_db_schema.html`
   - update `docs/reference/artifact_metadata.md`

2. Fetchers + raw cache:
   - `src/arrow/ingest/fmp/transcripts.py`
   - path helpers
   - tests for raw response/cache behavior
   - smoke test NVDA dates + one transcript

3. Parser + normalize one transcript:
   - parser
   - fiscal anchor resolver
   - `_normalize_one`
   - text units
   - chunks
   - tests 2, 3, 4, 5, 7, 9

4. Supersession proof:
   - fixture-driven v1 -> v2 flow
   - test 6

5. CLI + normal company orchestration:
   - `scripts/ingest_transcripts.py`
   - insert transcript step into `scripts/ingest_company.py`

6. Steward:
   - coverage vertical
   - expectations
   - command map
   - `transcript_artifact_orphans`
   - test 8

7. Backfill:
   - first run: `NVDA AMD CRWV`
   - second run: remaining active tickers
   - add CRWV and GEV suppressions with projected-completion expiries

## Backfill Plan

Current live inventory from FMP transcript dates:

```text
AMD    76
AMZN   81
AVGO   82
CRWV    4
GEV     9
GOOGL  82
INTC   81
META   55
MSFT   81
MU     79
NVDA   80
PLTR   22
TSLA   60
VRT    26
```

Total available periods: 818.

20-period expectation load: 253 required periods across current universe.

Recommendation:

- Backfill all available transcript periods once.
- Use missing-only ingest after that.
- Add suppressions for CRWV and GEV expected-coverage min-period findings.

## Accepted Risks

**Parser brittleness.** FMP is a third-party source. If content format changes, the 80% coverage gate falls back to `unparsed`, preserving searchability without fabricating wrong turns.

**Fiscal anchor ordering.** Transcripts cannot normalize before FMP financial facts. This is intentional. Standalone transcript ingest hard-fails with a clear operator message.

**No automation in V1.** Manual ingest and backfill prove the path first. Scheduled refresh comes after the vertical is stable.

**No speaker enrichment.** V1 preserves speaker labels in `unit_title`; roles and affiliations wait until they are needed by a real analyst workflow.

## Definition Of Done

- Migration applied.
- Schema visualization regenerated.
- Metadata docs updated.
- Transcript fetchers write raw responses and filesystem cache.
- Transcript artifacts write through `write_artifact`.
- Fiscal anchors come from current financial facts only.
- Parsed speaker turns are searchable through `artifact_text_units` and `artifact_text_chunks`.
- Fallback transcripts remain searchable.
- Supersession behavior is covered by tests.
- Coverage matrix includes `transcript`.
- `expected_coverage` includes transcript standards.
- Transcript orphan check is registered and tested.
- `scripts/ingest_transcripts.py` runs for at least NVDA.
- `scripts/ingest_company.py` includes transcript ingest in the normal flow.
