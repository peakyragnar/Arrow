# ASR Transcripts Ingest Plan

Status: active plan; pre-implementation. Design discussion captured 2026-05-08.

This document is the v1 plan to **replace FMP earnings-call transcripts** with a self-owned ASR pipeline that downloads audio from company IR webcasts, transcribes locally with open-source models, diarizes speakers, and stores the result on Arrow's existing artifact substrate.

It sits next to the existing financials, segments, employees, prices, estimates, SEC qualitative, press-release, and (legacy) FMP transcript verticals.

The plan was scoped through the Elon Loop — the surface ran to ~25 components; what survives is ~14. Cuts are deliberate; each deferred item carries the trigger that would bring it back.

## Goal

Own the transcription path end-to-end. Concretely:

- Stop calling FMP transcript endpoints. Sunset `src/arrow/agents/fmp_transcripts.py`, `scripts/build_transcript_universe.py`, `scripts/bulk_seed_transcripts.py`.
- Acquire audio from company IR webcasts (Q4Inc-first), transcribe with local Whisper-class models, diarize with pyannote, identify named speakers via voiceprint enrollment.
- Produce **verbatim, word-timestamped, named-speaker-attributed transcripts** that beat what FMP ships on every dimension that matters for analyst workflows.
- Keep the audio binary ephemeral; persist only the transcript, the speaker voiceprints, and provenance.

## Why Replace FMP

| FMP transcripts give us | What's missing for analyst-grade work |
|---|---|
| Cleaned text, paragraph-level | No verbatim disfluencies, no word-level timestamps |
| Crude speaker labels | No named-speaker identification (just labels), often wrong on Q&A handoffs |
| Coverage of US filers | No coverage for non-earnings calls (investor days, conferences) |
| One source of truth | Subject to FMP's editorial decisions and licensing terms |

We control the new pipeline. We also keep all 818 historical FMP transcript-periods in `artifacts` (`source='fmp'`) — the pipeline change is **forward-looking only**.

## What's In v1

| Component | Why it earns its keep |
|---|---|
| Q4Inc IR-page adapter | Q4 hosts the dominant share of US earnings webcasts; one solid adapter covers most of the focus universe. |
| Audio acquisition layer (`yt-dlp` + `ffmpeg`) | Handles MP4 + HLS m3u8; works on YouTube fallback for free. |
| `audio_artifacts` table | Provenance for every transcribed call, even after the binary is deleted. |
| `asr_transcripts` table, keyed on (audio_hash, model, model_version) | Deterministic re-runs; supports model upgrades without overwriting history. |
| `speaker_segments` table | Diarization output joinable to text offsets on the existing `artifact_text_units`. |
| `speaker_voiceprints` table | Pyannote 192-dim embeddings keyed on `(company_id, person_name, role)`. Enroll once per CEO/CFO. |
| Local default ASR: mlx-whisper large-v3-turbo | Free, fast on Apple Silicon (~2-5 min per 1-hour call), MIT-licensed. |
| Diarization: pyannote 3.1 | Best open-source DER; supports embedding extraction for our voiceprint flow. |
| LLM post-correction | Per-ticker glossary mined from existing FMP transcripts, applied via Claude/GPT pass for proper nouns + numerics. |
| Audio-delete-after-transcribe lifecycle | Audio is reproducible from source URL; storing it long-term is wasteful and creates a redistribution footprint. |
| Transcribe interface (swappable adapter) | `TranscribeBackend` protocol with `WhisperLocal` impl in v1; `DeepgramHosted` impl in v2 if we hit a quality wall. |
| Steward coverage check (`asr_transcript_present`) | Every recent FY-quarter has an ASR transcript or a logged reason for absence. |
| Steward freshness check (`asr_transcript_recency`) | Flag focus-universe tickers whose latest call hasn't been transcribed within N days of `published_at`. |
| FMP cutover marker on `companies` (`asr_cutover_period`) | Per-company explicit point at which we stopped accepting FMP transcripts. Default: forward from first ASR ingest. |

## What's Deferred or Cut

| Item | Action | Trigger to revisit |
|---|---|---|
| `DeepgramHosted` adapter | Defer to v2 | If `WhisperLocal` + LLM correction tops out below quality bar on a focus-universe call. |
| Per-vendor non-Q4 IR adapters (Notified, EQS, West, Investis) | Defer | First focus-universe ticker that isn't on Q4Inc. Build adapters reactively. |
| YouTube as primary source | Cut | Operator confirms IR-page is dominant; YouTube only as last-resort fallback. |
| Live-call capture (real-time recording) | Defer | Until backfill of archive replays is solid and a "transcript within an hour" need is real. |
| WhisperX integration (its own VAD/alignment stack) | Defer | If our own VAD + timestamp alignment is materially worse than what mlx-whisper provides. |
| `gpt-4o-transcribe` adapter | Defer | After Deepgram comparison; if Deepgram doesn't close the quality gap. |
| Q&A vs prepared-remarks classification | Defer | First analyst workflow that needs the distinction (e.g., "what changed in Q&A vs remarks?"). |
| Speaker role classifier (analyst affiliation, etc.) | Cut from v1 | When voiceprint enrollment alone fails to surface roles for non-exec speakers. |
| Investor-day / conference / fireside calls | Defer | After earnings-call pipeline stable. Same substrate, different acquisition. |
| Foreign filers (20-F / 6-K) | Cut | Per existing memory — foreign-filer support is its own focused project. |
| Real-time WER scoring against FMP historical | Defer | One-shot WER comparison on first proof call is enough to validate quality; don't ship as recurring check yet. |
| Voiceprint backfill from FMP historicals | Defer | After v1 voiceprint flow proven on a single new call. |
| Replacing `artifacts.artifact_type='transcript'` semantics | Cut | Existing FMP rows stay as-is; new ASR rows use the same type but `source='asr'`. No artifact-type fork. |

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│ Acquisition Layer                                            │
│   ┌──────────────┐  ┌──────────────┐  ┌─────────────┐       │
│   │ Q4Inc adapter│  │ YouTube adp  │  │ Manual adp  │       │
│   └──────┬───────┘  └──────┬───────┘  └─────┬───────┘       │
│          └─────────────────┴────────────────┘               │
│                          │                                   │
│                          ▼                                   │
│   data/scratch/audio/{ticker}/{event_id}.{ext}              │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Transcription Layer (swappable)                              │
│   ┌────────────────────┐    ┌──────────────────────┐        │
│   │ WhisperLocal       │    │ DeepgramHosted (v2)  │        │
│   │ mlx-whisper turbo  │    │ Nova-3 + diarize     │        │
│   └─────────┬──────────┘    └──────────┬───────────┘        │
│             └──────────────┬───────────┘                     │
│                            ▼                                 │
│   ASRResult { segments, words, language }                    │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Diarization + Speaker ID                                     │
│   pyannote/speaker-diarization-3.1                           │
│        │                                                     │
│        ├─► raw segments (SPEAKER_00, _01, ...)              │
│        │                                                     │
│        └─► pyannote/embedding → cosine match against         │
│            speaker_voiceprints → named labels                │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ LLM Post-Correction                                          │
│   Glossary (mined once from FMP historicals)                 │
│   + Claude/GPT pass: fix proper nouns, ticker symbols,       │
│     numerics. Conservative — don't rewrite.                  │
└─────────────────────────────────────────────────────────────┘
                           │
                           ▼
┌─────────────────────────────────────────────────────────────┐
│ Persistence                                                  │
│   audio_artifacts        (source URL, hash, captured_at,     │
│                           deleted_at)                        │
│   asr_transcripts        (audio_hash, model, model_version)  │
│   speaker_segments       (start_ms, end_ms, speaker_label,   │
│                           voiceprint_match_id)               │
│   artifacts              (artifact_type='transcript',        │
│                           source='asr')                      │
│   artifact_text_units    (one per speaker turn)              │
│   artifact_text_chunks   (existing chunking applied)         │
│                                                              │
│   THEN: delete audio binary from data/scratch/               │
└─────────────────────────────────────────────────────────────┘
```

## Schema (4 new tables)

### `audio_artifacts`

```sql
CREATE TABLE audio_artifacts (
    id              bigserial PRIMARY KEY,
    company_id      bigint NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    fiscal_year     int NOT NULL,
    fiscal_quarter  int NOT NULL,
    fiscal_period_key text NOT NULL,
    source_vendor   text NOT NULL,         -- 'q4inc' | 'youtube' | 'manual' | ...
    source_url      text NOT NULL,
    source_event_id text,                  -- vendor-specific identifier
    audio_hash      text NOT NULL,         -- sha256 of the downloaded binary
    audio_format    text NOT NULL,         -- 'mp3' | 'mp4' | 'm4a' | ...
    duration_sec    int,
    captured_at     timestamptz NOT NULL,
    deleted_at      timestamptz,           -- non-null after binary purge
    UNIQUE (company_id, fiscal_period_key, source_url)
);
```

### `asr_transcripts`

```sql
CREATE TABLE asr_transcripts (
    id              bigserial PRIMARY KEY,
    audio_artifact_id bigint NOT NULL REFERENCES audio_artifacts(id) ON DELETE CASCADE,
    artifact_id     bigint REFERENCES artifacts(id),  -- linked once we promote into artifacts
    model           text NOT NULL,         -- 'whisper-large-v3-turbo' | 'deepgram-nova-3' | ...
    model_version   text NOT NULL,         -- 'mlx-community/whisper-large-v3-turbo@2024-10-01' | ...
    backend         text NOT NULL,         -- 'whisper_local' | 'deepgram_hosted' | ...
    language        text NOT NULL DEFAULT 'en',
    raw_response_id bigint REFERENCES raw_responses(id),
    raw_payload_hash text NOT NULL,
    word_timestamps boolean NOT NULL,
    transcribed_at  timestamptz NOT NULL,
    UNIQUE (audio_artifact_id, model, model_version)
);
```

### `speaker_segments`

```sql
CREATE TABLE speaker_segments (
    id                  bigserial PRIMARY KEY,
    asr_transcript_id   bigint NOT NULL REFERENCES asr_transcripts(id) ON DELETE CASCADE,
    ordinal             int NOT NULL,
    start_ms            int NOT NULL,
    end_ms              int NOT NULL,
    raw_speaker_label   text NOT NULL,     -- 'SPEAKER_00' from pyannote
    voiceprint_match_id bigint REFERENCES speaker_voiceprints(id),
    voiceprint_confidence numeric(5,4),    -- cosine similarity, 0..1
    text_offset_start   int NOT NULL,
    text_offset_end     int NOT NULL,
    UNIQUE (asr_transcript_id, ordinal)
);
```

### `speaker_voiceprints`

```sql
CREATE TABLE speaker_voiceprints (
    id              bigserial PRIMARY KEY,
    company_id      bigint NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    person_name     text NOT NULL,
    role            text NOT NULL,         -- 'ceo' | 'cfo' | 'ir' | 'analyst' | 'operator' | ...
    embedding       vector(192),           -- pgvector if available; otherwise float8[]
    source_audio_artifact_id bigint REFERENCES audio_artifacts(id),
    source_clip_path text,                 -- 30-sec MP3 kept for re-extraction
    enrolled_at     timestamptz NOT NULL,
    superseded_at   timestamptz,
    superseded_by   bigint REFERENCES speaker_voiceprints(id),
    UNIQUE (company_id, person_name, enrolled_at)
);
```

Note on pgvector: not currently in Arrow per `docs/architecture/system.md`. v1 stores embeddings as `float8[]` with cosine match in Python; revisit pgvector if voiceprint cardinality grows past ~10k.

## Cache Layout

Endpoint-mirrored, matching existing FMP convention:

```
data/scratch/audio/{vendor}/{TICKER}/{event_id}.{ext}    ← deleted after transcribe
data/voiceprints/{TICKER}/{role}_{person_slug}.mp3        ← persistent 30-sec clips
data/raw/asr/{backend}/{TICKER}/{event_id}.json           ← raw ASR output
data/raw/diarize/pyannote-3.1/{TICKER}/{event_id}.json    ← raw diarization output
```

`data/scratch/` is a new top-level dir for ephemeral binaries. It's gitignored. Audio files there are deleted by the pipeline after successful transcription.

## Component Design

### 1. Audio acquisition layer

```python
# src/arrow/ingest/audio/contracts.py

@dataclass(frozen=True)
class AudioEvent:
    vendor: str
    event_id: str
    title: str
    event_date: date
    page_url: str

@dataclass(frozen=True)
class AudioRef:
    vendor: str
    event_id: str
    download_url: str
    format_hint: str   # 'mp3' | 'mp4' | 'm3u8'

@dataclass(frozen=True)
class AudioFetch:
    audio_artifact_id: int
    local_path: Path
    duration_sec: int
    audio_hash: str

class AudioSource(Protocol):
    def discover_events(self, ticker: str) -> list[AudioEvent]: ...
    def resolve_audio(self, event: AudioEvent) -> AudioRef: ...
    def fetch_audio(self, ref: AudioRef, *, dest: Path) -> AudioFetch: ...
```

V1 implementation: `Q4IncSource` only. `YouTubeSource` and `ManualSource` are stubs.

### 2. Transcription layer (swappable)

```python
# src/arrow/ingest/asr/contracts.py

@dataclass(frozen=True)
class ASRWord:
    text: str
    start_sec: float
    end_sec: float
    confidence: float | None

@dataclass(frozen=True)
class ASRSegment:
    text: str
    start_sec: float
    end_sec: float
    words: list[ASRWord]

@dataclass(frozen=True)
class ASRResult:
    backend: str
    model: str
    model_version: str
    language: str
    segments: list[ASRSegment]
    raw_payload: dict

class TranscribeBackend(Protocol):
    def transcribe(
        self,
        audio_path: Path,
        *,
        initial_prompt: str | None,
        language: str = "en",
    ) -> ASRResult: ...
```

V1 backend: `WhisperLocal` (mlx-whisper, large-v3-turbo, MLX framework, Apple Silicon native).
V2 backend: `DeepgramHosted` (Nova-3, batch endpoint, with `diarize=true`).

### 3. Diarization layer

```python
# src/arrow/ingest/asr/diarize.py

@dataclass(frozen=True)
class DiarSegment:
    start_sec: float
    end_sec: float
    raw_speaker: str        # 'SPEAKER_00' from pyannote

@dataclass(frozen=True)
class DiarResult:
    segments: list[DiarSegment]
    embeddings: dict[str, list[float]]   # raw_speaker → 192-dim vector
```

Single function:

```python
def diarize_with_embeddings(
    audio_path: Path, *, hf_token: str
) -> DiarResult: ...
```

Two pyannote pipelines run sequentially:
1. `pyannote/speaker-diarization-3.1` for segments
2. `pyannote/embedding` for per-speaker mean embedding

### 4. Speaker identification

```python
def identify_speakers(
    diar: DiarResult,
    voiceprints: list[VoiceprintRow],
    *,
    threshold: float = 0.55,
) -> dict[str, VoiceprintMatch | None]: ...
```

Mean-embedding-per-raw-speaker + cosine similarity against enrolled voiceprints. Threshold calibrated on first 5 calls; adjust if false-positive or false-negative rate is unacceptable. **Calibrate against live data before locking the threshold** (per memory rule).

### 5. LLM post-correction

```python
def post_correct(
    raw_text: str,
    *,
    glossary: TickerGlossary,
    model: str = "claude-sonnet-4-6",
) -> CorrectedText: ...
```

Glossary built once per ticker by extracting proper nouns + numeric tokens from existing FMP historical transcripts. Stored as `data/glossaries/{TICKER}.json`. Refreshed quarterly or on operator demand.

Correction prompt is conservative: "fix proper-noun and numeric misspellings only. Do not rewrite. Do not summarize. Preserve all disfluencies and verbatim text." Returns diff annotations for provenance.

### 6. Audio lifecycle (delete-after-transcribe)

Pipeline orchestrator:

1. `fetch_audio` → write binary to `data/scratch/audio/...`
2. `transcribe` → ASR result
3. `diarize_with_embeddings` → diar result
4. `identify_speakers` → labels
5. `post_correct` → corrected text
6. `write_artifact` + insert `asr_transcripts`, `speaker_segments`
7. `extract_voiceprint_clips` → for each newly-named speaker not yet enrolled, save 30-sec clip to `data/voiceprints/{TICKER}/`
8. `unlink(audio_path)` + `UPDATE audio_artifacts SET deleted_at = now()`

Steps 1-7 are typed callables with `actor` parameter (per "agent-shaped seams" memory).

### 7. FMP transcript sunset

Cutover model:
- `companies.asr_cutover_period` (text, nullable) — if set, no FMP transcript ingest is accepted at or after this fiscal period.
- Default: set to first successful ASR ingest's fiscal period for that ticker.
- Existing FMP transcript rows untouched.
- `fmp_transcripts.py` retained but flagged with module-level deprecation comment; eventually deleted in a follow-up commit once we're confident no callers remain.

## Steward Coverage / Expectations / Checks

Per "new verticals ship with their checks" memory, all of the following ship in the same commit as the schema migration:

**Coverage** (`src/arrow/steward/coverage.py`):
- Add `asr_transcript` to VERTICALS.
- Aggregate query: `artifacts WHERE artifact_type='transcript' AND source='asr' AND superseded_at IS NULL`.
- Detail query mirroring existing transcript pattern.

**Expectations** (`src/arrow/steward/expectations.py`):
```python
Expectation("asr_transcript", "present", {}),
Expectation("asr_transcript", "recency", {"max_age_days": 14}),
```

No `min_periods` for v1 — we're going-forward only. Backfill is operator-driven, not coverage-enforced.

**Checks**:
- `asr_transcript_orphan` — current ASR transcript artifacts with zero `artifact_text_units`. Parallel to existing `transcript_artifact_orphans`.
- `audio_artifact_undeleted` — `audio_artifacts` rows older than 7 days with `deleted_at IS NULL` and a successful `asr_transcripts` row. Indicates a delete-step bug.
- `voiceprint_unmatched_speakers` — segments where `voiceprint_match_id IS NULL` for executives with enrolled voiceprints. Indicates threshold drift or new speaker.

Threshold calibration: run all three checks against the first 5 ASR calls before promoting them to recurring (per memory: calibrate before coding).

**Commands map** (`expected_coverage.py`):
```python
"asr_transcript": f"uv run scripts/ingest_asr_transcripts.py {ticker}",
```

## CLI

New script:

```
scripts/ingest_asr_transcripts.py
```

Usage:
```bash
uv run scripts/ingest_asr_transcripts.py CRWV --event-url https://...
uv run scripts/ingest_asr_transcripts.py CRWV --discover  # use Q4Inc adapter
uv run scripts/ingest_asr_transcripts.py CRWV --fiscal FY2026Q1  # most recent matching event
```

Modify `scripts/ingest_company.py`:
- Replace existing `ingest_transcripts` step (FMP) with the ASR step, gated on `companies.asr_cutover_period`.
- For periods before cutover, the ingest skips silently.

## Build Sequence

All commits independently revertable. Each commit ships with its tests and (if user-facing) its steward checks.

### Commit 0 — proof on CRWV (no schema, no Arrow integration)

Smallest provable slice. Goal: convince ourselves the local stack works on a real call before touching the database.

Steps:
1. Discover CRWV's IR events page; locate 2026-05-07 call.
2. Resolve audio URL (Q4Inc player → MP4 or m3u8).
3. Download with `yt-dlp` to a scratch dir.
4. Transcribe with mlx-whisper turbo + initial_prompt seeded from public CRWV facts.
5. Diarize with pyannote 3.1; print raw labeled segments.
6. Hand-inspect ~30 segments. Decide pass/fail.

Output: one JSON file. No DB writes. No Python module structure yet. Pure script.

### Commit 1 — schema + Q4Inc adapter

- Migration `db/schema/021_asr_transcripts.sql` (4 new tables).
- Regenerate `arrow_db_schema.html`.
- `src/arrow/ingest/audio/q4inc.py` adapter implementing `AudioSource`.
- Tests for IR-page parsing on at least 2 Q4Inc-hosted IR sites.

### Commit 2 — local ASR backend

- `src/arrow/ingest/asr/whisper_local.py`.
- mlx-whisper integration; first-run model download.
- Initial-prompt builder from public ticker facts.
- Tests on a fixture audio clip (10-30 seconds is enough).

### Commit 3 — diarization + speaker ID

- `src/arrow/ingest/asr/diarize.py` with both pyannote pipelines.
- Voiceprint enrollment script: `scripts/enroll_voiceprint.py CRWV "Mike Intrator" ceo path/to/clip.mp3`.
- Cosine-match identification.
- HF token plumbing in `.env`.

### Commit 4 — orchestrator + audio-delete lifecycle

- `src/arrow/agents/asr_transcripts.py::ingest_asr_transcript(...)`.
- `scripts/ingest_asr_transcripts.py` thin CLI wrapper.
- Delete-after-success logic; `audio_artifacts.deleted_at` stamping.
- Tests for the lifecycle (mock backends).

### Commit 5 — LLM post-correction

- `src/arrow/ingest/asr/correct.py`.
- Glossary builder: scan FMP historical transcripts for proper nouns.
- Conservative correction prompt + diff capture.
- Tests on a deliberately-misspelled fixture.

### Commit 6 — steward coverage + expectations + checks

- `coverage.py` adds `asr_transcript`.
- `expectations.py` entries.
- 3 new checks; all calibrated against first 5 ASR calls before recurring.
- Update `expected_coverage.py` command map.

### Commit 7 — `ingest_company.py` integration

- Wire ASR step in place of FMP transcript step, gated on `asr_cutover_period`.
- Sunset comment on `fmp_transcripts.py`.
- Update `docs/architecture/system.md` Build Order.

### Commit 8 — operator runbook + AGENTS.md

- New `docs/reference/asr_transcripts_operator_runbook.md`.
- Add this plan to AGENTS.md "Current Source Of Truth".
- Update `docs/architecture/system.md` v1 Tables status.

## Test Bar

Required before each commit:

1. **Q4Inc adapter parses real pages.** Two known-Q4 tickers, real HTML fixtures.
2. **Whisper backend runs deterministic.** Same audio + same prompt + temperature=0 → byte-identical transcript across runs.
3. **Audio-delete lifecycle.** After successful pipeline run: scratch file gone, `audio_artifacts.deleted_at` set, `asr_transcripts` row exists, `speaker_segments` rows exist.
4. **Re-run with new model is idempotent.** Same audio_hash + new model = new `asr_transcripts` row, no overwrite.
5. **Voiceprint match on enrolled speaker.** Seed clip → enroll → run on same speaker → cosine ≥ threshold → match.
6. **Voiceprint reject on unenrolled.** Same flow with unenrolled speaker → cosine < threshold → match_id IS NULL.
7. **Cutover gate.** With `asr_cutover_period='FY2026Q1'`, FMP transcript ingest for CRWV FY2026Q1 is rejected; FY2025Q4 still works.
8. **Steward coverage.** Synthetic ticker with 1 recent ASR transcript shows present-pass + recency-pass. With 0 → present-fail.
9. **WER smoke against FMP historical.** On one ticker with both an FMP transcript and our re-transcribed version: WER ≤ 15%. Treated as a sanity check, not a hard gate.

## Dependencies

```
brew install yt-dlp ffmpeg
uv pip install mlx-whisper pyannote.audio
```

Environment:
```
HF_TOKEN=<huggingface token, with pyannote/speaker-diarization-3.1 EULA accepted>
ANTHROPIC_API_KEY=<for LLM post-correction>
```

Disk:
- ~1.7 GB one-time model weights in `~/.cache/huggingface/`
- ~30-100 MB transient per call in `data/scratch/audio/` (deleted after transcribe)
- ~500 KB persistent per identified speaker in `data/voiceprints/`

## Universe

V1 universe is the existing 14 active US filers in `companies`:

```
AMD, AMZN, AVGO, CRWV, GEV, GOOGL, INTC, META, MSFT, MU, NVDA, PLTR, TSLA, VRT
```

Forward-only ingest from cutover. Operator can extend the focus list at any time by seeding new companies through the existing path.

## Accepted Risks

**Acquisition fragility.** Q4Inc may change page structure. Mitigation: Q4 adapter is small, well-tested, and the failure mode is "no events found" not "wrong events" — operator notices immediately.

**Whisper hallucination on silence.** mlx-whisper inherits Whisper's tendency to fabricate text on long silences. Mitigation: Silero VAD pre-pass (built into faster-whisper but not mlx-whisper, so we add it as a preprocessing step in the WhisperLocal backend).

**Diarization false-positives on similar voices.** CEO + CFO with similar pitch may be conflated. Mitigation: Voiceprint enrollment defeats this by anchoring on speaker identity, not just acoustic separation.

**FMP cutover dropping calls.** If our ASR path fails for a focus-universe call and `asr_cutover_period` is set, that call has no transcript at all. Mitigation: `asr_transcript_recency` steward check fires within 14 days; operator manually triggers fallback (re-run, escalate to Deepgram).

**Audio-delete semantics.** If a downstream bug means we want to re-transcribe and the audio is gone, we re-download from `audio_artifacts.source_url` and verify `audio_hash` matches. If the source URL has rotted, we lose the ability to re-transcribe that specific call. Mitigation: source URLs from Q4Inc are typically stable for 5+ years; accept the small risk.

**LLM post-correction overwrites correct text.** A conservative prompt + diff capture lets us audit. If correction quality is bad, disable that stage; the pipeline still produces a usable raw transcript.

## Definition Of Done

- All 4 schema tables created; `arrow_db_schema.html` regenerated.
- Q4Inc adapter handles at least 80% of focus-universe IR pages without manual intervention.
- A focus-universe call (CRWV 2026-05-07 first) is transcribed, diarized, named-speaker-attributed, and persisted.
- Audio binary deleted after success; `audio_artifacts.deleted_at` populated.
- Voiceprint enrollment flow proven on at least one CEO + one CFO.
- LLM post-correction applied; diff visible in operator-readable form.
- `scripts/ingest_company.py` wired to ASR path; `fmp_transcripts.py` sunset commented.
- Steward coverage, expectations, checks land in the same commit as the schema.
- Operator runbook at `docs/reference/asr_transcripts_operator_runbook.md`.
- This plan referenced in AGENTS.md "Current Source Of Truth".
- A documented WER comparison on at least one historical CRWV call (FMP-shipped vs re-transcribed) — used to decide whether the Deepgram v2 swap is needed.

## Future v2 — Deepgram swap path

If v1 quality is insufficient on the focus universe (operator reads transcripts and finds them unusable for analyst work):

1. Implement `src/arrow/ingest/asr/deepgram_hosted.py` against `TranscribeBackend`.
2. Add `DEEPGRAM_API_KEY` to `.env`.
3. Per-company `companies.transcribe_backend` override; default `whisper_local`, switch to `deepgram_hosted` for problem tickers.
4. A/B WER vs FMP historical to validate the swap actually moves the needle.
5. Skip the diarization step when using Deepgram (it ships diarization built-in); voiceprint identification still runs in our code.

Cost ceiling for the swap: ~$80-150/year at our universe size — small compared to the operator-time cost of unusable transcripts.
