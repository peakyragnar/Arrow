# ASR Transcripts Operator Runbook

Status: live as of 2026-05-08. Three calls ingested through the v1
pipeline (CRWV, AMD, NET — all FY2026 Q1). Four execs voice-enrolled
(Lisa Su, Jean Hu, Matthew Prince, Thomas Seifert).

This is the working manual for self-transcribing earnings calls.
It complements [`docs/architecture/asr_transcripts_ingest_plan.md`](../architecture/asr_transcripts_ingest_plan.md),
which covers the architecture; this doc covers what to do day-to-day.

## When to use this

After a company in your active universe reports earnings:

1. The call typically posts on the IR website within 1-4 hours of the live event.
2. FMP's transcript may also appear within a day. We're going-forward on ASR for
   verbatim text, speaker turns, voiceprint identification — these are not in FMP.
3. Operator runs `scripts/ingest_asr_transcript.py` once per call to capture the
   transcript end-to-end into the database, alongside (or eventually superseding)
   any FMP version.

## Quick start

A single CLI command runs the entire pipeline for one call. Pick the audio source
flag based on the IR vendor:

```bash
# Q4Inc-hosted calls (most US public companies). Just paste the static.events.q4inc.com URL:
uv run scripts/ingest_asr_transcript.py CRWV \
  --fiscal FY2026Q1 --call-date 2026-05-07 \
  --audio-url "https://static.events.q4inc.com/edited-recordings/<event_id>/<uuid>.mp4"

# Encrypted HLS (Mediasite, etc — e.g. AMD). Use the YouTube mirror as fallback:
uv run scripts/ingest_asr_transcript.py AMD \
  --fiscal FY2026Q1 --call-date 2026-05-05 \
  --youtube-id zV00rljp-8A

# Q4 with future Playwright auto-discovery (not all vendors yet):
uv run scripts/ingest_asr_transcript.py CRWV \
  --fiscal FY2026Q1 --call-date 2026-05-07 \
  --q4-event-id 658779279
```

Wall time: **~15-20 minutes** end-to-end on M-series Mac.

## The three audio source paths

### Path 1 — Q4Inc (most common)

Q4 Inc hosts the dominant share of US-public-company IR webcasts. After the call
ends and the edited replay posts (typically a few hours later), the audio sits at
a public CloudFront URL:

```
https://static.events.q4inc.com/edited-recordings/{event_id}/{uuid}.mp4
```

**That URL is publicly downloadable from any machine** — no auth, no Cloudflare
challenge, no session cookies. The hard part is finding the UUID, which is only
exposed by the Q4 player's JavaScript.

**Operator workflow** (~30 seconds):

1. Open Chrome. Navigate to the company's IR events page (e.g.
   `investors.coreweave.com/events-and-presentations/default.aspx`).
2. Click the Q1 2026 (or whichever call you want) replay link.
3. The Q4 player loads. It may require a name+email registration form — fill in
   junk credentials ("Arrow Research", `research@arrow.example`).
4. **Open DevTools** (⌥⌘I) → **Network** tab → filter `mp4`.
5. Hit **Play** in the player.
6. A request to `static.events.q4inc.com/edited-recordings/...mp4` appears.
7. Right-click → **Copy** → **Copy link address**.
8. Paste the URL into the CLI command above.

### Path 2 — encrypted HLS (AMD-style)

Some IR vendors (Mediasite, Sonic Foundry) ship audio as AES-encrypted HLS
chunks. The `.ts` files are publicly fetchable but the bytes are encrypted; the
m3u8 manifest + decryption keys are gated behind the player's session.

**You'll know you've hit this case if**:
- DevTools Network shows many `.ts` files (all named with one path like
  `irn839xp_en_enc1_audio_part1.ts`)
- A direct `curl` of the `.ts` URL returns 200 but `file <path>` reports `data`
  (not MPEG-TS)
- Or `ffmpeg -i <path>` says "Invalid data found when processing input"

**Workaround**: use the YouTube mirror. IR-coverage YouTube channels (EARNMOAR,
Investing 101, Benzinga) routinely upload full earnings calls within hours. Find
the video ID:

```bash
# Search:
yt-dlp ytsearch10:"$TICKER Q1 2026 earnings conference call" \
  --skip-download --print "id,title,duration_string,uploader"

# Pick a video that:
#  - has duration ~1-1.5h (full call) NOT 5-15min (commentary)
#  - title says "Earnings Conference Call" not "Reaction" or "Breakdown"
#  - uploader is a known IR-coverage channel (EARNMOAR is the cleanest)
```

Then ingest with `--youtube-id <video_id>`.

### Path 3 — anything else (manual paste)

If a vendor doesn't match either path, the CLI accepts any `http(s)` URL:

```bash
uv run scripts/ingest_asr_transcript.py TICKER \
  --fiscal FY2026Q1 --call-date 2026-05-07 \
  --audio-url "https://some-vendor-cdn.example.com/audio.mp3"
```

The orchestrator will tag it as `vendor='manual'` and run the full pipeline.

## After a run — verify

Three quick checks:

### 1. Speaker breakdown looks right

```sql
SELECT unit_title, COUNT(*) turn_count, SUM(LENGTH(text)) chars
FROM artifact_text_units
WHERE artifact_id = <new_artifact_id>
GROUP BY unit_title ORDER BY chars DESC;
```

You want to see:
- **CEO**: largest by chars (CEOs do prepared remarks + Q&A — typically 30-40 min of speech)
- **CFO**: second-largest (financial review section)
- **Operator**: many turns, small chars (intros each Q&A, opens/closes call)
- **Investor Relations**: 1-2 turns, ~2-3k chars (safe-harbor opening only)
- **Analysts**: 4-7 named (Vivek Arya, Stacy Rasgon, etc.)
- **Some `SPEAKER_NN` raw labels OK**: pyannote misclusters short fragments. As
  long as they're <5% of total chars, ignore.

If CEO/CFO are stored as raw `SPEAKER_NN` or as `<unknown CEO>`, the Haiku
name-extraction step failed. See § "Common issues" → "Exec name not extracted".

### 2. Voiceprints enrolled

```sql
SELECT person_name, role, embedding_dim
FROM speaker_voiceprints sv JOIN companies c ON c.id=sv.company_id
WHERE c.ticker = 'TICKER' AND superseded_at IS NULL;
```

After a successful first-call ingest you should see CEO + CFO enrolled. (COO and
President are also in the auto-enrollment role list; they'll only show up if
they spoke and the IR introduced them by name.)

If the names look mistyped (Whisper homophone, e.g. `Gene Hu` instead of
`Jean Hu`), see § "Common issues" → "Whisper homophones".

### 3. Compare against FMP if available

If the call was also FMP-ingested, you can compare side-by-side:

```sql
SELECT a.id, a.source,
       (SELECT COUNT(*) FROM artifact_text_units WHERE artifact_id=a.id) text_units,
       (SELECT SUM(LENGTH(text)) FROM artifact_text_units WHERE artifact_id=a.id) chars
FROM artifacts a JOIN companies c ON c.id=a.company_id
WHERE c.ticker = 'TICKER' AND a.fiscal_period_label = 'FY2026 Q1'
  AND a.artifact_type = 'transcript' AND a.superseded_at IS NULL;
```

Our ASR run typically has **more text units and more chars** than FMP for the
same call — Whisper preserves verbatim text, disfluencies, and substantive
closing remarks that FMP's editorial pass strips. (Empirically: NET FY2026 Q1
FMP=43.6k chars vs our ASR=59.2k chars on the same audio.)

## Voiceprint enrollment — first-call vs subsequent

### First call for a ticker (fresh enrollment)

The pipeline depends on **getting the exec names right from the IR's introduction**.
Sequence:

1. IR person says: "Joining the call today are Lisa Su, our CEO, and Jean Hu,
   our CFO."
2. Whisper transcribes that sentence.
3. Haiku reads the IR's transcribed intro and returns `{"ceo": "Lisa Su", "cfo": "Jean Hu"}`.
4. State-machine identifies which pyannote-labeled speaker block belongs to each
   role (next non-IR/non-operator speaker after IR's handoff = CEO; following
   handoff target = CFO).
5. Voiceprints are enrolled keyed on the extracted names.

If Whisper mishears a name, you'll get the wrong canonical label — but the
**voice embedding itself is still correct**. See "Whisper homophones" below.

### Subsequent calls for the same ticker (auto-identification)

The pipeline runs identically EXCEPT in step 4 above:

```
For each pyannote-labeled speaker block:
    compute mean embedding from this call's segments
    cosine-match against every speaker_voiceprints row for company_id=X
    if best match >= 0.55:
        replace SPEAKER_NN with the matched person_name
```

**Whisper's name accuracy doesn't matter for already-enrolled execs.** The voice
match identifies them by acoustic fingerprint, regardless of how their name
appears in the transcribed text. The architecture decouples voice identity
(durable) from name spelling (cheap to fix once).

## Common issues + fixes

### Exec name not extracted (`<unknown CEO>` placeholder)

Symptom: speaker breakdown shows `<unknown CEO>` or `<unknown CFO>` as a label.

Cause: the IR's introduction phrasing didn't match Haiku's expectations, OR
Haiku wasn't reachable (API key missing).

Fix:

1. Run the relabel script — it'll re-run Haiku name extraction over the existing
   text:
   ```bash
   uv run scripts/relabel_asr_transcript.py <artifact_id> --dry-run
   uv run scripts/relabel_asr_transcript.py <artifact_id>
   ```
2. If the relabel still doesn't resolve names, look at the IR's intro turn:
   ```sql
   SELECT text FROM artifact_text_units
   WHERE artifact_id=<artifact_id> AND unit_ordinal <= 5;
   ```
   If the IR didn't actually introduce execs by name (some companies just have
   the CEO open directly), manually update via SQL:
   ```sql
   UPDATE artifact_text_units SET unit_title = 'Lisa Su (CEO)'
   WHERE artifact_id = <id> AND unit_title = '<unknown CEO>';
   ```
3. If voiceprints didn't enroll, manually insert one:
   ```sql
   INSERT INTO speaker_voiceprints (
     company_id, person_name, role, embedding, embedding_dim, embedding_model
   ) VALUES (<co>, 'Lisa Su', 'ceo', '<vector>', 256, 'pyannote/embedding-3.1');
   ```
   But you'd need the embedding from the diarization output — easier to re-run
   the full pipeline.

### Whisper homophones (Gene/Jean, Jordan/Jorden, etc.)

Symptom: voiceprint enrolled as `Gene Hu` when the actual CFO is `Jean Hu`.

Cause: Whisper hears phonetically; "Gene" and "Jean" are homophones in English.
Initial-prompt biasing helps but isn't bulletproof.

Fix (cheap — one SQL update):

```sql
-- Update text labels everywhere this name appears
UPDATE artifact_text_units
   SET unit_title = 'Jean Hu',
       text = REPLACE(text, 'Gene Hu', 'Jean Hu')
WHERE artifact_id = <id> AND unit_title = 'Gene Hu';

-- Update the voiceprint name (embedding stays — it's correct, only label was wrong)
UPDATE speaker_voiceprints SET person_name = 'Jean Hu'
WHERE company_id = <co> AND person_name = 'Gene Hu';
```

Important: **the voiceprint embedding is correct** even when the name was
mistyped. The embedding captures her actual voice acoustics. Renaming the text
label doesn't require re-running any audio processing.

### Pyannote misclustered short fragments (raw `SPEAKER_NN` labels persist)

Symptom: the speaker breakdown shows `SPEAKER_05`, `SPEAKER_10`, etc. for
analysts or short interjections.

Cause: pyannote's clustering occasionally splits a single speaker's brief
mid-sentence pieces (or an analyst's follow-up question) into a separate cluster
label. Pyannote-level edge case; not fixable post-hoc from text alone.

Mitigation: ignore unless the affected fragments exceed ~5% of total chars. A
diarization-tuning pass (lower `min_duration_off` threshold) is on the v3
roadmap.

### Analyst name typos (Stacy Rasgen, CJ Mews)

Symptom: in the operator's "next question comes from..." intros, an analyst's
name is misspelled. Both the operator's quote AND the analyst's turn label use
the typo.

Cause: Whisper mis-heard the operator. Sonnet's correction pass doesn't have a
ground-truth list of Wall Street analyst names to check against.

Fix: build a per-call analyst glossary and re-run Sonnet correction. Or fix
manually:

```sql
UPDATE artifact_text_units
   SET unit_title = 'Stacy Rasgon',
       text = REPLACE(text, 'Stacy Rasgen', 'Stacy Rasgon')
WHERE artifact_id = <id> AND text ILIKE '%Stacy Rasg%';
```

For systematic improvement, add the well-known analyst directory to Sonnet's
glossary. v2 work.

### LLM correction pass takes too long / 10-minute timeout

Symptom: pipeline fails at the correction step with "Streaming is required for
operations that may take longer than 10 minutes."

Cause: the Anthropic SDK enforces streaming when `max_tokens` is high enough to
risk a 10-minute call.

Fix: the orchestrator already caps `max_tokens=16000` to stay under the
threshold. If a call is unusually long (90+ minutes) and you hit this, switch
the call to streaming mode in `post_correct_with_llm`.

## The relabel script

`scripts/relabel_asr_transcript.py <artifact_id>` re-applies the speaker
identification logic to an already-persisted transcript **without re-downloading
audio or re-running Whisper/pyannote**. Useful when:

- The orchestrator's name-extraction logic has been improved and you want to
  apply it to historical artifacts
- An IR's intro phrasing was previously unrecognized and you've since taught
  the regex/Haiku layer to handle it
- You want to fix specific name-mapping issues without re-paying for Whisper +
  Sonnet

What it does:
1. Reads `artifact_text_units` for the given artifact
2. Detects the IR introduction block (regex on standard intro phrases)
3. Extracts CEO/CFO names from IR's text via Haiku (or regex fallback)
4. Walks the conversation linearly to remap labels
5. Updates `unit_title` columns in place via SQL UPDATE

What it does NOT do:
- Touch voiceprint embeddings
- Re-run audio processing
- Change the artifact_id

Use `--dry-run` first to preview the remap:

```bash
uv run scripts/relabel_asr_transcript.py 78809 --dry-run
```

## Cost reference

Per-call API cost:
- **Whisper** (mlx-whisper local) — $0
- **Pyannote** (local) — $0
- **Haiku name extraction** — ~$0.001
- **Sonnet full transcript correction** — ~$0.10
- **Total per call** — ~$0.10

At 50 tickers × 4 calls/year = 200 calls/year, this is ~$20/year in API spend.
Wall time is ~15-20 min/call on Mac local; could parallelize across cores or
move to cloud GPU for batch.

Audio storage:
- During run: ~30-100 MB MP4/MP3 + ~120 MB WAV in `data/scratch/`
- After successful persist: **all binaries deleted**. Only `audio_artifacts.deleted_at`
  timestamp + sha256 hash + source URL retained for provenance.

## Re-running an existing call

If you re-run `ingest_asr_transcript.py` for a (ticker, fiscal_period) that's
already persisted:

- **Same audio_hash + same model**: `write_artifact` returns the existing
  artifact_id with `created=False`. The pipeline does Whisper + Pyannote work
  but discards the output. Wasteful but harmless.
- **Different result** (e.g. you changed the initial-prompt or the orchestrator
  was upgraded): a new artifact gets created with `supersedes` pointing at the
  old one. Old artifact gets `superseded_at = now()`. Old text_units stay
  intact (historical evidence) but queries with `superseded_at IS NULL` filter
  return the new version.

Voiceprint behavior on re-run:
- `speaker_voiceprints` has `UNIQUE INDEX (company_id, person_name) WHERE
  superseded_at IS NULL`. If the same name resolves on re-run, no duplicate
  insert. If a different name resolves (e.g. you fixed the homophone), the new
  one inserts and the old one should be superseded — though the orchestrator
  doesn't currently auto-supersede prior voiceprints. Manual cleanup after
  re-runs may be needed.

## Hard rule: foreign filers excluded

Per the standing rule in [`docs/architecture/asr_transcripts_ingest_plan.md`](../architecture/asr_transcripts_ingest_plan.md):

**Do NOT ingest 20-F or 6-K filers** (TSM, ASML, BABA, TM, SAP, etc.). The
fiscal-period model and audit pipeline assume US filers; foreign-filer support
is a category of work, not edge-case patching.

## Where everything lives

| Artifact | Path |
|---|---|
| CLI | [`scripts/ingest_asr_transcript.py`](../../scripts/ingest_asr_transcript.py) |
| Relabel script | [`scripts/relabel_asr_transcript.py`](../../scripts/relabel_asr_transcript.py) |
| Orchestrator | [`src/arrow/agents/asr_transcripts.py`](../../src/arrow/agents/asr_transcripts.py) |
| Q4Inc adapter | [`src/arrow/ingest/audio/q4inc.py`](../../src/arrow/ingest/audio/q4inc.py) |
| YouTube adapter | [`src/arrow/ingest/audio/youtube.py`](../../src/arrow/ingest/audio/youtube.py) |
| Generic adapter | [`src/arrow/ingest/audio/generic.py`](../../src/arrow/ingest/audio/generic.py) |
| Schema | [`db/schema/025_asr_transcripts.sql`](../../db/schema/025_asr_transcripts.sql) |
| Architecture | [`docs/architecture/asr_transcripts_ingest_plan.md`](../architecture/asr_transcripts_ingest_plan.md) |
| Tests | [`tests/unit/test_audio_q4inc.py`](../../tests/unit/test_audio_q4inc.py) |

Scratch + cache (gitignored, deleted after persist):
| Path | Lifetime |
|---|---|
| `data/scratch/audio/{vendor}/{TICKER}/...` | until cleanup_audio() at end of pipeline |
| `data/scratch/wav/{TICKER}/...` | until cleanup at end of pipeline |
| `data/scratch/transcripts/whisper-turbo/{TICKER}/...json` | persists (small, useful for debugging) |
| `data/scratch/diarize/...` | persists when proof_diarize.py is run; orchestrator runs in-memory |
| `~/.cache/huggingface/` | persists indefinitely (model weights) |

## Known limitations (v1)

These are intentional tradeoffs documented for operator awareness — fixed in v2/v3:

1. **No automated event_id discovery for Q4 calls.** Operator looks up the
   event_id (from the IR events page) or pastes the audio URL once per call.
   Playwright auto-discovery of the URL given an event_id is implemented in
   `q4inc.py::discover_audio_url` but isn't yet hooked into a per-vendor IR
   page parser.
2. **Encrypted-HLS vendors require YouTube fallback.** AMD's Mediasite-hosted
   audio is unfetchable directly. v2 work would explore Playwright-driven
   replay capture using the user's authenticated Chrome session.
3. **Whisper homophones on first enrollment.** Gene/Jean, Jordan/Jorden, etc.
   The voiceprint embedding is unaffected, only the text label. Manual SQL fix
   takes ~30 seconds; future v2 work would build a canonical exec-name table
   from SEC 10-K Item 1 to bias both Whisper's prompt and Sonnet's correction.
4. **Pyannote occasionally misclusters short fragments.** Brief mid-sentence
   interjections from a single speaker can get split into a separate cluster
   label. Affects 1-5% of text per call. Not fixable post-hoc from text alone.
5. **Analyst-name typos in operator speech.** Stacy Rasgen vs Rasgon, CJ Mews
   vs Muse. Sonnet's correction pass doesn't have a Wall Street analyst
   directory. Per-call glossary build is v2 work.
6. **No auto-supersession of prior voiceprints on re-run.** If you fix a Gene→Jean
   typo by re-running the pipeline, you may end up with both voiceprints active.
   Manual cleanup needed.

## Operator checklist (per call)

Pre-flight:
- [ ] Confirm ticker is in `companies` table
- [ ] Note the call date and fiscal period
- [ ] Identify the audio source vendor (Q4 / encrypted-HLS / other)

Run:
- [ ] Get the audio URL or YouTube video_id
- [ ] Run `scripts/ingest_asr_transcript.py` with appropriate flags
- [ ] Wait for completion (15-20 min)

Post-flight:
- [ ] Verify the artifact landed alongside FMP rows (if any)
- [ ] Check speaker breakdown — CEO + CFO should be the largest, named
- [ ] Verify voiceprints enrolled if first-time for this ticker
- [ ] Spot-check transcript text for any homophone issues (Gene/Jean class)
- [ ] Apply manual SQL fixes if needed (homophones, analyst typos)

## See also

- [`docs/architecture/asr_transcripts_ingest_plan.md`](../architecture/asr_transcripts_ingest_plan.md) — architecture + design rationale
- [`docs/architecture/system.md`](../architecture/system.md) — overall Arrow architecture
- `db/schema/025_asr_transcripts.sql` — the four ASR-specific tables (audio_artifacts, asr_transcripts, speaker_voiceprints, speaker_segments)
- `scripts/proof_*.py` — original proof scripts from the CRWV validation; useful for understanding pipeline internals when debugging
