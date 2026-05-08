-- audio_artifacts, asr_transcripts, speaker_voiceprints, speaker_segments:
-- the self-transcription vertical for earnings calls (and eventually
-- investor days, conferences).
--
-- See docs/architecture/asr_transcripts_ingest_plan.md for the full
-- design rationale.
--
-- Why these four tables, not zero (i.e., why not put everything on
-- the existing artifacts row):
--   The transcript TEXT continues to live on artifacts + artifact_text_units
--   exactly like FMP transcripts. These four new tables hold ASR-specific
--   *provenance*: what audio file was downloaded, what model produced the
--   text, who spoke when, whose voice we recognized. Mixing that into the
--   artifacts table would clutter every transcript query (FMP and ASR
--   alike) with NULL columns and force every consumer to know the
--   provenance shape.
--
-- Why audio_artifacts.deleted_at (not hard-deleted rows):
--   Audio is reproducible from source_url, so we delete the binary after
--   successful transcription to save disk and reduce redistribution
--   surface. But we keep the row so future re-runs can verify they're
--   working from the same source via audio_hash, and so we can
--   re-download by URL if a new model version comes along.
--
-- Why no pgvector for speaker_voiceprints in v1:
--   pgvector is not in Arrow (per docs/architecture/system.md). Voiceprint
--   cardinality is small: ~3 enrolled execs per company × ~50 companies
--   = a few hundred rows. Cosine similarity in Python is fast enough at
--   this volume. Revisit if cardinality grows past ~10k.
--
-- Why speaker_voiceprints carries embedding_dim explicitly:
--   pyannote 3.1's embedding model produces 192-dim vectors. Future
--   embedding models may differ. Storing dim alongside the array lets
--   us reject mixing vectors from different models at insert time.


-- 1. AUDIO_ARTIFACTS -----------------------------------------------------
--
-- Provenance for every earnings-call audio we have ever fetched. Persists
-- even after the binary is deleted from disk (deleted_at IS NOT NULL).
-- The pair (audio_hash, source_url) lets us reproduce the same input
-- byte-for-byte if we need to re-transcribe.

CREATE TABLE audio_artifacts (
    id                  bigserial   PRIMARY KEY,
    company_id          bigint      NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,

    -- Fiscal anchor (matches artifacts table convention)
    fiscal_year         smallint    NOT NULL,
    fiscal_quarter      smallint    NOT NULL,
    fiscal_period_key   text        NOT NULL,

    -- Source provenance
    source_vendor       text        NOT NULL,
    source_url          text        NOT NULL,
    source_event_id     text,                   -- vendor-specific (Q4 event_id, YouTube video_id)
    source_uuid         text,                   -- the per-recording UUID inside the vendor

    -- Content identity (sha256 of the downloaded bytes)
    audio_hash          bytea       NOT NULL,
    audio_format        text        NOT NULL,
    audio_size_bytes    bigint,
    duration_sec        integer,

    -- Lifecycle
    captured_at         timestamptz NOT NULL DEFAULT now(),
    deleted_at          timestamptz,            -- NULL = binary still on disk

    CONSTRAINT audio_artifacts_vendor_check
        CHECK (source_vendor IN ('q4inc', 'youtube', 'manual', 'other')),
    CONSTRAINT audio_artifacts_format_check
        CHECK (audio_format IN ('mp3', 'mp4', 'm4a', 'wav', 'webm')),
    CONSTRAINT audio_artifacts_quarter_range
        CHECK (fiscal_quarter BETWEEN 1 AND 4),
    CONSTRAINT audio_artifacts_hash_len
        CHECK (octet_length(audio_hash) = 32),
    CONSTRAINT audio_artifacts_unique_per_period
        UNIQUE (company_id, fiscal_period_key, source_url)
);

CREATE INDEX audio_artifacts_company_period_idx
    ON audio_artifacts (company_id, fiscal_year, fiscal_quarter);

-- "Audio still on disk" — used by the steward check audio_artifact_undeleted
CREATE INDEX audio_artifacts_undeleted_idx
    ON audio_artifacts (captured_at DESC)
    WHERE deleted_at IS NULL;


-- 2. ASR_TRANSCRIPTS -----------------------------------------------------
--
-- Raw ASR-model output per (audio, model) pair. The actual transcript
-- TEXT lives on artifacts + artifact_text_units (see write_artifact path).
-- This table is the back-pointer that says "this artifact came from this
-- audio file, transcribed by this model on this date."
--
-- Keying on (audio_artifact_id, model, model_version) lets us re-run with
-- a newer Whisper version cleanly: new row, same audio, same downstream
-- artifact_id linkage if we choose to supersede.

CREATE TABLE asr_transcripts (
    id                  bigserial   PRIMARY KEY,
    audio_artifact_id   bigint      NOT NULL REFERENCES audio_artifacts(id) ON DELETE CASCADE,
    artifact_id         bigint      REFERENCES artifacts(id) ON DELETE SET NULL,

    -- Backend identity (so re-runs with newer models append, not overwrite)
    backend             text        NOT NULL,
    model               text        NOT NULL,
    model_version       text        NOT NULL,
    language            text        NOT NULL DEFAULT 'en',
    word_timestamps     boolean     NOT NULL DEFAULT false,

    -- sha256 of the raw model output (Whisper JSON, etc.)
    raw_payload_hash    bytea       NOT NULL,

    transcribed_at      timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT asr_transcripts_backend_check
        CHECK (backend IN ('whisper_local', 'whisper_hosted', 'deepgram_hosted',
                           'gpt_4o_transcribe', 'other')),
    CONSTRAINT asr_transcripts_payload_hash_len
        CHECK (octet_length(raw_payload_hash) = 32),
    CONSTRAINT asr_transcripts_unique_per_model
        UNIQUE (audio_artifact_id, model, model_version)
);

CREATE INDEX asr_transcripts_artifact_idx
    ON asr_transcripts (artifact_id) WHERE artifact_id IS NOT NULL;

CREATE INDEX asr_transcripts_audio_idx
    ON asr_transcripts (audio_artifact_id);


-- 3. SPEAKER_VOICEPRINTS -------------------------------------------------
--
-- One enrolled embedding per (company, person, role). Embeddings are
-- pyannote 3.1's 192-dim vectors stored as PostgreSQL float8[].
-- Cosine match in Python at insert time of new diarization output gives
-- us auto-identification of recurring speakers (CEO/CFO across quarters).
--
-- Supersession via superseded_at + superseded_by lets us re-enroll with
-- a higher-quality clip when one becomes available, without losing the
-- audit trail of which prior calls were identified using which voiceprint.

CREATE TABLE speaker_voiceprints (
    id                          bigserial         PRIMARY KEY,
    company_id                  bigint            NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    person_name                 text              NOT NULL,
    role                        text              NOT NULL,

    -- The embedding itself + its dimensionality (so we can reject mismatches
    -- if a future model produces different-sized vectors)
    embedding                   double precision[] NOT NULL,
    embedding_dim               smallint          NOT NULL,
    embedding_model             text              NOT NULL,    -- e.g. 'pyannote/embedding-3.1'

    -- Provenance: which audio + which clip path produced this voiceprint
    source_audio_artifact_id    bigint            REFERENCES audio_artifacts(id) ON DELETE SET NULL,
    source_clip_path            text,             -- relative path under data/voiceprints/

    -- Lifecycle
    enrolled_at                 timestamptz       NOT NULL DEFAULT now(),
    superseded_at               timestamptz,
    superseded_by               bigint            REFERENCES speaker_voiceprints(id),

    CONSTRAINT speaker_voiceprints_role_check
        CHECK (role IN ('ceo', 'cfo', 'coo', 'president', 'ir', 'operator',
                        'analyst', 'other')),
    CONSTRAINT speaker_voiceprints_embedding_nonempty
        CHECK (array_length(embedding, 1) IS NOT NULL),
    CONSTRAINT speaker_voiceprints_embedding_dim_match
        CHECK (array_length(embedding, 1) = embedding_dim),
    CONSTRAINT speaker_voiceprints_dim_range
        CHECK (embedding_dim BETWEEN 64 AND 1024)
);

-- Active voiceprints by company — the retrieval path during identification
CREATE INDEX speaker_voiceprints_company_role_idx
    ON speaker_voiceprints (company_id, role)
    WHERE superseded_at IS NULL;

-- One active voiceprint per (company, person_name) at a time
CREATE UNIQUE INDEX speaker_voiceprints_one_active_per_person
    ON speaker_voiceprints (company_id, person_name)
    WHERE superseded_at IS NULL;


-- 4. SPEAKER_SEGMENTS ----------------------------------------------------
--
-- Diarization output for an asr_transcript: who spoke when, joinable to
-- text offsets in the canonical content (which is stored in artifacts +
-- artifact_text_units). Includes voiceprint-match results when we
-- identify a speaker by cosine similarity against speaker_voiceprints.

CREATE TABLE speaker_segments (
    id                      bigserial   PRIMARY KEY,
    asr_transcript_id       bigint      NOT NULL REFERENCES asr_transcripts(id) ON DELETE CASCADE,
    ordinal                 integer     NOT NULL,

    -- Time within the audio
    start_ms                integer     NOT NULL,
    end_ms                  integer     NOT NULL,

    -- Diarizer's anonymous label (SPEAKER_NN) — preserved for replay
    raw_speaker_label       text        NOT NULL,

    -- Identity (resolved via voiceprint match if enrolled)
    voiceprint_match_id     bigint      REFERENCES speaker_voiceprints(id) ON DELETE SET NULL,
    voiceprint_confidence   numeric(5,4),

    -- Position within the canonical transcript content (artifacts.canonical_body)
    -- Mirrors artifact_text_units.start_offset/end_offset semantics.
    text_offset_start       integer     NOT NULL,
    text_offset_end         integer     NOT NULL,

    CONSTRAINT speaker_segments_time_nonneg
        CHECK (start_ms >= 0 AND end_ms >= 0),
    CONSTRAINT speaker_segments_time_ordered
        CHECK (end_ms >= start_ms),
    CONSTRAINT speaker_segments_offsets_nonneg
        CHECK (text_offset_start >= 0 AND text_offset_end >= 0),
    CONSTRAINT speaker_segments_offsets_ordered
        CHECK (text_offset_end >= text_offset_start),
    CONSTRAINT speaker_segments_confidence_range
        CHECK (voiceprint_confidence IS NULL
               OR (voiceprint_confidence >= 0 AND voiceprint_confidence <= 1)),
    CONSTRAINT speaker_segments_unique_ordinal
        UNIQUE (asr_transcript_id, ordinal)
);

CREATE INDEX speaker_segments_transcript_idx
    ON speaker_segments (asr_transcript_id);

-- For "find calls where this exec spoke" queries
CREATE INDEX speaker_segments_voiceprint_idx
    ON speaker_segments (voiceprint_match_id)
    WHERE voiceprint_match_id IS NOT NULL;
