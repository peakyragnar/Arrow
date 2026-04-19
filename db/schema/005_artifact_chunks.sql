-- artifact_chunks: derived retrieval units from artifacts.
--
-- Regeneratable: if chunking strategy changes, truncate and re-derive from
-- artifacts. Not source truth. Cheap to rebuild.
--
-- chunk_type starts with a deterministic initial set. Presentations' slide
-- blocks and video-transcript timestamp spans will extend this via a later
-- migration (ALTER TABLE ... DROP CONSTRAINT ... ADD CONSTRAINT ...) when
-- those ingest paths land.
--
-- FTS design (ADR-0008):
--   text        = raw display text (as-is from the source)
--   search_text = normalized FTS input (ingest-populated when the default
--                 'english' tokenization over raw text is noisy — e.g.
--                 remove speaker labels on transcript turns, drop table
--                 structure chrome, strip boilerplate preambles)
--   tsv         = GENERATED ALWAYS AS to_tsvector('english',
--                   COALESCE(search_text, text)) STORED
-- Ingest can opt into better FTS input without drift — the generated
-- column keeps Postgres as the invariant.

CREATE TABLE artifact_chunks (
    id                bigserial   PRIMARY KEY,
    artifact_id       bigint      NOT NULL REFERENCES artifacts(id) ON DELETE RESTRICT,

    -- What kind of chunk
    chunk_type        text        NOT NULL,

    -- Position within the artifact
    ordinal           integer     NOT NULL,

    -- Chunk-type-specific locators (nullable per type)
    section           text,
    speaker           text,
    starts_at         interval,
    ends_at           interval,

    -- Text content (ADR-0008 storage shape)
    text              text        NOT NULL,
    search_text       text,
    tsv               tsvector    GENERATED ALWAYS AS (
                          to_tsvector('english', COALESCE(search_text, text))
                      ) STORED,

    -- Fiscal / calendar (typically inherited from parent artifact; denormalized
    -- for fast cross-company queries without a join)
    fiscal_year       smallint,
    fiscal_quarter    smallint,
    calendar_year     smallint,
    calendar_quarter  smallint,

    -- Versioning + audit
    chunker_version   text,
    created_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT artifact_chunks_type_check CHECK (chunk_type IN (
        'section', 'speaker_turn', 'timestamp_span', 'table', 'paragraph'
    )),
    CONSTRAINT artifact_chunks_fiscal_quarter_range
        CHECK (fiscal_quarter IS NULL OR fiscal_quarter BETWEEN 1 AND 4),
    CONSTRAINT artifact_chunks_calendar_quarter_range
        CHECK (calendar_quarter IS NULL OR calendar_quarter BETWEEN 1 AND 4),
    CONSTRAINT artifact_chunks_ordinal_nonneg
        CHECK (ordinal >= 0),
    CONSTRAINT artifact_chunks_timestamp_order
        CHECK (starts_at IS NULL OR ends_at IS NULL OR ends_at >= starts_at),
    CONSTRAINT artifact_chunks_unique_ordinal
        UNIQUE (artifact_id, ordinal)
);

-- GIN index on the generated tsvector for FTS
CREATE INDEX artifact_chunks_tsv_idx ON artifact_chunks USING GIN (tsv);

-- Parent traversal
CREATE INDEX artifact_chunks_artifact_id_idx ON artifact_chunks (artifact_id);

-- Section-over-time: "all MD&A sections for NVDA"
CREATE INDEX artifact_chunks_section_idx
    ON artifact_chunks (section, artifact_id)
    WHERE section IS NOT NULL;

-- Speaker-over-time: "all Colette Kress quotes"
CREATE INDEX artifact_chunks_speaker_idx
    ON artifact_chunks (speaker, artifact_id)
    WHERE speaker IS NOT NULL;

-- Calendar-period aggregation
CREATE INDEX artifact_chunks_calendar_period_idx
    ON artifact_chunks (calendar_year, calendar_quarter)
    WHERE calendar_year IS NOT NULL;
