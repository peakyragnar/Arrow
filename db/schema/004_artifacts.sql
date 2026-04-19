-- artifacts: canonical immutable documents.
--
-- Every source document (10-K, 10-Q, 8-K, transcript, press release, news,
-- research primer, macro release) gets one row. Append-only. Corrections
-- flow via supersedes (self-FK) — a new row references the prior one, and
-- the prior row gets superseded_at set. Deletions are disallowed at the FK
-- level (ON DELETE RESTRICT) so lineage cannot silently break.
--
-- Double hash per system.md:
--   raw_hash       = SHA-256 of source bytes as received
--   canonical_hash = SHA-256 of canonicalized text (same filing across format
--                    or wrapper variations dedups to one identity)
--
-- Storage shape is hybrid (ADR-0007):
--   - common columns on this table for things most artifact types carry
--   - type-specific fields in artifact_metadata jsonb
--   - sidecar tables later only if a type grows enough operational weight
-- Metadata key conventions: docs/reference/artifact_metadata.md. Drift
-- there is the main risk the hybrid shape introduces; the conventions
-- doc is the guardrail.
--
-- Fiscal / calendar fields follow docs/reference/periods.md exactly. All
-- are nullable — artifacts without a period (research primers, macro
-- explainers) set none of them.

CREATE TABLE artifacts (
    id                      bigserial   PRIMARY KEY,
    ingest_run_id           bigint      REFERENCES ingest_runs(id) ON DELETE RESTRICT,

    -- Type + provenance
    artifact_type           text        NOT NULL,
    source                  text        NOT NULL,
    source_document_id      text,

    -- Content identity (hashes of original bytes; ADR-0005)
    raw_hash                bytea       NOT NULL,
    canonical_hash          bytea       NOT NULL,

    -- Company linkage (NULL for macro / non-company artifacts)
    ticker                  text,

    -- Fiscal truth (see docs/reference/periods.md)
    fiscal_year             smallint,
    fiscal_quarter          smallint,
    fiscal_period_label     text,
    period_end              date,
    period_type             text,

    -- Calendar normalization (pure function of period_end per periods.md)
    calendar_year           smallint,
    calendar_quarter        smallint,
    calendar_period_label   text,

    -- Document properties
    title                   text,
    url                     text,
    content_type            text,
    language                text,

    -- Time-aware fields
    published_at            timestamptz,
    effective_at            timestamptz,
    ingested_at             timestamptz NOT NULL DEFAULT now(),

    -- Supersession lineage (self-FK, RESTRICT; ADR-0009)
    supersedes              bigint      REFERENCES artifacts(id) ON DELETE RESTRICT,
    superseded_at           timestamptz,

    -- Research-corpus freshness (nullable; applies to research_note /
    -- industry_primer / product_primer / macro_primer)
    authored_by             text,
    last_reviewed_at        timestamptz,
    asserted_valid_through  date,

    -- Type-specific metadata (conventions: docs/reference/artifact_metadata.md)
    artifact_metadata       jsonb       NOT NULL DEFAULT '{}'::jsonb,

    CONSTRAINT artifacts_type_check CHECK (artifact_type IN (
        '10k', '10q', '8k',
        'transcript',
        'press_release', 'news_article',
        'presentation', 'video_transcript',
        'research_note', 'industry_primer', 'product_primer', 'macro_primer',
        'macro_release'
    )),
    CONSTRAINT artifacts_raw_hash_len
        CHECK (octet_length(raw_hash) = 32),
    CONSTRAINT artifacts_canonical_hash_len
        CHECK (octet_length(canonical_hash) = 32),
    CONSTRAINT artifacts_fiscal_quarter_range
        CHECK (fiscal_quarter IS NULL OR fiscal_quarter BETWEEN 1 AND 4),
    CONSTRAINT artifacts_calendar_quarter_range
        CHECK (calendar_quarter IS NULL OR calendar_quarter BETWEEN 1 AND 4),
    CONSTRAINT artifacts_period_type_check
        CHECK (period_type IS NULL OR period_type IN ('quarter', 'annual', 'stub')),
    CONSTRAINT artifacts_period_type_quarter_iff
        CHECK (
            period_type IS NULL
            OR (period_type IN ('annual', 'stub') AND fiscal_quarter IS NULL)
            OR (period_type = 'quarter' AND fiscal_quarter IS NOT NULL)
        ),
    CONSTRAINT artifacts_fiscal_period_label_format
        CHECK (
            fiscal_period_label IS NULL
            OR fiscal_period_label ~ '^FY[0-9]{4}( Q[1-4])?$'
        ),
    CONSTRAINT artifacts_calendar_period_label_format
        CHECK (
            calendar_period_label IS NULL
            OR calendar_period_label ~ '^CY[0-9]{4} Q[1-4]$'
        ),
    CONSTRAINT artifacts_supersedes_not_self
        CHECK (supersedes IS NULL OR supersedes <> id)
);

-- Lookup by (ticker, type), recent first — the most common analyst query
CREATE INDEX artifacts_ticker_type_published_idx
    ON artifacts (ticker, artifact_type, published_at DESC)
    WHERE ticker IS NOT NULL;

-- Current (non-superseded) rows — for default analyst "give me the latest"
CREATE INDEX artifacts_current_idx
    ON artifacts (ticker, artifact_type, published_at DESC)
    WHERE superseded_at IS NULL AND ticker IS NOT NULL;

-- Calendar-normalized comparisons across companies
CREATE INDEX artifacts_calendar_period_idx
    ON artifacts (calendar_year, calendar_quarter, artifact_type)
    WHERE calendar_year IS NOT NULL;

-- Fiscal-period lookups
CREATE INDEX artifacts_fiscal_period_idx
    ON artifacts (ticker, fiscal_year, fiscal_quarter)
    WHERE ticker IS NOT NULL AND fiscal_year IS NOT NULL;

-- Hash dedup and cross-source identity
CREATE INDEX artifacts_raw_hash_idx        ON artifacts (raw_hash);
CREATE INDEX artifacts_canonical_hash_idx  ON artifacts (canonical_hash);

-- Supersession chain traversal
CREATE INDEX artifacts_supersedes_idx
    ON artifacts (supersedes)
    WHERE supersedes IS NOT NULL;

-- Source-document identity: "give me the 10-K with accession X"
CREATE INDEX artifacts_source_doc_idx
    ON artifacts (source, source_document_id)
    WHERE source_document_id IS NOT NULL;

-- Research-corpus freshness: "find primers last reviewed > N months ago"
CREATE INDEX artifacts_research_freshness_idx
    ON artifacts (artifact_type, last_reviewed_at DESC)
    WHERE artifact_type IN ('research_note', 'industry_primer', 'product_primer', 'macro_primer');
