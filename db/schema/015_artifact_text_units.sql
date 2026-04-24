-- Generic text units/chunks for non-10-K/Q artifacts.
--
-- `artifact_sections` remains the canonical SEC 10-K / 10-Q section layer.
-- This layer is for artifacts whose structure is useful but not governed by
-- the 10-K/Q item schema, starting with earnings-release EX-99 press releases
-- attached to 8-K filings.

CREATE TABLE artifact_text_units (
    id                  bigserial   PRIMARY KEY,
    artifact_id         bigint      NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    company_id          bigint      REFERENCES companies(id) ON DELETE RESTRICT,
    fiscal_period_key   text,
    unit_ordinal        integer     NOT NULL,
    unit_type           text        NOT NULL,
    unit_key            text        NOT NULL,
    unit_title          text        NOT NULL,
    text                text        NOT NULL,
    start_offset        integer     NOT NULL,
    end_offset          integer     NOT NULL,
    extractor_version   text        NOT NULL,
    confidence          double precision NOT NULL,
    extraction_method   text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    tsv                 tsvector    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,

    CONSTRAINT artifact_text_units_ordinal_positive
        CHECK (unit_ordinal > 0),
    CONSTRAINT artifact_text_units_type_check
        CHECK (unit_type IN ('press_release')),
    CONSTRAINT artifact_text_units_offsets_nonneg
        CHECK (start_offset >= 0 AND end_offset >= 0),
    CONSTRAINT artifact_text_units_offsets_ordered
        CHECK (end_offset >= start_offset),
    CONSTRAINT artifact_text_units_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0),
    CONSTRAINT artifact_text_units_extraction_method_check
        CHECK (extraction_method IN ('deterministic', 'unparsed_fallback')),
    CONSTRAINT artifact_text_units_confidence_method_contract
        CHECK (
            (extraction_method = 'deterministic' AND confidence >= 0.85)
            OR (extraction_method = 'unparsed_fallback' AND confidence = 0.0)
        ),
    CONSTRAINT artifact_text_units_unique_ordinal
        UNIQUE (artifact_id, unit_ordinal)
);

CREATE INDEX artifact_text_units_artifact_id_idx
    ON artifact_text_units (artifact_id);

CREATE INDEX artifact_text_units_identity_idx
    ON artifact_text_units (company_id, unit_type, unit_key);

CREATE INDEX artifact_text_units_tsv_idx
    ON artifact_text_units USING GIN (tsv);


CREATE TABLE artifact_text_chunks (
    id                  bigserial   PRIMARY KEY,
    text_unit_id        bigint      NOT NULL REFERENCES artifact_text_units(id) ON DELETE CASCADE,
    chunk_ordinal       integer     NOT NULL,
    text                text        NOT NULL,
    search_text         text        NOT NULL,
    heading_path        text[]      NOT NULL DEFAULT '{}'::text[],
    start_offset        integer     NOT NULL,
    end_offset          integer     NOT NULL,
    chunker_version     text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    tsv                 tsvector    GENERATED ALWAYS AS (to_tsvector('english', search_text)) STORED,

    CONSTRAINT artifact_text_chunks_ordinal_positive
        CHECK (chunk_ordinal > 0),
    CONSTRAINT artifact_text_chunks_offsets_nonneg
        CHECK (start_offset >= 0 AND end_offset >= 0),
    CONSTRAINT artifact_text_chunks_offsets_ordered
        CHECK (end_offset >= start_offset),
    CONSTRAINT artifact_text_chunks_unique_ordinal
        UNIQUE (text_unit_id, chunk_ordinal)
);

CREATE INDEX artifact_text_chunks_text_unit_id_idx
    ON artifact_text_chunks (text_unit_id);

CREATE INDEX artifact_text_chunks_tsv_idx
    ON artifact_text_chunks USING GIN (tsv);

CREATE INDEX artifact_text_chunks_heading_path_idx
    ON artifact_text_chunks USING GIN (heading_path);
