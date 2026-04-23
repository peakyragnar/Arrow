-- SEC qualitative layer:
--   - extend artifacts with SEC filing identity / linkage columns
--   - add artifact_sections (canonical extracted sections)
--   - add artifact_section_chunks (standardized retrieval chunks)
--
-- Design source:
--   docs/architecture/sec_qualitative_layer.md
--
-- This migration is intentionally additive. Existing artifact rows remain
-- valid; SEC filing rows gain stronger shape for period-aware composition.

ALTER TABLE artifacts
    ADD COLUMN company_id bigint REFERENCES companies(id) ON DELETE RESTRICT,
    ADD COLUMN fiscal_period_key text,
    ADD COLUMN form_family text,
    ADD COLUMN cik text,
    ADD COLUMN accession_number text,
    ADD COLUMN raw_primary_doc_path text,
    ADD COLUMN amends_artifact_id bigint REFERENCES artifacts(id) ON DELETE RESTRICT;

ALTER TABLE artifacts
    ADD CONSTRAINT artifacts_form_family_check
        CHECK (form_family IS NULL OR form_family IN ('10-K', '10-Q')),
    ADD CONSTRAINT artifacts_form_family_matches_type
        CHECK (
            (artifact_type = '10k' AND form_family = '10-K')
            OR (artifact_type = '10q' AND form_family = '10-Q')
            OR (artifact_type NOT IN ('10k', '10q') AND form_family IS NULL)
        ),
    ADD CONSTRAINT artifacts_amends_not_self
        CHECK (amends_artifact_id IS NULL OR amends_artifact_id <> id);

-- Backfill company linkage and filing identity for existing SEC filing rows.
UPDATE artifacts a
SET company_id = c.id
FROM companies c
WHERE a.company_id IS NULL
  AND a.ticker IS NOT NULL
  AND c.ticker = a.ticker;

UPDATE artifacts
SET fiscal_period_key = fiscal_period_label
WHERE fiscal_period_key IS NULL
  AND fiscal_period_label IS NOT NULL;

UPDATE artifacts
SET form_family = CASE artifact_type
    WHEN '10k' THEN '10-K'
    WHEN '10q' THEN '10-Q'
    ELSE form_family
END
WHERE form_family IS NULL
  AND artifact_type IN ('10k', '10q');

UPDATE artifacts
SET cik = artifact_metadata->>'filer_cik'
WHERE cik IS NULL
  AND artifact_type IN ('10k', '10q', '8k')
  AND artifact_metadata ? 'filer_cik';

UPDATE artifacts
SET accession_number = COALESCE(
        artifact_metadata->>'accession_number',
        CASE
            WHEN source_document_id IS NOT NULL AND position(':' in source_document_id) = 0
            THEN source_document_id
            ELSE NULL
        END
    )
WHERE accession_number IS NULL
  AND artifact_type IN ('10k', '10q', '8k');

UPDATE artifacts
SET raw_primary_doc_path = format(
        'data/raw/sec/filings/%s/%s/%s',
        cik,
        accession_number,
        artifact_metadata->>'primary_document'
    )
WHERE raw_primary_doc_path IS NULL
  AND artifact_type IN ('10k', '10q', '8k')
  AND cik IS NOT NULL
  AND accession_number IS NOT NULL
  AND artifact_metadata ? 'primary_document';

UPDATE artifacts a
SET amends_artifact_id = (
    SELECT p.id
    FROM artifacts p
    WHERE p.id <> a.id
      AND p.company_id = a.company_id
      AND p.fiscal_period_key = a.fiscal_period_key
      AND p.form_family = a.form_family
      AND p.published_at < a.published_at
    ORDER BY p.published_at DESC, p.id DESC
    LIMIT 1
)
WHERE a.amends_artifact_id IS NULL
  AND a.artifact_type IN ('10k', '10q')
  AND a.artifact_metadata->>'amended' = 'true'
  AND a.company_id IS NOT NULL
  AND a.fiscal_period_key IS NOT NULL
  AND a.form_family IS NOT NULL;

CREATE INDEX artifacts_company_type_published_idx
    ON artifacts (company_id, artifact_type, published_at DESC)
    WHERE company_id IS NOT NULL;

CREATE INDEX artifacts_company_period_family_idx
    ON artifacts (company_id, fiscal_period_key, form_family, published_at DESC)
    WHERE company_id IS NOT NULL
      AND fiscal_period_key IS NOT NULL
      AND form_family IS NOT NULL;

CREATE INDEX artifacts_amends_artifact_idx
    ON artifacts (amends_artifact_id)
    WHERE amends_artifact_id IS NOT NULL;

CREATE UNIQUE INDEX artifacts_sec_accession_uniq
    ON artifacts (cik, accession_number)
    WHERE cik IS NOT NULL
      AND accession_number IS NOT NULL
      AND artifact_type IN ('10k', '10q', '8k');


CREATE TABLE artifact_sections (
    id                  bigserial   PRIMARY KEY,
    artifact_id         bigint      NOT NULL REFERENCES artifacts(id) ON DELETE CASCADE,
    company_id          bigint      NOT NULL REFERENCES companies(id) ON DELETE RESTRICT,
    fiscal_period_key   text        NOT NULL,
    form_family         text        NOT NULL,
    section_key         text        NOT NULL,
    section_title       text        NOT NULL,
    part_label          text,
    item_label          text,
    text                text        NOT NULL,
    start_offset        integer     NOT NULL,
    end_offset          integer     NOT NULL,
    extractor_version   text        NOT NULL,
    confidence          double precision NOT NULL,
    extraction_method   text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    tsv                 tsvector    GENERATED ALWAYS AS (to_tsvector('english', text)) STORED,

    CONSTRAINT artifact_sections_form_family_check
        CHECK (form_family IN ('10-K', '10-Q')),
    CONSTRAINT artifact_sections_key_check
        CHECK (section_key IN (
            'item_1_business',
            'item_1a_risk_factors',
            'item_1c_cybersecurity',
            'item_3_legal_proceedings',
            'item_7_mda',
            'item_7a_market_risk',
            'item_9a_controls',
            'item_9b_other_information',
            'part1_item2_mda',
            'part1_item3_market_risk',
            'part1_item4_controls',
            'part2_item1_legal_proceedings',
            'part2_item1a_risk_factors',
            'part2_item5_other_information',
            'unparsed_body'
        )),
    CONSTRAINT artifact_sections_offsets_nonneg
        CHECK (start_offset >= 0 AND end_offset >= 0),
    CONSTRAINT artifact_sections_offsets_ordered
        CHECK (end_offset >= start_offset),
    CONSTRAINT artifact_sections_confidence_range
        CHECK (confidence >= 0.0 AND confidence <= 1.0),
    CONSTRAINT artifact_sections_extraction_method_check
        CHECK (extraction_method IN ('deterministic', 'repair', 'unparsed_fallback')),
    CONSTRAINT artifact_sections_confidence_method_contract
        CHECK (
            (extraction_method = 'deterministic' AND confidence >= 0.85)
            OR (extraction_method = 'repair' AND confidence > 0.0 AND confidence < 0.85)
            OR (extraction_method = 'unparsed_fallback' AND confidence = 0.0 AND section_key = 'unparsed_body')
        ),
    CONSTRAINT artifact_sections_unique_per_artifact
        UNIQUE (artifact_id, section_key)
);

CREATE INDEX artifact_sections_artifact_id_idx
    ON artifact_sections (artifact_id);

CREATE INDEX artifact_sections_identity_idx
    ON artifact_sections (company_id, fiscal_period_key, form_family, section_key);

CREATE INDEX artifact_sections_tsv_idx
    ON artifact_sections USING GIN (tsv);


CREATE TABLE artifact_section_chunks (
    id                  bigserial   PRIMARY KEY,
    section_id          bigint      NOT NULL REFERENCES artifact_sections(id) ON DELETE CASCADE,
    chunk_ordinal       integer     NOT NULL,
    text                text        NOT NULL,
    search_text         text        NOT NULL,
    heading_path        text[]      NOT NULL DEFAULT '{}'::text[],
    start_offset        integer     NOT NULL,
    end_offset          integer     NOT NULL,
    chunker_version     text        NOT NULL,
    created_at          timestamptz NOT NULL DEFAULT now(),
    tsv                 tsvector    GENERATED ALWAYS AS (to_tsvector('english', search_text)) STORED,

    CONSTRAINT artifact_section_chunks_ordinal_positive
        CHECK (chunk_ordinal > 0),
    CONSTRAINT artifact_section_chunks_offsets_nonneg
        CHECK (start_offset >= 0 AND end_offset >= 0),
    CONSTRAINT artifact_section_chunks_offsets_ordered
        CHECK (end_offset >= start_offset),
    CONSTRAINT artifact_section_chunks_unique_ordinal
        UNIQUE (section_id, chunk_ordinal)
);

CREATE INDEX artifact_section_chunks_section_id_idx
    ON artifact_section_chunks (section_id);

CREATE INDEX artifact_section_chunks_tsv_idx
    ON artifact_section_chunks USING GIN (tsv);

CREATE INDEX artifact_section_chunks_heading_path_idx
    ON artifact_section_chunks USING GIN (heading_path);


CREATE VIEW artifact_sections_meta AS
SELECT
    id,
    artifact_id,
    company_id,
    fiscal_period_key,
    form_family,
    section_key,
    section_title,
    part_label,
    item_label,
    start_offset,
    end_offset,
    extractor_version,
    confidence,
    extraction_method,
    created_at
FROM artifact_sections;
