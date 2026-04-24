-- Add dimension identity to financial_facts for segment facts.
--
-- Segment revenue is still a financial fact: it has the same company,
-- fiscal/calendar, PIT, supersession, and provenance contract as statement
-- facts. The dimension columns keep segment identity out of `concept` so
-- `concept = 'revenue'` remains stable while product/geography labels vary.

ALTER TABLE financial_facts
    ADD COLUMN dimension_type text,
    ADD COLUMN dimension_key text,
    ADD COLUMN dimension_label text,
    ADD COLUMN dimension_source text;

ALTER TABLE financial_facts
    ADD CONSTRAINT financial_facts_dimension_type_check
        CHECK (
            dimension_type IS NULL
            OR dimension_type IN ('product', 'geography', 'operating_segment')
        ),
    ADD CONSTRAINT financial_facts_dimension_key_format
        CHECK (
            dimension_key IS NULL
            OR dimension_key ~ '^[a-z0-9]+(_[a-z0-9]+)*$'
        ),
    ADD CONSTRAINT financial_facts_dimension_contract
        CHECK (
            (
                statement = 'segment'
                AND dimension_type IS NOT NULL
                AND dimension_key IS NOT NULL
                AND dimension_label IS NOT NULL
                AND dimension_source IS NOT NULL
            )
            OR (
                statement <> 'segment'
                AND dimension_type IS NULL
                AND dimension_key IS NULL
                AND dimension_label IS NULL
                AND dimension_source IS NULL
            )
        );

-- The original unique constraint did not include dimensions. Replace it
-- with expression indexes that treat NULL dimension fields as equal for
-- non-segment rows while allowing distinct segment dimensions to coexist.
ALTER TABLE financial_facts
    DROP CONSTRAINT financial_facts_unique_extraction;

DROP INDEX financial_facts_one_current_idx;

CREATE UNIQUE INDEX financial_facts_unique_extraction_idx
    ON financial_facts (
        company_id,
        concept,
        period_end,
        period_type,
        source_raw_response_id,
        extraction_version,
        COALESCE(dimension_type, ''),
        COALESCE(dimension_key, ''),
        COALESCE(dimension_source, '')
    );

CREATE UNIQUE INDEX financial_facts_one_current_idx
    ON financial_facts (
        company_id,
        concept,
        period_end,
        period_type,
        extraction_version,
        COALESCE(dimension_type, ''),
        COALESCE(dimension_key, ''),
        COALESCE(dimension_source, '')
    )
    WHERE superseded_at IS NULL;

CREATE INDEX financial_facts_dimension_idx
    ON financial_facts (company_id, dimension_type, dimension_key, period_end DESC)
    WHERE statement = 'segment';

COMMENT ON COLUMN financial_facts.dimension_type IS
  'Dimension class for segment facts. NULL for non-segment rows. Current values: product, geography, operating_segment.';
COMMENT ON COLUMN financial_facts.dimension_key IS
  'Normalized company-local segment key, e.g. data_center. NULL for non-segment rows.';
COMMENT ON COLUMN financial_facts.dimension_label IS
  'Source/vendor segment label as reported, e.g. Data Center. NULL for non-segment rows.';
COMMENT ON COLUMN financial_facts.dimension_source IS
  'Source endpoint for the dimension label, e.g. fmp:revenue-product-segmentation. NULL for non-segment rows.';
