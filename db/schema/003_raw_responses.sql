-- raw_responses: verbatim vendor payload cache (canonical).
--
-- Pairs with the filesystem cache under data/raw/{vendor}/. Filesystem is
-- the byte-exact replay source; this table is the queryable index.
--
-- Storage rule (by content type):
--   JSON responses (FMP, most REST APIs) -> body_jsonb
--   Non-JSON responses (SEC HTML, PDFs)  -> body_raw
-- Exactly one of the two is populated per row (CHECK body_xor).
--
-- Identity rule:
--   row identity     = (id)                                  — one fetch event
--   request identity = (vendor, endpoint, params_hash)       — logical request
-- Multiple rows per request identity are expected and normal — re-fetching
-- is how revisions are captured. Append-only; no row is ever mutated.
--
-- Hash discipline:
--   raw_hash       = SHA-256 of the original bytes as received
--   canonical_hash = SHA-256 of a canonicalized representation (parse,
--                    stable key ordering, stable number rendering).
--                    Lets the same payload dedup across vendor wrapper
--                    variations without dropping nulls or altering
--                    semantics. Vendor/endpoint-specific canonicalizers
--                    may be more opinionated later; the default is
--                    conservative.

CREATE TABLE raw_responses (
    id                bigserial   PRIMARY KEY,
    ingest_run_id     bigint      NOT NULL REFERENCES ingest_runs(id) ON DELETE RESTRICT,

    -- Request identity
    vendor            text        NOT NULL,
    endpoint          text        NOT NULL,
    params            jsonb       NOT NULL DEFAULT '{}'::jsonb,
    params_hash       bytea       NOT NULL,
    request_url       text,                                      -- for debugging; optional

    -- Response
    http_status       smallint    NOT NULL,
    content_type      text        NOT NULL,
    response_headers  jsonb,
    body_jsonb        jsonb,
    body_raw          bytea,

    -- Hashes of original bytes
    raw_hash          bytea       NOT NULL,
    canonical_hash    bytea       NOT NULL,

    -- Timing
    fetched_at        timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT raw_responses_body_xor
        CHECK ((body_jsonb IS NULL) <> (body_raw IS NULL)),
    CONSTRAINT raw_responses_params_hash_len
        CHECK (octet_length(params_hash) = 32),
    CONSTRAINT raw_responses_raw_hash_len
        CHECK (octet_length(raw_hash) = 32),
    CONSTRAINT raw_responses_canonical_hash_len
        CHECK (octet_length(canonical_hash) = 32)
);

-- Request-identity, time-ordered: "most recent response for this request"
CREATE INDEX raw_responses_request_fetched_idx
    ON raw_responses (vendor, endpoint, params_hash, fetched_at DESC);

-- Hash-based dedup and cross-request identity
CREATE INDEX raw_responses_raw_hash_idx       ON raw_responses (raw_hash);
CREATE INDEX raw_responses_canonical_hash_idx ON raw_responses (canonical_hash);

-- Join support for ingest_runs
CREATE INDEX raw_responses_ingest_run_id_idx  ON raw_responses (ingest_run_id);
