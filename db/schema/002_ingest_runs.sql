-- ingest_runs: operational log for every ingest invocation.
--
-- One row per run. A "run" is a single invocation of an ingest script or
-- scheduled worker (e.g. `scripts/backfill_fmp.py NVDA`, or the daily cron
-- pulling incremental price data). raw_responses.ingest_run_id points here.
--
-- Append-only. Status progresses started -> (succeeded | failed | partial)
-- with finished_at set on any terminal transition.

CREATE TABLE ingest_runs (
    id              bigserial   PRIMARY KEY,
    run_kind        text        NOT NULL,
    vendor          text        NOT NULL,
    ticker_scope    text[],                                 -- NULL = universe / not ticker-scoped
    status          text        NOT NULL DEFAULT 'started',
    started_at      timestamptz NOT NULL DEFAULT now(),
    finished_at     timestamptz,
    counts          jsonb       NOT NULL DEFAULT '{}'::jsonb,
    error_message   text,
    error_details   jsonb,
    code_version    text,                                   -- git SHA at run time

    CONSTRAINT ingest_runs_status_check
        CHECK (status IN ('started', 'succeeded', 'failed', 'partial')),
    CONSTRAINT ingest_runs_run_kind_check
        CHECK (run_kind IN ('backfill', 'incremental', 'reconciliation', 'manual')),
    CONSTRAINT ingest_runs_finished_at_check
        CHECK (
            (status = 'started'  AND finished_at IS NULL)
         OR (status <> 'started' AND finished_at IS NOT NULL)
        )
);

-- Recent runs by vendor, newest first (audit: "what did FMP do yesterday")
CREATE INDEX ingest_runs_vendor_started_at_idx
    ON ingest_runs (vendor, started_at DESC);

-- Fast lookup of non-success runs for debugging / alerting
CREATE INDEX ingest_runs_status_problem_idx
    ON ingest_runs (started_at DESC)
    WHERE status IN ('failed', 'partial');
