-- Jungle Scout fetches for Helium10 P1/P2/P3 ASINs (/helium-js/* routes).
-- STAGING ONLY. Creates three new tables and alters nothing.

CREATE TABLE IF NOT EXISTS dev_az_helium_js_job (
    job_id        BIGSERIAL   PRIMARY KEY,
    source        VARCHAR(24) NOT NULL,              -- helium_found | helium_missing | manual
    source_run_id BIGINT,                            -- Helium run the ASINs came from
    status        VARCHAR(16) NOT NULL,              -- running | completed | failed | interrupted
    phase         VARCHAR(16),
    counts        JSONB,
    error         TEXT,
    started_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    heartbeat_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at   TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_dev_az_helium_js_job_source
    ON dev_az_helium_js_job (source, source_run_id);

-- One row per ASIN of a job. result: found | missing | failed
CREATE TABLE IF NOT EXISTS dev_az_helium_js_asin (
    job_id          BIGINT      NOT NULL,
    asin            VARCHAR(20) NOT NULL,
    product_result  VARCHAR(8),
    sales_result    VARCHAR(8),
    sales_days      INTEGER,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (job_id, asin)
);

-- Jungle Scout rows the /helium-js jobs wrote, so the staging -> production
-- publish (internal/promote) leaves them out; they are moved to production by
-- hand. A row is left out while its last write is the job's (within a minute).
CREATE TABLE IF NOT EXISTS dev_az_helium_js_written (
    table_name       VARCHAR(48) NOT NULL,
    asin             VARCHAR(20) NOT NULL,
    marketplace      VARCHAR(8)  NOT NULL DEFAULT '',
    from_date        DATE        NOT NULL,
    to_date          DATE        NOT NULL,
    written_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    written_at_local TIMESTAMP   NOT NULL DEFAULT LOCALTIMESTAMP,
    PRIMARY KEY (table_name, asin, marketplace, from_date, to_date)
);

-- Rollback:
-- DROP TABLE IF EXISTS dev_az_helium_js_written
-- DROP TABLE IF EXISTS dev_az_helium_js_asin
-- DROP TABLE IF EXISTS dev_az_helium_js_job
