-- Jungle Scout fetches for Helium10 P1/P2/P3 ASINs (/helium-js/* routes).
-- STAGING ONLY. Creates two new tables and alters nothing.

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

-- Rollback:
-- DROP TABLE IF EXISTS dev_az_helium_js_asin
-- DROP TABLE IF EXISTS dev_az_helium_js_job
