-- Tracks JungleScout API call counts per day per endpoint.
-- Apply to BOTH staging and production databases (the service dual-writes
-- counter increments). Replace the dev_az_ prefix as appropriate.

CREATE TABLE IF NOT EXISTS dev_az_js_api_usage (
    usage_date DATE NOT NULL,
    endpoint VARCHAR(64) NOT NULL,
    call_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (usage_date, endpoint)
);

CREATE INDEX IF NOT EXISTS idx_dev_js_api_usage_date
    ON dev_az_js_api_usage(usage_date DESC);
