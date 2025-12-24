-- Create JungleScout sync status table for tracking JungleScout sync progress
-- This table tracks the sync status of each ASIN from the active products table

-- Development environment
CREATE TABLE IF NOT EXISTS dev_az_jungle_scout_sync_status (
    asin VARCHAR(20) PRIMARY KEY,
    has_product_data BOOLEAN DEFAULT false,
    has_sales_data BOOLEAN DEFAULT false,
    error TEXT,
    product_data_synced_at TIMESTAMP,
    sales_estimate_data_synced_at TIMESTAMP,
    product_fetch_attempted_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);


-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_dev_js_sync_has_product_data ON dev_az_jungle_scout_sync_status(has_product_data);
CREATE INDEX IF NOT EXISTS idx_dev_js_sync_has_sales_data ON dev_az_jungle_scout_sync_status(has_sales_data);
CREATE INDEX IF NOT EXISTS idx_dev_js_sync_error ON dev_az_jungle_scout_sync_status(error) WHERE error IS NOT NULL;

