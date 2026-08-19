-- Did today's run actually land data for its ASINs?
-- NOTE: product_data.updated_at is NOT the write time (it carries a value from
-- the JungleScout payload — rows dated 2026-08-19 have updated_at back in 2022).
-- So "this run" is identified by report_date = CURRENT_DATE, not by updated_at.

\echo '=== 1. PRODUCT: today ASINs vs sync_status ==='
SELECT count(*) AS asins_flagged,
       count(p.asin) AS have_product_row_today,
       count(*) - count(p.asin) AS missing
FROM dev_az_jungle_scout_sync_status s
LEFT JOIN dev_az_jungle_scout_product_data p
       ON p.asin = s.asin AND p.report_date = CURRENT_DATE
WHERE s.updated_at > now() - interval '2 hours' AND s.has_product_data;

\echo '=== 2. RUN OUTCOME by flag ==='
SELECT has_product_data AS prod, has_sales_data AS sales,
       (error IS NOT NULL) AS err, count(*)
FROM dev_az_jungle_scout_sync_status
WHERE updated_at > now() - interval '2 hours'
GROUP BY 1,2,3 ORDER BY 4 DESC;

\echo '=== 3. SALES rows written ==='
SELECT count(DISTINCT asin) AS asins, count(*) AS rows, max(updated_at) AS last_write
FROM dev_az_jungle_scout_sales_estimate_data
WHERE updated_at > now() - interval '2 hours';

\echo '=== 4. PRODUCT data quality (today) ==='
SELECT count(*) AS rows,
       count(*) FILTER (WHERE title IS NULL OR title='') AS no_title,
       count(*) FILTER (WHERE price IS NULL)             AS no_price,
       count(*) FILTER (WHERE parent_asin IS NULL)       AS no_parent
FROM dev_az_jungle_scout_product_data WHERE report_date = CURRENT_DATE;

\echo '=== 5. ERRORS grouped ==='
SELECT left(error, 90) AS error_sample, count(*)
FROM dev_az_jungle_scout_sync_status
WHERE error IS NOT NULL AND updated_at > now() - interval '2 hours'
GROUP BY 1 ORDER BY 2 DESC LIMIT 10;
