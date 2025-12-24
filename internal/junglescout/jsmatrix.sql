CREATE TABLE dev_az_jungle_scout_sales_estimate_data (
    id SERIAL PRIMARY KEY,
    asin VARCHAR(10) NOT NULL,
    marketplace VARCHAR(10) NOT NULL DEFAULT 'us',
    is_parent BOOLEAN NOT NULL DEFAULT false,
    is_variant BOOLEAN NOT NULL DEFAULT false,
    is_standalone BOOLEAN NOT NULL DEFAULT false,
    parent_asin VARCHAR(20),
    date DATE NOT NULL,
    estimated_units_sold INTEGER NOT NULL,
    last_known_price DECIMAL(10, 2) NOT NULL,
    rank INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster queries by ASIN
CREATE INDEX IF NOT EXISTS idx_sales_estimates_asin ON dev_az_jungle_scout_sales_estimate_data(asin);

-- Index for faster queries by date range
CREATE INDEX IF NOT EXISTS idx_sales_estimates_date ON dev_az_jungle_scout_sales_estimate_data(date);

-- Composite index for ASIN + date range queries
CREATE INDEX IF NOT EXISTS idx_sales_estimates_asin_date ON dev_az_jungle_scout_sales_estimate_data(asin, date);


-- Dummy data for sales_estimates table
-- 30 days of data for ASIN B08JYQLKXZ
-- Note: Replace 'sales_estimates' with your environment-specific table name
-- For dev: dev_az_sales_estimates
-- For staging: staging_az_sales_estimates
-- For prod: prod_az_sales_estimates

-- Insert 30 days of sales data (2024-10-04 to 2024-11-02)
INSERT INTO dev_az_jungle_scout_sales_estimate_data (asin, marketplace, is_parent, is_variant, is_standalone, parent_asin, date, estimated_units_sold, last_known_price, rank) VALUES
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-11-02', 1563, 29.80, 1234),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-11-01', 1388, 29.80, 1256),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-31', 1391, 29.80, 1289),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-30', 1452, 29.80, 1245),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-29', 1432, 29.80, 1267),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-28', 1387, 29.80, 1298),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-27', 1349, 29.80, 1312),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-26', 1430, 29.80, 1276),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-25', 1465, 29.80, 1243),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-24', 1383, 29.80, 1287),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-23', 1404, 29.80, 1265),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-22', 1390, 29.80, 1294),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-21', 1426, 29.80, 1258),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-20', 1501, 29.80, 1221),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-19', 1478, 29.80, 1235),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-18', 1356, 29.80, 1305),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-17', 1412, 29.80, 1272),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-16', 1445, 29.80, 1249),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-15', 1398, 29.80, 1281),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-14', 1523, 29.80, 1215),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-13', 1487, 29.80, 1238),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-12', 1372, 29.80, 1295),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-11', 1419, 29.80, 1268),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-10', 1461, 29.80, 1247),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-09', 1395, 29.80, 1283),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-08', 1437, 29.80, 1262),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-07', 1509, 29.80, 1228),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-06', 1368, 29.80, 1301),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-05', 1442, 29.80, 1254),
('B08JYQLKXZ', 'us', false, true, false, 'B09RWP4NXB', '2024-10-04', 1489, 29.80, 1237);

-- Verify the data
SELECT
    asin,
    COUNT(*) as days_count,
    MIN(date) as start_date,
    MAX(date) as end_date,
    SUM(estimated_units_sold) as total_units,
    ROUND(SUM(estimated_units_sold * last_known_price)::numeric, 2) as total_revenue
FROM dev_az_jungle_scout_sales_estimate_data
WHERE asin = 'B08JYQLKXZ'
GROUP BY asin;


CREATE TABLE dev_az_jungle_scout_product_data (
    asin                TEXT        NOT NULL,
    report_date         DATE        NOT NULL,
    id                  TEXT,
    title               TEXT,
    price               NUMERIC(12,2),
    reviews             INTEGER,
    category            TEXT,
    rating              NUMERIC(3,2),
    image_url           TEXT,
    parent_asin         TEXT,
    is_variant          BOOLEAN,
    seller_type         TEXT,
    variants            JSONB,
    breadcrumb_path     TEXT,
    is_standalone       BOOLEAN,
    is_parent           BOOLEAN,
    is_available        BOOLEAN,
    brand               TEXT,
    product_rank        INTEGER,
    weight_value        NUMERIC(8,3),
    weight_unit         TEXT,
    length_value        NUMERIC(8,3),
    width_value         NUMERIC(8,3),
    height_value        NUMERIC(8,3),
    dimensions_unit     TEXT,
    listing_quality_score INTEGER,
    number_of_sellers   INTEGER,
    buy_box_owner       TEXT,
    buy_box_owner_seller_id TEXT,
    date_first_available DATE,
    date_first_available_is_estimated BOOLEAN,
    approximate_30_day_revenue NUMERIC(14,2),
    approximate_30_day_units_sold INTEGER,
    subcategory_ranks   JSONB,
    fee_breakdown       JSONB,
    ean_list            JSONB,
    isbn_list           JSONB,
    upc_list            JSONB,
    gtin_list           JSONB,
    variant_reviews     INTEGER,
    updated_at          TIMESTAMPTZ,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (asin, report_date)
);
CREATE INDEX idx_js_prod_asin   ON dev_az_jungle_scout_product_data (asin);
CREATE INDEX idx_js_prod_rptdt  ON dev_az_jungle_scout_product_data (report_date);

WITH payload AS (
    SELECT $JSON${
    "data": [
        {
            "id": "us/B01N1UX8RW",
            "type": "sales_estimate_result",
            "attributes": {
                "asin": "B01N1UX8RW",
                "is_parent": false,
                "is_variant": true,
                "is_standalone": false,
                "parent_asin": "B0FX4QDHVW",
                "variants": [],
                "data": [
                    {
                        "date": "2024-11-01",
                        "estimated_units_sold": 1408,
                        "last_known_price": 23.99
                    },
                ]
            }
        }
    ]
}$JSON$::jsonb AS js
)
INSERT INTO dev_az_jungle_scout_sales_estimate_data
      (asin, marketplace, is_parent, is_variant, is_standalone,
       parent_asin, date, estimated_units_sold, last_known_price)
SELECT
       attr->>'asin',
       split_part(item->>'id', '/', 1),          -- “us”
       (attr->>'is_parent')::boolean,
       (attr->>'is_variant')::boolean,
       (attr->>'is_standalone')::boolean,
       attr->>'parent_asin',
       (day->>'date')::date,
       (day->>'estimated_units_sold')::int,
       (day->>'last_known_price')::decimal(10,2)
FROM payload
CROSS JOIN LATERAL jsonb_array_elements(js->'data') AS item
CROSS JOIN LATERAL jsonb_array_elements(item->'attributes'->'data') AS day
CROSS JOIN LATERAL (SELECT item->'attributes') AS t(attr);

-- 1.  Put the JSON in a CTE so we can reuse it
WITH payload AS (
    SELECT
     CURRENT_DATE  AS report_date,
    $JSON${
        "data": [
            {
                "id": "us/B016MAK38U",
                "type": "product_database_result",
                "attributes": {
                    "title": "Redragon K552 Mechanical Gaming Keyboard, 87-Key Compact, LED Gaming Keyboard with Red Switches, Anti-Ghosting, Metal Frame for PC Gaming & Typing, Beginner-Friendly (Black)",
                    "price": 28.25,
                    "reviews": 39594,
                    "category": "Video Games",
                    "rating": 4.5,
                    "image_url": "https://m.media-amazon.com/images/I/41LSGymHc7L._SL75_.jpg ",
                    "parent_asin": "B0FBX6R5HL",
                    "is_variant": true,
                    "seller_type": "FBA",
                    "variants": [
                        "B0DK6R6HXP",
                        "B016MAK38U",
                        "B0DXZL1DNG",
                        "B0F5GTZ3P8",
                        "B0DYMVJ9LB",
                        "B0F5GG4S9W",
                        "B0DXZYJPK8",
                        "B086VRM89H"
                    ],
                    "breadcrumb_path": "Video Games › PC › Accessories › Gaming Keyboards",
                    "is_standalone": false,
                    "is_parent": false,
                    "is_available": true,
                    "brand": "Redragon",
                    "product_rank": 626,
                    "weight_value": 2.251,
                    "weight_unit": "pounds",
                    "length_value": 1.73,
                    "width_value": 6.34,
                    "height_value": 14.61,
                    "dimensions_unit": "inches",
                    "listing_quality_score": 6,
                    "number_of_sellers": 1,
                    "buy_box_owner": "Redragon zone",
                    "buy_box_owner_seller_id": "A2N5Q13T20FFT2",
                    "date_first_available": "2014-09-25",
                    "date_first_available_is_estimated": false,
                    "approximate_30_day_revenue": 43777.28,
                    "approximate_30_day_units_sold": 0,
                    "subcategory_ranks": [
                        {
                            "subcategory": "PC Gaming Keyboards",
                            "rank": 53,
                            "id": "us/402051011"
                        }
                    ],
                    "fee_breakdown": {
                        "fba_fee": 6.44,
                        "referral_fee": 2.25,
                        "variable_closing_fee": 0,
                        "total_fees": 9.69
                    },
                    "ean_list": [
                        "0740002400346",
                        "0780682638264",
                        "0796594786894",
                        "4895173505713",
                        "6950376704436",
                        "6950376772589"
                    ],
                    "isbn_list": null,
                    "upc_list": [
                        "740002400346",
                        "780682638264",
                        "796594786894"
                    ],
                    "gtin_list": [
                        "00740002400346"
                    ],
                    "variant_reviews": 6874,
                    "updated_at": "2025-11-04T18:15:57Z"
                }
            }
        ]
    }$JSON$::jsonb                                      AS js
)

INSERT INTO dev_az_jungle_scout_product_data AS t
        (asin,  report_date,  id,  title,  price,  reviews,  category,  rating,
         image_url,  parent_asin,  is_variant,  seller_type,  variants,
         breadcrumb_path,  is_standalone,  is_parent,  is_available,  brand,
         product_rank,  weight_value,  weight_unit,  length_value,  width_value,
         height_value,  dimensions_unit,  listing_quality_score,
         number_of_sellers,  buy_box_owner,  buy_box_owner_seller_id,
         date_first_available,  date_first_available_is_estimated,
         approximate_30_day_revenue,  approximate_30_day_units_sold,
         subcategory_ranks,  fee_breakdown,  ean_list,  isbn_list,  upc_list,
         gtin_list,  variant_reviews,  updated_at,  created_at)
SELECT
    split_part(item->>'id', '/', 2),        -- ASIN only
    p.report_date,
    item->>'id',
    (attr->>'title')::text,
    (attr->>'price')::numeric,
    (attr->>'reviews')::int,
    (attr->>'category')::text,
    (attr->>'rating')::numeric,
    (attr->>'image_url')::text,
    (attr->>'parent_asin')::text,
    (attr->>'is_variant')::bool,
    (attr->>'seller_type')::text,
    to_jsonb(attr->'variants'),
    (attr->>'breadcrumb_path')::text,
    (attr->>'is_standalone')::bool,
    (attr->>'is_parent')::bool,
    (attr->>'is_available')::bool,
    (attr->>'brand')::text,
    (attr->>'product_rank')::int,
    (attr->>'weight_value')::numeric,
    (attr->>'weight_unit')::text,
    (attr->>'length_value')::numeric,
    (attr->>'width_value')::numeric,
    (attr->>'height_value')::numeric,
    (attr->>'dimensions_unit')::text,
    (attr->>'listing_quality_score')::int,
    (attr->>'number_of_sellers')::int,
    (attr->>'buy_box_owner')::text,
    (attr->>'buy_box_owner_seller_id')::text,
    (attr->>'date_first_available')::date,
    (attr->>'date_first_available_is_estimated')::bool,
    (attr->>'approximate_30_day_revenue')::numeric,
    (attr->>'approximate_30_day_units_sold')::int,
    to_jsonb(attr->'subcategory_ranks'),
    to_jsonb(attr->'fee_breakdown'),
    to_jsonb(attr->'ean_list'),
    to_jsonb(attr->'isbn_list'),
    to_jsonb(attr->'upc_list'),
    to_jsonb(attr->'gtin_list'),
    (attr->>'variant_reviews')::int,
    (attr->>'updated_at')::timestamptz,
    now()
FROM payload p
CROSS JOIN LATERAL jsonb_array_elements(p.js->'data') AS item
CROSS JOIN LATERAL jsonb_extract_path(item, 'attributes') AS attr
ON CONFLICT (asin, report_date) DO UPDATE
SET
    title                          = EXCLUDED.title,
    price                          = EXCLUDED.price,
    reviews                        = EXCLUDED.reviews,
    category                       = EXCLUDED.category,
    rating                         = EXCLUDED.rating,
    image_url                      = EXCLUDED.image_url,
    parent_asin                    = EXCLUDED.parent_asin,
    is_variant                     = EXCLUDED.is_variant,
    seller_type                    = EXCLUDED.seller_type,
    variants                       = EXCLUDED.variants,
    breadcrumb_path                = EXCLUDED.breadcrumb_path,
    is_standalone                  = EXCLUDED.is_standalone,
    is_parent                      = EXCLUDED.is_parent,
    is_available                   = EXCLUDED.is_available,
    brand                          = EXCLUDED.brand,
    product_rank                   = EXCLUDED.product_rank,
    weight_value                   = EXCLUDED.weight_value,
    weight_unit                    = EXCLUDED.weight_unit,
    length_value                   = EXCLUDED.length_value,
    width_value                    = EXCLUDED.width_value,
    height_value                   = EXCLUDED.height_value,
    dimensions_unit                = EXCLUDED.dimensions_unit,
    listing_quality_score          = EXCLUDED.listing_quality_score,
    number_of_sellers              = EXCLUDED.number_of_sellers,
    buy_box_owner                  = EXCLUDED.buy_box_owner,
    buy_box_owner_seller_id        = EXCLUDED.buy_box_owner_seller_id,
    date_first_available           = EXCLUDED.date_first_available,
    date_first_available_is_estimated = EXCLUDED.date_first_available_is_estimated,
    approximate_30_day_revenue     = EXCLUDED.approximate_30_day_revenue,
    approximate_30_day_units_sold  = EXCLUDED.approximate_30_day_units_sold,
    subcategory_ranks              = EXCLUDED.subcategory_ranks,
    fee_breakdown                  = EXCLUDED.fee_breakdown,
    ean_list                       = EXCLUDED.ean_list,
    isbn_list                      = EXCLUDED.isbn_list,
    upc_list                       = EXCLUDED.upc_list,
    gtin_list                      = EXCLUDED.gtin_list,
    variant_reviews                = EXCLUDED.variant_reviews,
    updated_at                     = EXCLUDED.updated_at,
    created_at                     = now();

CREATE TABLE dev_az_jungle_scout_keywords_data (
    id TEXT PRIMARY KEY,
    type TEXT NOT NULL,
    country TEXT,
    name TEXT,
    primary_asin TEXT,
    monthly_trend NUMERIC,
    monthly_search_volume_exact NUMERIC,
    quarterly_trend NUMERIC,
    monthly_search_volume_broad NUMERIC,
    dominant_category TEXT,
    recommended_promotions NUMERIC,
    sp_brand_ad_bid NUMERIC,
    ppc_bid_broad NUMERIC,
    ppc_bid_exact NUMERIC,
    ease_of_ranking_score NUMERIC,
    relevancy_score NUMERIC,
    organic_product_count NUMERIC,
    sponsored_product_count NUMERIC,
    updated_at TIMESTAMP WITH TIME ZONE,
    organic_rank NUMERIC,
    sponsored_rank NUMERIC,
    overall_rank NUMERIC,
    organic_ranking_asins_count NUMERIC,
    sponsored_ranking_asins_count NUMERIC,
    avg_competitor_organic_rank NUMERIC,
    avg_competitor_sponsored_rank NUMERIC,
    relative_organic_position NUMERIC,
    relative_sponsored_position NUMERIC,
    variation_lowest_organic_rank NUMERIC,
    variation_lowest_sponsored_rank NUMERIC
);

CREATE INDEX idx_country ON dev_az_jungle_scout_keywords_data(country);
CREATE INDEX idx_primary_asin ON dev_az_jungle_scout_keywords_data(primary_asin);
CREATE INDEX idx_name ON dev_az_jungle_scout_keywords_data(name);
CREATE INDEX idx_updated_at ON dev_az_jungle_scout_keywords_data(updated_at DESC);
CREATE INDEX idx_monthly_search_volume_exact ON dev_az_jungle_scout_keywords_data(monthly_search_volume_exact DESC);
CREATE INDEX idx_dominant_category ON dev_az_jungle_scout_keywords_data(dominant_category);
CREATE INDEX idx_ease_of_ranking_score ON dev_az_jungle_scout_keywords_data(ease_of_ranking_score DESC);

INSERT INTO dev_az_jungle_scout_keywords_data (
    id, type, country, name, primary_asin, monthly_trend,
    monthly_search_volume_exact, quarterly_trend, monthly_search_volume_broad,
    dominant_category, recommended_promotions, sp_brand_ad_bid,
    ppc_bid_broad, ppc_bid_exact, ease_of_ranking_score,
    relevancy_score, organic_product_count, sponsored_product_count,
    updated_at, organic_rank, sponsored_rank, overall_rank,
    organic_ranking_asins_count, sponsored_ranking_asins_count,
    avg_competitor_organic_rank, avg_competitor_sponsored_rank,
    relative_organic_position, relative_sponsored_position,
    variation_lowest_organic_rank, variation_lowest_sponsored_rank
)
SELECT
    item->>'id',
    item->>'type',
    (item->'attributes')->>'country',
    (item->'attributes')->>'name',
    (item->'attributes')->>'primary_asin',
    ((item->'attributes')->>'monthly_trend')::NUMERIC,
    ((item->'attributes')->>'monthly_search_volume_exact')::NUMERIC,
    ((item->'attributes')->>'quarterly_trend')::NUMERIC,
    ((item->'attributes')->>'monthly_search_volume_broad')::NUMERIC,
    (item->'attributes')->>'dominant_category',
    ((item->'attributes')->>'recommended_promotions')::NUMERIC,
    ((item->'attributes')->>'sp_brand_ad_bid')::NUMERIC,
    ((item->'attributes')->>'ppc_bid_broad')::NUMERIC,
    ((item->'attributes')->>'ppc_bid_exact')::NUMERIC,
    ((item->'attributes')->>'ease_of_ranking_score')::NUMERIC,
    ((item->'attributes')->>'relevancy_score')::NUMERIC,
    ((item->'attributes')->>'organic_product_count')::NUMERIC,
    ((item->'attributes')->>'sponsored_product_count')::NUMERIC,
    ((item->'attributes')->>'updated_at')::TIMESTAMP WITH TIME ZONE,
    ((item->'attributes')->>'organic_rank')::NUMERIC,
    ((item->'attributes')->>'sponsored_rank')::NUMERIC,
    ((item->'attributes')->>'overall_rank')::NUMERIC,
    ((item->'attributes')->>'organic_ranking_asins_count')::NUMERIC,
    ((item->'attributes')->>'sponsored_ranking_asins_count')::NUMERIC,
    ((item->'attributes')->>'avg_competitor_organic_rank')::NUMERIC,
    ((item->'attributes')->>'avg_competitor_sponsored_rank')::NUMERIC,
    ((item->'attributes')->>'relative_organic_position')::NUMERIC,
    ((item->'attributes')->>'relative_sponsored_position')::NUMERIC,
    ((item->'attributes')->>'variation_lowest_organic_rank')::NUMERIC,
    ((item->'attributes')->>'variation_lowest_sponsored_rank')::NUMERIC
FROM jsonb_array_elements('{"data": [...]}'::jsonb->'data') AS item
ON CONFLICT (id) DO NOTHING;