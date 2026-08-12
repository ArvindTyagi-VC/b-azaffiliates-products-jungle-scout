# JungleScout Sync Service

A Go-based microservice for synchronizing product and sales data from JungleScout API to PostgreSQL databases with dual-write architecture (staging + production).

## Table of Contents

- [Architecture](#architecture)
- [Features](#features)
- [Environment Variables](#environment-variables)
- [Database Schema](#database-schema)
- [API Endpoints](#api-endpoints)
- [Debug Mode](#debug-mode)
- [Hourly Sync Flow](#hourly-sync-flow)
- [Master Sync Flow](#master-sync-flow)
- [API Usage Tracking](#api-usage-tracking)
- [Performance Optimizations](#performance-optimizations)
- [Error Handling](#error-handling)
- [Discord Notifications](#discord-notifications)
- [Setup & Deployment](#setup--deployment)

---

## Architecture

```
                                   +------------------+
                                   |  Cloud Scheduler  |
                                   |  (Hourly Trigger) |
                                   +--------+---------+
                                            |
                                            | Triggers Cloud Job
                                            v
                                   +------------------+
                                   |    Cloud Job     |
                                   | (Runs to complete)|
                                   +--------+---------+
                                            |
                                            | POST /admin/hourly-sync
                                            | X-API-KEY: <secret>
                                            v
+------------------+              +-------------------+              +------------------+
|                  |   REST API   |                   |   REST API   |                  |
|  Admin Dashboard +------------->+   Go Sync Service +------------->+  JungleScout API |
|  (JWT Auth)      |              |                   |              |                  |
+------------------+              +---------+---------+              +------------------+
                                            |
                                            | Dual Write
                                            | (Staging first, then Production)
                                            v
                              +-------------+-------------+
                              |                           |
                    +---------v---------+       +---------v---------+
                    |                   |       |                   |
                    |  Staging Database |       | Production Database|
                    |  (PostgreSQL)     |       |  (PostgreSQL)     |
                    |                   |       |                   |
                    +-------------------+       +-------------------+
```

### Dual-Write Pattern

All write operations follow this pattern:
1. Write to **Staging** first
2. If staging succeeds, write to **Production** with 3 retries (1s, 2s backoff)
3. If staging fails, operation is aborted (no production write)

**Note:** `jungle_scout_sync_status` table is **staging-only** (tracking/metadata). Actual data tables (`product_data`, `sales_estimate_data`) are written to both databases.

---

## Features

- **Hourly Sync**: Automated cloud job that syncs 100 ASINs per hour
- **Master Sync**: Manual full sync of all visible ASINs
- **Smart ASIN Selection**: Prioritizes new ASINs, then stale data (>30 days)
- **Product Not Found Handling**: 15-day retry mechanism for ASINs not in JungleScout
- **Dual-Write Architecture**: Staging-first writes with production retry
- **5-Worker Pool**: Concurrent sales data fetching with 5 parallel workers
- **Batch INSERT Optimization**: 100-row batch inserts for 90x query reduction
- **Critical Error Handling**: `stopRequested` mechanism to halt on DB failures
- **Rate Limiting**: Built-in API rate limiter for JungleScout API
- **Error Categorization**: Errors grouped by type (DB, API, Parse, Other)
- **Discord Notifications**: Color-coded sync reports with detailed metrics
- **Debug Mode**: Verbose logging for testing and troubleshooting (disabled by default)
- **API Usage Tracking**: Per-day, per-endpoint counter persisted to `js_api_usage` (dual-write); records every JungleScout HTTP roundtrip including retries

---

## Environment Variables

```env
# ============================================
# DATABASE - STAGING
# ============================================
DB_STAGING_HOST=your-staging-db-host.com
DB_STAGING_PORT=5432
DB_STAGING_USER=your_staging_user
DB_STAGING_PASS=your_staging_password
DB_STAGING_NAME=your_staging_database
DB_STAGING_TABLE_PREFIX=dev_az_

# ============================================
# DATABASE - PRODUCTION
# ============================================
DB_PROD_HOST=your-production-db-host.com
DB_PROD_PORT=5432
DB_PROD_USER=your_prod_user
DB_PROD_PASS=your_prod_password
DB_PROD_NAME=your_prod_database
DB_PROD_TABLE_PREFIX=prod_az_

# ============================================
# SERVER
# ============================================
PORT=8080

# ============================================
# AUTHENTICATION
# ============================================
JWT_SECRET=your-jwt-secret-key-here
SYNC_API_KEY=your-secure-api-key-for-cloud-jobs

# ============================================
# EXTERNAL APIS
# ============================================
JUNGLE_SCOUT_API_KEY=your-jungle-scout-api-key

# ============================================
# NOTIFICATIONS (Optional - has fallback)
# ============================================
DISCORD_WEBHOOK_URL=https://discord.com/api/webhooks/...
```

### Variable Reference

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `DB_STAGING_HOST` | Yes | - | Staging PostgreSQL host |
| `DB_STAGING_PORT` | Yes | - | Staging PostgreSQL port |
| `DB_STAGING_USER` | Yes | - | Staging database username |
| `DB_STAGING_PASS` | Yes | - | Staging database password |
| `DB_STAGING_NAME` | Yes | - | Staging database name |
| `DB_STAGING_TABLE_PREFIX` | Yes | - | Table prefix (e.g., `dev_az_`) |
| `DB_PROD_HOST` | Yes | - | Production PostgreSQL host |
| `DB_PROD_PORT` | Yes | - | Production PostgreSQL port |
| `DB_PROD_USER` | Yes | - | Production database username |
| `DB_PROD_PASS` | Yes | - | Production database password |
| `DB_PROD_NAME` | Yes | - | Production database name |
| `DB_PROD_TABLE_PREFIX` | Yes | - | Table prefix (e.g., `prod_az_`) |
| `PORT` | No | `8080` | Server port |
| `JWT_SECRET` | Yes | - | JWT signing secret |
| `SYNC_API_KEY` | Yes | - | API key for hourly-sync endpoint |
| `JUNGLE_SCOUT_API_KEY` | Yes | - | JungleScout API key |
| `DISCORD_WEBHOOK_URL` | No | Hardcoded | Discord webhook URL |

---

## Database Schema

### 1. jungle_scout_sync_status (Staging Only)

Tracks sync state for each ASIN. **This table exists only in staging database** - production does not have this table.

```sql
CREATE TABLE IF NOT EXISTS {prefix}jungle_scout_sync_status (
    asin VARCHAR(20) PRIMARY KEY,
    has_product_data BOOLEAN DEFAULT false,
    has_sales_data BOOLEAN DEFAULT false,
    error TEXT,
    product_data_synced_at TIMESTAMP,
    sales_estimate_data_synced_at TIMESTAMP,
    product_fetch_attempted_at TIMESTAMP,    -- For 15-day retry logic
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index for faster ASIN selection queries
CREATE INDEX IF NOT EXISTS idx_js_sync_product_fetch_attempted
ON {prefix}jungle_scout_sync_status(product_fetch_attempted_at)
WHERE has_product_data = false;
```

### 2. jungle_scout_product_data

Stores product information from JungleScout.

```sql
CREATE TABLE IF NOT EXISTS {prefix}jungle_scout_product_data (
    asin VARCHAR(20),
    report_date DATE,
    id VARCHAR(50),
    title TEXT,
    price DECIMAL(10,2),
    reviews INTEGER,
    category TEXT,
    rating DECIMAL(3,2),
    image_url TEXT,
    parent_asin VARCHAR(20),
    is_variant BOOLEAN,
    seller_type VARCHAR(50),
    variants JSONB,
    breadcrumb_path TEXT,
    is_standalone BOOLEAN,
    is_parent BOOLEAN,
    is_available BOOLEAN,
    brand VARCHAR(255),
    product_rank INTEGER,
    weight_value DECIMAL(10,2),
    weight_unit VARCHAR(20),
    length_value DECIMAL(10,2),
    width_value DECIMAL(10,2),
    height_value DECIMAL(10,2),
    dimensions_unit VARCHAR(20),
    listing_quality_score INTEGER,
    number_of_sellers INTEGER,
    buy_box_owner VARCHAR(255),
    buy_box_owner_seller_id VARCHAR(50),
    date_first_available DATE,
    date_first_available_is_estimated BOOLEAN,
    approximate_30_day_revenue DECIMAL(15,2),
    approximate_30_day_units_sold INTEGER,
    subcategory_ranks JSONB,
    fee_breakdown JSONB,
    ean_list JSONB,
    isbn_list JSONB,
    upc_list JSONB,
    gtin_list JSONB,
    variant_reviews INTEGER,
    updated_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, report_date)
);
```

### 3. jungle_scout_sales_estimate_data

Stores daily sales estimates.

```sql
CREATE TABLE IF NOT EXISTS {prefix}jungle_scout_sales_estimate_data (
    asin VARCHAR(20),
    marketplace VARCHAR(10),
    is_parent BOOLEAN,
    is_variant BOOLEAN,
    is_standalone BOOLEAN,
    parent_asin VARCHAR(20),
    date DATE,
    estimated_units_sold INTEGER,
    last_known_price DECIMAL(10,2),
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (asin, marketplace, date)
);
```

### 4. js_api_usage (Both DBs — dual-write)

Counts JungleScout API calls per day, per endpoint. Written to **both** staging
and production via the same dual-write pattern as the data tables. See
[API Usage Tracking](#api-usage-tracking) for the full flow.

```sql
CREATE TABLE IF NOT EXISTS {prefix}js_api_usage (
    usage_date DATE NOT NULL,
    endpoint VARCHAR(64) NOT NULL,
    call_count INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    PRIMARY KEY (usage_date, endpoint)
);

CREATE INDEX IF NOT EXISTS idx_{prefix}js_api_usage_date
    ON {prefix}js_api_usage(usage_date DESC);
```

The DDL is in [internal/junglescout/create_js_api_usage.sql](internal/junglescout/create_js_api_usage.sql) (hardcoded `dev_az_` prefix; copy and adjust for production).

---

## API Endpoints

### Health & Diagnostics

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| GET | `/health` | None | Health check |
| GET | `/test-db` | None | Database connectivity test |

### Admin Endpoints (JWT Auth)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/admin/sync-jungle-scout` | JWT + Admin | Sync sales estimate data |
| POST | `/admin/sync-product-database` | JWT + Admin | Sync product database data |
| POST | `/admin/master-sync` | JWT + Admin | Full master sync (all ASINs) |
| GET | `/admin/master-sync/status` | JWT + Admin | Get master sync status |

### Cloud Job Endpoints (API Key Auth)

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | `/admin/hourly-sync` | X-API-KEY | Hourly sync (100 ASINs) |
| GET | `/admin/hourly-sync/status` | X-API-KEY | Get hourly sync status |

### Request Examples

#### Hourly Sync (Cloud Job)
```bash
curl -X POST "https://your-api.com/admin/hourly-sync?marketplace=us" \
  -H "X-API-KEY: your-sync-api-key"
```

#### Master Sync (Admin)
```bash
curl -X POST "https://your-api.com/admin/master-sync?marketplace=us&date_range=1year&sync_mode=fresh" \
  -H "Authorization: Bearer <jwt-token>"
```

### Query Parameters

| Parameter | Endpoint | Values | Default | Description |
|-----------|----------|--------|---------|-------------|
| `marketplace` | All sync | `us`, `uk`, etc. | `us` | Amazon marketplace |
| `date_range` | master-sync | `1month`, `1year` | `1month` | Sales data range |
| `sync_mode` | master-sync | `fresh`, `resume` | `fresh` | Sync mode |
| `debug` | hourly-sync | `true`, `false` | `false` | Enable verbose logging |

---

## Debug Mode

Debug mode enables verbose logging for testing and troubleshooting. **Logs are only printed when debug mode is enabled.**

### Enabling Debug Mode

Add `?debug=true` to the hourly-sync endpoint:

```bash
# With debug logging enabled
curl -X POST "https://your-api.com/admin/hourly-sync?marketplace=us&debug=true" \
  -H "X-API-KEY: your-sync-api-key"

# Normal mode (no verbose logs)
curl -X POST "https://your-api.com/admin/hourly-sync?marketplace=us" \
  -H "X-API-KEY: your-sync-api-key"
```

### Sample Debug Output

```
[HOURLY_SYNC] ========== DEBUG MODE ENABLED ==========
[HOURLY_SYNC] Starting hourly sync with marketplace=us
[HOURLY_SYNC] ========== HOURLY SYNC STARTED ==========
[HOURLY_SYNC] Marketplace: us
[HOURLY_SYNC] Debug mode: true
[HOURLY_SYNC] ---------- STEP 1: CLEANUP ----------
[HOURLY_SYNC] Removing ASINs with product_visibility=false from sync_status...
[HOURLY_SYNC] [Cleanup] Getting product table name...
[HOURLY_SYNC] [Cleanup] Product table: dev_az_product
[HOURLY_SYNC] [Cleanup] Staging sync table: dev_az_jungle_scout_sync_status
[HOURLY_SYNC] [Cleanup] Executing DELETE on staging...
[HOURLY_SYNC] [Cleanup] Staging: 0 rows deleted
[HOURLY_SYNC] [Cleanup] Executing DELETE on production (with retry)...
[HOURLY_SYNC] [Cleanup] Production: DELETE successful on attempt 1
[HOURLY_SYNC] Cleanup complete: 0 ASINs removed
[HOURLY_SYNC] ---------- STEP 2: ADD NEW ASINs ----------
[HOURLY_SYNC] Adding new visible ASINs to sync_status...
[HOURLY_SYNC] [AddNew] Getting product table name...
[HOURLY_SYNC] [AddNew] Inserting new ASINs into staging sync_status...
[HOURLY_SYNC] [AddNew] Staging: 5 new ASINs inserted
[HOURLY_SYNC] [AddNew] Production: INSERT successful on attempt 1
[HOURLY_SYNC] New ASINs added to sync_status: 5
[HOURLY_SYNC] ---------- STEP 3: SELECT ASINs ----------
[HOURLY_SYNC] Selecting up to 100 ASINs (priority: new first, then stale >30 days)...
[HOURLY_SYNC] [Select] Sync table: dev_az_jungle_scout_sync_status
[HOURLY_SYNC] [Select] Retry threshold (15 days): 2024-01-01 10:00:00
[HOURLY_SYNC] [Select] Querying NEW ASINs (has_product_data=false, retry period passed)...
[HOURLY_SYNC] [Select] Found 5 NEW ASINs
[HOURLY_SYNC] [Select] Remaining slots for stale ASINs: 45
[HOURLY_SYNC] [Select] Found 10 STALE ASINs
[HOURLY_SYNC] [Select] Total ASINs selected: 15
[HOURLY_SYNC] Selected 15 ASINs total:
[HOURLY_SYNC]   - New ASINs: 5
[HOURLY_SYNC]   - Stale ASINs: 10
[HOURLY_SYNC]   - New ASIN list: [B01ABC B02DEF B03GHI B04JKL B05MNO]
[HOURLY_SYNC] ---------- STEP 4 & 5: SYNC PRODUCT & SALES DATA ----------
[HOURLY_SYNC] [SyncASINs] Starting sync for 15 ASINs
[HOURLY_SYNC] [SyncASINs] ===== STEP 4: PRODUCT DATA FETCH =====
[HOURLY_SYNC] [SyncASINs] Calling JungleScout Product API for 15 ASINs...
[HOURLY_SYNC] [SyncASINs] API call #1 completed
[HOURLY_SYNC] [SyncASINs] Product API returned 14 products (requested 15)
[HOURLY_SYNC] [SyncASINs] Storing product data to databases...
[HOURLY_SYNC] [SyncASINs] Successfully stored: 14 ASINs
[HOURLY_SYNC] [SyncASINs] 1 ASINs NOT FOUND in JungleScout API (will retry in 15 days):
[HOURLY_SYNC] [SyncASINs] Not found list: [B05MNO]
[HOURLY_SYNC] [NotFound] Marking ASIN B05MNO as 'product not found' (will retry in 15 days)
[HOURLY_SYNC] [SyncASINs] Product sync summary: 14 success, 0 failed, 1 not found
[HOURLY_SYNC] [SyncASINs] ===== STEP 5: SALES DATA FETCH =====
[HOURLY_SYNC] [SyncASINs] Fetching sales data for 14 successful product ASINs using 5-worker pool...
[HOURLY_SYNC] [Worker 0] Fetching sales for ASIN B01ABC: 2023-01-15 to 2024-01-15 (1 year (new ASIN))
[HOURLY_SYNC] [Worker 1] Fetching sales for ASIN B02DEF: 2023-01-15 to 2024-01-15 (1 year (new ASIN))
[HOURLY_SYNC] [Worker 2] Fetching sales for ASIN B03GHI: 2024-01-01 to 2024-01-15 (since last sync)
[HOURLY_SYNC] [Worker 0] Sales API returned 366 data points for ASIN B01ABC
[HOURLY_SYNC] [Sales] Staging: inserted 366 data points for ASIN B01ABC
[HOURLY_SYNC] [Sales] Production: inserted 366 data points for ASIN B01ABC
[HOURLY_SYNC] [Worker 0] Successfully stored sales data for ASIN B01ABC
...
[HOURLY_SYNC] [SyncASINs] Sales sync summary: 14 successful, 0 failed
[HOURLY_SYNC] [SyncASINs] Total API calls made: 15
[HOURLY_SYNC] ========== HOURLY SYNC COMPLETED ==========
[HOURLY_SYNC] Duration: 45.123s
[HOURLY_SYNC] Total ASINs processed: 15
[HOURLY_SYNC] Successful product syncs: 14
[HOURLY_SYNC] Successful sales syncs: 14
[HOURLY_SYNC] Failed ASINs: 1
[HOURLY_SYNC] Total API calls: 15
[HOURLY_SYNC] Sending Discord notification...
[HOURLY_SYNC] Discord notification sent
```

### Log Categories

| Prefix | Description |
|--------|-------------|
| `[Cleanup]` | Step 1 - Removing invisible ASINs |
| `[AddNew]` | Step 2 - Adding new ASINs to sync_status |
| `[Select]` | Step 3 - ASIN selection logic |
| `[SyncASINs]` | Steps 4 & 5 - Product and sales data sync |
| `[Worker N]` | Worker pool logs (N = 0-4) |
| `[Sales]` | Sales batch insert operations |
| `[NotFound]` | Marking ASINs as not found in JungleScout |
| `ERROR:` | Error messages within any step |

---

## Hourly Sync Flow

The hourly sync is designed for cloud scheduler execution, processing up to **100 ASINs per hour**.

### Flow Diagram

```
                    Cloud Scheduler Trigger
                    POST /admin/hourly-sync
                              |
                              v
                    +-------------------+
                    | API Key Validation |
                    +-------------------+
                              |
                              v
+------------------------------------------------------------------+
| STEP 1: CLEANUP                                                   |
| Delete sync_status records for ASINs with product_visibility=false|
| Result: CleanedUpASINs count                                      |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| STEP 2: ADD NEW ASINs                                             |
| Insert visible ASINs that don't exist in sync_status              |
| (has_product_data=false, has_sales_data=false)                    |
| Result: NewASINsAdded count                                       |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| STEP 3: SELECT ASINs (max 100)                                    |
|                                                                   |
| Priority 1: NEW ASINs                                             |
|   WHERE has_product_data = false                                  |
|   AND (product_fetch_attempted_at IS NULL                         |
|        OR product_fetch_attempted_at < NOW - 15 days)             |
|   ORDER BY created_at ASC                                         |
|                                                                   |
| Priority 2: STALE ASINs (fill remaining)                          |
|   WHERE has_product_data = true                                   |
|   AND product_data_synced_at < NOW - 30 days                      |
|   ORDER BY product_data_synced_at ASC                             |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| STEP 4: FETCH PRODUCT DATA (1 API call for batch)                 |
|                                                                   |
| For each ASIN in API response:                                    |
|   - Store product data (dual-write)                               |
|   - Set has_product_data = true                                   |
|   - Set product_data_synced_at = NOW                              |
|                                                                   |
| For ASINs NOT in response (not found in JungleScout):             |
|   - Set product_fetch_attempted_at = NOW                          |
|   - Set error = "Product not found in JungleScout API"            |
|   - Will retry in 15 days                                         |
+------------------------------------------------------------------+
                              |
                              v
+------------------------------------------------------------------+
| STEP 5: FETCH SALES DATA (5-Worker Pool, Batch INSERT)            |
| Only for ASINs with successful product sync                       |
|                                                                   |
| 5 concurrent workers process ASINs in parallel:                   |
|   - Each worker fetches sales data for assigned ASINs             |
|   - Batch INSERT (100 rows/query) for 90x faster writes           |
|   - stopRequested check halts all workers on critical DB error    |
|                                                                   |
| NEW ASIN (sales_estimate_data_synced_at = NULL):                  |
|   - Fetch 1 year of data (~366 data points)                       |
|                                                                   |
| STALE ASIN (has previous sync):                                   |
|   - Fetch from sales_estimate_data_synced_at to yesterday         |
+------------------------------------------------------------------+
                              |
                              v
                    +-------------------+
                    | Discord Notification |
                    +-------------------+
```

### Constants

| Constant | Value | Description |
|----------|-------|-------------|
| `HourlySyncASINLimit` | 100 | Max ASINs per hourly sync |
| `StaleDataThresholdDays` | 30 | Days before data is considered stale |
| `ProductNotFoundRetryDays` | 15 | Days before retrying not-found ASINs |
| `ProductBatchSize` | 100 | Max ASINs per product API call |
| `SalesWorkerCount` | 5 | Concurrent workers for sales data fetching |
| `SalesBatchInsertSize` | 100 | Rows per batch INSERT query |

### ASIN Selection Logic

| Scenario | New Count | Stale Count | Total |
|----------|-----------|-------------|-------|
| 120 new ASINs exist | 100 | 0 | 100 |
| 60 new + 100 stale | 60 | 40 | 100 |
| 0 new + 150 stale | 0 | 100 | 100 |
| 30 new + 30 stale | 30 | 30 | 60 |
| All data fresh | 0 | 0 | 0 (skip) |

---

## Master Sync Flow

Manual full sync of all ASINs with `product_visibility = true`.

### Sync Modes

| Mode | Description |
|------|-------------|
| `fresh` | Reset all sync flags, fetch fresh data for all ASINs |
| `resume` | Continue from where last sync left off |

### Flow

1. Fetch all ASINs from product table where `product_visibility = true`
2. Initialize/reset sync_status entries
3. Sync product data in batches of 100
4. Sync sales data for ASINs with successful product data
5. Send Discord notification

---

## API Usage Tracking

Every JungleScout HTTP request is persisted as a per-day, per-endpoint counter
in `js_api_usage`. JungleScout bills per request, so this gives an
authoritative view of consumption that survives across processes and matches
what the upstream provider sees.

### What gets counted

One increment per **HTTP roundtrip that received a response**, regardless of
status code. This means:

- A successful 200 response → +1
- A 4xx / 5xx response → +1 (JS still saw the request)
- A 429 followed by a successful retry → +2 (each attempt counts)
- A network error before the request reached JS → **not counted**

This semantic is intentional — it aligns with billing, even though it can
diverge from the in-memory `total_api_calls` field returned in sync responses
(which counts top-level invocations, not retries).

### Endpoint keys

Two values are written to the `endpoint` column:

| Key | Source | Triggered by |
|-----|--------|--------------|
| `product_database_query` | Product API (POST) | Hourly sync, master sync, `/admin/sync-product-database` |
| `sales_estimates_query` | Sales API (GET) | Hourly sync, master sync, `/admin/sync-jungle-scout` |

These constants are defined in [internal/junglescout/api_usage.go](internal/junglescout/api_usage.go) (`EndpointProductDatabaseQuery`, `EndpointSalesEstimatesQuery`).

### Architecture

```
+------------------------+
| JungleScout API call   |
| (any of 4 entry points)|
+-----------+------------+
            |
            v
+------------------------+
|  HTTP roundtrip        |   <-- request lands at JS
|  resp, err = client.Do |
+-----------+------------+
            |
       err == nil ? --no--> network error: skip recording, retry/abort
            | yes
            v
+------------------------+
| recorder.Record(key)   |
+-----------+------------+
            |
            v
+--------------------------------+
| STAGING upsert (synchronous)   |
| INSERT ... ON CONFLICT         |
| SET call_count = call_count+1  |
+-----------+--------------------+
            |
       success? --no--> log warning, ABORT (skip prod)
            | yes
            v
+--------------------------------+
| PRODUCTION upsert (3 retries)  |
| 1s, 2s backoff between tries   |
+-----------+--------------------+
            |
       success? --no--> log warning, swallow error
            | yes
            v
       (return — caller never blocked)
```

### Where increments happen

There are **two paths** that hit the recorder:

**1. Through the JS client** (`internal/junglescout/client.go`)

The `Client.doRequest` method invokes `recorder.Record(endpointKey)` after
every successful `httpClient.Do(req)`. This covers:

- `Client.FetchProductData` → `product_database_query`
- `Client.FetchSalesEstimateData` → `sales_estimates_query`

These are called by `MasterSyncManager` and `HourlySyncManager`.

**2. Through legacy raw-HTTP handlers** (`internal/api/handlers/jsmatrix.go`)

The `/admin/sync-jungle-scout` and `/admin/sync-product-database` endpoints
predate the JS client and use `http.Client` directly. They call
`recorder.Record(...)` explicitly after each `client.Do(req)`.

### Dual-write semantics

The recorder is constructed with both staging and production DB clients in
[cmd/server/main.go](cmd/server/main.go) and [cmd/job/main.go](cmd/job/main.go):

```go
apiUsageRecorder := junglescout.NewDBAPIUsageRecorder(stagingClient, productionClient)
```

Each `Record` call performs:

1. **Staging upsert** (synchronous, single attempt). On failure: log
   `[API_USAGE] Staging upsert failed...` and **return without writing prod**
   (matches the existing data-table dual-write convention).
2. **Production upsert** (up to 3 attempts, sleeping 1s then 2s between
   retries). On final failure: log `[API_USAGE] Production upsert failed...`
   and return.

**Errors are never propagated to the caller.** A counter write must never
break the JS API call — the recorder is best-effort by design.

### Upsert query

```sql
INSERT INTO {prefix}js_api_usage (usage_date, endpoint, call_count, created_at, updated_at)
VALUES (CURRENT_DATE, $1, 1, NOW(), NOW())
ON CONFLICT (usage_date, endpoint) DO UPDATE
SET call_count = {prefix}js_api_usage.call_count + 1,
    updated_at = NOW();
```

A new row is created on the first call of a (date, endpoint) pair; every
subsequent call increments `call_count` by 1.

### Querying usage

```sql
-- Today's usage so far
SELECT endpoint, call_count, updated_at
FROM dev_az_js_api_usage
WHERE usage_date = CURRENT_DATE
ORDER BY endpoint;

-- Yesterday's totals
SELECT endpoint, call_count
FROM dev_az_js_api_usage
WHERE usage_date = CURRENT_DATE - INTERVAL '1 day';

-- Last 7 days, per endpoint
SELECT usage_date, endpoint, call_count
FROM dev_az_js_api_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL '7 days'
ORDER BY usage_date DESC, endpoint;

-- Daily total across endpoints
SELECT usage_date, SUM(call_count) AS total_calls
FROM dev_az_js_api_usage
WHERE usage_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY usage_date
ORDER BY usage_date DESC;
```

### Failure modes

| Scenario | Behavior |
|----------|----------|
| Staging DB down at write time | Warning logged, prod write skipped, JS API call still succeeds |
| Production DB down at write time | 3 retries with backoff; if all fail, warning logged, JS API call still succeeds |
| Counter rows missing in production | Staging is the source of truth — production may lag if it was unreachable |
| New endpoint added in code without DDL change | Works — `endpoint VARCHAR(64)` accepts any value, no schema change needed |
| Recorder = nil (not configured) | All `Record` calls become no-ops; no errors |

### Caveats

- **Counts include retries.** A 429 plus successful retry counts as 2. The
  in-memory `total_api_calls` only counts top-level invocations, so the two
  numbers can diverge during throttling. The persisted counter is the
  billing-aligned authoritative count.
- **Staging and production counts can drift.** If staging succeeds but all
  production retries fail, staging will be ahead. Treat staging as
  authoritative for any reconciliation.
- **Both DBs must have the table.** Apply `create_js_api_usage.sql` to staging
  and a prod-prefixed copy to production before deploying, or every API call
  will spam `[API_USAGE] Production upsert failed...` warnings.

---

## Performance Optimizations

### Batch INSERT for Sales Data

Instead of inserting 366 rows one-by-one (1 year of daily sales data), the service uses batch INSERT:

```sql
-- Single query inserts up to 100 rows
INSERT INTO sales_estimate_data (asin, marketplace, ...)
VALUES
  ($1, $2, ...),
  ($10, $11, ...),
  ...  -- up to 100 rows
ON CONFLICT (asin, marketplace, date) DO UPDATE SET ...
```

| Metric | Before (row-by-row) | After (batch) |
|--------|---------------------|---------------|
| Queries per ASIN | ~732 | ~8 |
| Time per ASIN | 10-30s | 1-3s |
| For 38 ASINs | ~27,000 queries | ~300 queries |

### 5-Worker Pool

Sales data fetching uses 5 concurrent workers:

```
Worker 0 ─────► ASIN 1 ─► ASIN 6 ─► ASIN 11 ...
Worker 1 ─────► ASIN 2 ─► ASIN 7 ─► ASIN 12 ...
Worker 2 ─────► ASIN 3 ─► ASIN 8 ─► ASIN 13 ...
Worker 3 ─────► ASIN 4 ─► ASIN 9 ─► ASIN 14 ...
Worker 4 ─────► ASIN 5 ─► ASIN 10 ─► ASIN 15 ...
```

- Workers pull ASINs from a shared channel
- Rate limiter in API client handles concurrency automatically
- `stopRequested` flag halts all workers on critical DB errors

---

## Error Handling

### Error Categories

| Type | Key | Examples |
|------|-----|----------|
| Database | `db` | Connection timeout, constraint violation |
| API | `api` | Rate limit, timeout, invalid response |
| Parse | `parse` | JSON unmarshal, date parsing |
| Other | `other` | Uncategorized errors |

### Error Summary Structure

```json
{
  "error_summary": {
    "db_errors": 2,
    "api_errors": 1,
    "parse_errors": 0,
    "other_errors": 0,
    "sample_errors": [
      "Staging product store for B01XYZ failed: connection refused",
      "Sales fetch for B02ABC failed: rate limit exceeded"
    ]
  }
}
```

### Retry Mechanisms

| Scenario | Retries | Backoff |
|----------|---------|---------|
| Production DB write | 3 | 1s, 2s |
| JungleScout API call | 3 | Built into client |
| Product not found | After 15 days | - |

---

## Discord Notifications

### Color Coding

| Color | Hex Code | Condition |
|-------|----------|-----------|
| Green | `3066993` | Success, no failures |
| Yellow | `16776960` | Completed with some failures |
| Red | `15158332` | Critical failure, sync stopped |

### Notification Fields

```
+------------------------------------------+
| JungleScout Hourly Sync                  |
|------------------------------------------|
| Status: Completed successfully           |
|                                          |
| Processed: 100 ASINs   New: 12           |
| Stale: 38              Product: 48/50    |
| Sales: 45              Duration: 2m 15s  |
| Cleaned: 5             Added: 3          |
| API Calls: 51                            |
|                                          |
| Errors (if any):                         |
| DB: 2 | API: 1 | Parse: 0 | Other: 0     |
|                                          |
| Sample Errors:                           |
| "connection timeout..."                  |
+------------------------------------------+
```

---

## Setup & Deployment

### Local Development

```bash
# 1. Clone repository
git clone <repo-url>
cd b-azaffiliates-products-jungle-scout

# 2. Set environment variables
cp .env.example .env
# Edit .env with your values

# 3. Run database migrations
# sync_status: staging only
psql -h <staging-host> -U <user> -d <staging-db> -f internal/junglescout/create_asin_sync_status.sql

# js_api_usage: BOTH databases (dual-write)
psql -h <staging-host> -U <user> -d <staging-db> -f internal/junglescout/create_js_api_usage.sql
psql -h <prod-host>    -U <user> -d <prod-db>    -f internal/junglescout/create_js_api_usage.sql
# (Note: the SQL file hardcodes the dev_az_ prefix — copy/edit for production prefixes.)

# 4. Build and run
go build -o server .
./server
```

### Docker Deployment

```dockerfile
FROM golang:1.21-alpine AS builder
WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o server .

FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/
COPY --from=builder /app/server .
EXPOSE 8080
CMD ["./server"]
```

### GCP Cloud Job Deployment

Cloud Jobs are ideal for this service as they run to completion and stop (no idle charges).

#### Step 1: Build and Push Docker Image

```bash
# Build the image
docker build -t gcr.io/YOUR_PROJECT_ID/jungle-scout-sync:latest .

# Push to Google Container Registry
docker push gcr.io/YOUR_PROJECT_ID/jungle-scout-sync:latest
```

#### Step 2: Create Cloud Job

```bash
gcloud run jobs create jungle-scout-hourly-sync \
  --image gcr.io/YOUR_PROJECT_ID/jungle-scout-sync:latest \
  --region us-central1 \
  --set-env-vars "DB_STAGING_HOST=xxx,DB_STAGING_PORT=5432,..." \
  --set-secrets "JUNGLE_SCOUT_API_KEY=jungle-scout-api-key:latest,SYNC_API_KEY=sync-api-key:latest" \
  --memory 512Mi \
  --cpu 1 \
  --max-retries 1 \
  --task-timeout 30m
```

#### Step 3: Create Cloud Scheduler to Trigger Job

```bash
gcloud scheduler jobs create http jungle-scout-sync-scheduler \
  --location us-central1 \
  --schedule "0 * * * *" \
  --uri "https://us-central1-run.googleapis.com/apis/run.googleapis.com/v1/namespaces/YOUR_PROJECT_ID/jobs/jungle-scout-hourly-sync:run" \
  --http-method POST \
  --oauth-service-account-email YOUR_SERVICE_ACCOUNT@YOUR_PROJECT_ID.iam.gserviceaccount.com
```

#### Alternative: Direct HTTP Trigger (if running as always-on service)

If you deploy as a Cloud Run service instead of a job:

```bash
# Cloud Scheduler to hit the HTTP endpoint directly
gcloud scheduler jobs create http jungle-scout-sync-trigger \
  --location us-central1 \
  --schedule "0 * * * *" \
  --uri "https://your-service.run.app/admin/hourly-sync?marketplace=us" \
  --http-method POST \
  --headers "X-API-KEY=YOUR_SYNC_API_KEY"
```

#### Environment Variables for Cloud Job

Set these in the Cloud Job configuration:

| Variable | Secret Manager? | Description |
|----------|-----------------|-------------|
| `DB_STAGING_HOST` | No | Staging DB host |
| `DB_STAGING_PORT` | No | Staging DB port |
| `DB_STAGING_USER` | Yes (recommended) | Staging DB user |
| `DB_STAGING_PASS` | Yes | Staging DB password |
| `DB_STAGING_NAME` | No | Staging DB name |
| `DB_STAGING_TABLE_PREFIX` | No | Table prefix |
| `DB_PROD_*` | Same as above | Production DB settings |
| `JUNGLE_SCOUT_API_KEY` | Yes | JungleScout API key |
| `SYNC_API_KEY` | Yes | API key for authentication |
| `DISCORD_WEBHOOK_URL` | Yes (optional) | Discord notifications |

#### Viewing Cloud Job Logs

```bash
# View latest execution logs
gcloud run jobs executions list --job jungle-scout-hourly-sync

# View specific execution logs
gcloud logging read "resource.type=cloud_run_job AND resource.labels.job_name=jungle-scout-hourly-sync" --limit 100
```

---

## API Response Examples

### Hourly Sync Response

```json
{
  "message": "Hourly sync completed",
  "status": {
    "total_asins_processed": 50,
    "new_asins_synced": 12,
    "stale_asins_synced": 38,
    "successful_product_sync": 48,
    "successful_sales_sync": 45,
    "failed_asins": 2,
    "cleaned_up_asins": 5,
    "new_asins_added": 3,
    "total_api_calls": 51,
    "started_at": "2024-01-15T10:00:00Z",
    "completed_at": "2024-01-15T10:05:32Z",
    "is_running": false,
    "stopped_early": false,
    "stop_reason": "",
    "error_summary": {
      "db_errors": 0,
      "api_errors": 2,
      "parse_errors": 0,
      "other_errors": 0,
      "sample_errors": ["Sales fetch for B01XYZ failed: timeout"]
    }
  }
}
```

### Master Sync Status Response

```json
{
  "status": {
    "total_asins": 1500,
    "processed_asins": 750,
    "successful_product_sync": 740,
    "successful_sales_sync": 720,
    "failed_asins": 10,
    "started_at": "2024-01-15T10:00:00Z",
    "completed_at": null,
    "is_running": true,
    "current_batch": 8,
    "total_batches": 15,
    "date_range": "1year",
    "stopped_early": false,
    "total_api_calls": 758,
    "errors": []
  }
}
```

---

## Troubleshooting

### Common Issues

| Issue | Cause | Solution |
|-------|-------|----------|
| Discord notification not sending | Webhook URL invalid | Test with curl, check URL |
| Sync stuck at 0 ASINs | No visible products | Check product_visibility in product table |
| All ASINs failing | Database connection | Check DB credentials and connectivity |
| "Product not found" for all | Invalid marketplace | Verify marketplace parameter |
| API rate limiting | Too many requests | Reduce concurrency, add delays |

### Debug Commands

```bash
# Test Discord webhook
curl -X POST "https://discord.com/api/webhooks/..." \
  -H "Content-Type: application/json" \
  -d '{"content": "Test"}'

# Check sync status
curl "https://your-api.com/admin/hourly-sync/status" \
  -H "X-API-KEY: your-key"

# Test database connectivity
curl "https://your-api.com/test-db"
```

---

## License

Proprietary - All rights reserved.
