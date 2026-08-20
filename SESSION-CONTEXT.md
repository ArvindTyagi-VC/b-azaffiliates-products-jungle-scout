# Session context — JungleScout sync (2026-08-19)

Handoff notes. Point a new session at this file to pick up where this one left off.

---

## 1. State of the branch

- Branch `vx3-js-cron`, in sync with `origin/vx3-js-cron`.
- Last commit: **`bc684ac`** — *feat: SYNC_MAX_PER_RUN throttle, defaulting to the full parent set*.
- Untracked local helpers, deliberately NOT committed: `verify-run.ps1`, `verify-run.sql`, `watch-sync.ps1`, and this file.

## 2. What changed this session

The **500-ASIN hard limit was removed**. One line:

```go
// internal/api/handlers/jsmaster_sync.go:136
SyncMaxPerRun = envInt("SYNC_MAX_PER_RUN", 0)   // was 500
```

`0` is the "throttle off" sentinel the selection code already understood, so the
effective ceiling falls back to `SYNC_ASIN_LIMIT` (150,000) and a run now covers
the whole parent set. The stale `TEMPORARY: hardcoded to 500 … BEFORE THE CRON
GOES LIVE this must go back to 0` comment was replaced.

Two tests added in `cmd/job/main_test.go`: throttled run exits 0, guard breach exits 1.

**There are two different limits. Do not collapse them:**

| | meaning | on reaching it |
|---|---|---|
| `SYNC_ASIN_LIMIT` (150,000) | runaway guard, sized above the parent set | run is TRUNCATED, `LimitReached`, exit non-zero, CRITICAL log |
| `SYNC_MAX_PER_RUN` (0 = off) | operator asking for a smaller run on purpose | `ThrottledPerRun`, exit 0, no alarm |

## 3. Hard constraint carried into future edits

**Never delete log statements in this code.** The job logs only to its own stdout;
the `[CONFIG]`, `[Select]` and `CRITICAL` lines are the only way to reconstruct a
run. When refactoring, move a log with its code path rather than dropping it.
Verify the count did not drop:

```bash
git show HEAD:internal/api/handlers/jsmaster_sync.go | grep -cE "log\.Printf|monitorLog\(|debugLog\("
grep -cE "log\.Printf|monitorLog\(|debugLog\(" internal/api/handlers/jsmaster_sync.go
```

## 4. How a run actually works

`RunHourlySync` ("hourly" is a legacy name — it is the ten-day cycle now):

1. **Cleanup** — drop `sync_status` rows that are no longer parents-to-sync.
2. **Add new** — insert every current parent (`syncStatusAddNewQuery` → `parentASINSourceSQL`
   in `internal/api/handlers/jsparent_fanout.go:131`). No LIMIT anywhere in this path.
   Parent set = mapping rows whose child has `asin_visibility = true`, UNION visible
   ASINs with no mapping row (treated as their own parent).
3. **Select** (`selectASINsToSync`, three disjoint tiers, deduped by ASIN):
   - tier 1: never fetched, or past the not-found retry window (`NOT_FOUND_RETRY_DAYS` = 10)
   - tier 2: `product_data_synced_at` older than `STALE_THRESHOLD_DAYS` (10)
   - tier 3: product fresh but `has_sales_data = false`
4. **Fetch** — product data batched 100/request (API hard cap); sales estimates via
   12 workers on one 14 req/s limiter. Sales window: never-synced → **1 year** back to
   yesterday; otherwise **incremental** from last sync.
5. **Fan-out** — `fanOutProductRow` / `fanOutSalesRows` copy each parent row down to
   its visible children in SQL. **Children never hit the JungleScout API.** That is how
   ~107k parent fetches cover 296,365 visible ASINs.

## 5. Measured numbers (vx-3 staging, 2026-08-19)

- Parent set / rows in `dev_az_jungle_scout_sync_status`: **107,029**
- Next run would select **106,635**: tier1 105,685 + tier2 742 + tier3 208.
  The other 394 are skipped only because they are fresh (<10 days, both data types).
- Well under the 150,000 guard, so no truncation alarm.
- Throughput measured on the last real run: **~79 ASINs/min** → a full 107k run is **~22 hours**.
- Sales rows already in `dev_az_jungle_scout_sales_estimate_data`: ~20.4M.
- Only recurring error: `status 422` on sales fetch (JungleScout has no rank data for
  that ASIN). Product data still lands for them. Bounded and harmless.

## 6. Open decisions — none of these are code bugs

1. **`MAX_FULL_BACKFILLS` = 5,000 is the real bottleneck.** It caps how many
   never-synced ASINs pull a year of sales per run. With ~105k never fetched, the first
   full run writes product data for all of them but sales history for only ~5,000
   (`DeferSales`). At 5,000/run that is ~21 runs ≈ 7 months on the ten-day cadence.
   Raising it trades against tens of millions of sales rows in a single execution.
2. **Cloud Run Job task timeout.** The plan says 6h; a full run is ~22h. Either raise
   the timeout or use `SYNC_MAX_PER_RUN` to chunk the first backlog run — that is the
   case the throttle exists for.
3. **API quota.** The cadence needs ~227k JungleScout calls/month vs ~73k before (3.1x).
   Contract quota still unconfirmed.
4. **Cron cutover is manual and not done**: Cloud Scheduler → the **Cloud Run Job**
   (`cmd/job`), not `POST /admin/hourly-sync`. `0 2 1,11,21 * *` Asia/Kolkata,
   `--max-retries 0`.

## 7. DB quirks that will mislead you

- **Product rows: filter on `created_at`, not `updated_at`.** New inserts only set
  `created_at`; `updated_at` is touched on upsert conflict. `verify-run.sql` filters on
  `updated_at` and therefore reads zero rows during a fresh run even while inserts flow.
- **Timestamp columns are inconsistent.** `product_data.created_at/updated_at` are
  `timestamptz` (UTC); `sync_status.*` and `sales_estimate_data.*` are naive `timestamp`.
  Worse, within `sync_status`, `product_data_synced_at` is written in **IST** while
  `sales_estimate_data_synced_at` is written in **UTC** — same moment, 5.5h apart.
  Do not compare those two columns to each other.

## 8. Commands

```powershell
# load .env, then open psql against staging
Get-Content .env | Where-Object { $_ -match '^\s*[A-Z_]+=' } | ForEach-Object { $k,$v = $_ -split '=',2; Set-Item -Path "env:$($k.Trim())" -Value $v.Trim() }
$env:PGPASSWORD = $env:DB_STAGING_PASS
psql -h $env:DB_STAGING_HOST -p $env:DB_STAGING_PORT -U $env:DB_STAGING_USER -d $env:DB_STAGING_NAME
```

```sql
-- progress + freshness
SELECT count(*) AS total,
       count(*) FILTER (WHERE has_product_data) AS product_done,
       count(*) FILTER (WHERE has_sales_data)   AS sales_done,
       count(*) FILTER (WHERE error IS NOT NULL) AS errored,
       count(*) FILTER (WHERE NOT has_product_data AND error IS NULL) AS pending,
       max(updated_at) AS last_write,
       round(extract(epoch FROM now() - max(updated_at))) AS secs_idle
FROM dev_az_jungle_scout_sync_status;

-- is it alive? (created_at, NOT updated_at)
SELECT count(*) FILTER (WHERE created_at > now() - interval '10 minutes') AS rows_10m
FROM dev_az_jungle_scout_product_data;

-- what the next run would select, per tier
SELECT count(*) FILTER (WHERE has_product_data = false
         AND (product_fetch_attempted_at IS NULL OR product_fetch_attempted_at < now() - interval '10 days')) AS tier1_new,
       count(*) FILTER (WHERE has_product_data AND product_data_synced_at IS NOT NULL
         AND product_data_synced_at < now() - interval '10 days') AS tier2_stale,
       count(*) FILTER (WHERE has_product_data AND has_sales_data = false
         AND (product_data_synced_at IS NULL OR product_data_synced_at >= now() - interval '10 days')) AS tier3_salesgap
FROM dev_az_jungle_scout_sync_status;
```

```bash
go build ./... && go test ./cmd/job/ ./internal/api/handlers/
go run ./cmd/job                       # full run — ~22h with the throttle off
SYNC_MAX_PER_RUN=500 go run ./cmd/job  # deliberately small run, exits 0
```

Check whether a run is alive: `Get-Process -Name job,go -ErrorAction SilentlyContinue`.
A run that ends normally just disappears — that is not a crash.
