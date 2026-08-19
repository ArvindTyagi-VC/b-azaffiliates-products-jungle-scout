package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"azaffiliates/internal/database"
	"azaffiliates/internal/junglescout"

	"github.com/gin-gonic/gin"
	"github.com/lib/pq"
)

// SyncStatus represents the overall status of the sync operation
type SyncStatus struct {
	TotalASINs            int        `json:"total_asins"`
	ProcessedASINs        int        `json:"processed_asins"`
	SuccessfulProductSync int        `json:"successful_product_sync"`
	SuccessfulSalesSync   int        `json:"successful_sales_sync"`
	FailedASINs           int        `json:"failed_asins"`
	StartedAt             time.Time  `json:"started_at"`
	CompletedAt           *time.Time `json:"completed_at,omitempty"`
	IsRunning             bool       `json:"is_running"`
	CurrentBatch          int        `json:"current_batch"`
	TotalBatches          int        `json:"total_batches"`
	DateRange             string     `json:"date_range"`
	StoppedEarly          bool       `json:"stopped_early"`
	StopReason            string     `json:"stop_reason,omitempty"`
	TotalAPICalls         int        `json:"total_api_calls"`
	IsManual              bool       `json:"is_manual"`
	ManualSource          string     `json:"manual_source,omitempty"` // uploaded CSV filename
	Errors                []string   `json:"errors,omitempty"`
}

// MasterSyncManager manages the sync process
type MasterSyncManager struct {
	stagingClient    *database.PostgreSQLClient
	productionClient *database.PostgreSQLClient
	jsClient         *junglescout.Client
	status           *SyncStatus
	statusMutex      sync.RWMutex
	logger           *log.Logger
	dateRange        string // "1month" or "1year"
	stopRequested    bool   // Flag to stop the sync process
	criticalErrors   int    // Count of critical errors
	apiCallCount     int    // Simple counter for API calls

	// Table names used by the parent -> child fan-out, resolved once per run so
	// the frontend_asin lookup is not repeated per ASIN. prodTables stays zero
	// when running staging-only.
	stagingTables syncTables
	prodTables    syncTables

	// Manual mode (is_manual=true): the parent ASINs come from an uploaded CSV
	// instead of the database selection. manualASINs is the de-duplicated list;
	// it also scopes every sync_status query for the run, so a manual sync never
	// picks up ASINs that were left pending by an earlier automatic sync.
	isManual     bool
	manualASINs  []string
	manualSource string // uploaded CSV filename, for logs and status
}

var (
	// Global sync manager instance for status tracking
	globalSyncManager *MasterSyncManager
	syncManagerMutex  sync.Mutex
)

// ============================================================================
// HOURLY SYNC MANAGER - For Cloud Job Execution
// ============================================================================

// ProductBatchSize is the number of ASINs sent per JungleScout product call. It
// is NOT configurable: the API rejects more than 100 per request and the client
// enforces the same limit, so this must stay at 100.
const ProductBatchSize = 100

// Sync tuning, read from the environment at process start so a cadence change is
// a config edit rather than a deploy. Defaults below describe the ten-day cycle
// (sync on the 1st, 11th and 21st) that replaced the original hourly schedule.
//
// The old hourly values were HourlySyncASINLimit=100, StaleDataThresholdDays=30
// and ProductNotFoundRetryDays=15. Those three are tuned to each other and to the
// schedule: at 100 ASINs an hour the parent set came round about once a month,
// which is why staleness was 30 days. On a ten-day cycle one run must cover the
// whole parent set, so the limit becomes a runaway guard rather than a slice size
// and the two day-thresholds have to drop to the cycle length. Changing the cron
// without changing StaleDataThresholdDays makes the 2nd and 3rd run of each month
// select nothing at all.
var (
	// HourlySyncASINLimit caps one run. It is a guard, not a target: it sits above
	// the parent set so a healthy run is never truncated, and exists so a corrupted
	// mapping table or a mis-scoped visibility flag cannot turn one run into a
	// million-ASIN job. A run that reaches it is an incident, not a full sync —
	// selectASINsToSync logs loudly and the job exits non-zero.
	//
	// Sizing: measured on vx-3-staging on 2026-08-19, the parent set is 107,032
	// (296,365 visible ASINs). The default below leaves roughly 40% headroom. This
	// value has to be revisited as the catalogue grows: once the parent set passes
	// the cap, EVERY run truncates and reports failure. Check it against
	// parentASINSourceSQL whenever the catalogue changes materially.
	HourlySyncASINLimit = envInt("SYNC_ASIN_LIMIT", 150000)

	// StaleDataThresholdDays is how old product data may be before it is refetched.
	// Must match the schedule interval.
	StaleDataThresholdDays = envInt("STALE_THRESHOLD_DAYS", 10)

	// ProductNotFoundRetryDays is how long an ASIN that JungleScout does not know
	// about is left alone. Keep it at or below the cycle length: a value above it
	// quantises the retry to the next-but-one run (15 on a 10-day cycle means an
	// effective 20-day retry).
	ProductNotFoundRetryDays = envInt("NOT_FOUND_RETRY_DAYS", 10)

	// SalesWorkerCount is the size of the sales-fetch worker pool. All workers
	// share one rate limiter (14 req/s), so raising this fills the existing budget
	// instead of exceeding it — it cannot cause 429s on its own.
	SalesWorkerCount = envInt("SALES_WORKERS", 12)

	// MaxFailureRate is the share of processed ASINs that may fail before the run
	// is reported as failed. Consumed by cmd/job to pick its exit code.
	MaxFailureRate = envFloat("MAX_FAILURE_RATE", 0.10)

	// MaxFullBackfills caps how many never-synced ASINs one run will pull a full
	// year of sales data for. Beyond the cap an ASIN still gets its product data
	// and its incremental sales window; the year-long backfill waits for the next
	// run. Without this, a large batch of new ASINs writes tens of millions of
	// sales rows in a single execution.
	MaxFullBackfills = envInt("MAX_FULL_BACKFILLS", 5000)

	// SalesRetryPasses is how many extra passes the run makes over ASINs that
	// failed on transient errors (429, network, timeout) before they are recorded
	// as failed. 0 disables the retry pass.
	SalesRetryPasses = envInt("SALES_RETRY_PASSES", 1)

	// MaxConsecutiveDBFailures is how many database write failures in a row are
	// tolerated before the run is abandoned. A single failure used to stop the
	// whole run: cheap when a run was 100 ASINs, but on a full-set run it throws
	// away the tens of thousands of ASINs that had not been reached yet, and the
	// next attempt is a whole cycle away. One failure is now retried; only a
	// sustained run of them means the database is genuinely gone.
	MaxConsecutiveDBFailures = envInt("MAX_CONSECUTIVE_DB_FAILURES", 25)
)

// envInt reads an integer environment variable, falling back to def when the
// variable is unset, empty or unparseable. A bad value is logged rather than
// fatal: a typo in one tuning knob must not stop the sync from running.
func envInt(key string, def int) int {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		log.Printf("[CONFIG] %s=%q is not an integer, using default %d", key, raw, def)
		return def
	}
	return v
}

// envFloat reads a float environment variable with the same fallback rules as
// envInt.
func envFloat(key string, def float64) float64 {
	raw := strings.TrimSpace(os.Getenv(key))
	if raw == "" {
		return def
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Printf("[CONFIG] %s=%q is not a number, using default %.4f", key, raw, def)
		return def
	}
	return v
}

// LogSyncTuning writes the resolved tuning values once at startup. Worth having
// in the logs of every run: when a cycle behaves unexpectedly, the first question
// is always which thresholds it actually ran with.
func LogSyncTuning() {
	log.Printf("[CONFIG] SYNC_ASIN_LIMIT=%d STALE_THRESHOLD_DAYS=%d NOT_FOUND_RETRY_DAYS=%d SALES_WORKERS=%d MAX_FAILURE_RATE=%.2f MAX_FULL_BACKFILLS=%d SALES_RETRY_PASSES=%d",
		HourlySyncASINLimit, StaleDataThresholdDays, ProductNotFoundRetryDays,
		SalesWorkerCount, MaxFailureRate, MaxFullBackfills, SalesRetryPasses)

	if StaleDataThresholdDays < ProductNotFoundRetryDays {
		log.Printf("[CONFIG] WARNING: NOT_FOUND_RETRY_DAYS (%d) exceeds STALE_THRESHOLD_DAYS (%d) — ASINs missing from JungleScout will be skipped on the next run and only retried on the one after",
			ProductNotFoundRetryDays, StaleDataThresholdDays)
	}
}

// ErrorSummary tracks errors by category for smart logging
type ErrorSummary struct {
	DBErrors     int      `json:"db_errors"`
	APIErrors    int      `json:"api_errors"`
	ParseErrors  int      `json:"parse_errors"`
	OtherErrors  int      `json:"other_errors"`
	SampleErrors []string `json:"sample_errors"` // Max 3 sample error messages
}

// HourlySyncStatus represents the status of an hourly sync operation
type HourlySyncStatus struct {
	TotalASINsProcessed   int           `json:"total_asins_processed"`
	NewASINsSynced        int           `json:"new_asins_synced"`
	StaleASINsSynced      int           `json:"stale_asins_synced"`
	SuccessfulProductSync int           `json:"successful_product_sync"`
	SuccessfulSalesSync   int           `json:"successful_sales_sync"`
	FailedASINs           int           `json:"failed_asins"`
	CleanedUpASINs        int           `json:"cleaned_up_asins"`
	NewASINsAdded         int           `json:"new_asins_added"`
	TotalAPICalls         int           `json:"total_api_calls"`
	StartedAt             time.Time     `json:"started_at"`
	CompletedAt           *time.Time    `json:"completed_at,omitempty"`
	IsRunning             bool          `json:"is_running"`
	StoppedEarly          bool          `json:"stopped_early"`
	StopReason            string        `json:"stop_reason,omitempty"`
	IsManual              bool          `json:"is_manual"`
	ManualSource          string        `json:"manual_source,omitempty"` // uploaded CSV filename
	ErrorSummary          *ErrorSummary `json:"error_summary"`

	// SalesGapASINs counts ASINs pulled in because their product data was current
	// but their sales fetch had previously failed.
	SalesGapASINs int `json:"sales_gap_asins"`

	// LimitReached is true when selection hit SYNC_ASIN_LIMIT and the run was
	// therefore truncated. The cap sits far above the parent set, so this means
	// something upstream is wrong — it is an incident, not a successful full sync.
	LimitReached bool `json:"limit_reached"`

	// DeferredBackfills counts never-synced ASINs whose full-year sales fetch was
	// postponed to a later run by MAX_FULL_BACKFILLS. Their product data was still
	// written; no data is lost, it just arrives on the next cycle.
	DeferredBackfills int `json:"deferred_backfills"`

	// RetriedASINs counts ASINs that failed on a transient error and were picked
	// up again by the end-of-run retry pass.
	RetriedASINs int `json:"retried_asins"`

	// RecoveredASINs counts how many of those retries then succeeded.
	RecoveredASINs int `json:"recovered_asins"`
}

// ASINSyncInfo holds information about an ASIN to sync
type ASINSyncInfo struct {
	ASIN                  string
	IsNew                 bool // has_product_data=false and no product_data_synced_at
	SalesEstimateSyncedAt *time.Time

	// DeferSales postpones this ASIN's sales fetch to a later run. Set by
	// selectASINsToSync once MaxFullBackfills never-synced ASINs are already in
	// the batch, so one run cannot pull a year of sales for tens of thousands of
	// ASINs at once.
	//
	// Deferring means skipping the sales fetch entirely rather than fetching a
	// shorter window: sales_estimate_data_synced_at must stay NULL so the next run
	// still recognises this ASIN as needing the full year. Fetching a short window
	// and stamping the timestamp would silently strand the missing history.
	DeferSales bool

	// SalesGapOnly marks an ASIN selected because its product data is current but
	// its sales fetch previously failed. Product data is refetched anyway (the
	// product call is batched, so including it is nearly free) but the reason it
	// is in the run is the sales gap.
	SalesGapOnly bool
}

// SalesDataPoint holds data for a single sales data point for batch insert
type SalesDataPoint struct {
	ASIN           string
	Marketplace    string
	IsParent       bool
	IsVariant      bool
	IsStandalone   bool
	ParentASIN     *string
	Date           string
	EstimatedUnits int
	LastKnownPrice float64
}

// HourlySyncManager manages the hourly incremental sync process
type HourlySyncManager struct {
	stagingClient    *database.PostgreSQLClient
	productionClient *database.PostgreSQLClient
	jsClient         *junglescout.Client
	status           *HourlySyncStatus
	statusMutex      sync.RWMutex
	stopRequested    bool
	apiCallCount     int
	debugMode        bool // When true, prints verbose logs

	// Table names used by the parent -> child fan-out, resolved once per run.
	// prodTables stays zero when running staging-only.
	stagingTables syncTables
	prodTables    syncTables

	// Manual mode (is_manual=true): the ASINs come from an uploaded CSV instead
	// of the cleanup/add-new/select pipeline. They are treated as parent ASINs
	// and are NOT capped at HourlySyncASINLimit — the CSV decides the workload.
	isManual     bool
	manualASINs  []string
	manualSource string // uploaded CSV filename, for logs and status

	// Transient sales failures collected during the main pass and retried once it
	// finishes. Only plausibly temporary failures go in here — rate limits,
	// network errors, timeouts. A 422 MISSING_RANK_DATA, or an ASIN JungleScout
	// does not know about, is a real answer rather than a transient error, and
	// retrying it only burns quota.
	retryMutex  sync.Mutex
	retryQueue  []ASINSyncInfo
	inRetryPass bool // true while draining retryQueue, so retries do not re-queue

	// consecutiveDBFailures counts database write failures with no success in
	// between. Read and written atomically from the sales worker pool. See
	// noteDBFailure for why a streak, rather than a single failure, ends the run.
	consecutiveDBFailures int32

	// Per-ASIN accounting. The retry pass runs the same syncSelectedASINs path as
	// the main pass, so plain counters would count an ASIN twice — once when it
	// failed and again when it was retried — and the failure-rate gate in cmd/job
	// divides one counter by the other. Membership sets make the totals exact
	// regardless of how many times an ASIN is attempted.
	countMutex   sync.Mutex
	processedSet map[string]bool // every ASIN attempted at least once
	failedSet    map[string]bool // ASINs whose latest attempt did not succeed
	productOKSet map[string]bool // ASINs with a product row written this run
	salesOKSet   map[string]bool // ASINs with sales data written this run
}

// markProductStored records ASINs whose product row was written. Set-based so the
// retry pass, which re-runs the same product call, cannot inflate the count.
func (m *HourlySyncManager) markProductStored(asins ...string) {
	m.countMutex.Lock()
	defer m.countMutex.Unlock()

	if m.productOKSet == nil {
		m.productOKSet = make(map[string]bool)
	}
	for _, asin := range asins {
		m.productOKSet[asin] = true
	}

	m.statusMutex.Lock()
	m.status.SuccessfulProductSync = len(m.productOKSet)
	m.statusMutex.Unlock()
}

// markSalesStored records ASINs whose sales data was written, and reports whether
// this ASIN is newly counted (false when a retry re-stored data already counted).
func (m *HourlySyncManager) markSalesStored(asin string) bool {
	m.countMutex.Lock()
	defer m.countMutex.Unlock()

	if m.salesOKSet == nil {
		m.salesOKSet = make(map[string]bool)
	}
	fresh := !m.salesOKSet[asin]
	m.salesOKSet[asin] = true

	m.statusMutex.Lock()
	m.status.SuccessfulSalesSync = len(m.salesOKSet)
	m.statusMutex.Unlock()
	return fresh
}

// markASINAttempted records that an ASIN was attempted. Counting distinct ASINs
// keeps TotalASINsProcessed equal to the size of the workload even when the retry
// pass re-attempts some of them.
func (m *HourlySyncManager) markASINAttempted(asins ...string) {
	m.countMutex.Lock()
	defer m.countMutex.Unlock()

	if m.processedSet == nil {
		m.processedSet = make(map[string]bool)
	}
	for _, asin := range asins {
		m.processedSet[asin] = true
	}

	m.statusMutex.Lock()
	m.status.TotalASINsProcessed = len(m.processedSet)
	m.statusMutex.Unlock()
}

// markASINFailed records an ASIN as currently failed. Idempotent: failing twice
// counts once.
func (m *HourlySyncManager) markASINFailed(asins ...string) {
	m.countMutex.Lock()
	defer m.countMutex.Unlock()

	if m.failedSet == nil {
		m.failedSet = make(map[string]bool)
	}
	for _, asin := range asins {
		m.failedSet[asin] = true
	}

	m.statusMutex.Lock()
	m.status.FailedASINs = len(m.failedSet)
	m.statusMutex.Unlock()
}

// markASINResolved clears a previously recorded failure, so an ASIN recovered by
// the retry pass stops counting against the run.
func (m *HourlySyncManager) markASINResolved(asins ...string) {
	m.countMutex.Lock()
	defer m.countMutex.Unlock()

	if m.failedSet == nil {
		return
	}
	for _, asin := range asins {
		delete(m.failedSet, asin)
	}

	m.statusMutex.Lock()
	m.status.FailedASINs = len(m.failedSet)
	m.statusMutex.Unlock()
}

// isRetryPass reports whether the run is currently draining the retry queue.
func (m *HourlySyncManager) isRetryPass() bool {
	m.retryMutex.Lock()
	defer m.retryMutex.Unlock()
	return m.inRetryPass
}

// queueForRetry records an ASIN whose sales fetch failed on what looks like a
// transient error, for the end-of-run retry pass. Calls made during the retry
// pass itself are ignored so a persistently failing ASIN cannot loop.
func (m *HourlySyncManager) queueForRetry(info ASINSyncInfo) {
	if SalesRetryPasses <= 0 {
		return
	}
	m.retryMutex.Lock()
	defer m.retryMutex.Unlock()
	if m.inRetryPass {
		return
	}
	m.retryQueue = append(m.retryQueue, info)
}

// takeRetryQueue returns the queued ASINs and clears the queue.
func (m *HourlySyncManager) takeRetryQueue() []ASINSyncInfo {
	m.retryMutex.Lock()
	defer m.retryMutex.Unlock()
	queued := m.retryQueue
	m.retryQueue = nil
	return queued
}

// noteDBFailure records one database write failure and reports whether the run
// should now be abandoned. A single failure is not fatal: the writes are
// idempotent upserts, so the ASIN can simply be retried. Only a sustained streak
// — MaxConsecutiveDBFailures in a row with no success in between — is treated as
// the database being gone, at which point continuing would just log the same
// error tens of thousands of times.
func (m *HourlySyncManager) noteDBFailure(context string, err error) bool {
	streak := atomic.AddInt32(&m.consecutiveDBFailures, 1)

	if int(streak) < MaxConsecutiveDBFailures {
		m.debugLog("[DB] %s failed (%d consecutive): %v", context, streak, err)
		return false
	}

	msg := fmt.Sprintf("%d consecutive database failures — abandoning the run (last: %s: %v)", streak, context, err)
	log.Printf("%s CRITICAL: %s", m.logPrefix(), msg)
	m.addHourlyError("db", msg)
	m.stopRequested = true
	m.statusMutex.Lock()
	if m.status.StopReason == "" {
		m.status.StopReason = msg
	}
	m.statusMutex.Unlock()
	return true
}

// noteDBSuccess clears the consecutive-failure streak.
func (m *HourlySyncManager) noteDBSuccess() {
	atomic.StoreInt32(&m.consecutiveDBFailures, 0)
}

// isTransientAPIError reports whether an error from the JungleScout client is
// worth retrying. The client already retries a 429 three times internally and
// then gives up with "max retries exceeded", so reaching here means the whole
// budget was spent — exactly the case the end-of-run pass exists for, since by
// then the rate window has usually moved on.
//
// Deliberately NOT retried: 422 MISSING_RANK_DATA (a normal JungleScout answer
// for an ASIN with no rank data) and 4xx responses other than 429, which describe
// the request rather than the moment.
func isTransientAPIError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())

	for _, notTransient := range []string{"missing_rank_data", "422"} {
		if strings.Contains(msg, notTransient) {
			return false
		}
	}

	for _, transient := range []string{
		"max retries exceeded",
		"429",
		"too many requests",
		"timeout",
		"timed out",
		"connection reset",
		"connection refused",
		"eof",
		"no such host",
		"i/o timeout",
		"temporary failure",
		"502", "503", "504",
	} {
		if strings.Contains(msg, transient) {
			return true
		}
	}
	return false
}

// resolveFanOutTables resolves and caches the table names used by the parent ->
// child fan-out for both databases. Called once at the start of a run.
func (m *HourlySyncManager) resolveFanOutTables() error {
	tables, err := resolveSyncTables(m.stagingClient)
	if err != nil {
		return fmt.Errorf("staging: %w", err)
	}
	m.stagingTables = tables

	if m.productionClient != nil {
		prodTables, err := resolveSyncTables(m.productionClient)
		if err != nil {
			return fmt.Errorf("production: %w", err)
		}
		m.prodTables = prodTables
	}
	return nil
}

// logPrefix tags every line of a run so a manual run can be grepped out of the
// hourly cron noise in production logs.
func (m *HourlySyncManager) logPrefix() string {
	if m.isManual {
		return "[HOURLY_SYNC][MANUAL]"
	}
	return "[HOURLY_SYNC]"
}

// debugLog prints a log message only if debug mode is enabled
func (m *HourlySyncManager) debugLog(format string, args ...interface{}) {
	if m.debugMode {
		msg := fmt.Sprintf(format, args...)
		log.Printf("%s %s", m.logPrefix(), msg)
	}
}

// monitorLog prints a log message that is ALWAYS emitted for a manual run, and
// only under debug for the automatic cron run.
//
// A manual run is human-triggered against production and the caller needs to be
// able to prove, from the logs alone, which ASINs were sent to JungleScout and
// which database they were written to. The hourly cron runs unattended every
// hour, so its log volume is left as it was.
func (m *HourlySyncManager) monitorLog(format string, args ...interface{}) {
	if !m.isManual && !m.debugMode {
		return
	}
	log.Printf("%s %s", m.logPrefix(), fmt.Sprintf(format, args...))
}

// logASINList prints a full ASIN list in chunks so no ASIN is hidden behind a
// truncated log line. Used to evidence exactly what went to JungleScout.
func (m *HourlySyncManager) logASINList(label string, asins []string) {
	const perLine = 20
	m.monitorLog("%s (%d):", label, len(asins))
	for i := 0; i < len(asins); i += perLine {
		end := i + perLine
		if end > len(asins) {
			end = len(asins)
		}
		m.monitorLog("  [%04d-%04d] %s", i+1, end, strings.Join(asins[i:end], " "))
	}
}

// logTargetDatabases records which databases this run will touch, resolved from
// the connections themselves rather than from config, so a production run can
// be verified from its logs.
func (m *HourlySyncManager) logTargetDatabases() {
	describe := func(client *database.PostgreSQLClient) string {
		if client == nil {
			return "none"
		}
		var dbName, dbUser, host string
		err := client.DB.QueryRow(
			`SELECT current_database(), current_user, COALESCE(inet_server_addr()::text, 'local')`,
		).Scan(&dbName, &dbUser, &host)
		if err != nil {
			return fmt.Sprintf("unknown (identity query failed: %v, prefix=%q)", err, client.TablePrefix)
		}
		return fmt.Sprintf("db=%s user=%s host=%s prefix=%q", dbName, dbUser, host, client.TablePrefix)
	}

	m.monitorLog("Target STAGING     : %s", describe(m.stagingClient))
	if m.productionClient == nil {
		m.monitorLog("Target PRODUCTION  : none (STAGING-ONLY MODE - no production writes)")
		return
	}
	m.monitorLog("Target PRODUCTION  : %s", describe(m.productionClient))
}

var (
	globalHourlySyncManager *HourlySyncManager
	hourlySyncManagerMutex  sync.Mutex
)

// NewHourlySyncManager creates a new hourly sync manager. Pass a recorder to
// persist per-call API usage; nil disables recording.
func NewHourlySyncManager(stagingClient, productionClient *database.PostgreSQLClient, debugMode bool, recorder junglescout.APIUsageRecorder) *HourlySyncManager {
	jsClient := junglescout.NewClient()
	jsClient.SetUsageRecorder(recorder)
	return &HourlySyncManager{
		stagingClient:    stagingClient,
		productionClient: productionClient,
		jsClient:         jsClient,
		apiCallCount:     0,
		debugMode:        debugMode,
	}
}

// NewMasterSyncManager creates a new sync manager with both staging and
// production clients. Pass a recorder to persist per-call API usage; nil
// disables recording.
func NewMasterSyncManager(stagingClient, productionClient *database.PostgreSQLClient, recorder junglescout.APIUsageRecorder) *MasterSyncManager {
	jsClient := junglescout.NewClient()
	jsClient.SetUsageRecorder(recorder)
	return &MasterSyncManager{
		stagingClient:    stagingClient,
		productionClient: productionClient,
		jsClient:         jsClient,
		logger:           log.New(log.Writer(), "[MASTER_SYNC] ", log.LstdFlags|log.Lshortfile),
		apiCallCount:     0,
	}
}

// JSMasterSync handles the master synchronization of all ASINs
//
// Query/form params:
//   - marketplace: Amazon marketplace (default: "us")
//   - date_range:  "1month" or "1year" (default: "1month")
//   - sync_mode:   "fresh" or "resume" (default: "fresh")
//   - is_manual:   "true" to take the parent ASINs from an uploaded CSV instead
//     of the database selection. Requires a multipart/form-data
//     upload in field "file" (see jsmanual_asins.go).
func JSMasterSync(stagingClient, productionClient *database.PostgreSQLClient, recorder junglescout.APIUsageRecorder) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Manual mode: parse and validate the CSV BEFORE claiming the global sync
		// manager, so a bad upload cannot leave a half-configured run behind.
		isManual := isManualRequested(c)
		var upload *ManualASINUpload
		if isManual {
			parsed, err := readManualASINUpload(c)
			if err != nil {
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			upload = parsed
		}

		// Check if sync is already running
		syncManagerMutex.Lock()
		if globalSyncManager != nil && globalSyncManager.status != nil && globalSyncManager.status.IsRunning {
			syncManagerMutex.Unlock()
			c.JSON(400, gin.H{
				"error":  "Sync is already in progress",
				"status": globalSyncManager.GetStatus(),
			})
			return
		}

		// Create new sync manager with both clients
		globalSyncManager = NewMasterSyncManager(stagingClient, productionClient, recorder)
		if isManual {
			globalSyncManager.isManual = true
			globalSyncManager.manualASINs = upload.ASINs
			globalSyncManager.manualSource = upload.Filename
		}
		syncManagerMutex.Unlock()

		// Get parameters (query string, falling back to the multipart form so a
		// manual run can send everything in one body)
		marketplace := paramOrDefault(c, "marketplace", "us")
		dateRange := paramOrDefault(c, "date_range", "1month") // Default to 1 month

		// Sync mode determines how to handle existing data
		// "fresh" - Reset all flags and fetch fresh data for all ASINs (default for scheduled syncs)
		// "resume" - Continue from where it left off (for failed syncs)
		// "force" - Same as fresh (kept for backward compatibility)
		syncMode := paramOrDefault(c, "sync_mode", "fresh")

		// Handle legacy force_resync parameter
		forceResync := paramOrDefault(c, "force_resync", "false") == "true"
		if forceResync {
			syncMode = "fresh"
		}

		// Validate sync mode
		if syncMode != "fresh" && syncMode != "resume" {
			c.JSON(400, gin.H{
				"error": "Invalid sync_mode. Must be either 'fresh' or 'resume'",
			})
			return
		}

		// Validate date range
		if dateRange != "1month" && dateRange != "1year" {
			c.JSON(400, gin.H{
				"error": "Invalid date_range. Must be either '1month' or '1year'",
			})
			return
		}

		// Set date range in sync manager
		globalSyncManager.dateRange = dateRange

		// Start sync in background
		go globalSyncManager.RunSync(marketplace, syncMode)

		response := gin.H{
			"message":    "Master sync started",
			"status":     globalSyncManager.GetStatus(),
			"date_range": dateRange,
			"sync_mode":  syncMode,
			"is_manual":  isManual,
		}
		if isManual {
			response["message"] = fmt.Sprintf("Master sync started for %d ASINs from the uploaded CSV", len(upload.ASINs))
			response["upload"] = upload
		}

		c.JSON(200, response)
	}
}

// GetJSSyncStatus returns the current sync status
func GetJSSyncStatus(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		syncManagerMutex.Lock()
		defer syncManagerMutex.Unlock()

		if globalSyncManager == nil {
			c.JSON(200, gin.H{
				"message": "No sync has been initiated yet",
			})
			return
		}

		c.JSON(200, gin.H{
			"status": globalSyncManager.GetStatus(),
		})
	}
}

// RunSync executes the full sync process
func (m *MasterSyncManager) RunSync(marketplace string, syncMode string) {
	m.logger.Println("Starting master sync process...")

	// Initialize status
	m.statusMutex.Lock()
	m.status = &SyncStatus{
		StartedAt:    time.Now(),
		IsRunning:    true,
		DateRange:    m.dateRange,
		IsManual:     m.isManual,
		ManualSource: m.manualSource,
		Errors:       []string{},
	}
	m.statusMutex.Unlock()

	m.logger.Printf("Starting sync with date range: %s, mode: %s, manual: %v", m.dateRange, syncMode, m.isManual)

	defer func() {
		// Mark sync as completed
		m.statusMutex.Lock()
		now := time.Now()
		m.status.CompletedAt = &now
		m.status.IsRunning = false

		// Check if sync was stopped early
		if m.stopRequested {
			m.status.StoppedEarly = true
			if m.status.StopReason == "" {
				m.status.StopReason = "Critical errors encountered"
			}
		}

		m.statusMutex.Unlock()

		if m.stopRequested {
			m.logger.Printf("Sync STOPPED EARLY due to: %s. Total: %d, Successful Product: %d, Successful Sales: %d, Failed: %d, API Calls: %d",
				m.status.StopReason, m.status.TotalASINs, m.status.SuccessfulProductSync,
				m.status.SuccessfulSalesSync, m.status.FailedASINs, m.apiCallCount)
		} else {
			m.logger.Printf("Sync completed normally. Total: %d, Successful Product: %d, Successful Sales: %d, Failed: %d, Total API Calls: %d",
				m.status.TotalASINs, m.status.SuccessfulProductSync,
				m.status.SuccessfulSalesSync, m.status.FailedASINs, m.apiCallCount)
		}

		// Send Discord notification
		m.sendDiscordNotification(syncMode)
	}()

	// Step 0: Resolve the table names the parent/child fan-out needs, once per run
	if err := m.resolveFanOutTables(); err != nil {
		m.addError(fmt.Sprintf("Failed to resolve fan-out tables: %v", err))
		m.logger.Printf("Error resolving fan-out tables: %v", err)
		return
	}

	// Step 1: Determine the PARENT ASINs to sync (children are filled in by
	// fan-out). In manual mode the uploaded CSV *is* the parent set, so the
	// database selection is skipped entirely.
	var asins []string
	if m.isManual {
		asins = m.manualASINs
		m.logger.Printf("MANUAL MODE: using %d parent ASINs from uploaded CSV %q (database ASIN selection skipped)",
			len(asins), m.manualSource)
	} else {
		var err error
		asins, err = m.fetchParentASINsToSync()
		if err != nil {
			m.addError(fmt.Sprintf("Failed to fetch parent ASINs: %v", err))
			m.logger.Printf("Error fetching parent ASINs: %v", err)
			return
		}
	}

	m.statusMutex.Lock()
	m.status.TotalASINs = len(asins)
	m.status.TotalBatches = (len(asins) + 99) / 100 // Calculate total batches (100 ASINs per batch)
	m.statusMutex.Unlock()

	m.logger.Printf("Found %d parent ASINs to sync (children are copied from their parent)", len(asins))

	// Step 2: Initialize or update asin_sync_status table
	if err := m.initializeASINSyncStatus(asins, syncMode); err != nil {
		m.addError(fmt.Sprintf("Failed to initialize ASIN sync status: %v", err))
		m.logger.Printf("Error initializing ASIN sync status: %v", err)
		return
	}

	// Step 3: Sync product data in batches of 100
	m.syncProductData(asins, marketplace)

	// Step 4: Sync sales estimate data for ASINs with successful product data
	m.syncSalesEstimateData(marketplace)
}

// resolveFanOutTables resolves and caches the table names used by the parent ->
// child fan-out for both databases. Called once at the start of a run.
func (m *MasterSyncManager) resolveFanOutTables() error {
	tables, err := resolveSyncTables(m.stagingClient)
	if err != nil {
		return fmt.Errorf("staging: %w", err)
	}
	m.stagingTables = tables

	if m.productionClient != nil {
		prodTables, err := resolveSyncTables(m.productionClient)
		if err != nil {
			return fmt.Errorf("production: %w", err)
		}
		m.prodTables = prodTables
	}
	return nil
}

// fetchParentASINsToSync retrieves the PARENT ASINs the sync must fetch from
// JungleScout: every parent with at least one visible child, plus visible ASINs
// that have no mapping row (treated as their own parent). Children are never
// fetched directly — their data is copied from the parent by the fan-out.
// READ operation - uses stagingClient only
func (m *MasterSyncManager) fetchParentASINsToSync() ([]string, error) {
	query := fmt.Sprintf(`
		SELECT asin FROM (%s) src
		ORDER BY asin
	`, parentASINSourceSQL(m.stagingTables))

	rows, err := m.stagingClient.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query parent ASINs: %w", err)
	}
	defer rows.Close()

	var asins []string
	for rows.Next() {
		var asin string
		if err := rows.Scan(&asin); err != nil {
			m.logger.Printf("Warning: failed to scan ASIN: %v", err)
			continue
		}
		asins = append(asins, asin)
	}

	if err = rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ASINs: %w", err)
	}

	return asins, nil
}

// manualASINFilter returns an extra WHERE predicate restricting a
// jungle_scout_sync_status selection to the CSV-supplied ASINs, or "" when the
// run is not manual. param is the placeholder to bind manualASINArgs() to.
//
// Without it, a manual run would also pick up every ASIN some earlier automatic
// run left with has_product_data = false, and burn JungleScout calls on them.
func (m *MasterSyncManager) manualASINFilter(param string) string {
	if !m.isManual {
		return ""
	}
	return fmt.Sprintf("AND asin = ANY(%s::text[])", param)
}

// manualASINArgs returns the query args that go with manualASINFilter: the ASIN
// array in manual mode, nothing otherwise.
func (m *MasterSyncManager) manualASINArgs() []interface{} {
	if !m.isManual {
		return nil
	}
	return []interface{}{pq.Array(m.manualASINs)}
}

// initializeASINSyncStatus creates/updates entries in the jungle_scout_sync_status table
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) initializeASINSyncStatus(asins []string, syncMode string) error {
	stagingTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	var productionTableName string
	if m.productionClient != nil {
		productionTableName = m.productionClient.TableName("jungle_scout_sync_status")
	}

	// Query used per ASIN. Fresh mode resets every flag so the ASIN is fetched
	// again; resume mode only makes sure the row exists (manual mode only — a CSV
	// can name ASINs that were never queued, and those would otherwise be
	// invisible to the sync_status-driven steps that follow).
	const freshUpsert = `
		INSERT INTO %s (asin, has_product_data, has_sales_data, error, updated_at)
		VALUES ($1, false, false, NULL, CURRENT_TIMESTAMP)
		ON CONFLICT (asin) DO UPDATE SET
			has_product_data = false,
			has_sales_data = false,
			error = NULL,
			product_data_synced_at = NULL,
			sales_estimate_data_synced_at = NULL,
			updated_at = CURRENT_TIMESTAMP
	`
	const insertMissing = `
		INSERT INTO %s (asin, has_product_data, has_sales_data, error, updated_at)
		VALUES ($1, false, false, NULL, CURRENT_TIMESTAMP)
		ON CONFLICT (asin) DO NOTHING
	`

	queryTemplate := freshUpsert
	if syncMode != "fresh" {
		// Resume mode: keep existing sync status, don't reset anything
		if !m.isManual {
			m.logger.Println("RESUME MODE: Keeping existing sync status for all ASINs")
			return nil
		}
		m.logger.Println("RESUME MODE (manual): Keeping existing sync status, only queueing CSV ASINs that are missing")
		queryTemplate = insertMissing
	} else {
		// Fresh sync: reset all fields for ALL ASINs to get latest data
		m.logger.Printf("FRESH SYNC MODE: Resetting %d ASINs to fetch latest data", len(asins))
	}

	// Helper function to run transaction on a database
	runTransaction := func(db *sql.DB, tableName string, dbName string) error {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start %s transaction: %w", dbName, err)
		}
		defer tx.Rollback()

		upsertQuery := fmt.Sprintf(queryTemplate, tableName)

		stmt, err := tx.Prepare(upsertQuery)
		if err != nil {
			return fmt.Errorf("failed to prepare %s statement: %w", dbName, err)
		}
		defer stmt.Close()

		for _, asin := range asins {
			if _, err := stmt.Exec(asin); err != nil {
				m.logger.Printf("Warning: failed to initialize ASIN %s on %s: %v", asin, dbName, err)
			}
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit %s transaction: %w", dbName, err)
		}
		return nil
	}

	// Step 1: Write to STAGING first
	if err := runTransaction(m.stagingClient.DB, stagingTableName, "staging"); err != nil {
		return err
	}
	m.logger.Println("Staging sync status initialized successfully")

	// Staging-only mode: no production client, nothing more to do.
	if m.productionClient == nil {
		return nil
	}

	// Step 2: Write to PRODUCTION with retry (3 attempts)
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		prodErr = runTransaction(m.productionClient.DB, productionTableName, "production")
		if prodErr == nil {
			m.logger.Println("Production sync status initialized successfully")
			return nil
		}
		m.logger.Printf("Production sync status init attempt %d failed: %v", attempt, prodErr)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	return fmt.Errorf("production sync status init failed after 3 retries: %w", prodErr)
}

// syncProductData syncs product data in batches of 100 ASINs
// READ operations use stagingClient, WRITE operations use dual-write
func (m *MasterSyncManager) syncProductData(allASINs []string, marketplace string) {
	m.logger.Println("Starting product data sync...")

	// Test database connections before starting
	if err := m.stagingClient.DB.Ping(); err != nil {
		m.logger.Printf("CRITICAL: Staging database connection failed before product sync: %v", err)
		m.stopRequested = true
		m.addError(fmt.Sprintf("Staging database unreachable: %v", err))
		return
	}
	if m.productionClient != nil {
		if err := m.productionClient.DB.Ping(); err != nil {
			m.logger.Printf("CRITICAL: Production database connection failed before product sync: %v", err)
			m.stopRequested = true
			m.addError(fmt.Sprintf("Production database unreachable: %v", err))
			return
		}
	}

	// Filter ASINs to only those that need product data sync (READ from staging).
	// In manual mode the selection is additionally scoped to the uploaded ASINs.
	statusTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	query := fmt.Sprintf(`
		SELECT asin
		FROM %s
		WHERE (has_product_data = false OR has_product_data IS NULL)
		%s
		ORDER BY asin
	`, statusTableName, m.manualASINFilter("$1"))

	rows, err := m.stagingClient.DB.Query(query, m.manualASINArgs()...)
	if err != nil {
		m.logger.Printf("CRITICAL: Failed to fetch ASINs needing product sync: %v", err)
		m.stopRequested = true
		m.addError(fmt.Sprintf("Database query failure: %v", err))
		return
	}
	defer rows.Close()

	var asins []string
	for rows.Next() {
		var asin string
		if err := rows.Scan(&asin); err != nil {
			continue
		}
		asins = append(asins, asin)
	}

	m.logger.Printf("Found %d ASINs that need product data sync (out of %d total)", len(asins), len(allASINs))

	// Update status with actual ASINs to process
	m.statusMutex.Lock()
	m.status.TotalBatches = (len(asins) + 99) / 100 // Update total batches for ASINs that actually need sync
	m.statusMutex.Unlock()

	if len(asins) == 0 {
		m.logger.Println("No ASINs need product data sync, skipping...")
		return
	}

	batchSize := 100
	totalBatches := (len(asins) + batchSize - 1) / batchSize

	for i := 0; i < len(asins); i += batchSize {
		end := i + batchSize
		if end > len(asins) {
			end = len(asins)
		}

		batch := asins[i:end]
		currentBatch := (i / batchSize) + 1

		m.statusMutex.Lock()
		m.status.CurrentBatch = currentBatch
		m.statusMutex.Unlock()

		// Check if sync should stop due to critical errors
		if m.stopRequested {
			m.logger.Printf("CRITICAL: Sync stopped due to critical database errors")
			m.addError("Sync aborted due to critical database errors")
			break
		}

		m.logger.Printf("Processing product batch %d/%d (%d ASINs)", currentBatch, totalBatches, len(batch))

		// Fetch product data from JungleScout
		apiResponse, err := m.jsClient.FetchProductData(batch, marketplace)

		// Increment API call counter
		m.apiCallCount++
		m.statusMutex.Lock()
		m.status.TotalAPICalls = m.apiCallCount
		m.statusMutex.Unlock()

		if err != nil {
			m.addError(fmt.Sprintf("Batch %d product fetch failed: %v", currentBatch, err))
			m.logger.Printf("Error fetching product data for batch %d: %v", currentBatch, err)
			m.updateBatchError(batch, fmt.Sprintf("Product fetch error: %v", err))
			continue
		}

		// Process and store the product data
		successCount := m.storeProductData(apiResponse, marketplace)

		// Check if we should stop due to database errors
		if successCount == 0 && len(apiResponse.Data) > 0 {
			m.criticalErrors++
			if m.criticalErrors >= 3 {
				m.logger.Printf("CRITICAL: Too many database failures. Stopping sync.")
				m.stopRequested = true
				m.addError("Too many consecutive database failures")
				break
			}
		} else {
			m.criticalErrors = 0 // Reset on success
		}

		m.statusMutex.Lock()
		m.status.ProcessedASINs += len(batch)
		m.status.SuccessfulProductSync += successCount
		m.status.FailedASINs += len(batch) - successCount
		m.statusMutex.Unlock()

		m.logger.Printf("Batch %d completed: %d/%d successful", currentBatch, successCount, len(batch))

		// Add small delay between batches to be respectful to the API
		if i+batchSize < len(asins) {
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// storeProductData stores the fetched product data and updates sync status
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) storeProductData(apiResponse *junglescout.ProductAPIResponse, marketplace string) int {
	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return 0
	}

	// Table names for both databases
	stagingProductTable := m.stagingClient.TableName("jungle_scout_product_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodProductTable, prodStatusTable string
	if m.productionClient != nil {
		prodProductTable = m.productionClient.TableName("jungle_scout_product_data")
		prodStatusTable = m.productionClient.TableName("jungle_scout_sync_status")
	}
	reportDate := time.Now().Format("2006-01-02")

	// Build query template (table name will be substituted)
	buildProductQuery := func(tableName string) string {
		return fmt.Sprintf(`
			INSERT INTO %s (
				asin, report_date, id, title, price, reviews, category, rating,
				image_url, parent_asin, is_variant, seller_type, variants,
				breadcrumb_path, is_standalone, is_parent, is_available, brand,
				product_rank, weight_value, weight_unit, length_value, width_value,
				height_value, dimensions_unit, listing_quality_score,
				number_of_sellers, buy_box_owner, buy_box_owner_seller_id,
				date_first_available, date_first_available_is_estimated,
				approximate_30_day_revenue, approximate_30_day_units_sold,
				subcategory_ranks, fee_breakdown, ean_list, isbn_list, upc_list,
				gtin_list, variant_reviews, updated_at, created_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
				$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
				$29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42
			)
			ON CONFLICT (asin, report_date) DO UPDATE SET
				title = EXCLUDED.title,
				price = EXCLUDED.price,
				reviews = EXCLUDED.reviews,
				category = EXCLUDED.category,
				rating = EXCLUDED.rating,
				image_url = EXCLUDED.image_url,
				parent_asin = EXCLUDED.parent_asin,
				is_variant = EXCLUDED.is_variant,
				seller_type = EXCLUDED.seller_type,
				variants = EXCLUDED.variants,
				breadcrumb_path = EXCLUDED.breadcrumb_path,
				is_standalone = EXCLUDED.is_standalone,
				is_parent = EXCLUDED.is_parent,
				is_available = EXCLUDED.is_available,
				brand = EXCLUDED.brand,
				product_rank = EXCLUDED.product_rank,
				weight_value = EXCLUDED.weight_value,
				weight_unit = EXCLUDED.weight_unit,
				length_value = EXCLUDED.length_value,
				width_value = EXCLUDED.width_value,
				height_value = EXCLUDED.height_value,
				dimensions_unit = EXCLUDED.dimensions_unit,
				listing_quality_score = EXCLUDED.listing_quality_score,
				number_of_sellers = EXCLUDED.number_of_sellers,
				buy_box_owner = EXCLUDED.buy_box_owner,
				buy_box_owner_seller_id = EXCLUDED.buy_box_owner_seller_id,
				date_first_available = EXCLUDED.date_first_available,
				date_first_available_is_estimated = EXCLUDED.date_first_available_is_estimated,
				approximate_30_day_revenue = EXCLUDED.approximate_30_day_revenue,
				approximate_30_day_units_sold = EXCLUDED.approximate_30_day_units_sold,
				subcategory_ranks = EXCLUDED.subcategory_ranks,
				fee_breakdown = EXCLUDED.fee_breakdown,
				ean_list = EXCLUDED.ean_list,
				isbn_list = EXCLUDED.isbn_list,
				upc_list = EXCLUDED.upc_list,
				gtin_list = EXCLUDED.gtin_list,
				variant_reviews = EXCLUDED.variant_reviews,
				updated_at = EXCLUDED.updated_at
		`, tableName)
	}

	buildStatusQuery := func(tableName string) string {
		return fmt.Sprintf(`
			UPDATE %s
			SET has_product_data = $2,
			    error = $3,
			    product_data_synced_at = $4,
			    updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)
	}

	successCount := 0
	var criticalError error

	for _, product := range apiResponse.Data {
		attrs := product.Attributes

		// Extract ASIN from ID
		asin := product.ID
		if strings.Contains(product.ID, "/") {
			parts := strings.Split(product.ID, "/")
			if len(parts) == 2 {
				asin = parts[1]
			}
		}

		// Convert data to JSON
		variantsJSON, _ := json.Marshal(attrs.Variants)
		subcategoryRanksJSON, _ := json.Marshal(attrs.SubcategoryRanks)
		feeBreakdownJSON, _ := json.Marshal(attrs.FeeBreakdown)
		eanListJSON, _ := json.Marshal(attrs.EANList)
		isbnListJSON, _ := json.Marshal(attrs.ISBNList)
		upcListJSON, _ := json.Marshal(attrs.UPCList)
		gtinListJSON, _ := json.Marshal(attrs.GTINList)

		// Parse dates
		var dateFirstAvailable sql.NullTime
		if attrs.DateFirstAvailable != "" {
			if parsedDate, err := time.Parse("2006-01-02", attrs.DateFirstAvailable); err == nil {
				dateFirstAvailable = sql.NullTime{Time: parsedDate, Valid: true}
			}
		}

		var updatedAt sql.NullTime
		if attrs.UpdatedAt != "" {
			if parsedTime, err := time.Parse(time.RFC3339, attrs.UpdatedAt); err == nil {
				updatedAt = sql.NullTime{Time: parsedTime, Valid: true}
			}
		}

		// Stop immediately if we've encountered a critical error
		if criticalError != nil {
			break
		}

		// Prepare args for product query
		productArgs := []interface{}{
			asin, reportDate, product.ID, attrs.Title, attrs.Price, attrs.Reviews,
			attrs.Category, attrs.Rating, attrs.ImageURL, attrs.ParentASIN,
			attrs.IsVariant, attrs.SellerType, variantsJSON, attrs.BreadcrumbPath,
			attrs.IsStandalone, attrs.IsParent, attrs.IsAvailable, attrs.Brand,
			attrs.ProductRank, attrs.WeightValue, attrs.WeightUnit, attrs.LengthValue,
			attrs.WidthValue, attrs.HeightValue, attrs.DimensionsUnit,
			attrs.ListingQualityScore, attrs.NumberOfSellers, attrs.BuyBoxOwner,
			attrs.BuyBoxOwnerSellerID, dateFirstAvailable,
			attrs.DateFirstAvailableIsEstimated, attrs.Approximate30DayRevenue,
			attrs.Approximate30DayUnitsSold, subcategoryRanksJSON, feeBreakdownJSON,
			eanListJSON, isbnListJSON, upcListJSON, gtinListJSON,
			attrs.VariantReviews, updatedAt, time.Now(),
		}

		// Step 1: Write to STAGING first
		_, err := m.stagingClient.DB.Exec(buildProductQuery(stagingProductTable), productArgs...)
		if err != nil {
			m.logger.Printf("CRITICAL: Failed to store product data for ASIN %s on staging: %v", asin, err)
			m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, false, fmt.Sprintf("Failed to store product data: %v", err), nil)
			criticalError = err
			m.stopRequested = true
			m.addError(fmt.Sprintf("Staging database failure storing ASIN %s: %v", asin, err))
			break
		}

		// Update staging sync status
		if _, err := m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, true, nil, time.Now()); err != nil {
			m.logger.Printf("CRITICAL: Failed to update staging sync status for ASIN %s: %v", asin, err)
			criticalError = err
			m.stopRequested = true
			m.addError(fmt.Sprintf("Staging database failure updating status for ASIN %s: %v", asin, err))
			break
		}

		// Step 2: Write to PRODUCTION with retry (3 attempts).
		// Skipped entirely when running staging-only (no production client).
		if m.productionClient != nil {
			var prodErr error
			for attempt := 1; attempt <= 3; attempt++ {
				_, prodErr = m.productionClient.DB.Exec(buildProductQuery(prodProductTable), productArgs...)
				if prodErr == nil {
					// Update production sync status
					_, prodErr = m.productionClient.DB.Exec(buildStatusQuery(prodStatusTable), asin, true, nil, time.Now())
					if prodErr == nil {
						break
					}
				}
				m.logger.Printf("Production write attempt %d for ASIN %s failed: %v", attempt, asin, prodErr)
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}

			if prodErr != nil {
				m.logger.Printf("CRITICAL: Failed to store product data for ASIN %s on production after 3 retries: %v", asin, prodErr)
				m.productionClient.DB.Exec(buildStatusQuery(prodStatusTable), asin, false, fmt.Sprintf("Failed to store product data after 3 retries: %v", prodErr), nil)
				criticalError = prodErr
				m.stopRequested = true
				m.addError(fmt.Sprintf("Production database failure storing ASIN %s: %v", asin, prodErr))
				break
			}
		}

		// Step 3: Copy this parent's row onto every visible child.
		// A fan-out failure is NOT critical: the parent row is already stored and
		// the API call is not wasted, so log it and keep going.
		if childRows, fanErr := fanOutProductRow(m.stagingClient, m.stagingTables, asin, reportDate); fanErr != nil {
			m.logger.Printf("WARNING: staging %v", fanErr)
			m.addError(fmt.Sprintf("Staging product fan-out failed for parent %s: %v", asin, fanErr))
		} else if childRows > 0 {
			m.logger.Printf("Copied product data for parent %s to %d child ASINs", asin, childRows)
		}

		if m.productionClient != nil {
			if _, fanErr := fanOutProductRow(m.productionClient, m.prodTables, asin, reportDate); fanErr != nil {
				m.logger.Printf("WARNING: production %v", fanErr)
				m.addError(fmt.Sprintf("Production product fan-out failed for parent %s: %v", asin, fanErr))
			}
		}

		successCount++
	}

	// Log critical error if occurred
	if criticalError != nil {
		m.logger.Printf("CRITICAL: Stopping product sync due to database error. Successfully stored %d/%d products before failure.",
			successCount, len(apiResponse.Data))
		return successCount
	}

	return successCount
}

// syncSalesEstimateData syncs sales estimate data for ASINs with successful product data
// READ operations use stagingClient, WRITE operations use dual-write
func (m *MasterSyncManager) syncSalesEstimateData(marketplace string) {
	m.logger.Println("Starting sales estimate data sync...")

	// Test database connections before starting
	if err := m.stagingClient.DB.Ping(); err != nil {
		m.logger.Printf("CRITICAL: Staging database connection failed before sales sync: %v", err)
		m.stopRequested = true
		m.addError(fmt.Sprintf("Staging database unreachable: %v", err))
		return
	}
	if m.productionClient != nil {
		if err := m.productionClient.DB.Ping(); err != nil {
			m.logger.Printf("CRITICAL: Production database connection failed before sales sync: %v", err)
			m.stopRequested = true
			m.addError(fmt.Sprintf("Production database unreachable: %v", err))
			return
		}
	}

	// Get ASINs that have product data but no sales data (READ from staging).
	// In manual mode the selection is additionally scoped to the uploaded ASINs.
	statusTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	query := fmt.Sprintf(`
		SELECT asin
		FROM %s
		WHERE has_product_data = true AND has_sales_data = false
		%s
		ORDER BY asin
	`, statusTableName, m.manualASINFilter("$1"))

	rows, err := m.stagingClient.DB.Query(query, m.manualASINArgs()...)
	if err != nil {
		m.addError(fmt.Sprintf("Failed to fetch ASINs for sales sync: %v", err))
		m.logger.Printf("Error fetching ASINs for sales sync: %v", err)
		return
	}
	defer rows.Close()

	var asins []string
	for rows.Next() {
		var asin string
		if err := rows.Scan(&asin); err != nil {
			continue
		}
		asins = append(asins, asin)
	}

	m.logger.Printf("Found %d ASINs that need sales data sync (have product data but no sales data)", len(asins))

	if len(asins) == 0 {
		m.logger.Println("No ASINs need sales data sync, skipping...")
		return
	}

	// Calculate date range based on dateRange parameter
	// JungleScout requires end_date to be at least 1 day in the past
	endDate := time.Now().AddDate(0, 0, -1).Format("2006-01-02") // Yesterday
	var startDate string

	if m.dateRange == "1year" {
		// Fetch 1 year of data (from 1 year before yesterday)
		startDate = time.Now().AddDate(-1, 0, -1).Format("2006-01-02")
		m.logger.Printf("Fetching 1 year of sales data (from %s to %s)", startDate, endDate)
	} else {
		// Default to 1 month of data (from 1 month before yesterday)
		startDate = time.Now().AddDate(0, -1, -1).Format("2006-01-02")
		m.logger.Printf("Fetching 1 month of sales data (from %s to %s)", startDate, endDate)
	}

	// Process each ASIN individually (sales API only supports 1 ASIN per request)
	var successCount int32
	var failCount int32

	// Use a worker pool to process ASINs concurrently but with controlled concurrency
	workerCount := 5 // Process 5 ASINs concurrently
	asinChan := make(chan string, len(asins))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for asin := range asinChan {
				// Check if we should stop processing
				if m.stopRequested {
					m.logger.Printf("Worker %d: Stopping due to critical errors", workerID)
					return
				}

				m.logger.Printf("Worker %d: Processing sales data for ASIN %s", workerID, asin)

				// Fetch sales estimate data
				apiResponse, err := m.jsClient.FetchSalesEstimateData(asin, marketplace, startDate, endDate)

				// Increment API call counter (thread-safe)
				m.statusMutex.Lock()
				m.apiCallCount++
				m.status.TotalAPICalls = m.apiCallCount
				m.statusMutex.Unlock()

				if err != nil {
					m.logger.Printf("Worker %d: Error fetching sales data for ASIN %s: %v", workerID, asin, err)
					m.updateASINSyncStatus(asin, true, false, fmt.Sprintf("Sales fetch error: %v", err))
					atomic.AddInt32(&failCount, 1)
					continue
				}

				// Store sales data
				if !m.storeSalesEstimateData(apiResponse, marketplace) {
					atomic.AddInt32(&failCount, 1)

					// Check if it was a critical database error
					if m.stopRequested {
						m.logger.Printf("Worker %d: Stopping due to database critical error", workerID)
						return
					}
				} else {
					atomic.AddInt32(&successCount, 1)
				}
			}
		}(i)
	}

	// Send ASINs to workers
	for _, asin := range asins {
		asinChan <- asin
	}
	close(asinChan)

	// Wait for all workers to complete
	wg.Wait()

	m.statusMutex.Lock()
	m.status.SuccessfulSalesSync = int(successCount)
	m.status.FailedASINs += int(failCount)
	m.statusMutex.Unlock()

	m.logger.Printf("Sales data sync completed: %d successful, %d failed", successCount, failCount)
}

// storeSalesEstimateData stores the fetched sales estimate data with individual commits
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) storeSalesEstimateData(apiResponse *junglescout.SalesEstimateAPIResponse, marketplace string) bool {
	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return false
	}

	// Table names for both databases
	stagingSalesTable := m.stagingClient.TableName("jungle_scout_sales_estimate_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodSalesTable, prodStatusTable string
	if m.productionClient != nil {
		prodSalesTable = m.productionClient.TableName("jungle_scout_sales_estimate_data")
		prodStatusTable = m.productionClient.TableName("jungle_scout_sync_status")
	}

	// Process the first (and only) data item
	jsData := apiResponse.Data[0]
	attrs := jsData.Attributes

	// Handle parent_asin
	var parentASIN *string
	if attrs.ParentASIN != "" {
		parentASIN = &attrs.ParentASIN
	}

	// Track success/failure for each data point
	successCount := 0
	failCount := 0
	var lastError error

	// Build sales upsert query template
	buildSalesQuery := func(tableName string) string {
		return fmt.Sprintf(`
			INSERT INTO %s (asin, marketplace, is_parent, is_variant, is_standalone,
			                parent_asin, date, estimated_units_sold, last_known_price)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (asin, marketplace, date)
			DO UPDATE SET
				is_parent = EXCLUDED.is_parent,
				is_variant = EXCLUDED.is_variant,
				is_standalone = EXCLUDED.is_standalone,
				parent_asin = EXCLUDED.parent_asin,
				estimated_units_sold = EXCLUDED.estimated_units_sold,
				last_known_price = EXCLUDED.last_known_price,
				updated_at = CURRENT_TIMESTAMP
		`, tableName)
	}

	// Insert each daily data point with dual-write
	for _, dataPoint := range attrs.Data {
		args := []interface{}{
			attrs.ASIN, marketplace, attrs.IsParent, attrs.IsVariant,
			attrs.IsStandalone, parentASIN, dataPoint.Date,
			dataPoint.EstimatedUnitsSold, dataPoint.LastKnownPrice,
		}

		// Step 1: Write to STAGING first
		_, err := m.stagingClient.DB.Exec(buildSalesQuery(stagingSalesTable), args...)
		if err != nil {
			m.logger.Printf("CRITICAL: Failed to insert sales data for ASIN %s, date %s on staging: %v",
				attrs.ASIN, dataPoint.Date, err)
			failCount++
			lastError = err

			// Stop on first staging error
			m.logger.Printf("CRITICAL: Staging database error. Stopping sync to prevent API waste.")
			m.addError(fmt.Sprintf("Staging database critical failure for ASIN %s: %v", attrs.ASIN, err))
			m.stopRequested = true
			m.updateASINSyncStatus(attrs.ASIN, true, false,
				fmt.Sprintf("Critical staging DB error: %v", err))
			return false
		}

		// Step 2: Write to PRODUCTION with retry (3 attempts).
		// Skipped entirely when running staging-only (no production client).
		if m.productionClient != nil {
			var prodErr error
			for attempt := 1; attempt <= 3; attempt++ {
				_, prodErr = m.productionClient.DB.Exec(buildSalesQuery(prodSalesTable), args...)
				if prodErr == nil {
					break
				}
				m.logger.Printf("Production write attempt %d for ASIN %s date %s failed: %v", attempt, attrs.ASIN, dataPoint.Date, prodErr)
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}

			if prodErr != nil {
				m.logger.Printf("CRITICAL: Failed to insert sales data for ASIN %s, date %s on production after 3 retries: %v",
					attrs.ASIN, dataPoint.Date, prodErr)
				failCount++
				lastError = prodErr

				// Stop on production failure after retries
				m.addError(fmt.Sprintf("Production database critical failure for ASIN %s: %v", attrs.ASIN, prodErr))
				m.stopRequested = true
				m.updateASINSyncStatus(attrs.ASIN, true, false,
					fmt.Sprintf("Critical production DB error after 3 retries: %v", prodErr))
				return false
			}
		}

		successCount++
	}

	// Copy every sales row of this parent onto its visible children.
	// Non-critical: the parent rows are stored and the API call is not wasted.
	if successCount > 0 {
		if childRows, fanErr := fanOutSalesRows(m.stagingClient, m.stagingTables, attrs.ASIN, marketplace, ""); fanErr != nil {
			m.logger.Printf("WARNING: staging %v", fanErr)
			m.addError(fmt.Sprintf("Staging sales fan-out failed for parent %s: %v", attrs.ASIN, fanErr))
		} else if childRows > 0 {
			m.logger.Printf("Copied %d sales rows from parent %s to its child ASINs", childRows, attrs.ASIN)
		}

		if m.productionClient != nil {
			if _, fanErr := fanOutSalesRows(m.productionClient, m.prodTables, attrs.ASIN, marketplace, ""); fanErr != nil {
				m.logger.Printf("WARNING: production %v", fanErr)
				m.addError(fmt.Sprintf("Production sales fan-out failed for parent %s: %v", attrs.ASIN, fanErr))
			}
		}
	}

	// Update sync status based on results (dual-write)
	if successCount > 0 {
		buildSuccessStatusQuery := func(tableName string) string {
			return fmt.Sprintf(`
				UPDATE %s
				SET has_sales_data = true,
				    sales_estimate_data_synced_at = CURRENT_TIMESTAMP,
				    error = NULL,
				    updated_at = CURRENT_TIMESTAMP
				WHERE asin = $1
			`, tableName)
		}

		// Update staging status
		if _, err := m.stagingClient.DB.Exec(buildSuccessStatusQuery(stagingStatusTable), attrs.ASIN); err != nil {
			m.logger.Printf("Failed to update staging sync status for ASIN %s: %v", attrs.ASIN, err)
		}

		// Update production status with retry (skipped when staging-only)
		if m.productionClient != nil {
			var prodErr error
			for attempt := 1; attempt <= 3; attempt++ {
				_, prodErr = m.productionClient.DB.Exec(buildSuccessStatusQuery(prodStatusTable), attrs.ASIN)
				if prodErr == nil {
					break
				}
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}
			if prodErr != nil {
				m.logger.Printf("Failed to update production sync status for ASIN %s after 3 retries: %v", attrs.ASIN, prodErr)
			}
		}

		m.logger.Printf("Sales data for ASIN %s: %d/%d succeeded, %d failed",
			attrs.ASIN, successCount, len(attrs.Data), failCount)
		return true
	}

	// Complete failure - update error status on both DBs
	buildErrorStatusQuery := func(tableName string) string {
		return fmt.Sprintf(`
			UPDATE %s
			SET error = $2,
			    updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)
	}

	errorMsg := fmt.Sprintf("Failed to sync all %d records. Last error: %v", len(attrs.Data), lastError)
	m.stagingClient.DB.Exec(buildErrorStatusQuery(stagingStatusTable), attrs.ASIN, errorMsg)
	if m.productionClient != nil {
		m.productionClient.DB.Exec(buildErrorStatusQuery(prodStatusTable), attrs.ASIN, errorMsg)
	}

	m.logger.Printf("Failed to sync any sales data for ASIN %s", attrs.ASIN)
	return false
}

// updateASINSyncStatus updates the sync status for a specific ASIN
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) updateASINSyncStatus(asin string, hasProductData, hasSalesData bool, errorMsg string) {
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodStatusTable string
	if m.productionClient != nil {
		prodStatusTable = m.productionClient.TableName("jungle_scout_sync_status")
	}

	buildQuery := func(tableName string, hasError bool) string {
		if hasError {
			return fmt.Sprintf(`
				UPDATE %s
				SET has_product_data = $2,
				    has_sales_data = $3,
				    error = $4,
				    updated_at = CURRENT_TIMESTAMP
				WHERE asin = $1
			`, tableName)
		}
		return fmt.Sprintf(`
			UPDATE %s
			SET has_product_data = $2,
			    has_sales_data = $3,
			    error = NULL,
			    updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)
	}

	var args []interface{}
	hasError := errorMsg != ""
	if hasError {
		args = []interface{}{asin, hasProductData, hasSalesData, errorMsg}
	} else {
		args = []interface{}{asin, hasProductData, hasSalesData}
	}

	// Step 1: Write to STAGING first
	if _, err := m.stagingClient.DB.Exec(buildQuery(stagingStatusTable, hasError), args...); err != nil {
		m.logger.Printf("Failed to update staging sync status for ASIN %s: %v", asin, err)
	}

	// Step 2: Write to PRODUCTION with retry (skipped when staging-only)
	if m.productionClient == nil {
		return
	}
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		_, prodErr = m.productionClient.DB.Exec(buildQuery(prodStatusTable, hasError), args...)
		if prodErr == nil {
			break
		}
		m.logger.Printf("Production status update attempt %d for ASIN %s failed: %v", attempt, asin, prodErr)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	if prodErr != nil {
		m.logger.Printf("Failed to update production sync status for ASIN %s after 3 retries: %v", asin, prodErr)
	}
}

// updateBatchError updates error status for a batch of ASINs
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) updateBatchError(asins []string, errorMsg string) {
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodStatusTable string
	if m.productionClient != nil {
		prodStatusTable = m.productionClient.TableName("jungle_scout_sync_status")
	}

	// Helper function to run batch update on a database
	runBatchUpdate := func(db *sql.DB, tableName string, dbName string) error {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start %s transaction: %w", dbName, err)
		}
		defer tx.Rollback()

		query := fmt.Sprintf(`
			UPDATE %s
			SET error = $2,
			    updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)

		stmt, err := tx.Prepare(query)
		if err != nil {
			return fmt.Errorf("failed to prepare %s statement: %w", dbName, err)
		}
		defer stmt.Close()

		for _, asin := range asins {
			stmt.Exec(asin, errorMsg)
		}

		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit %s transaction: %w", dbName, err)
		}
		return nil
	}

	// Step 1: Write to STAGING first
	if err := runBatchUpdate(m.stagingClient.DB, stagingStatusTable, "staging"); err != nil {
		m.logger.Printf("Failed to update staging batch error: %v", err)
	}

	// Step 2: Write to PRODUCTION with retry (skipped when staging-only)
	if m.productionClient == nil {
		return
	}
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		prodErr = runBatchUpdate(m.productionClient.DB, prodStatusTable, "production")
		if prodErr == nil {
			break
		}
		m.logger.Printf("Production batch error update attempt %d failed: %v", attempt, prodErr)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
	if prodErr != nil {
		m.logger.Printf("Failed to update production batch error after 3 retries: %v", prodErr)
	}
}

// addError adds an error to the status
func (m *MasterSyncManager) addError(errorMsg string) {
	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	if m.status != nil {
		m.status.Errors = append(m.status.Errors, errorMsg)
	}
}

// GetStatus returns a copy of the current status
func (m *MasterSyncManager) GetStatus() SyncStatus {
	m.statusMutex.RLock()
	defer m.statusMutex.RUnlock()

	if m.status == nil {
		return SyncStatus{}
	}

	return *m.status
}

// asinSourceLabel describes where this run's parent ASINs came from.
func (m *MasterSyncManager) asinSourceLabel() string {
	return asinSourceLabel(m.isManual, m.manualSource)
}

// asinSourceLabel describes where this run's ASINs came from.
func (m *HourlySyncManager) asinSourceLabel() string {
	return asinSourceLabel(m.isManual, m.manualSource)
}

// sendDiscordNotification sends a webhook notification to Discord with sync results
func (m *MasterSyncManager) sendDiscordNotification(syncMode string) {
	// Get webhook URL from environment or use the provided one
	webhookURL := os.Getenv("DISCORD_WEBHOOK_URL")
	if webhookURL == "" {
		// Use the provided webhook URL as default
		webhookURL = "https://discord.com/api/webhooks/1422178082483212348/g7q_D2qbjrZNMbIgmGV8AuegJCL7GOLA0QZrcoPMgB5J4Cpxnl_PCexzDzUP6sbJMxTz"
	}

	// Calculate duration
	duration := time.Since(m.status.StartedAt).Round(time.Second)

	// Determine color based on status
	var color int
	var statusText string
	if m.stopRequested {
		color = 15158332 // Red
		statusText = fmt.Sprintf("⛔ Sync STOPPED EARLY: %s", m.status.StopReason)
	} else if m.status.FailedASINs > 0 {
		color = 16776960 // Yellow
		statusText = "⚠️ Sync completed with some failures"
	} else {
		color = 3066993 // Green
		statusText = "✅ Sync completed successfully"
	}

	// Create Discord embed
	embed := map[string]interface{}{
		"title":       "JungleScout Master Sync Report",
		"description": statusText,
		"color":       color,
		"fields": []map[string]interface{}{
			{
				"name":   "📊 Total ASINs",
				"value":  fmt.Sprintf("%d", m.status.TotalASINs),
				"inline": true,
			},
			{
				"name":   "✅ Product Syncs",
				"value":  fmt.Sprintf("%d", m.status.SuccessfulProductSync),
				"inline": true,
			},
			{
				"name":   "📈 Sales Syncs",
				"value":  fmt.Sprintf("%d", m.status.SuccessfulSalesSync),
				"inline": true,
			},
			{
				"name":   "❌ Failed",
				"value":  fmt.Sprintf("%d", m.status.FailedASINs),
				"inline": true,
			},
			{
				"name":   "🔌 API Calls",
				"value":  fmt.Sprintf("%d", m.apiCallCount),
				"inline": true,
			},
			{
				"name":   "⏱️ Duration",
				"value":  duration.String(),
				"inline": true,
			},
			{
				"name":   "📅 Date Range",
				"value":  m.dateRange,
				"inline": true,
			},
			{
				"name":   "🔄 Sync Mode",
				"value":  syncMode,
				"inline": true,
			},
			{
				"name":   "🗂️ ASIN Source",
				"value":  m.asinSourceLabel(),
				"inline": true,
			},
		},
		"timestamp": m.status.StartedAt.Format(time.RFC3339),
		"footer": map[string]string{
			"text": "JungleScout Sync Bot",
		},
	}

	// Add errors field if there are any
	if len(m.status.Errors) > 0 {
		errorText := strings.Join(m.status.Errors[:minInt(5, len(m.status.Errors))], "\n")
		if len(m.status.Errors) > 5 {
			errorText += fmt.Sprintf("\n... and %d more errors", len(m.status.Errors)-5)
		}
		embed["fields"] = append(embed["fields"].([]map[string]interface{}), map[string]interface{}{
			"name":   "🚨 Errors",
			"value":  errorText,
			"inline": false,
		})
	}

	// Create webhook payload
	payload := map[string]interface{}{
		"content": fmt.Sprintf("Sync completed: %d Total, %d Product, %d Sales, %d Failed, %d API Calls",
			m.status.TotalASINs, m.status.SuccessfulProductSync,
			m.status.SuccessfulSalesSync, m.status.FailedASINs, m.apiCallCount),
		"embeds": []interface{}{embed},
	}

	// Convert payload to JSON
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		m.logger.Printf("Failed to marshal Discord webhook payload: %v", err)
		return
	}

	// Send webhook
	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		m.logger.Printf("Failed to send Discord webhook: %v", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		m.logger.Printf("Discord webhook returned status %d", resp.StatusCode)
	} else {
		m.logger.Println("Discord notification sent successfully")
	}
}

// minInt returns the minimum of two integers
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// execOnBothDBs executes a query on staging first, then production with 3 retries
func (m *MasterSyncManager) execOnBothDBs(query string, args ...interface{}) error {
	// Write to staging first
	if _, err := m.stagingClient.DB.Exec(query, args...); err != nil {
		return fmt.Errorf("staging: %w", err)
	}

	// Write to production with retry (skipped when staging-only)
	if m.productionClient == nil {
		return nil
	}
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		if _, prodErr = m.productionClient.DB.Exec(query, args...); prodErr == nil {
			return nil
		}
		m.logger.Printf("Production attempt %d failed: %v", attempt, prodErr)
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second) // 1s, 2s backoff
		}
	}
	return fmt.Errorf("production failed after 3 retries: %w", prodErr)
}

// ============================================================================
// HOURLY SYNC HANDLER FUNCTIONS
// ============================================================================

// JSHourlySync handles the hourly synchronization endpoint for cloud jobs
// (POST /admin/hourly-sync).
//
// Query/form params:
//   - marketplace: Amazon marketplace (default: "us")
//   - debug: Enable verbose logging (default: "false")
//   - is_manual: "true" to take the ASINs from an uploaded CSV. Equivalent to
//     calling POST /admin/hourly-sync/manual, which is the preferred
//     entry point for a CSV-driven run.
func JSHourlySync(stagingClient, productionClient *database.PostgreSQLClient, recorder junglescout.APIUsageRecorder) gin.HandlerFunc {
	return runHourlySyncHandler(stagingClient, productionClient, recorder, false)
}

// JSManualHourlySync handles the manual, CSV-driven hourly sync
// (POST /admin/hourly-sync/manual). It is the same run as JSHourlySync with
// is_manual forced on, so the CSV is mandatory: a request without one is
// rejected instead of silently falling back to the automatic ASIN selection.
//
// The uploaded ASINs are treated as parent ASINs and are NOT capped at
// HourlySyncASINLimit, so the request runs as long as the CSV needs — keep
// manual uploads small enough for the caller's timeout.
//
// Form params (multipart/form-data):
//   - file: the CSV of parent ASINs (required, see jsmanual_asins.go)
//   - marketplace: Amazon marketplace (default: "us")
//   - debug: Enable verbose logging (default: "false")
func JSManualHourlySync(stagingClient, productionClient *database.PostgreSQLClient, recorder junglescout.APIUsageRecorder) gin.HandlerFunc {
	return runHourlySyncHandler(stagingClient, productionClient, recorder, true)
}

// runHourlySyncHandler builds the handler shared by the automatic and manual
// hourly endpoints. forceManual makes the CSV upload mandatory.
func runHourlySyncHandler(stagingClient, productionClient *database.PostgreSQLClient, recorder junglescout.APIUsageRecorder, forceManual bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Manual mode: parse and validate the CSV BEFORE claiming the global sync
		// manager, so a bad upload cannot leave a half-configured run behind.
		isManual := forceManual || isManualRequested(c)
		var upload *ManualASINUpload
		if isManual {
			log.Printf("[HOURLY_SYNC][MANUAL] ===== MANUAL HOURLY SYNC REQUEST RECEIVED =====")
			log.Printf("[HOURLY_SYNC][MANUAL] %s %s from %s", c.Request.Method, c.Request.URL.Path, c.ClientIP())

			parsed, err := readManualASINUpload(c)
			if err != nil {
				log.Printf("[HOURLY_SYNC][MANUAL] REJECTED: %v", err)
				c.JSON(400, gin.H{"error": err.Error()})
				return
			}
			upload = parsed

			log.Printf("[HOURLY_SYNC][MANUAL] CSV %q parsed: %d valid ASINs, %d duplicates dropped, %d invalid, %d data rows read",
				upload.Filename, upload.Valid, upload.Duplicates, upload.InvalidCount, upload.DataRows)
			if upload.InvalidCount > 0 {
				log.Printf("[HOURLY_SYNC][MANUAL] Rejected values (up to %d shown): %v", maxInvalidASINSamples, upload.Invalid)
			}
		}

		hourlySyncManagerMutex.Lock()
		if globalHourlySyncManager != nil &&
			globalHourlySyncManager.status != nil &&
			globalHourlySyncManager.status.IsRunning {
			hourlySyncManagerMutex.Unlock()
			if isManual {
				log.Printf("[HOURLY_SYNC][MANUAL] REJECTED: an hourly sync is already in progress")
			}
			c.JSON(400, gin.H{
				"error":  "Hourly sync is already in progress",
				"status": globalHourlySyncManager.GetStatus(),
			})
			return
		}

		// Parse parameters (query string, falling back to the multipart form so a
		// manual run can send everything in one body)
		marketplace := paramOrDefault(c, "marketplace", "us")
		debugMode := paramOrDefault(c, "debug", "false") == "true"

		globalHourlySyncManager = NewHourlySyncManager(stagingClient, productionClient, debugMode, recorder)
		if isManual {
			globalHourlySyncManager.isManual = true
			globalHourlySyncManager.manualASINs = upload.ASINs
			globalHourlySyncManager.manualSource = upload.Filename
		}
		hourlySyncManagerMutex.Unlock()

		if debugMode {
			log.Println("[HOURLY_SYNC] ========== DEBUG MODE ENABLED ==========")
			log.Printf("[HOURLY_SYNC] Starting hourly sync with marketplace=%s, manual=%v", marketplace, isManual)
		}

		// Run sync synchronously (cloud jobs expect completion)
		startedAt := time.Now()
		globalHourlySyncManager.RunHourlySync(marketplace)

		if isManual {
			status := globalHourlySyncManager.GetStatus()
			log.Printf("[HOURLY_SYNC][MANUAL] ===== MANUAL HOURLY SYNC FINISHED in %s =====", time.Since(startedAt).Round(time.Second))
			log.Printf("[HOURLY_SYNC][MANUAL] CSV=%q uploaded=%d processed=%d product_ok=%d sales_ok=%d failed=%d api_calls=%d stopped_early=%v",
				upload.Filename, len(upload.ASINs), status.TotalASINsProcessed, status.SuccessfulProductSync,
				status.SuccessfulSalesSync, status.FailedASINs, status.TotalAPICalls, status.StoppedEarly)
			if status.StoppedEarly {
				log.Printf("[HOURLY_SYNC][MANUAL] STOP REASON: %s", status.StopReason)
			}
			if status.ErrorSummary != nil {
				log.Printf("[HOURLY_SYNC][MANUAL] Errors - db=%d api=%d parse=%d other=%d samples=%v",
					status.ErrorSummary.DBErrors, status.ErrorSummary.APIErrors,
					status.ErrorSummary.ParseErrors, status.ErrorSummary.OtherErrors,
					status.ErrorSummary.SampleErrors)
			}
		}

		response := gin.H{
			"message":    "Hourly sync completed",
			"status":     globalHourlySyncManager.GetStatus(),
			"debug_mode": debugMode,
			"is_manual":  isManual,
		}
		if isManual {
			response["message"] = fmt.Sprintf("Manual hourly sync completed for %d ASINs from the uploaded CSV", len(upload.ASINs))
			response["upload"] = upload
		}

		c.JSON(200, response)
	}
}

// GetJSHourlySyncStatus returns the current hourly sync status
func GetJSHourlySyncStatus(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		hourlySyncManagerMutex.Lock()
		defer hourlySyncManagerMutex.Unlock()

		if globalHourlySyncManager == nil {
			c.JSON(200, gin.H{
				"message": "No hourly sync has been initiated yet",
			})
			return
		}

		c.JSON(200, gin.H{
			"status": globalHourlySyncManager.GetStatus(),
		})
	}
}

// RunHourlySync executes the hourly sync process
func (m *HourlySyncManager) RunHourlySync(marketplace string) {
	m.monitorLog("========== HOURLY SYNC STARTED ==========")
	m.monitorLog("Marketplace: %s | manual: %v | source: %s", marketplace, m.isManual, m.asinSourceLabel())
	m.debugLog("Debug mode: %v", m.debugMode)

	m.statusMutex.Lock()
	m.status = &HourlySyncStatus{
		StartedAt:    time.Now(),
		IsRunning:    true,
		IsManual:     m.isManual,
		ManualSource: m.manualSource,
		ErrorSummary: &ErrorSummary{SampleErrors: []string{}},
	}
	m.statusMutex.Unlock()

	defer func() {
		m.statusMutex.Lock()
		now := time.Now()
		m.status.CompletedAt = &now
		m.status.IsRunning = false
		if m.stopRequested {
			m.status.StoppedEarly = true
		}
		duration := now.Sub(m.status.StartedAt)
		m.statusMutex.Unlock()

		m.monitorLog("========== HOURLY SYNC COMPLETED ==========")
		m.monitorLog("Duration: %v", duration)
		m.monitorLog("Total ASINs processed: %d", m.status.TotalASINsProcessed)
		m.monitorLog("Successful product syncs: %d", m.status.SuccessfulProductSync)
		m.monitorLog("Successful sales syncs: %d", m.status.SuccessfulSalesSync)
		m.monitorLog("Failed ASINs: %d", m.status.FailedASINs)
		m.monitorLog("Total API calls: %d", m.apiCallCount)
		m.debugLog("Sending Discord notification...")

		m.sendHourlySyncDiscordNotification()
		m.debugLog("Discord notification sent")
	}()

	// ==================== STEP 0: RESOLVE TABLES ====================
	m.monitorLog("---------- STEP 0: RESOLVE TABLES ----------")
	m.logTargetDatabases()
	if err := m.resolveFanOutTables(); err != nil {
		m.addHourlyError("db", fmt.Sprintf("Failed to resolve fan-out tables: %v", err))
		log.Printf("%s CRITICAL: Failed to resolve fan-out tables: %v", m.logPrefix(), err)
		m.statusMutex.Lock()
		m.status.StoppedEarly = true
		m.status.StopReason = "Failed to resolve fan-out tables"
		m.statusMutex.Unlock()
		return
	}
	m.monitorLog("Staging tables    : asin=%s mapping=%s product=%s sales=%s sync_status=%s",
		m.stagingTables.asin, m.stagingTables.mapping, m.stagingTables.product, m.stagingTables.sales,
		m.stagingClient.TableName("jungle_scout_sync_status"))
	if m.productionClient != nil {
		m.monitorLog("Production tables : asin=%s mapping=%s product=%s sales=%s",
			m.prodTables.asin, m.prodTables.mapping, m.prodTables.product, m.prodTables.sales)
	}

	// ==================== STEPS 1-3: DECIDE WHAT TO SYNC ====================
	// Manual mode replaces the whole cleanup/add-new/select pipeline: the CSV is
	// the ASIN list. Cleanup in particular MUST be skipped — it deletes every
	// sync_status row outside the database-derived parent set, which has nothing
	// to do with what the caller uploaded.
	var asinsToSync []ASINSyncInfo
	if m.isManual {
		m.monitorLog("---------- STEPS 1-3: MANUAL ASIN LIST ----------")
		m.monitorLog("MANUAL MODE: %d ASINs from uploaded CSV %q", len(m.manualASINs), m.manualSource)
		m.monitorLog("SKIPPED step 1 cleanup      : no sync_status rows are deleted")
		m.monitorLog("SKIPPED step 2 add-new      : no ASINs are pulled from %s", m.stagingTables.asin)
		m.monitorLog("SKIPPED step 3 selection    : the %d CSV ASINs are the entire workload (no %d-ASIN cap, no new/stale query)",
			len(m.manualASINs), HourlySyncASINLimit)
		m.logASINList("CSV ASINs to sync", m.manualASINs)

		queued, err := m.ensureManualSyncStatusRows()
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Failed to queue manual ASINs: %v", err))
			log.Printf("%s CRITICAL: Failed to queue manual ASINs: %v", m.logPrefix(), err)
			m.stopRequested = true
			m.statusMutex.Lock()
			m.status.StopReason = "Failed to queue manual ASINs"
			m.statusMutex.Unlock()
			return
		}
		m.statusMutex.Lock()
		m.status.NewASINsAdded = queued
		m.statusMutex.Unlock()
		m.monitorLog("Queued in %s: %d new rows inserted, %d already present",
			m.stagingClient.TableName("jungle_scout_sync_status"), queued, len(m.manualASINs)-queued)

		asinsToSync, err = m.manualASINSyncInfo()
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Failed to load manual ASIN state: %v", err))
			log.Printf("%s CRITICAL: Failed to load manual ASIN state: %v", m.logPrefix(), err)
			m.stopRequested = true
			m.statusMutex.Lock()
			m.status.StopReason = "Failed to load manual ASIN state"
			m.statusMutex.Unlock()
			return
		}
	} else {
		// ==================== STEP 1: CLEANUP ====================
		m.debugLog("---------- STEP 1: CLEANUP ----------")
		m.debugLog("Removing ASINs that are no longer parents-to-sync from sync_status...")
		cleanedUp, err := m.cleanupSyncStatus()
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Cleanup failed: %v", err))
			log.Printf("[HOURLY_SYNC] CRITICAL: Cleanup failed: %v", err)
		}
		m.statusMutex.Lock()
		m.status.CleanedUpASINs = cleanedUp
		m.statusMutex.Unlock()
		m.debugLog("Cleanup complete: %d ASINs removed", cleanedUp)

		// ==================== STEP 2: ADD NEW ASINs ====================
		m.debugLog("---------- STEP 2: ADD NEW ASINs ----------")
		m.debugLog("Adding new visible ASINs to sync_status...")
		newASINs, err := m.addNewASINsToSyncStatus()
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Failed to add new ASINs: %v", err))
			log.Printf("[HOURLY_SYNC] CRITICAL: Failed to add new ASINs: %v", err)
		}
		m.statusMutex.Lock()
		m.status.NewASINsAdded = newASINs
		m.statusMutex.Unlock()
		m.debugLog("New ASINs added to sync_status: %d", newASINs)

		// ==================== STEP 3: SELECT ASINs ====================
		m.debugLog("---------- STEP 3: SELECT ASINs ----------")
		m.debugLog("Selecting up to %d ASINs (priority: new first, then stale >%d days)...",
			HourlySyncASINLimit, StaleDataThresholdDays)
		asinsToSync, err = m.selectASINsToSync()
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Failed to select ASINs: %v", err))
			log.Printf("[HOURLY_SYNC] CRITICAL: Failed to select ASINs: %v", err)
			m.stopRequested = true
			m.statusMutex.Lock()
			m.status.StopReason = "Failed to select ASINs"
			m.statusMutex.Unlock()
			return
		}
	}

	if len(asinsToSync) == 0 {
		m.debugLog("No ASINs to sync (all data is fresh)")
		m.debugLog("========== HOURLY SYNC SKIPPED (NO WORK) ==========")
		return
	}

	// Count new vs stale vs sales-gap
	newCount, staleCount, gapCount, deferredCount := 0, 0, 0, 0
	var newASINsList, staleASINsList []string
	for _, info := range asinsToSync {
		if info.DeferSales {
			deferredCount++
		}
		switch {
		case info.IsNew:
			newCount++
			newASINsList = append(newASINsList, info.ASIN)
		case info.SalesGapOnly:
			gapCount++
		default:
			staleCount++
			staleASINsList = append(staleASINsList, info.ASIN)
		}
	}
	m.statusMutex.Lock()
	m.status.NewASINsSynced = newCount
	m.status.StaleASINsSynced = staleCount
	m.status.SalesGapASINs = gapCount
	m.statusMutex.Unlock()

	m.monitorLog("To sync: %d ASINs total (%d never synced -> 1 year of sales, %d stale -> incremental sales, %d sales-gap re-drive)",
		len(asinsToSync), newCount, staleCount, gapCount)
	if deferredCount > 0 {
		m.monitorLog("Of those, %d full-year backfills are DEFERRED to a later run — their product data is still written", deferredCount)
	}
	if len(newASINsList) > 0 {
		m.debugLog("  - New ASIN list: %v", newASINsList)
	}
	if len(staleASINsList) > 0 {
		m.debugLog("  - Stale ASIN list: %v", staleASINsList)
	}

	// ==================== STEP 4 & 5: SYNC DATA ====================
	m.monitorLog("---------- STEP 4 & 5: SYNC PRODUCT & SALES DATA ----------")

	// The product API takes ProductBatchSize ASINs per call. An automatic run is
	// capped at HourlySyncASINLimit and so is always a single batch; a manual CSV
	// can be any size, so walk it in batches. syncSelectedASINs accumulates into
	// the status counters, which makes repeated calls safe.
	totalBatches := (len(asinsToSync) + ProductBatchSize - 1) / ProductBatchSize
	m.monitorLog("Workload: %d ASINs in %d batch(es) of up to %d", len(asinsToSync), totalBatches, ProductBatchSize)

	// Heartbeat: a full-set run is quiet for a long time, and a slow run has to be
	// distinguishable from a hung one in the logs.
	runStartedAt := time.Now()
	processed := 0

	for i := 0; i < len(asinsToSync); i += ProductBatchSize {
		batchNo := (i / ProductBatchSize) + 1
		if m.stopRequested {
			m.monitorLog("STOPPING before batch %d/%d due to critical errors - %d ASINs left unprocessed",
				batchNo, totalBatches, len(asinsToSync)-i)
			break
		}

		end := i + ProductBatchSize
		if end > len(asinsToSync) {
			end = len(asinsToSync)
		}
		m.monitorLog("===== BATCH %d/%d (%d ASINs) =====", batchNo, totalBatches, end-i)
		m.syncSelectedASINs(asinsToSync[i:end], marketplace)

		processed = end
		if batchNo%progressEveryBatches == 0 || end == len(asinsToSync) {
			m.logProgress(processed, len(asinsToSync), runStartedAt)
		}
	}

	// ==================== STEP 6: RETRY PASS ====================
	// ASINs that failed on something plausibly temporary get another attempt before
	// the run gives up on them. Without this, a rate-limit burst or a brief network
	// blip strands those ASINs until the next scheduled run — which on a ten-day
	// cycle is ten days of missing data rather than the hour it used to be.
	m.runRetryPasses(marketplace)
}

// progressEveryBatches controls how often the heartbeat line is written during the
// main loop. At 100 ASINs per batch this is roughly every 2,000 ASINs.
const progressEveryBatches = 20

// logProgress writes a heartbeat with throughput and a projected finish time.
func (m *HourlySyncManager) logProgress(done, total int, startedAt time.Time) {
	elapsed := time.Since(startedAt)
	if done <= 0 || total <= 0 {
		return
	}

	rate := float64(done) / elapsed.Seconds()
	remaining := "unknown"
	if rate > 0 {
		remaining = time.Duration(float64(total-done) / rate * float64(time.Second)).Round(time.Minute).String()
	}

	m.monitorLog("[Progress] %d/%d ASINs (%.1f%%) in %s — %.1f ASIN/s, ~%s remaining, %d API calls so far",
		done, total, float64(done)/float64(total)*100, elapsed.Round(time.Second), rate, remaining, m.apiCallCount)
}

// runRetryPasses re-attempts ASINs whose sales fetch failed on a transient error.
// Each pass rebuilds the batch from the queue, so an ASIN that fails again simply
// falls out: queueForRetry ignores calls made while inRetryPass is set.
//
// Anything still failing after the passes is recorded as a genuine failure in
// sync_status, which is what makes it visible to the next run's sales-gap tier.
func (m *HourlySyncManager) runRetryPasses(marketplace string) {
	if SalesRetryPasses <= 0 {
		return
	}

	for pass := 1; pass <= SalesRetryPasses; pass++ {
		if m.stopRequested {
			m.monitorLog("[Retry] Skipping pass %d: run already abandoned", pass)
			return
		}

		queued := m.takeRetryQueue()
		if len(queued) == 0 {
			if pass == 1 {
				m.monitorLog("---------- STEP 6: RETRY PASS ----------")
				m.monitorLog("[Retry] Nothing queued — no transient failures this run")
			}
			return
		}

		m.monitorLog("---------- STEP 6: RETRY PASS %d/%d ----------", pass, SalesRetryPasses)
		m.monitorLog("[Retry] Re-attempting %d ASINs that failed on transient errors", len(queued))

		m.statusMutex.Lock()
		m.status.RetriedASINs += len(queued)
		before := m.status.SuccessfulSalesSync
		m.statusMutex.Unlock()

		// Suppress re-queueing so a persistently failing ASIN cannot loop, and so
		// this pass ends with a settled outcome for every ASIN in it.
		m.retryMutex.Lock()
		m.inRetryPass = true
		m.retryMutex.Unlock()

		for i := 0; i < len(queued); i += ProductBatchSize {
			if m.stopRequested {
				m.monitorLog("[Retry] Stopping mid-pass: run abandoned")
				break
			}
			end := i + ProductBatchSize
			if end > len(queued) {
				end = len(queued)
			}
			m.syncSelectedASINs(queued[i:end], marketplace)
		}

		m.retryMutex.Lock()
		m.inRetryPass = false
		m.retryMutex.Unlock()

		m.statusMutex.Lock()
		recovered := m.status.SuccessfulSalesSync - before
		m.status.RecoveredASINs += recovered
		m.statusMutex.Unlock()

		m.monitorLog("[Retry] Pass %d recovered %d of %d ASINs", pass, recovered, len(queued))
	}
}

// ensureManualSyncStatusRows queues the uploaded ASINs in sync_status without
// touching rows that already exist. A CSV may name ASINs that were never part of
// the database-derived parent set, and the product/sales writes update
// sync_status by ASIN — without a row those updates would silently no-op.
// NOTE: sync_status is staging-only, production writes are skipped.
func (m *HourlySyncManager) ensureManualSyncStatusRows() (int, error) {
	stagingSyncTable := m.stagingClient.TableName("jungle_scout_sync_status")

	insertQuery := fmt.Sprintf(`
		INSERT INTO %s (asin, has_product_data, has_sales_data, updated_at)
		SELECT u.asin, false, false, CURRENT_TIMESTAMP
		FROM unnest($1::text[]) AS u(asin)
		ON CONFLICT (asin) DO NOTHING
	`, stagingSyncTable)

	result, err := m.stagingClient.DB.Exec(insertQuery, pq.Array(m.manualASINs))
	if err != nil {
		return 0, fmt.Errorf("staging insert failed: %w", err)
	}

	rowsInserted, _ := result.RowsAffected()
	return int(rowsInserted), nil
}

// manualASINSyncInfo turns the uploaded ASIN list into the sync list, annotated
// with what sync_status already knows about each ASIN so the sales fetch still
// picks the right date range (1 year for never-synced ASINs, incremental from
// the last sync otherwise). CSV order is preserved.
func (m *HourlySyncManager) manualASINSyncInfo() ([]ASINSyncInfo, error) {
	syncTable := m.stagingClient.TableName("jungle_scout_sync_status")

	query := fmt.Sprintf(`
		SELECT asin, has_product_data, sales_estimate_data_synced_at
		FROM %s
		WHERE asin = ANY($1::text[])
	`, syncTable)

	rows, err := m.stagingClient.DB.Query(query, pq.Array(m.manualASINs))
	if err != nil {
		return nil, fmt.Errorf("failed to query manual ASIN state: %w", err)
	}
	defer rows.Close()

	known := make(map[string]ASINSyncInfo, len(m.manualASINs))
	for rows.Next() {
		var asin string
		var hasProductData sql.NullBool
		var salesSyncedAt sql.NullTime
		if err := rows.Scan(&asin, &hasProductData, &salesSyncedAt); err != nil {
			return nil, fmt.Errorf("failed to scan manual ASIN state: %w", err)
		}
		info := ASINSyncInfo{ASIN: asin, IsNew: !hasProductData.Valid || !hasProductData.Bool}
		if salesSyncedAt.Valid {
			info.SalesEstimateSyncedAt = &salesSyncedAt.Time
		}
		known[asin] = info
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating manual ASIN state: %w", err)
	}

	results := make([]ASINSyncInfo, 0, len(m.manualASINs))
	for _, asin := range m.manualASINs {
		if info, ok := known[asin]; ok {
			results = append(results, info)
			continue
		}
		// No sync_status row (the insert above should have created one, so this
		// only happens on a race): treat it as brand new.
		results = append(results, ASINSyncInfo{ASIN: asin, IsNew: true})
	}
	return results, nil
}

// cleanupSyncStatus removes sync_status records for ASINs with asin_visibility=false
// NOTE: sync_status is staging-only, production writes are skipped
func (m *HourlySyncManager) cleanupSyncStatus() (int, error) {
	stagingSyncTable := m.stagingClient.TableName("jungle_scout_sync_status")
	m.debugLog("[Cleanup] Staging sync table: %s", stagingSyncTable)
	m.debugLog("[Cleanup] ASIN table: %s, mapping table: %s", m.stagingTables.asin, m.stagingTables.mapping)

	deleteQuery := syncStatusCleanupQuery(stagingSyncTable, m.stagingTables)

	m.debugLog("[Cleanup] Executing DELETE on staging...")
	result, err := m.stagingClient.DB.Exec(deleteQuery)
	if err != nil {
		m.debugLog("[Cleanup] ERROR: Staging cleanup failed: %v", err)
		return 0, fmt.Errorf("staging cleanup failed: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()
	m.debugLog("[Cleanup] Staging: %d rows deleted", rowsDeleted)
	m.debugLog("[Cleanup] Skipping production (sync_status is staging-only)")

	return int(rowsDeleted), nil
}

// addNewASINsToSyncStatus inserts ASINs that exist in product table but not in sync_status
// NOTE: sync_status is staging-only, production writes are skipped
func (m *HourlySyncManager) addNewASINsToSyncStatus() (int, error) {
	stagingSyncTable := m.stagingClient.TableName("jungle_scout_sync_status")

	insertQuery := syncStatusAddNewQuery(stagingSyncTable, m.stagingTables)

	m.debugLog("[AddNew] Inserting new parent ASINs into staging sync_status...")
	result, err := m.stagingClient.DB.Exec(insertQuery)
	if err != nil {
		m.debugLog("[AddNew] ERROR: Staging insert failed: %v", err)
		return 0, fmt.Errorf("staging insert failed: %w", err)
	}

	rowsInserted, _ := result.RowsAffected()
	m.debugLog("[AddNew] Staging: %d new ASINs inserted", rowsInserted)
	m.debugLog("[AddNew] Skipping production (sync_status is staging-only)")

	return int(rowsInserted), nil
}

// scanASINSyncInfo drains a (asin, sales_estimate_data_synced_at) result set into
// ASINSyncInfo values. Shared by the three selection tiers below.
func scanASINSyncInfo(rows *sql.Rows) ([]ASINSyncInfo, error) {
	defer rows.Close()

	var out []ASINSyncInfo
	for rows.Next() {
		var info ASINSyncInfo
		var salesSyncedAt sql.NullTime
		if err := rows.Scan(&info.ASIN, &salesSyncedAt); err != nil {
			return nil, fmt.Errorf("failed to scan ASIN row: %w", err)
		}
		if salesSyncedAt.Valid {
			info.SalesEstimateSyncedAt = &salesSyncedAt.Time
		}
		out = append(out, info)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating ASIN rows: %w", err)
	}
	return out, nil
}

// selectASINsToSync picks the ASINs one run will process, in three tiers:
//
//  1. NEW — never fetched, or previously not found in JungleScout and past the
//     retry window.
//  2. STALE — product data older than StaleDataThresholdDays.
//  3. SALES GAP — product data still current, but the sales fetch previously
//     failed or never completed.
//
// Tier 3 exists because selection used to key off product_data_synced_at alone.
// An ASIN whose product write succeeded and whose sales write failed looked
// perfectly fresh, so nothing re-drove it until its product data went stale —
// one full cycle later. It also catches rows with has_product_data=true but a
// NULL product_data_synced_at, which tier 2 cannot see at all.
//
// The three tiers are disjoint by construction (tier 1 requires
// has_product_data=false; tier 2 requires a non-NULL timestamp below the
// threshold; tier 3 requires the timestamp to be NULL or at/above it), but the
// results are still de-duplicated by ASIN so a future edit to any predicate
// cannot produce the same ASIN twice in one batch.
func (m *HourlySyncManager) selectASINsToSync() ([]ASINSyncInfo, error) {
	syncTable := m.stagingClient.TableName("jungle_scout_sync_status")
	retryThreshold := time.Now().AddDate(0, 0, -ProductNotFoundRetryDays)
	staleThreshold := time.Now().AddDate(0, 0, -StaleDataThresholdDays)

	m.debugLog("[Select] Sync table: %s", syncTable)
	m.debugLog("[Select] Not-found retry threshold (%d days): %s",
		ProductNotFoundRetryDays, retryThreshold.Format("2006-01-02 15:04:05"))
	m.debugLog("[Select] Stale threshold (%d days): %s",
		StaleDataThresholdDays, staleThreshold.Format("2006-01-02 15:04:05"))

	var results []ASINSyncInfo
	seen := make(map[string]bool)

	// add appends rows that have not already been selected by an earlier tier.
	add := func(batch []ASINSyncInfo, isNew, salesGapOnly bool) int {
		added := 0
		for _, info := range batch {
			if seen[info.ASIN] {
				continue
			}
			seen[info.ASIN] = true
			info.IsNew = isNew
			info.SalesGapOnly = salesGapOnly
			results = append(results, info)
			added++
		}
		return added
	}

	// ---- Tier 1: never fetched (or past the not-found retry window) ----
	m.debugLog("[Select] Querying NEW ASINs (has_product_data=false, retry window passed)...")
	newRows, err := m.stagingClient.DB.Query(fmt.Sprintf(`
		SELECT asin, sales_estimate_data_synced_at
		FROM %s
		WHERE has_product_data = false
		AND (product_fetch_attempted_at IS NULL OR product_fetch_attempted_at < $1)
		ORDER BY created_at ASC, asin ASC
		LIMIT %d
	`, syncTable, HourlySyncASINLimit), retryThreshold)
	if err != nil {
		m.debugLog("[Select] ERROR: Failed to query new ASINs: %v", err)
		return nil, fmt.Errorf("failed to query new ASINs: %w", err)
	}
	newBatch, err := scanASINSyncInfo(newRows)
	if err != nil {
		m.debugLog("[Select] ERROR: %v", err)
		return nil, fmt.Errorf("new ASINs: %w", err)
	}
	newCount := add(newBatch, true, false)
	m.debugLog("[Select] Found %d NEW ASINs", newCount)

	// ---- Tier 2: product data older than the staleness threshold ----
	if remaining := HourlySyncASINLimit - len(results); remaining > 0 {
		m.debugLog("[Select] Querying STALE ASINs (%d slots remaining)...", remaining)
		staleRows, err := m.stagingClient.DB.Query(fmt.Sprintf(`
			SELECT asin, sales_estimate_data_synced_at
			FROM %s
			WHERE has_product_data = true
			AND product_data_synced_at IS NOT NULL
			AND product_data_synced_at < $1
			ORDER BY product_data_synced_at ASC
			LIMIT %d
		`, syncTable, remaining), staleThreshold)
		if err != nil {
			m.debugLog("[Select] ERROR: Failed to query stale ASINs: %v", err)
			return nil, fmt.Errorf("failed to query stale ASINs: %w", err)
		}
		staleBatch, err := scanASINSyncInfo(staleRows)
		if err != nil {
			m.debugLog("[Select] ERROR: %v", err)
			return nil, fmt.Errorf("stale ASINs: %w", err)
		}
		m.debugLog("[Select] Found %d STALE ASINs", add(staleBatch, false, false))
	}

	// ---- Tier 3: product fresh, sales missing ----
	if remaining := HourlySyncASINLimit - len(results); remaining > 0 {
		m.debugLog("[Select] Querying SALES GAP ASINs (%d slots remaining)...", remaining)
		gapRows, err := m.stagingClient.DB.Query(fmt.Sprintf(`
			SELECT asin, sales_estimate_data_synced_at
			FROM %s
			WHERE has_product_data = true
			AND has_sales_data = false
			AND (product_data_synced_at IS NULL OR product_data_synced_at >= $1)
			ORDER BY updated_at ASC, asin ASC
			LIMIT %d
		`, syncTable, remaining), staleThreshold)
		if err != nil {
			m.debugLog("[Select] ERROR: Failed to query sales-gap ASINs: %v", err)
			return nil, fmt.Errorf("failed to query sales-gap ASINs: %w", err)
		}
		gapBatch, err := scanASINSyncInfo(gapRows)
		if err != nil {
			m.debugLog("[Select] ERROR: %v", err)
			return nil, fmt.Errorf("sales-gap ASINs: %w", err)
		}
		gapCount := add(gapBatch, false, true)
		m.debugLog("[Select] Found %d SALES GAP ASINs", gapCount)
		if gapCount > 0 {
			m.monitorLog("[Select] %d ASINs re-driven for a missing sales fetch (product data was already current)", gapCount)
		}
	}

	// A run that reaches the cap has been truncated. The cap sits far above the
	// parent set, so this is a signal that something upstream is wrong — an
	// unfiltered mapping table, a visibility flag applied to the wrong scope —
	// not a normal full sync. Say so loudly: a truncated run otherwise looks
	// exactly like a clean one in the logs and in the Discord summary.
	if len(results) >= HourlySyncASINLimit {
		msg := fmt.Sprintf("selection reached SYNC_ASIN_LIMIT (%d) — the run is TRUNCATED and some ASINs were not queued; treat this as an incident, not a full sync", HourlySyncASINLimit)
		log.Printf("%s CRITICAL: %s", m.logPrefix(), msg)
		m.addHourlyError("other", msg)
		m.statusMutex.Lock()
		m.status.LimitReached = true
		m.statusMutex.Unlock()
	}

	// Cap the number of never-synced ASINs that will pull a full year of sales in
	// this run. Deferred ASINs still get their product data; only the year-long
	// sales fetch waits. See ASINSyncInfo.DeferSales for why this defers rather
	// than shortens the window.
	fullBackfills, deferred := 0, 0
	for i := range results {
		if results[i].SalesEstimateSyncedAt != nil {
			continue // incremental fetch, cheap
		}
		fullBackfills++
		if MaxFullBackfills > 0 && fullBackfills > MaxFullBackfills {
			results[i].DeferSales = true
			deferred++
		}
	}
	if deferred > 0 {
		m.monitorLog("[Select] %d full-year sales backfills deferred to a later run (MAX_FULL_BACKFILLS=%d, %d needed)",
			deferred, MaxFullBackfills, fullBackfills)
		m.statusMutex.Lock()
		m.status.DeferredBackfills = deferred
		m.statusMutex.Unlock()
	}

	m.monitorLog("[Select] Total ASINs selected: %d (cap %d)", len(results), HourlySyncASINLimit)
	return results, nil
}

// syncSelectedASINs syncs product and sales data for one batch of at most
// ProductBatchSize ASINs. Status counters are accumulated, not assigned, so a
// manual run can call this once per batch.
func (m *HourlySyncManager) syncSelectedASINs(asins []ASINSyncInfo, marketplace string) {
	m.debugLog("[SyncASINs] Starting sync for %d ASINs", len(asins))

	asinStrings := make([]string, len(asins))
	requestedASINs := make(map[string]bool)
	for i, info := range asins {
		asinStrings[i] = info.ASIN
		requestedASINs[info.ASIN] = true
	}

	// Step 4: Fetch and store product data.
	// The list logged here is EXACTLY the payload sent to JungleScout — nothing
	// else is fetched for this batch.
	m.monitorLog("[SyncASINs] ===== STEP 4: PRODUCT DATA FETCH =====")
	m.logASINList("[SyncASINs] Sending to JungleScout Product API", asinStrings)
	fetchStartedAt := time.Now()
	apiResponse, err := m.jsClient.FetchProductData(asinStrings, marketplace)
	m.apiCallCount++
	m.statusMutex.Lock()
	m.status.TotalAPICalls = m.apiCallCount
	m.statusMutex.Unlock()
	m.monitorLog("[SyncASINs] Product API call #%d finished in %s", m.apiCallCount, time.Since(fetchStartedAt).Round(time.Millisecond))

	if err != nil {
		transient := isTransientAPIError(err)
		m.addHourlyError("api", fmt.Sprintf("Product fetch failed: %v", err))
		log.Printf("%s Product fetch failed for %d ASINs (transient=%v): %v",
			m.logPrefix(), len(asinStrings), transient, err)
		m.debugLog("[SyncASINs] ERROR: Product API call failed: %v", err)

		m.markASINAttempted(asinStrings...)
		m.markASINFailed(asinStrings...)

		// A single rate-limited product call takes out a whole batch of 100, which
		// makes it the most expensive failure in the run. Queue the batch so the
		// retry pass gets another attempt rather than losing all 100 for a cycle.
		if transient {
			for _, info := range asins {
				m.queueForRetry(info)
			}
			m.monitorLog("[SyncASINs] %d ASINs queued for retry after a transient product-fetch failure", len(asins))
		}
		return
	}

	returnedCount := 0
	if apiResponse != nil {
		returnedCount = len(apiResponse.Data)
	}
	m.monitorLog("[SyncASINs] Product API returned %d products (requested %d)", returnedCount, len(asinStrings))

	// Store product data and get successful ASINs
	m.monitorLog("[SyncASINs] Writing product rows to the database...")
	successfulASINs, returnedASINs := m.storeHourlyProductData(apiResponse, marketplace, requestedASINs)
	m.logASINList("[SyncASINs] Stored successfully", successfulASINs)

	// Mark ASINs not returned by API as "product not found"
	// (retried after ProductNotFoundRetryDays)
	notFoundCount := 0
	var notFoundASINs []string
	for asin := range requestedASINs {
		if !returnedASINs[asin] {
			m.markProductNotFound(asin)
			notFoundASINs = append(notFoundASINs, asin)
			notFoundCount++
		}
	}

	if notFoundCount > 0 {
		m.logASINList(fmt.Sprintf("[SyncASINs] NOT FOUND in JungleScout (retry in %d days)", ProductNotFoundRetryDays), notFoundASINs)
		m.addHourlyError("api", fmt.Sprintf("%d ASINs not found in JungleScout API (will retry in %d days)", notFoundCount, ProductNotFoundRetryDays))
	}

	// Product-level accounting. An ASIN whose product row was written is not
	// resolved yet — its sales fetch still has to succeed — so it is only cleared
	// from the failed set once sales completes. What is settled here is the
	// opposite: ASINs the API did not return are failures for this run.
	m.markASINAttempted(asinStrings...)
	if notFoundCount > 0 {
		m.markASINFailed(notFoundASINs...)
	}
	storedOK := make(map[string]bool, len(successfulASINs))
	for _, asin := range successfulASINs {
		storedOK[asin] = true
	}
	for asin := range requestedASINs {
		// Returned by the API but not stored: a write problem, not a not-found.
		if returnedASINs[asin] && !storedOK[asin] {
			m.markASINFailed(asin)
		}
	}

	m.markProductStored(successfulASINs...)

	m.monitorLog("[SyncASINs] Product sync summary: %d stored, %d failed, %d not found (of %d requested)",
		len(successfulASINs), len(asins)-len(successfulASINs)-notFoundCount, notFoundCount, len(asins))

	// Step 5: Sync sales data for successful ASINs only
	m.monitorLog("[SyncASINs] ===== STEP 5: SALES DATA FETCH =====")
	// Build map for quick lookup of ASIN info
	asinInfoMap := make(map[string]ASINSyncInfo)
	for _, info := range asins {
		asinInfoMap[info.ASIN] = info
	}

	// Drop ASINs whose full-year backfill was deferred by MAX_FULL_BACKFILLS. Their
	// product data is already written; skipping the sales call leaves
	// sales_estimate_data_synced_at NULL so the next run still sees the full year
	// as outstanding.
	salesTargets := make([]string, 0, len(successfulASINs))
	deferredHere := 0
	for _, asin := range successfulASINs {
		if info, ok := asinInfoMap[asin]; ok && info.DeferSales {
			deferredHere++
			continue
		}
		salesTargets = append(salesTargets, asin)
	}
	if deferredHere > 0 {
		m.monitorLog("[SyncASINs] %d ASINs skipped for sales this run (full-year backfill deferred)", deferredHere)
	}

	m.monitorLog("[SyncASINs] Fetching sales data for %d ASINs (1 API call each, %d workers)...",
		len(salesTargets), SalesWorkerCount)

	// Worker pool for concurrent sales fetching. All workers share the client's
	// single 14 req/s rate limiter, so the pool size controls how much of that
	// budget is used, not how fast requests leave — raising it cannot cause 429s.
	var salesSuccessCount int32
	var salesFailCount int32
	var salesRetryQueued int32
	asinChan := make(chan string, len(salesTargets))
	var wg sync.WaitGroup

	// Start workers
	for i := 0; i < SalesWorkerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for asin := range asinChan {
				// Only a genuinely unrecoverable condition stops the run now. A single
				// failed row no longer sets stopRequested (see storeHourlySalesData),
				// so reaching this means the database is effectively gone.
				if m.stopRequested {
					m.debugLog("[Worker %d] Stopping: run marked unrecoverable", workerID)
					return
				}

				info, exists := asinInfoMap[asin]
				if !exists {
					continue
				}

				endDate := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
				var startDate string
				var dateRangeDesc string

				if info.SalesEstimateSyncedAt == nil {
					// New ASIN: fetch 1 year of data
					startDate = time.Now().AddDate(-1, 0, -1).Format("2006-01-02")
					dateRangeDesc = "1 year (new ASIN)"
				} else {
					// Existing ASIN: fetch from last sync date
					startDate = info.SalesEstimateSyncedAt.Format("2006-01-02")
					dateRangeDesc = fmt.Sprintf("since last sync (%s)", startDate)
				}

				m.monitorLog("[Worker %d] Sales fetch %s: %s -> %s (%s)",
					workerID, asin, startDate, endDate, dateRangeDesc)

				salesResponse, err := m.jsClient.FetchSalesEstimateData(asin, marketplace, startDate, endDate)

				// Increment API call counter (thread-safe)
				m.statusMutex.Lock()
				m.apiCallCount++
				m.status.TotalAPICalls = m.apiCallCount
				m.statusMutex.Unlock()

				if err != nil {
					transient := isTransientAPIError(err)
					lastChance := m.isRetryPass() // no further attempt is coming
					log.Printf("%s [Worker %d] ERROR: Sales fetch for %s failed (transient=%v, final=%v): %v",
						m.logPrefix(), workerID, asin, transient, lastChance, err)
					m.addHourlyError("api", fmt.Sprintf("Sales fetch for %s failed: %v", asin, err))

					// Always count it against the run. markASINResolved clears this again
					// if a later pass succeeds, so the total stays exact either way.
					m.markASINFailed(asin)

					if transient && !lastChance {
						// Hold off on writing to sync_status: an error there reads as a
						// settled outcome, and the retry pass may still succeed.
						m.queueForRetry(info)
						atomic.AddInt32(&salesRetryQueued, 1)
					} else {
						m.updateHourlySyncStatus(asin, true, false, fmt.Sprintf("Sales fetch error: %v", err))
						atomic.AddInt32(&salesFailCount, 1)
					}
					continue
				}

				dataPoints := 0
				if salesResponse != nil && len(salesResponse.Data) > 0 {
					dataPoints = len(salesResponse.Data[0].Attributes.Data)
				}

				if m.storeHourlySalesData(salesResponse, marketplace) {
					if m.markSalesStored(asin) {
						atomic.AddInt32(&salesSuccessCount, 1)
					}
					// Fully done: product row written and sales stored. Clear any failure
					// recorded for this ASIN on an earlier attempt.
					m.markASINResolved(asin)
					m.monitorLog("[Worker %d] %s OK: %d sales data points stored", workerID, asin, dataPoints)
				} else {
					m.markASINFailed(asin)

					// A write failure is retryable too: the upserts are idempotent, so a
					// second attempt is safe and usually clears a transient DB hiccup.
					if !m.isRetryPass() {
						m.queueForRetry(info)
						atomic.AddInt32(&salesRetryQueued, 1)
						log.Printf("%s [Worker %d] %s FAILED to store sales data (%d data points returned) - queued for retry",
							m.logPrefix(), workerID, asin, dataPoints)
					} else {
						atomic.AddInt32(&salesFailCount, 1)
						log.Printf("%s [Worker %d] %s FAILED to store sales data on the final attempt (%d data points returned)",
							m.logPrefix(), workerID, asin, dataPoints)
					}

					if m.stopRequested {
						m.debugLog("[Worker %d] Stopping: run marked unrecoverable", workerID)
						return
					}
				}
			}
		}(i)
	}

	// Send ASINs to workers
	for _, asin := range salesTargets {
		asinChan <- asin
	}
	close(asinChan)

	// Wait for all workers to complete
	wg.Wait()

	if queued := atomic.LoadInt32(&salesRetryQueued); queued > 0 {
		m.monitorLog("[SyncASINs] %d ASINs queued for the end-of-run retry pass", queued)
	}

	// SuccessfulSalesSync is maintained by markSalesStored, which is set-based so a
	// retry that re-stores already-counted data does not inflate it. Nothing to
	// accumulate here.
	m.monitorLog("[SyncASINs] Sales sync summary: %d newly stored, %d failed", salesSuccessCount, salesFailCount)
	m.monitorLog("[SyncASINs] Total API calls so far: %d", m.apiCallCount)
}

// markProductNotFound marks an ASIN as not found in JungleScout API. It will be
// retried after ProductNotFoundRetryDays.
// NOTE: sync_status is staging-only, production writes are skipped
func (m *HourlySyncManager) markProductNotFound(asin string) {
	m.debugLog("[NotFound] Marking ASIN %s as 'product not found' (will retry in %d days)", asin, ProductNotFoundRetryDays)

	stagingTable := m.stagingClient.TableName("jungle_scout_sync_status")

	updateQuery := fmt.Sprintf(`
		UPDATE %s
		SET product_fetch_attempted_at = CURRENT_TIMESTAMP,
		    error = 'Product not found in JungleScout API',
		    updated_at = CURRENT_TIMESTAMP
		WHERE asin = $1
	`, stagingTable)

	// Update staging only
	_, err := m.stagingClient.DB.Exec(updateQuery, asin)
	if err != nil {
		m.debugLog("[NotFound] ERROR: Failed to update staging for ASIN %s: %v", asin, err)
	} else {
		m.debugLog("[NotFound] Staging updated for ASIN %s", asin)
	}
}

// storeHourlyProductData stores product data and returns (successful ASINs, returned ASINs from API)
// NOTE: sync_status is staging-only, production only gets product_data writes
func (m *HourlySyncManager) storeHourlyProductData(apiResponse *junglescout.ProductAPIResponse, marketplace string, requestedASINs map[string]bool) ([]string, map[string]bool) {
	returnedASINs := make(map[string]bool)

	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return nil, returnedASINs
	}

	stagingProductTable := m.stagingClient.TableName("jungle_scout_product_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodProductTable string
	if m.productionClient != nil {
		prodProductTable = m.productionClient.TableName("jungle_scout_product_data")
	}
	reportDate := time.Now().Format("2006-01-02")

	buildProductQuery := func(tableName string) string {
		return fmt.Sprintf(`
			INSERT INTO %s (
				asin, report_date, id, title, price, reviews, category, rating,
				image_url, parent_asin, is_variant, seller_type, variants,
				breadcrumb_path, is_standalone, is_parent, is_available, brand,
				product_rank, weight_value, weight_unit, length_value, width_value,
				height_value, dimensions_unit, listing_quality_score,
				number_of_sellers, buy_box_owner, buy_box_owner_seller_id,
				date_first_available, date_first_available_is_estimated,
				approximate_30_day_revenue, approximate_30_day_units_sold,
				subcategory_ranks, fee_breakdown, ean_list, isbn_list, upc_list,
				gtin_list, variant_reviews, updated_at, created_at
			)
			VALUES (
				$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
				$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
				$29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42
			)
			ON CONFLICT (asin, report_date) DO UPDATE SET
				title = EXCLUDED.title, price = EXCLUDED.price, reviews = EXCLUDED.reviews,
				category = EXCLUDED.category, rating = EXCLUDED.rating, image_url = EXCLUDED.image_url,
				parent_asin = EXCLUDED.parent_asin, is_variant = EXCLUDED.is_variant,
				seller_type = EXCLUDED.seller_type, variants = EXCLUDED.variants,
				breadcrumb_path = EXCLUDED.breadcrumb_path, is_standalone = EXCLUDED.is_standalone,
				is_parent = EXCLUDED.is_parent, is_available = EXCLUDED.is_available,
				brand = EXCLUDED.brand, product_rank = EXCLUDED.product_rank,
				weight_value = EXCLUDED.weight_value, weight_unit = EXCLUDED.weight_unit,
				length_value = EXCLUDED.length_value, width_value = EXCLUDED.width_value,
				height_value = EXCLUDED.height_value, dimensions_unit = EXCLUDED.dimensions_unit,
				listing_quality_score = EXCLUDED.listing_quality_score,
				number_of_sellers = EXCLUDED.number_of_sellers, buy_box_owner = EXCLUDED.buy_box_owner,
				buy_box_owner_seller_id = EXCLUDED.buy_box_owner_seller_id,
				date_first_available = EXCLUDED.date_first_available,
				date_first_available_is_estimated = EXCLUDED.date_first_available_is_estimated,
				approximate_30_day_revenue = EXCLUDED.approximate_30_day_revenue,
				approximate_30_day_units_sold = EXCLUDED.approximate_30_day_units_sold,
				subcategory_ranks = EXCLUDED.subcategory_ranks, fee_breakdown = EXCLUDED.fee_breakdown,
				ean_list = EXCLUDED.ean_list, isbn_list = EXCLUDED.isbn_list,
				upc_list = EXCLUDED.upc_list, gtin_list = EXCLUDED.gtin_list,
				variant_reviews = EXCLUDED.variant_reviews, updated_at = EXCLUDED.updated_at
		`, tableName)
	}

	buildStatusQuery := func(tableName string) string {
		return fmt.Sprintf(`
			UPDATE %s
			SET has_product_data = $2, error = $3, product_data_synced_at = $4, updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)
	}

	var successfulASINs []string

	for _, product := range apiResponse.Data {
		attrs := product.Attributes
		asin := product.ID
		if strings.Contains(product.ID, "/") {
			parts := strings.Split(product.ID, "/")
			if len(parts) == 2 {
				asin = parts[1]
			}
		}

		// Track this ASIN as returned by the API
		returnedASINs[asin] = true

		variantsJSON, _ := json.Marshal(attrs.Variants)
		subcategoryRanksJSON, _ := json.Marshal(attrs.SubcategoryRanks)
		feeBreakdownJSON, _ := json.Marshal(attrs.FeeBreakdown)
		eanListJSON, _ := json.Marshal(attrs.EANList)
		isbnListJSON, _ := json.Marshal(attrs.ISBNList)
		upcListJSON, _ := json.Marshal(attrs.UPCList)
		gtinListJSON, _ := json.Marshal(attrs.GTINList)

		var dateFirstAvailable sql.NullTime
		if attrs.DateFirstAvailable != "" {
			if parsedDate, err := time.Parse("2006-01-02", attrs.DateFirstAvailable); err == nil {
				dateFirstAvailable = sql.NullTime{Time: parsedDate, Valid: true}
			}
		}

		var updatedAt sql.NullTime
		if attrs.UpdatedAt != "" {
			if parsedTime, err := time.Parse(time.RFC3339, attrs.UpdatedAt); err == nil {
				updatedAt = sql.NullTime{Time: parsedTime, Valid: true}
			}
		}

		productArgs := []interface{}{
			asin, reportDate, product.ID, attrs.Title, attrs.Price, attrs.Reviews,
			attrs.Category, attrs.Rating, attrs.ImageURL, attrs.ParentASIN,
			attrs.IsVariant, attrs.SellerType, variantsJSON, attrs.BreadcrumbPath,
			attrs.IsStandalone, attrs.IsParent, attrs.IsAvailable, attrs.Brand,
			attrs.ProductRank, attrs.WeightValue, attrs.WeightUnit, attrs.LengthValue,
			attrs.WidthValue, attrs.HeightValue, attrs.DimensionsUnit,
			attrs.ListingQualityScore, attrs.NumberOfSellers, attrs.BuyBoxOwner,
			attrs.BuyBoxOwnerSellerID, dateFirstAvailable,
			attrs.DateFirstAvailableIsEstimated, attrs.Approximate30DayRevenue,
			attrs.Approximate30DayUnitsSold, subcategoryRanksJSON, feeBreakdownJSON,
			eanListJSON, isbnListJSON, upcListJSON, gtinListJSON,
			attrs.VariantReviews, updatedAt, time.Now(),
		}

		// Staging write
		_, err := m.stagingClient.DB.Exec(buildProductQuery(stagingProductTable), productArgs...)
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Staging product store for %s failed: %v", asin, err))
			log.Printf("%s [Product] %s: staging write to %s FAILED: %v", m.logPrefix(), asin, stagingProductTable, err)
			m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, false, fmt.Sprintf("Failed: %v", err), nil)
			continue
		}
		m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, true, nil, time.Now())

		// Production with retry (product_data only, sync_status is staging-only).
		// Skipped entirely when running staging-only (no production client).
		if m.productionClient != nil {
			var prodErr error
			for attempt := 1; attempt <= 3; attempt++ {
				_, prodErr = m.productionClient.DB.Exec(buildProductQuery(prodProductTable), productArgs...)
				if prodErr == nil {
					break
				}
				if attempt < 3 {
					time.Sleep(time.Duration(attempt) * time.Second)
				}
			}

			if prodErr != nil {
				m.addHourlyError("db", fmt.Sprintf("Production product store for %s failed: %v", asin, prodErr))
				log.Printf("%s [Product] %s: production write to %s FAILED after 3 retries: %v",
					m.logPrefix(), asin, prodProductTable, prodErr)
				continue
			}
		}

		// Copy this parent's row onto every visible child. Non-critical: the
		// parent row is stored and the API call is not wasted.
		stagingChildRows, fanErr := fanOutProductRow(m.stagingClient, m.stagingTables, asin, reportDate)
		if fanErr != nil {
			m.addHourlyError("db", fmt.Sprintf("Staging product fan-out for %s failed: %v", asin, fanErr))
			log.Printf("%s [Product] WARNING: staging %v", m.logPrefix(), fanErr)
		}

		var prodChildRows int64
		if m.productionClient != nil {
			if prodChildRows, fanErr = fanOutProductRow(m.productionClient, m.prodTables, asin, reportDate); fanErr != nil {
				m.addHourlyError("db", fmt.Sprintf("Production product fan-out for %s failed: %v", asin, fanErr))
				log.Printf("%s [Product] WARNING: production %v", m.logPrefix(), fanErr)
			}
		}

		m.monitorLog("[Product] %s stored (report_date=%s) | children copied: staging=%d production=%d",
			asin, reportDate, stagingChildRows, prodChildRows)

		successfulASINs = append(successfulASINs, asin)
	}

	return successfulASINs, returnedASINs
}

// batchInsertSalesData inserts multiple sales data points in a single query for better performance
// Returns number of rows inserted and any error
func (m *HourlySyncManager) batchInsertSalesData(db *sql.DB, tableName string, dataPoints []SalesDataPoint, batchSize int) (int, error) {
	if len(dataPoints) == 0 {
		return 0, nil
	}

	totalInserted := 0

	// Process in batches
	for i := 0; i < len(dataPoints); i += batchSize {
		end := i + batchSize
		if end > len(dataPoints) {
			end = len(dataPoints)
		}
		batch := dataPoints[i:end]

		// Build multi-row INSERT query
		// Each row has 9 parameters
		var valueStrings []string
		var args []interface{}
		paramCount := 1

		for _, dp := range batch {
			valueStrings = append(valueStrings, fmt.Sprintf(
				"($%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d, $%d)",
				paramCount, paramCount+1, paramCount+2, paramCount+3, paramCount+4,
				paramCount+5, paramCount+6, paramCount+7, paramCount+8,
			))
			args = append(args,
				dp.ASIN, dp.Marketplace, dp.IsParent, dp.IsVariant, dp.IsStandalone,
				dp.ParentASIN, dp.Date, dp.EstimatedUnits, dp.LastKnownPrice,
			)
			paramCount += 9
		}

		query := fmt.Sprintf(`
			INSERT INTO %s (asin, marketplace, is_parent, is_variant, is_standalone,
			                parent_asin, date, estimated_units_sold, last_known_price)
			VALUES %s
			ON CONFLICT (asin, marketplace, date)
			DO UPDATE SET
				is_parent = EXCLUDED.is_parent,
				is_variant = EXCLUDED.is_variant,
				is_standalone = EXCLUDED.is_standalone,
				parent_asin = EXCLUDED.parent_asin,
				estimated_units_sold = EXCLUDED.estimated_units_sold,
				last_known_price = EXCLUDED.last_known_price,
				updated_at = CURRENT_TIMESTAMP
		`, tableName, strings.Join(valueStrings, ", "))

		_, err := db.Exec(query, args...)
		if err != nil {
			return totalInserted, fmt.Errorf("batch insert failed at offset %d: %w", i, err)
		}

		totalInserted += len(batch)
	}

	return totalInserted, nil
}

// storeHourlySalesData stores sales data for a single ASIN using batch inserts
// NOTE: sync_status is staging-only, production only gets sales_estimate_data writes
func (m *HourlySyncManager) storeHourlySalesData(apiResponse *junglescout.SalesEstimateAPIResponse, marketplace string) bool {
	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return false
	}

	stagingSalesTable := m.stagingClient.TableName("jungle_scout_sales_estimate_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	var prodSalesTable string
	if m.productionClient != nil {
		prodSalesTable = m.productionClient.TableName("jungle_scout_sales_estimate_data")
	}

	jsData := apiResponse.Data[0]
	attrs := jsData.Attributes

	var parentASIN *string
	if attrs.ParentASIN != "" {
		parentASIN = &attrs.ParentASIN
	}

	// Convert API response to SalesDataPoint slice for batch insert
	dataPoints := make([]SalesDataPoint, 0, len(attrs.Data))
	for _, dp := range attrs.Data {
		dataPoints = append(dataPoints, SalesDataPoint{
			ASIN:           attrs.ASIN,
			Marketplace:    marketplace,
			IsParent:       attrs.IsParent,
			IsVariant:      attrs.IsVariant,
			IsStandalone:   attrs.IsStandalone,
			ParentASIN:     parentASIN,
			Date:           dp.Date,
			EstimatedUnits: dp.EstimatedUnitsSold,
			LastKnownPrice: dp.LastKnownPrice,
		})
	}

	batchSize := 100 // Optimal batch size for PostgreSQL

	// Earliest date in this response, used to bound the child fan-out to the window
	// just fetched instead of re-copying the parent's whole history every run. Dates
	// arrive as YYYY-MM-DD, so a lexicographic minimum is the chronological one.
	minDate := ""
	for _, dp := range dataPoints {
		if dp.Date == "" {
			continue
		}
		if minDate == "" || dp.Date < minDate {
			minDate = dp.Date
		}
	}
	if minDate == "" {
		// No usable dates: fall back to the unbounded copy rather than silently
		// skipping the fan-out, which would leave children behind.
		m.debugLog("[Sales] %s: no dates in response, falling back to an unbounded fan-out", attrs.ASIN)
	}

	// Step 1: Batch insert to STAGING
	stagingInserted, stagingErr := m.batchInsertSalesData(m.stagingClient.DB, stagingSalesTable, dataPoints, batchSize)
	if stagingErr != nil {
		m.addHourlyError("db", fmt.Sprintf("Staging sales batch insert for %s failed: %v", attrs.ASIN, stagingErr))
		log.Printf("%s [Sales] staging insert into %s failed for %s: %v",
			m.logPrefix(), stagingSalesTable, attrs.ASIN, stagingErr)
		// Returning false puts this ASIN on the retry queue. The run is only
		// abandoned once failures stop being isolated.
		m.noteDBFailure(fmt.Sprintf("staging sales insert for %s", attrs.ASIN), stagingErr)
		return false
	}
	m.noteDBSuccess()
	m.monitorLog("[Sales] %s: %d rows upserted into staging %s", attrs.ASIN, stagingInserted, stagingSalesTable)

	// Step 2: Batch insert to PRODUCTION with retry.
	// Skipped entirely when running staging-only (no production client).
	if m.productionClient != nil {
		var prodErr error
		var prodInserted int
		for attempt := 1; attempt <= 3; attempt++ {
			prodInserted, prodErr = m.batchInsertSalesData(m.productionClient.DB, prodSalesTable, dataPoints, batchSize)
			if prodErr == nil {
				break
			}
			m.debugLog("[Sales] Production batch insert attempt %d for %s failed: %v", attempt, attrs.ASIN, prodErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}

		if prodErr != nil {
			m.addHourlyError("db", fmt.Sprintf("Production sales batch insert for %s failed after 3 retries: %v", attrs.ASIN, prodErr))
			log.Printf("%s [Sales] production insert into %s failed for %s after 3 retries: %v",
				m.logPrefix(), prodSalesTable, attrs.ASIN, prodErr)
			m.noteDBFailure(fmt.Sprintf("production sales insert for %s", attrs.ASIN), prodErr)
			return false
		}
		m.noteDBSuccess()
		m.monitorLog("[Sales] %s: %d rows upserted into production %s", attrs.ASIN, prodInserted, prodSalesTable)
	}

	// Copy this parent's sales rows onto its visible children. Non-critical.
	if stagingInserted > 0 {
		stagingChildRows, fanErr := fanOutSalesRows(m.stagingClient, m.stagingTables, attrs.ASIN, marketplace, minDate)
		if fanErr != nil {
			m.addHourlyError("db", fmt.Sprintf("Staging sales fan-out for %s failed: %v", attrs.ASIN, fanErr))
			log.Printf("%s [Sales] WARNING: staging %v", m.logPrefix(), fanErr)
		}

		var prodChildRows int64
		if m.productionClient != nil {
			if prodChildRows, fanErr = fanOutSalesRows(m.productionClient, m.prodTables, attrs.ASIN, marketplace, minDate); fanErr != nil {
				m.addHourlyError("db", fmt.Sprintf("Production sales fan-out for %s failed: %v", attrs.ASIN, fanErr))
				log.Printf("%s [Sales] WARNING: production %v", m.logPrefix(), fanErr)
			}
		}

		m.monitorLog("[Sales] %s: child rows copied staging=%d production=%d",
			attrs.ASIN, stagingChildRows, prodChildRows)
	}

	// Update sync_status (staging only)
	if stagingInserted > 0 {
		updateStatusQuery := fmt.Sprintf(`
			UPDATE %s
			SET has_sales_data = true, sales_estimate_data_synced_at = CURRENT_TIMESTAMP,
			    error = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, stagingStatusTable)

		m.stagingClient.DB.Exec(updateStatusQuery, attrs.ASIN)
		m.debugLog("[Sales] Updated sync_status for ASIN %s (staging-only)", attrs.ASIN)
		return true
	}

	return false
}

// updateHourlySyncStatus updates sync status for a specific ASIN
// NOTE: sync_status is staging-only, production writes are skipped
func (m *HourlySyncManager) updateHourlySyncStatus(asin string, hasProductData, hasSalesData bool, errorMsg string) {
	stagingTable := m.stagingClient.TableName("jungle_scout_sync_status")

	buildQuery := func(tableName string) string {
		if errorMsg != "" {
			return fmt.Sprintf(`
				UPDATE %s SET has_product_data = $2, has_sales_data = $3, error = $4, updated_at = CURRENT_TIMESTAMP
				WHERE asin = $1
			`, tableName)
		}
		return fmt.Sprintf(`
			UPDATE %s SET has_product_data = $2, has_sales_data = $3, error = NULL, updated_at = CURRENT_TIMESTAMP
			WHERE asin = $1
		`, tableName)
	}

	var args []interface{}
	if errorMsg != "" {
		args = []interface{}{asin, hasProductData, hasSalesData, errorMsg}
	} else {
		args = []interface{}{asin, hasProductData, hasSalesData}
	}

	m.stagingClient.DB.Exec(buildQuery(stagingTable), args...)
	m.debugLog("[SyncStatus] Skipping production update (sync_status is staging-only)")
}

// addHourlyError adds an error to the error summary
func (m *HourlySyncManager) addHourlyError(errorType, errorMsg string) {
	m.statusMutex.Lock()
	defer m.statusMutex.Unlock()

	if m.status == nil || m.status.ErrorSummary == nil {
		return
	}

	switch errorType {
	case "db":
		m.status.ErrorSummary.DBErrors++
	case "api":
		m.status.ErrorSummary.APIErrors++
	case "parse":
		m.status.ErrorSummary.ParseErrors++
	default:
		m.status.ErrorSummary.OtherErrors++
	}

	if len(m.status.ErrorSummary.SampleErrors) < 3 {
		m.status.ErrorSummary.SampleErrors = append(m.status.ErrorSummary.SampleErrors, errorMsg)
	}
}

// GetStatus returns a copy of the current hourly sync status
func (m *HourlySyncManager) GetStatus() HourlySyncStatus {
	m.statusMutex.RLock()
	defer m.statusMutex.RUnlock()

	if m.status == nil {
		return HourlySyncStatus{}
	}
	return *m.status
}

// sendHourlySyncDiscordNotification sends a Discord notification with smart logging
func (m *HourlySyncManager) sendHourlySyncDiscordNotification() {
	webhookURL := os.Getenv("DISCORD_WEBHOOK_URL")
	if webhookURL == "" {
		webhookURL = "https://discord.com/api/webhooks/1422178082483212348/g7q_D2qbjrZNMbIgmGV8AuegJCL7GOLA0QZrcoPMgB5J4Cpxnl_PCexzDzUP6sbJMxTz"
	}

	duration := time.Since(m.status.StartedAt).Round(time.Second)

	// Determine color and status
	var color int
	var statusText string
	totalErrors := 0
	if m.status.ErrorSummary != nil {
		totalErrors = m.status.ErrorSummary.DBErrors + m.status.ErrorSummary.APIErrors +
			m.status.ErrorSummary.ParseErrors + m.status.ErrorSummary.OtherErrors
	}

	if m.status.StoppedEarly {
		color = 15158332 // Red
		statusText = fmt.Sprintf("❌ STOPPED: %s", m.status.StopReason)
	} else if m.status.FailedASINs > 0 || totalErrors > 0 {
		color = 16776960 // Yellow
		statusText = "⚠️ Completed with issues"
	} else {
		color = 3066993 // Green
		statusText = "✅ Completed successfully"
	}

	// Build fields
	fields := []map[string]interface{}{
		{"name": "📊 Processed", "value": fmt.Sprintf("%d ASINs", m.status.TotalASINsProcessed), "inline": true},
		{"name": "🆕 New", "value": fmt.Sprintf("%d", m.status.NewASINsSynced), "inline": true},
		{"name": "🔄 Stale", "value": fmt.Sprintf("%d", m.status.StaleASINsSynced), "inline": true},
		{"name": "📦 Product", "value": fmt.Sprintf("%d ✓ | %d ✗", m.status.SuccessfulProductSync, m.status.TotalASINsProcessed-m.status.SuccessfulProductSync), "inline": true},
		{"name": "📈 Sales", "value": fmt.Sprintf("%d ✓", m.status.SuccessfulSalesSync), "inline": true},
		{"name": "⏱️ Duration", "value": duration.String(), "inline": true},
		{"name": "🧹 Cleaned", "value": fmt.Sprintf("%d", m.status.CleanedUpASINs), "inline": true},
		{"name": "➕ Added", "value": fmt.Sprintf("%d", m.status.NewASINsAdded), "inline": true},
		{"name": "🔌 API Calls", "value": fmt.Sprintf("%d", m.apiCallCount), "inline": true},
		{"name": "🗂️ ASIN Source", "value": m.asinSourceLabel(), "inline": true},
	}

	// Add error breakdown if any
	if totalErrors > 0 && m.status.ErrorSummary != nil {
		errorBreakdown := fmt.Sprintf("DB: %d | API: %d | Parse: %d | Other: %d",
			m.status.ErrorSummary.DBErrors, m.status.ErrorSummary.APIErrors,
			m.status.ErrorSummary.ParseErrors, m.status.ErrorSummary.OtherErrors)
		fields = append(fields, map[string]interface{}{
			"name": "❌ Errors", "value": errorBreakdown, "inline": false,
		})

		if len(m.status.ErrorSummary.SampleErrors) > 0 {
			sampleText := strings.Join(m.status.ErrorSummary.SampleErrors, "\n")
			if len(sampleText) > 500 {
				sampleText = sampleText[:500] + "..."
			}
			fields = append(fields, map[string]interface{}{
				"name": "📝 Sample Errors", "value": "```" + sampleText + "```", "inline": false,
			})
		}
	}

	embed := map[string]interface{}{
		"title":       "🔄 JungleScout Hourly Sync",
		"description": statusText,
		"color":       color,
		"fields":      fields,
		"timestamp":   m.status.StartedAt.Format(time.RFC3339),
		"footer":      map[string]string{"text": "Hourly Sync Bot"},
	}

	payload := map[string]interface{}{
		"embeds": []interface{}{embed},
	}

	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("[HOURLY_SYNC] CRITICAL: Failed to marshal Discord payload: %v", err)
		return
	}

	resp, err := http.Post(webhookURL, "application/json", bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("[HOURLY_SYNC] CRITICAL: Failed to send Discord webhook: %v", err)
		return
	}
	defer resp.Body.Close()
}
