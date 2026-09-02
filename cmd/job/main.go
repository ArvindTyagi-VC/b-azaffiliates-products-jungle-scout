// cmd/job/main.go
// Cloud Run Job entrypoint for the scheduled JungleScout sync.
//
// Originally an hourly job that processed 100 ASINs per run. It now runs on a
// ten-day cycle (the 1st, 11th and 21st of each month), where a single execution
// covers the whole parent set. The behavioural differences that matter here are
// about failure reporting rather than sync logic:
//
//   - A run is long. Cloud Scheduler must trigger this job directly; a Cloud Run
//     service caps requests at 60 minutes and would cut the run off mid-sync.
//   - A run is rare. Exit codes have to be honest, because a missed run is not
//     retried for another ten days. Reporting success after half the ASINs failed
//     used to be survivable; now it hides a loss that nobody notices for a cycle.
//   - Two runs must not overlap. A hung execution could still be going when the
//     next one starts, so the run takes an advisory lock first.
package main

import (
	"context"
	"log"
	"os"
	"time"

	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/database"
	"azaffiliates/internal/junglescout"
	"azaffiliates/internal/promote"
)

// syncAdvisoryLockKey identifies the advisory lock that serialises sync runs. Any
// stable arbitrary number works; it only has to be the same in every process that
// competes for the lock.
const syncAdvisoryLockKey int64 = 771_207_001

func main() {
	log.Println("=== Starting Cloud Run Job: JungleScout Sync ===")

	// Get configuration from environment variables
	marketplace := getEnv("MARKETPLACE", "us")

	// DEBUG_MODE was compared against "false", which enabled verbose logging when
	// the flag was set to false and disabled it when set to true. Harmless when a
	// run was 100 ASINs; on a full-set run the inverted flag produces an enormous
	// log for anyone who explicitly turned debugging off.
	debugMode := os.Getenv("DEBUG_MODE") == "true"

	log.Printf("Configuration: marketplace=%s, debug=%v", marketplace, debugMode)
	handlers.LogSyncTuning()

	// The publish tail copies what this run wrote in staging into the same
	// tables in production, after the sync finishes.
	promoteCfg := promote.LoadConfig()
	promoteCfg.Log()
	promoteOnly := os.Getenv("PROMOTE_ONLY") == "true"

	// Initialize Staging PostgreSQL client
	log.Println("Initializing Staging PostgreSQL client...")
	stagingClient, err := database.InitPostgreSQL(
		os.Getenv("DB_STAGING_HOST"),
		os.Getenv("DB_STAGING_PORT"),
		os.Getenv("DB_STAGING_USER"),
		os.Getenv("DB_STAGING_PASS"),
		os.Getenv("DB_STAGING_NAME"),
		os.Getenv("DB_STAGING_TABLE_PREFIX"),
	)
	if err != nil {
		log.Fatalf("FATAL: Failed to initialize Staging PostgreSQL client: %v", err)
	}
	defer stagingClient.Close()

	// Test Staging connectivity
	if err := stagingClient.TestConnectivity(); err != nil {
		log.Fatalf("FATAL: Staging PostgreSQL connectivity test failed: %v", err)
	}
	log.Println("Staging PostgreSQL connectivity test passed")

	// Serialise runs. Staging holds the lock because it is the database that is
	// always present — production is optional and read-only today.
	ctx := context.Background()
	lock, acquired, err := stagingClient.TryAdvisoryLock(ctx, syncAdvisoryLockKey)
	if err != nil {
		log.Fatalf("FATAL: Failed to take the sync advisory lock: %v", err)
	}
	if !acquired {
		// Not an error: a previous run is still going. Exiting non-zero makes it
		// visible, because on a ten-day cadence a skipped run is worth noticing.
		log.Println("ERROR: Another sync run is already holding the advisory lock — exiting without syncing")
		log.Println("If no run is active, a previous execution may have died mid-run; the lock clears when its session ends")
		os.Exit(1)
	}
	defer func() {
		if err := lock.Release(ctx); err != nil {
			log.Printf("WARNING: %v", err)
		}
	}()

	// Initialize Production PostgreSQL client (optional).
	// vx-3 prod is read-only, so the sync runs STAGING-ONLY when prod creds are
	// absent. When DB_PROD_HOST is empty, productionClient stays nil and every
	// production write is skipped downstream.
	var productionClient *database.PostgreSQLClient
	if os.Getenv("DB_PROD_HOST") != "" {
		log.Println("Initializing Production PostgreSQL client...")
		productionClient, err = database.InitPostgreSQL(
			os.Getenv("DB_PROD_HOST"),
			os.Getenv("DB_PROD_PORT"),
			os.Getenv("DB_PROD_USER"),
			os.Getenv("DB_PROD_PASS"),
			os.Getenv("DB_PROD_NAME"),
			os.Getenv("DB_PROD_TABLE_PREFIX"),
		)
		if err != nil {
			log.Fatalf("FATAL: Failed to initialize Production PostgreSQL client: %v", err)
		}
		defer productionClient.Close()

		// Test Production connectivity
		if err := productionClient.TestConnectivity(); err != nil {
			log.Fatalf("FATAL: Production PostgreSQL connectivity test failed: %v", err)
		}
		log.Println("Production PostgreSQL connectivity test passed")
	} else {
		log.Println("DB_PROD_HOST not set — running STAGING-ONLY (production writes disabled)")
	}

	// Build the JungleScout API usage recorder (dual-write).
	apiUsageRecorder := junglescout.NewDBAPIUsageRecorder(stagingClient, productionClient)

	// Decide which of the two production write paths is live. They are mutually
	// exclusive by design: the sync's inline dual-write puts every row on the
	// wire twice, which on a full-set run is a second round-trip per row for
	// ~22 hours, and it leaves nothing behind to reconcile with if production
	// was unreachable mid-run. When the publish tail is enabled it takes over,
	// and the sync writes staging only.
	publishing := promoteCfg.Enabled && productionClient != nil
	syncMirror := productionClient
	if publishing {
		syncMirror = nil
		log.Println("Publish tail ENABLED — inline dual-write disabled; production is written once, after the sync")
	} else if promoteCfg.Enabled {
		log.Println("PROMOTE_ENABLED is set but DB_PROD_HOST is not — nothing to publish to, the tail will be skipped")
	}

	// The floor has to be read BEFORE the sync writes anything: on the very
	// first publish it is what confines the copy to this run's rows instead of
	// the whole table. Afterwards the stored bookmark takes over.
	var floor promote.Floor
	if publishing {
		floor, err = promote.CaptureFloor(ctx, stagingClient)
		if err != nil {
			// Not fatal to the sync — but without a floor a first publish has no
			// lower bound, so the tail is dropped rather than left to guess.
			log.Printf("CRITICAL: could not capture the publish floor, the tail will be skipped: %v", err)
			publishing = false
		}
	}

	if promoteOnly {
		// Re-publish without re-syncing. Used to catch production up after an
		// outage, or to finish a tail that was cut short.
		log.Println("PROMOTE_ONLY=true — skipping the sync, publishing only")
		if !publishing {
			log.Println("FATAL: PROMOTE_ONLY requires PROMOTE_ENABLED=true and production credentials")
			os.Exit(1)
		}
		report := promote.New(stagingClient, productionClient, promoteCfg).Run(ctx, floor)
		logPublishReport(report)
		if err := report.Err(); err != nil {
			log.Printf("FAILED: publish to production: %v", err)
			os.Exit(1)
		}
		os.Exit(0)
	}

	// Create sync manager and run sync
	log.Println("Creating HourlySyncManager...")
	syncManager := handlers.NewHourlySyncManager(stagingClient, syncMirror, debugMode, apiUsageRecorder)

	log.Println("Starting sync execution...")
	syncManager.RunHourlySync(marketplace)

	status := syncManager.GetStatus()

	log.Printf("=== Sync Completed ===")
	log.Printf("Total ASINs Processed  : %d", status.TotalASINsProcessed)
	log.Printf("Successful Product Sync: %d", status.SuccessfulProductSync)
	log.Printf("New ASINs Synced       : %d", status.NewASINsSynced)
	log.Printf("Refreshed ASINs Synced  : %d", status.StaleASINsSynced)
	log.Printf("Retried / Recovered    : %d / %d", status.RetriedASINs, status.RecoveredASINs)
	log.Printf("Failed ASINs           : %d", status.FailedASINs)
	log.Printf("Total API Calls        : %d", status.TotalAPICalls)

	// Exit code is decided before publishing, because it also decides whether to
	// publish at all: a run that stopped early, was truncated, or lost too many
	// ASINs must not push its partial result into production.
	exitCode := exitCodeFor(status)

	switch {
	case !publishing:
		log.Println("Publish tail skipped — not enabled, or production is not configured")
	case exitCode != 0:
		log.Println("Publish tail SKIPPED — the sync run was not healthy, production is left untouched")
	default:
		report := promote.New(stagingClient, productionClient, promoteCfg).Run(ctx, floor)
		logPublishReport(report)
		if err := report.Err(); err != nil {
			log.Printf("FAILED: publish to production: %v", err)
			log.Println("Staging is complete and the bookmark is checkpointed — a PROMOTE_ONLY re-run will resume")
			exitCode = 1
		}
	}

	os.Exit(exitCode)
}

// logPublishReport writes the per-table outcome of a publish. A publish runs
// unattended once every ten days, so its own log is the only record of what
// moved.
func logPublishReport(report promote.Report) {
	log.Printf("=== Publish to Production Completed ===")
	for _, t := range report.Tables {
		if t.Err != nil {
			log.Printf("  %-40s FAILED after %d row(s): %v", t.Table, t.RowsSent, t.Err)
			continue
		}
		log.Printf("  %-40s read=%-8d sent=%-8d guarded=%-8d watermark=%s%s",
			t.Table, t.RowsRead, t.RowsSent, t.RowsGuarded, t.Watermark.Format(time.RFC3339),
			throttleNote(t.Throttled))
	}
	log.Printf("  %-40s %d row(s) in %s", "TOTAL", report.RowsSent(), report.Duration.Round(time.Second))
}

func throttleNote(throttled bool) string {
	if throttled {
		return " (THROTTLED by PROMOTE_MAX_ROWS_PER_RUN — re-run with PROMOTE_ONLY=true to continue)"
	}
	return ""
}

// exitCodeFor decides the job's exit code from the finished run.
//
// The previous rule was "fail only if every ASIN failed", which meant a run that
// lost half the catalogue exited 0 and showed up in Cloud Run as a success. The
// rule now covers the three ways a run can be bad while still producing some rows:
// it stopped early, it was truncated by the safety cap, or too large a share of
// ASINs failed.
func exitCodeFor(status handlers.HourlySyncStatus) int {
	if status.StoppedEarly {
		log.Printf("FAILED: sync stopped early — %s", status.StopReason)
		return 1
	}

	// Selection hit SYNC_ASIN_LIMIT. The cap sits far above the parent set, so
	// reaching it means the workload was not what it should have been and an
	// unknown number of ASINs were never queued.
	if status.LimitReached {
		log.Println("FAILED: selection hit SYNC_ASIN_LIMIT — the run was truncated and is not a full sync")
		return 1
	}

	if status.TotalASINsProcessed == 0 {
		// Nothing to do is a legitimate outcome: every ASIN was already fresh.
		log.Println("No ASINs required syncing — nothing to do")
		return 0
	}

	failureRate := float64(status.FailedASINs) / float64(status.TotalASINsProcessed)
	log.Printf("Failure rate: %.2f%% (%d of %d), threshold %.2f%%",
		failureRate*100, status.FailedASINs, status.TotalASINsProcessed,
		handlers.MaxFailureRate*100)

	if failureRate > handlers.MaxFailureRate {
		log.Printf("FAILED: failure rate %.2f%% exceeds MAX_FAILURE_RATE (%.2f%%)",
			failureRate*100, handlers.MaxFailureRate*100)
		return 1
	}

	if status.FailedASINs > 0 {
		log.Printf("Completed with %d failed ASINs, within the allowed failure rate", status.FailedASINs)
	} else {
		log.Println("Sync job completed successfully")
	}
	return 0
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
