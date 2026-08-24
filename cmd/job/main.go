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

	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/database"
	"azaffiliates/internal/junglescout"
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

	// Create sync manager and run sync
	log.Println("Creating HourlySyncManager...")
	syncManager := handlers.NewHourlySyncManager(stagingClient, productionClient, debugMode, apiUsageRecorder)

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

	os.Exit(exitCodeFor(status))
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
