// cmd/job/main.go
// Cloud Run Job entrypoint for hourly sync
package main

import (
	"log"
	"os"

	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/database"
)

func main() {
	log.Println("=== Starting Cloud Run Job: Hourly Sync ===")

	// Get configuration from environment variables
	marketplace := getEnv("MARKETPLACE", "us")
	debugMode := os.Getenv("DEBUG_MODE") == "false"

	log.Printf("Configuration: marketplace=%s, debug=%v", marketplace, debugMode)

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

	// Initialize Production PostgreSQL client
	log.Println("Initializing Production PostgreSQL client...")
	productionClient, err := database.InitPostgreSQL(
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

	// Create sync manager and run sync
	log.Println("Creating HourlySyncManager...")
	syncManager := handlers.NewHourlySyncManager(stagingClient, productionClient, debugMode)

	log.Println("Starting hourly sync execution...")
	syncManager.RunHourlySync(marketplace)

	// Get final status and determine exit code
	status := syncManager.GetStatus()

	log.Printf("=== Sync Completed ===")
	log.Printf("Total ASINs Processed: %d", status.TotalASINsProcessed)
	log.Printf("Successful Product Sync: %d", status.SuccessfulProductSync)
	log.Printf("Successful Sales Sync: %d", status.SuccessfulSalesSync)
	log.Printf("Failed ASINs: %d", status.FailedASINs)

	// Exit with appropriate code
	if status.StoppedEarly {
		log.Printf("ERROR: Sync stopped early - %s", status.StopReason)
		os.Exit(1)
	}

	// Consider job failed if all sync attempts failed
	if status.FailedASINs > 0 && status.SuccessfulProductSync == 0 {
		log.Println("ERROR: All sync attempts failed")
		os.Exit(1)
	}

	log.Println("Hourly sync job completed successfully")
	os.Exit(0)
}

// getEnv returns the value of an environment variable or a default value
func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}
