// cmd/server/main.go
package main

import (
	"log"
	"os"

	"azaffiliates/internal/api"
	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/database"
	"azaffiliates/internal/junglescout"
)

func main() {
	// Record the resolved sync thresholds up front. The hourly-sync endpoints share
	// this configuration with the scheduled job, so when a run behaves unexpectedly
	// the first question is always which values it actually ran with.
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
		log.Fatalf("Failed to initialize Staging PostgreSQL client: %v", err)
	}
	defer stagingClient.Close()

	// Test Staging connectivity
	log.Println("Testing Staging PostgreSQL connectivity...")
	if err := stagingClient.TestConnectivity(); err != nil {
		log.Fatalf("Staging PostgreSQL connectivity test failed: %v", err)
	}
	log.Println("Staging PostgreSQL connectivity test passed!")

	// Initialize Production PostgreSQL client (optional).
	// vx-3 prod is read-only, so the server runs STAGING-ONLY when prod creds are
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
			log.Fatalf("Failed to initialize Production PostgreSQL client: %v", err)
		}
		defer productionClient.Close()

		// Test Production connectivity
		log.Println("Testing Production PostgreSQL connectivity...")
		if err := productionClient.TestConnectivity(); err != nil {
			log.Fatalf("Production PostgreSQL connectivity test failed: %v", err)
		}
		log.Println("Production PostgreSQL connectivity test passed!")
	} else {
		log.Println("DB_PROD_HOST not set — running STAGING-ONLY (production writes disabled)")
	}

	// Set up port with default value
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	// Build the JungleScout API usage recorder (dual-write).
	apiUsageRecorder := junglescout.NewDBAPIUsageRecorder(stagingClient, productionClient)

	// Initialize server with both database clients
	server := api.NewServer(stagingClient, productionClient, apiUsageRecorder)

	log.Printf("Starting server on port %s", port)
	server.Run(":" + port)
}
