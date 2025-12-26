// cmd/server/main.go
package main

import (
	"log"
	"os"

	"azaffiliates/internal/api"
	"azaffiliates/internal/database"
)

func main() {
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
		log.Fatalf("Failed to initialize Production PostgreSQL client: %v", err)
	}
	defer productionClient.Close()

	// Test Production connectivity
	log.Println("Testing Production PostgreSQL connectivity...")
	if err := productionClient.TestConnectivity(); err != nil {
		log.Fatalf("Production PostgreSQL connectivity test failed: %v", err)
	}
	log.Println("Production PostgreSQL connectivity test passed!")

	// Set up port with default value
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	// Initialize server with both database clients
	server := api.NewServer(stagingClient, productionClient)

	log.Printf("Starting server on port %s", port)
	server.Run(":" + port)
}
