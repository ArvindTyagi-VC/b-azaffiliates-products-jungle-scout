package api

import (
	"log"
	"os"
	"strconv"

	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/auth"
	"azaffiliates/internal/heliumjs"
	"azaffiliates/internal/junglescout"
)

// setupRoutes configures all the routes for the API
func (s *Server) setupRoutes() {
	log.Println("Setting up routes...")
	router := s.router

	// Basic health and database test endpoints
	router.GET("/health", s.healthCheck)
	router.GET("/test-db", s.testDatabaseConnectivity)

	protected := router.Group("")
	protected.Use(auth.JWTAuth())
	adminRoutes := protected.Group("/admin")
	adminRoutes.Use(auth.AdminRoleCheck())
	{
		adminRoutes.POST("/sync-jungle-scout", handlers.SyncJungleScoutSalesEstimateData(s.GetStagingClient(), s.GetProductionClient(), s.GetAPIUsageRecorder()))
		adminRoutes.POST("/sync-product-database", handlers.SyncJungleScoutProductDatabaseData(s.GetStagingClient(), s.GetProductionClient(), s.GetAPIUsageRecorder()))

		// Master sync endpoints for JungleScout data
		adminRoutes.POST("/master-sync", handlers.JSMasterSync(s.GetStagingClient(), s.GetProductionClient(), s.GetAPIUsageRecorder()))
		adminRoutes.GET("/master-sync/status", handlers.GetJSSyncStatus(s.GetStagingClient(), s.GetProductionClient()))

	}

	// Cloud job routes (API key authentication for scheduled jobs)
	cloudJobRoutes := router.Group("/admin")
	cloudJobRoutes.Use(auth.APIKeyAuth())
	{
		// Hourly sync endpoint for cloud scheduler
		cloudJobRoutes.POST("/hourly-sync", handlers.JSHourlySync(s.GetStagingClient(), s.GetProductionClient(), s.GetAPIUsageRecorder()))

		// Manual hourly sync: same run, but the ASINs come from an uploaded CSV
		// (multipart field "file") instead of the automatic selection.
		cloudJobRoutes.POST("/hourly-sync/manual", handlers.JSManualHourlySync(s.GetStagingClient(), s.GetProductionClient(), s.GetAPIUsageRecorder()))

		cloudJobRoutes.GET("/hourly-sync/status", handlers.GetJSHourlySyncStatus(s.GetStagingClient(), s.GetProductionClient()))
	}

	// Jungle Scout fetches for Helium10 P1/P2/P3 ASINs. Staging only: the
	// service has no production client and refuses any database but staging.
	heliumJS := heliumjs.NewService(s.GetStagingClient(), s.stagingOnlyJSClient(), envInt("HELIUM_JS_WORKERS", 8))
	heliumJSRoutes := router.Group("/helium-js")
	heliumJSRoutes.Use(auth.APIKeyAuth())
	{
		heliumJSRoutes.POST("/fetch", handlers.HeliumJSFetch(heliumJS))
		heliumJSRoutes.POST("/fetch-found", handlers.HeliumJSFetchFromRun(heliumJS, heliumjs.SourceHeliumFound))
		heliumJSRoutes.POST("/fetch-missing", handlers.HeliumJSFetchFromRun(heliumJS, heliumjs.SourceHeliumMissing))
		heliumJSRoutes.GET("/jobs", handlers.HeliumJSJobs(heliumJS))
		heliumJSRoutes.GET("/jobs/:job_id", handlers.HeliumJSJob(heliumJS))
		heliumJSRoutes.GET("/jobs/:job_id/asins", handlers.HeliumJSJobASINs(heliumJS))
	}

	log.Println("Routes set up successfully")
	log.Printf("Server configured with Staging: %v, Production: %v",
		s.stagingClient != nil, s.productionClient != nil)
}

// stagingOnlyJSClient is a Jungle Scout client whose usage is recorded on
// staging only, or nil when JUNGLE_SCOUT_API_KEY is unset.
func (s *Server) stagingOnlyJSClient() *junglescout.Client {
	if os.Getenv("JUNGLE_SCOUT_API_KEY") == "" {
		return nil
	}
	client := junglescout.NewClient()
	client.SetUsageRecorder(junglescout.NewDBAPIUsageRecorder(s.GetStagingClient(), nil))
	return client
}

// envInt reads a positive integer from the environment, or def.
func envInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}
