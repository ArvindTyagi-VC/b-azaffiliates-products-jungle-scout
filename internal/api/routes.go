package api

import (
	"log"

	"azaffiliates/internal/api/handlers"
	"azaffiliates/internal/auth"
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
		adminRoutes.POST("/sync-jungle-scout", handlers.SyncJungleScoutSalesEstimateData(s.GetStagingClient(), s.GetProductionClient()))
		adminRoutes.POST("/sync-product-database", handlers.SyncJungleScoutProductDatabaseData(s.GetStagingClient(), s.GetProductionClient()))

		// Master sync endpoints for JungleScout data
		adminRoutes.POST("/master-sync", handlers.JSMasterSync(s.GetStagingClient(), s.GetProductionClient()))
		adminRoutes.GET("/master-sync/status", handlers.GetJSSyncStatus(s.GetStagingClient(), s.GetProductionClient()))

	}

	// Cloud job routes (API key authentication for scheduled jobs)
	cloudJobRoutes := router.Group("/admin")
	cloudJobRoutes.Use(auth.APIKeyAuth())
	{
		// Hourly sync endpoint for cloud scheduler
		cloudJobRoutes.POST("/hourly-sync", handlers.JSHourlySync(s.GetStagingClient(), s.GetProductionClient()))
		cloudJobRoutes.GET("/hourly-sync/status", handlers.GetJSHourlySyncStatus(s.GetStagingClient(), s.GetProductionClient()))
	}

	log.Println("Routes set up successfully")
	log.Printf("Server configured with Staging: %v, Production: %v",
		s.stagingClient != nil, s.productionClient != nil)
}
