package api

import (
	"log"
	"math/rand"
	"net/http"
	"time"

	"azaffiliates/internal/database"

	"github.com/gin-contrib/cors"
	"github.com/gin-gonic/gin"
)

// Server represents the HTTP server
type Server struct {
	router           *gin.Engine
	stagingClient    *database.PostgreSQLClient
	productionClient *database.PostgreSQLClient
}

// NewServer creates a new server instance with both staging and production clients
func NewServer(stagingClient, productionClient *database.PostgreSQLClient) *Server {
	// Initialize random seed
	rand.Seed(time.Now().UnixNano())

	// Initialize Gin router
	router := gin.Default()

	// Configure trusted proxies for Cloud Run deployment
	// Cloud Run uses Google's load balancers, so we trust Google Cloud's internal networks
	router.SetTrustedProxies([]string{
		"127.0.0.1",      // localhost (for local dev)
		"::1",            // localhost IPv6 (for local dev)
		"10.0.0.0/8",     // Google Cloud internal network range
		"172.16.0.0/12",  // Private network range used by GCP
		"192.168.0.0/16", // Private network range used by GCP
	})

	// Set up CORS middleware for all environments
	corsConfig := cors.Config{
		AllowOrigins: []string{
			"https://www.amazon.com",
			"http://localhost:3000",
			"https://vc-amzn-aff-app-3fbbd.firebaseapp.com",
			"https://vc-amzn-aff-app-3fbbd.web.app",
			"https://staging-vc-amzn-aff-app.web.app",
			"https://staging-vc-amzn-aff-app.firebaseapp.com",
			"https://azaffiliates.vcommission.com",
			"https://azaffiliates-staging.vcommission.com",
			"https://dev-vc-amzn-aff-app.web.app",
			"https://dev-vc-amzn-aff-app.firebaseapp.com",
			"http://localhost:5173",
			"http://150.241.245.146",
			"https://e2c-dashboard.web.app",
			"https://azaffiliates-v1.web.app",
			"https://azaffiliates-prod.web.app",
			"https://dev.vtech.in.net",
			"https://azaffiliates-dev.web.app",
			"https://ecommerce.vcommission.com",
			"https://velocityx.vcommission.com",
			"https://azaffiliates-staging.web.app",
			"https://admin.velocityx.global",
		},
		AllowMethods:     []string{"GET", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"},
		AllowHeaders:     []string{"Origin", "Content-Type", "Accept", "Authorization", "X-Request-Time", "X-Token-Refresh-Attempt"},
		ExposeHeaders:    []string{"Content-Length"},
		AllowCredentials: true,
		MaxAge:           12 * time.Hour,
	}

	router.Use(cors.New(corsConfig))

	// Create server
	server := &Server{
		router:           router,
		stagingClient:    stagingClient,
		productionClient: productionClient,
	}

	// Setup routes
	server.setupRoutes()

	return server
}

// Run starts the HTTP server
func (s *Server) Run(addr string) error {
	log.Printf("Starting server on %s", addr)
	return s.router.Run(addr)
}

// GetStagingClient returns the Staging PostgreSQL client
func (s *Server) GetStagingClient() *database.PostgreSQLClient {
	return s.stagingClient
}

// GetProductionClient returns the Production PostgreSQL client
func (s *Server) GetProductionClient() *database.PostgreSQLClient {
	return s.productionClient
}

// GetRouter returns the Gin router
func (s *Server) GetRouter() *gin.Engine {
	return s.router
}

// healthCheck provides a basic health check endpoint
func (s *Server) healthCheck(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"status": "OK",
	})
}

// testDatabaseConnectivity tests both database connections
func (s *Server) testDatabaseConnectivity(c *gin.Context) {
	result := gin.H{
		"staging":    "not_tested",
		"production": "not_tested",
		"status":     "ok",
	}

	// Test Staging PostgreSQL connectivity
	if s.stagingClient != nil {
		if err := s.stagingClient.TestConnectivity(); err != nil {
			result["staging"] = "failed: " + err.Error()
			result["status"] = "error"
		} else {
			result["staging"] = "connected"
		}
	}

	// Test Production PostgreSQL connectivity
	if s.productionClient != nil {
		if err := s.productionClient.TestConnectivity(); err != nil {
			result["production"] = "failed: " + err.Error()
			result["status"] = "error"
		} else {
			result["production"] = "connected"
		}
	}

	if result["status"] == "error" {
		c.JSON(http.StatusInternalServerError, result)
		return
	}

	c.JSON(http.StatusOK, result)
}
