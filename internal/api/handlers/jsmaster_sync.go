package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"azaffiliates/internal/database"
	"azaffiliates/internal/junglescout"
	"azaffiliates/internal/utils"

	"github.com/gin-gonic/gin"
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
}

var (
	// Global sync manager instance for status tracking
	globalSyncManager *MasterSyncManager
	syncManagerMutex  sync.Mutex
)

// ============================================================================
// HOURLY SYNC MANAGER - For Cloud Job Execution
// ============================================================================

const (
	HourlySyncASINLimit    = 50
	StaleDataThresholdDays = 30
)

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
	ErrorSummary          *ErrorSummary `json:"error_summary"`
}

// ASINSyncInfo holds information about an ASIN to sync
type ASINSyncInfo struct {
	ASIN                  string
	IsNew                 bool // has_product_data=false and no product_data_synced_at
	SalesEstimateSyncedAt *time.Time
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
}

var (
	globalHourlySyncManager *HourlySyncManager
	hourlySyncManagerMutex  sync.Mutex
)

// NewHourlySyncManager creates a new hourly sync manager
func NewHourlySyncManager(stagingClient, productionClient *database.PostgreSQLClient) *HourlySyncManager {
	return &HourlySyncManager{
		stagingClient:    stagingClient,
		productionClient: productionClient,
		jsClient:         junglescout.NewClient(),
		apiCallCount:     0,
	}
}

// NewMasterSyncManager creates a new sync manager with both staging and production clients
func NewMasterSyncManager(stagingClient, productionClient *database.PostgreSQLClient) *MasterSyncManager {
	return &MasterSyncManager{
		stagingClient:    stagingClient,
		productionClient: productionClient,
		jsClient:         junglescout.NewClient(), // Simple client without tracking
		logger:           log.New(log.Writer(), "[MASTER_SYNC] ", log.LstdFlags|log.Lshortfile),
		apiCallCount:     0,
	}
}

// JSMasterSync handles the master synchronization of all ASINs
func JSMasterSync(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
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
		globalSyncManager = NewMasterSyncManager(stagingClient, productionClient)
		syncManagerMutex.Unlock()

		// Get query parameters
		marketplace := c.DefaultQuery("marketplace", "us")
		dateRange := c.DefaultQuery("date_range", "1month") // Default to 1 month

		// Sync mode determines how to handle existing data
		// "fresh" - Reset all flags and fetch fresh data for all ASINs (default for scheduled syncs)
		// "resume" - Continue from where it left off (for failed syncs)
		// "force" - Same as fresh (kept for backward compatibility)
		syncMode := c.DefaultQuery("sync_mode", "fresh")

		// Handle legacy force_resync parameter
		forceResync := c.DefaultQuery("force_resync", "false") == "true"
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

		c.JSON(200, gin.H{
			"message":    "Master sync started",
			"status":     globalSyncManager.GetStatus(),
			"date_range": dateRange,
			"sync_mode":  syncMode,
		})
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
		StartedAt: time.Now(),
		IsRunning: true,
		DateRange: m.dateRange,
		Errors:    []string{},
	}
	m.statusMutex.Unlock()

	m.logger.Printf("Starting sync with date range: %s, mode: %s", m.dateRange, syncMode)

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

	// Step 1: Fetch all ASINs from active products table where visibility=true
	asins, err := m.fetchActiveASINs()
	if err != nil {
		m.addError(fmt.Sprintf("Failed to fetch ASINs: %v", err))
		m.logger.Printf("Error fetching ASINs: %v", err)
		return
	}

	m.statusMutex.Lock()
	m.status.TotalASINs = len(asins)
	m.status.TotalBatches = (len(asins) + 99) / 100 // Calculate total batches (100 ASINs per batch)
	m.statusMutex.Unlock()

	m.logger.Printf("Found %d ASINs to sync", len(asins))

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

// fetchActiveASINs retrieves all ASINs from the active products table where visibility=true
// READ operation - uses stagingClient only
func (m *MasterSyncManager) fetchActiveASINs() ([]string, error) {
	tableName, _ := utils.GetTableName(m.stagingClient, "product")
	query := fmt.Sprintf(`
		SELECT DISTINCT asin
		FROM %s
		WHERE product_visibility = true AND asin IS NOT NULL AND asin != ''
		ORDER BY asin
	`, tableName)

	rows, err := m.stagingClient.DB.Query(query)
	if err != nil {
		return nil, fmt.Errorf("failed to query ASINs: %w", err)
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

// initializeASINSyncStatus creates/updates entries in the jungle_scout_sync_status table
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) initializeASINSyncStatus(asins []string, syncMode string) error {
	stagingTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	productionTableName := m.productionClient.TableName("jungle_scout_sync_status")

	// Resume mode: keep existing sync status, don't reset anything
	if syncMode != "fresh" {
		m.logger.Println("RESUME MODE: Keeping existing sync status for all ASINs")
		return nil
	}

	// Fresh sync: reset all fields for ALL ASINs to get latest data
	m.logger.Println("FRESH SYNC MODE: Resetting all ASINs to fetch latest data")

	// Helper function to run transaction on a database
	runTransaction := func(db *sql.DB, tableName string, dbName string) error {
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("failed to start %s transaction: %w", dbName, err)
		}
		defer tx.Rollback()

		upsertQuery := fmt.Sprintf(`
			INSERT INTO %s (asin, has_product_data, has_sales_data, error, updated_at)
			VALUES ($1, false, false, NULL, CURRENT_TIMESTAMP)
			ON CONFLICT (asin) DO UPDATE SET
				has_product_data = false,
				has_sales_data = false,
				error = NULL,
				product_data_synced_at = NULL,
				sales_estimate_data_synced_at = NULL,
				updated_at = CURRENT_TIMESTAMP
		`, tableName)

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
	if err := m.productionClient.DB.Ping(); err != nil {
		m.logger.Printf("CRITICAL: Production database connection failed before product sync: %v", err)
		m.stopRequested = true
		m.addError(fmt.Sprintf("Production database unreachable: %v", err))
		return
	}

	// Filter ASINs to only those that need product data sync (READ from staging)
	statusTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	query := fmt.Sprintf(`
		SELECT asin
		FROM %s
		WHERE has_product_data = false OR has_product_data IS NULL
		ORDER BY asin
	`, statusTableName)

	rows, err := m.stagingClient.DB.Query(query)
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
	prodProductTable := m.productionClient.TableName("jungle_scout_product_data")
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")
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

		// Step 2: Write to PRODUCTION with retry (3 attempts)
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
	if err := m.productionClient.DB.Ping(); err != nil {
		m.logger.Printf("CRITICAL: Production database connection failed before sales sync: %v", err)
		m.stopRequested = true
		m.addError(fmt.Sprintf("Production database unreachable: %v", err))
		return
	}

	// Get ASINs that have product data but no sales data (READ from staging)
	statusTableName := m.stagingClient.TableName("jungle_scout_sync_status")
	query := fmt.Sprintf(`
		SELECT asin
		FROM %s
		WHERE has_product_data = true AND has_sales_data = false
		ORDER BY asin
	`, statusTableName)

	rows, err := m.stagingClient.DB.Query(query)
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
	prodSalesTable := m.productionClient.TableName("jungle_scout_sales_estimate_data")
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")

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

		// Step 2: Write to PRODUCTION with retry (3 attempts)
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

		successCount++
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

		// Update production status with retry
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
	m.productionClient.DB.Exec(buildErrorStatusQuery(prodStatusTable), attrs.ASIN, errorMsg)

	m.logger.Printf("Failed to sync any sales data for ASIN %s", attrs.ASIN)
	return false
}

// updateASINSyncStatus updates the sync status for a specific ASIN
// WRITE operation - staging first, then production with retry
func (m *MasterSyncManager) updateASINSyncStatus(asin string, hasProductData, hasSalesData bool, errorMsg string) {
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")

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

	// Step 2: Write to PRODUCTION with retry (3 attempts)
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
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")

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

	// Step 2: Write to PRODUCTION with retry (3 attempts)
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

	// Write to production with retry (3 attempts)
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
func JSHourlySync(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		hourlySyncManagerMutex.Lock()
		if globalHourlySyncManager != nil &&
			globalHourlySyncManager.status != nil &&
			globalHourlySyncManager.status.IsRunning {
			hourlySyncManagerMutex.Unlock()
			c.JSON(400, gin.H{
				"error":  "Hourly sync is already in progress",
				"status": globalHourlySyncManager.GetStatus(),
			})
			return
		}

		globalHourlySyncManager = NewHourlySyncManager(stagingClient, productionClient)
		hourlySyncManagerMutex.Unlock()

		marketplace := c.DefaultQuery("marketplace", "us")

		// Run sync synchronously (cloud jobs expect completion)
		globalHourlySyncManager.RunHourlySync(marketplace)

		c.JSON(200, gin.H{
			"message": "Hourly sync completed",
			"status":  globalHourlySyncManager.GetStatus(),
		})
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
	m.statusMutex.Lock()
	m.status = &HourlySyncStatus{
		StartedAt:    time.Now(),
		IsRunning:    true,
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
		m.statusMutex.Unlock()

		m.sendHourlySyncDiscordNotification()
	}()

	// Step 1: Cleanup - Remove ASINs that no longer exist or are not visible
	cleanedUp, err := m.cleanupSyncStatus()
	if err != nil {
		m.addHourlyError("db", fmt.Sprintf("Cleanup failed: %v", err))
		log.Printf("[HOURLY_SYNC] CRITICAL: Cleanup failed: %v", err)
	}
	m.statusMutex.Lock()
	m.status.CleanedUpASINs = cleanedUp
	m.statusMutex.Unlock()

	// Step 2: Add new ASINs to sync_status
	newASINs, err := m.addNewASINsToSyncStatus()
	if err != nil {
		m.addHourlyError("db", fmt.Sprintf("Failed to add new ASINs: %v", err))
		log.Printf("[HOURLY_SYNC] CRITICAL: Failed to add new ASINs: %v", err)
	}
	m.statusMutex.Lock()
	m.status.NewASINsAdded = newASINs
	m.statusMutex.Unlock()

	// Step 3: Select 50 ASINs to process (priority: new first, then stale)
	asinsToSync, err := m.selectASINsToSync()
	if err != nil {
		m.addHourlyError("db", fmt.Sprintf("Failed to select ASINs: %v", err))
		log.Printf("[HOURLY_SYNC] CRITICAL: Failed to select ASINs: %v", err)
		m.stopRequested = true
		m.status.StopReason = "Failed to select ASINs"
		return
	}

	if len(asinsToSync) == 0 {
		return
	}

	// Count new vs stale
	newCount, staleCount := 0, 0
	for _, info := range asinsToSync {
		if info.IsNew {
			newCount++
		} else {
			staleCount++
		}
	}
	m.statusMutex.Lock()
	m.status.NewASINsSynced = newCount
	m.status.StaleASINsSynced = staleCount
	m.statusMutex.Unlock()

	// Step 4 & 5: Sync product data and sales data for selected ASINs
	m.syncSelectedASINs(asinsToSync, marketplace)
}

// cleanupSyncStatus removes sync_status records for ASINs with product_visibility=false
func (m *HourlySyncManager) cleanupSyncStatus() (int, error) {
	productTableName, err := utils.GetTableName(m.stagingClient, "product")
	if err != nil {
		return 0, fmt.Errorf("failed to get product table name: %w", err)
	}

	stagingSyncTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodSyncTable := m.productionClient.TableName("jungle_scout_sync_status")

	buildDeleteQuery := func(syncTable string) string {
		return fmt.Sprintf(`
			DELETE FROM %s
			WHERE asin NOT IN (
				SELECT DISTINCT asin
				FROM %s
				WHERE product_visibility = true
				AND asin IS NOT NULL
				AND asin != ''
			)
		`, syncTable, productTableName)
	}

	result, err := m.stagingClient.DB.Exec(buildDeleteQuery(stagingSyncTable))
	if err != nil {
		return 0, fmt.Errorf("staging cleanup failed: %w", err)
	}

	rowsDeleted, _ := result.RowsAffected()

	// Production with retry
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		_, prodErr = m.productionClient.DB.Exec(buildDeleteQuery(prodSyncTable))
		if prodErr == nil {
			break
		}
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	if prodErr != nil {
		return int(rowsDeleted), fmt.Errorf("production cleanup failed after 3 retries: %w", prodErr)
	}

	return int(rowsDeleted), nil
}

// addNewASINsToSyncStatus inserts ASINs that exist in product table but not in sync_status
func (m *HourlySyncManager) addNewASINsToSyncStatus() (int, error) {
	productTableName, err := utils.GetTableName(m.stagingClient, "product")
	if err != nil {
		return 0, fmt.Errorf("failed to get product table name: %w", err)
	}

	stagingSyncTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodSyncTable := m.productionClient.TableName("jungle_scout_sync_status")

	buildInsertQuery := func(syncTable string) string {
		return fmt.Sprintf(`
			INSERT INTO %s (asin, has_product_data, has_sales_data, updated_at)
			SELECT DISTINCT p.asin, false, false, CURRENT_TIMESTAMP
			FROM %s p
			WHERE p.product_visibility = true
			AND p.asin IS NOT NULL
			AND p.asin != ''
			AND p.asin NOT IN (SELECT asin FROM %s)
		`, syncTable, productTableName, syncTable)
	}

	result, err := m.stagingClient.DB.Exec(buildInsertQuery(stagingSyncTable))
	if err != nil {
		return 0, fmt.Errorf("staging insert failed: %w", err)
	}

	rowsInserted, _ := result.RowsAffected()

	// Production with retry
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		_, prodErr = m.productionClient.DB.Exec(buildInsertQuery(prodSyncTable))
		if prodErr == nil {
			break
		}
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}

	if prodErr != nil {
		return int(rowsInserted), fmt.Errorf("production insert failed after 3 retries: %w", prodErr)
	}

	return int(rowsInserted), nil
}

// selectASINsToSync selects up to 50 ASINs prioritizing new ASINs, then stale ASINs
func (m *HourlySyncManager) selectASINsToSync() ([]ASINSyncInfo, error) {
	syncTable := m.stagingClient.TableName("jungle_scout_sync_status")

	// Priority 1: New ASINs (never synced)
	newASINsQuery := fmt.Sprintf(`
		SELECT asin, sales_estimate_data_synced_at
		FROM %s
		WHERE has_product_data = false
		AND product_data_synced_at IS NULL
		ORDER BY created_at ASC, asin ASC
		LIMIT %d
	`, syncTable, HourlySyncASINLimit)

	rows, err := m.stagingClient.DB.Query(newASINsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query new ASINs: %w", err)
	}

	var results []ASINSyncInfo
	for rows.Next() {
		var info ASINSyncInfo
		var salesSyncedAt sql.NullTime
		if err := rows.Scan(&info.ASIN, &salesSyncedAt); err != nil {
			rows.Close()
			return nil, fmt.Errorf("failed to scan new ASIN: %w", err)
		}
		info.IsNew = true
		if salesSyncedAt.Valid {
			info.SalesEstimateSyncedAt = &salesSyncedAt.Time
		}
		results = append(results, info)
	}
	rows.Close()

	// Priority 2: Stale ASINs (>30 days old)
	remaining := HourlySyncASINLimit - len(results)
	if remaining > 0 {
		staleThreshold := time.Now().AddDate(0, 0, -StaleDataThresholdDays)

		staleASINsQuery := fmt.Sprintf(`
			SELECT asin, sales_estimate_data_synced_at
			FROM %s
			WHERE has_product_data = true
			AND product_data_synced_at IS NOT NULL
			AND product_data_synced_at < $1
			ORDER BY product_data_synced_at ASC
			LIMIT %d
		`, syncTable, remaining)

		staleRows, err := m.stagingClient.DB.Query(staleASINsQuery, staleThreshold)
		if err != nil {
			return nil, fmt.Errorf("failed to query stale ASINs: %w", err)
		}

		for staleRows.Next() {
			var info ASINSyncInfo
			var salesSyncedAt sql.NullTime
			if err := staleRows.Scan(&info.ASIN, &salesSyncedAt); err != nil {
				staleRows.Close()
				return nil, fmt.Errorf("failed to scan stale ASIN: %w", err)
			}
			info.IsNew = false
			if salesSyncedAt.Valid {
				info.SalesEstimateSyncedAt = &salesSyncedAt.Time
			}
			results = append(results, info)
		}
		staleRows.Close()
	}

	return results, nil
}

// syncSelectedASINs syncs product and sales data for the selected ASINs
func (m *HourlySyncManager) syncSelectedASINs(asins []ASINSyncInfo, marketplace string) {
	asinStrings := make([]string, len(asins))
	for i, info := range asins {
		asinStrings[i] = info.ASIN
	}

	// Step 4: Fetch and store product data
	apiResponse, err := m.jsClient.FetchProductData(asinStrings, marketplace)
	m.apiCallCount++
	m.statusMutex.Lock()
	m.status.TotalAPICalls = m.apiCallCount
	m.statusMutex.Unlock()

	if err != nil {
		m.addHourlyError("api", fmt.Sprintf("Product fetch failed: %v", err))
		log.Printf("[HOURLY_SYNC] CRITICAL: Product fetch failed: %v", err)
		m.statusMutex.Lock()
		m.status.FailedASINs = len(asins)
		m.status.TotalASINsProcessed = len(asins)
		m.statusMutex.Unlock()
		return
	}

	successfulASINs := m.storeHourlyProductData(apiResponse, marketplace)

	m.statusMutex.Lock()
	m.status.SuccessfulProductSync = len(successfulASINs)
	m.status.FailedASINs = len(asins) - len(successfulASINs)
	m.status.TotalASINsProcessed = len(asins)
	m.statusMutex.Unlock()

	// Step 5: Sync sales data for successful ASINs
	asinInfoMap := make(map[string]ASINSyncInfo)
	for _, info := range asins {
		asinInfoMap[info.ASIN] = info
	}

	salesSuccessCount := 0
	for _, asin := range successfulASINs {
		info, exists := asinInfoMap[asin]
		if !exists {
			continue
		}

		endDate := time.Now().AddDate(0, 0, -1).Format("2006-01-02")
		var startDate string

		if info.SalesEstimateSyncedAt == nil {
			// New ASIN: fetch 1 year of data
			startDate = time.Now().AddDate(-1, 0, -1).Format("2006-01-02")
		} else {
			// Existing ASIN: fetch from last sync date
			startDate = info.SalesEstimateSyncedAt.Format("2006-01-02")
		}

		salesResponse, err := m.jsClient.FetchSalesEstimateData(asin, marketplace, startDate, endDate)
		m.apiCallCount++
		m.statusMutex.Lock()
		m.status.TotalAPICalls = m.apiCallCount
		m.statusMutex.Unlock()

		if err != nil {
			m.addHourlyError("api", fmt.Sprintf("Sales fetch for %s failed: %v", asin, err))
			m.updateHourlySyncStatus(asin, true, false, fmt.Sprintf("Sales fetch error: %v", err))
			continue
		}

		if m.storeHourlySalesData(salesResponse, marketplace) {
			salesSuccessCount++
		}
	}

	m.statusMutex.Lock()
	m.status.SuccessfulSalesSync = salesSuccessCount
	m.statusMutex.Unlock()
}

// storeHourlyProductData stores product data and returns list of successful ASINs
func (m *HourlySyncManager) storeHourlyProductData(apiResponse *junglescout.ProductAPIResponse, marketplace string) []string {
	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return nil
	}

	stagingProductTable := m.stagingClient.TableName("jungle_scout_product_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodProductTable := m.productionClient.TableName("jungle_scout_product_data")
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")
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
			m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, false, fmt.Sprintf("Failed: %v", err), nil)
			continue
		}
		m.stagingClient.DB.Exec(buildStatusQuery(stagingStatusTable), asin, true, nil, time.Now())

		// Production with retry
		var prodErr error
		for attempt := 1; attempt <= 3; attempt++ {
			_, prodErr = m.productionClient.DB.Exec(buildProductQuery(prodProductTable), productArgs...)
			if prodErr == nil {
				m.productionClient.DB.Exec(buildStatusQuery(prodStatusTable), asin, true, nil, time.Now())
				break
			}
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}

		if prodErr != nil {
			m.addHourlyError("db", fmt.Sprintf("Production product store for %s failed: %v", asin, prodErr))
			m.productionClient.DB.Exec(buildStatusQuery(prodStatusTable), asin, false, fmt.Sprintf("Failed after 3 retries: %v", prodErr), nil)
			continue
		}

		successfulASINs = append(successfulASINs, asin)
	}

	return successfulASINs
}

// storeHourlySalesData stores sales data for a single ASIN
func (m *HourlySyncManager) storeHourlySalesData(apiResponse *junglescout.SalesEstimateAPIResponse, marketplace string) bool {
	if apiResponse == nil || len(apiResponse.Data) == 0 {
		return false
	}

	stagingSalesTable := m.stagingClient.TableName("jungle_scout_sales_estimate_data")
	stagingStatusTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodSalesTable := m.productionClient.TableName("jungle_scout_sales_estimate_data")
	prodStatusTable := m.productionClient.TableName("jungle_scout_sync_status")

	jsData := apiResponse.Data[0]
	attrs := jsData.Attributes

	var parentASIN *string
	if attrs.ParentASIN != "" {
		parentASIN = &attrs.ParentASIN
	}

	buildSalesQuery := func(tableName string) string {
		return fmt.Sprintf(`
			INSERT INTO %s (asin, marketplace, is_parent, is_variant, is_standalone,
			                parent_asin, date, estimated_units_sold, last_known_price)
			VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
			ON CONFLICT (asin, marketplace, date)
			DO UPDATE SET
				is_parent = EXCLUDED.is_parent, is_variant = EXCLUDED.is_variant,
				is_standalone = EXCLUDED.is_standalone, parent_asin = EXCLUDED.parent_asin,
				estimated_units_sold = EXCLUDED.estimated_units_sold,
				last_known_price = EXCLUDED.last_known_price, updated_at = CURRENT_TIMESTAMP
		`, tableName)
	}

	successCount := 0
	for _, dataPoint := range attrs.Data {
		args := []interface{}{
			attrs.ASIN, marketplace, attrs.IsParent, attrs.IsVariant,
			attrs.IsStandalone, parentASIN, dataPoint.Date,
			dataPoint.EstimatedUnitsSold, dataPoint.LastKnownPrice,
		}

		// Staging write
		_, err := m.stagingClient.DB.Exec(buildSalesQuery(stagingSalesTable), args...)
		if err != nil {
			m.addHourlyError("db", fmt.Sprintf("Staging sales store for %s failed: %v", attrs.ASIN, err))
			continue
		}

		// Production with retry
		var prodErr error
		for attempt := 1; attempt <= 3; attempt++ {
			_, prodErr = m.productionClient.DB.Exec(buildSalesQuery(prodSalesTable), args...)
			if prodErr == nil {
				break
			}
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}

		if prodErr != nil {
			m.addHourlyError("db", fmt.Sprintf("Production sales store for %s failed: %v", attrs.ASIN, prodErr))
			continue
		}

		successCount++
	}

	if successCount > 0 {
		buildSuccessStatusQuery := func(tableName string) string {
			return fmt.Sprintf(`
				UPDATE %s
				SET has_sales_data = true, sales_estimate_data_synced_at = CURRENT_TIMESTAMP,
				    error = NULL, updated_at = CURRENT_TIMESTAMP
				WHERE asin = $1
			`, tableName)
		}

		m.stagingClient.DB.Exec(buildSuccessStatusQuery(stagingStatusTable), attrs.ASIN)
		for attempt := 1; attempt <= 3; attempt++ {
			_, err := m.productionClient.DB.Exec(buildSuccessStatusQuery(prodStatusTable), attrs.ASIN)
			if err == nil {
				break
			}
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}
		return true
	}

	return false
}

// updateHourlySyncStatus updates sync status for a specific ASIN
func (m *HourlySyncManager) updateHourlySyncStatus(asin string, hasProductData, hasSalesData bool, errorMsg string) {
	stagingTable := m.stagingClient.TableName("jungle_scout_sync_status")
	prodTable := m.productionClient.TableName("jungle_scout_sync_status")

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
	for attempt := 1; attempt <= 3; attempt++ {
		_, err := m.productionClient.DB.Exec(buildQuery(prodTable), args...)
		if err == nil {
			break
		}
		if attempt < 3 {
			time.Sleep(time.Duration(attempt) * time.Second)
		}
	}
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
