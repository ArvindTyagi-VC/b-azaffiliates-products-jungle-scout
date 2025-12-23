package handlers

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"azaffiliates/internal/database"

	"github.com/gin-gonic/gin"
)

// JungleScout API response structures
type JungleScoutDataPoint struct {
	Date               string  `json:"date"`
	EstimatedUnitsSold int     `json:"estimated_units_sold"`
	LastKnownPrice     float64 `json:"last_known_price"`
}

type JungleScoutAttributes struct {
	ASIN         string                 `json:"asin"`
	IsParent     bool                   `json:"is_parent"`
	IsVariant    bool                   `json:"is_variant"`
	IsStandalone bool                   `json:"is_standalone"`
	ParentASIN   string                 `json:"parent_asin"`
	Variants     []string               `json:"variants"`
	Data         []JungleScoutDataPoint `json:"data"`
}

type JungleScoutData struct {
	ID         string                `json:"id"`
	Type       string                `json:"type"`
	Attributes JungleScoutAttributes `json:"attributes"`
}

type JungleScoutAPIResponse struct {
	Data []JungleScoutData `json:"data"`
}

type ProductDatabaseRequestBody struct {
	ASINs []string `json:"asins" binding:"required"`
}

type JungleScoutProductAttributes struct {
	Title                         string                   `json:"title"`
	Price                         float64                  `json:"price"`
	Reviews                       int                      `json:"reviews"`
	Category                      string                   `json:"category"`
	Rating                        float64                  `json:"rating"`
	ImageURL                      string                   `json:"image_url"`
	ParentASIN                    string                   `json:"parent_asin"`
	IsVariant                     bool                     `json:"is_variant"`
	SellerType                    string                   `json:"seller_type"`
	Variants                      []string                 `json:"variants"`
	BreadcrumbPath                string                   `json:"breadcrumb_path"`
	IsStandalone                  bool                     `json:"is_standalone"`
	IsParent                      bool                     `json:"is_parent"`
	IsAvailable                   bool                     `json:"is_available"`
	Brand                         string                   `json:"brand"`
	ProductRank                   *int                     `json:"product_rank"`
	WeightValue                   float64                  `json:"weight_value"`
	WeightUnit                    string                   `json:"weight_unit"`
	LengthValue                   float64                  `json:"length_value"`
	WidthValue                    float64                  `json:"width_value"`
	HeightValue                   float64                  `json:"height_value"`
	DimensionsUnit                string                   `json:"dimensions_unit"`
	ListingQualityScore           *int                     `json:"listing_quality_score"`
	NumberOfSellers               *int                     `json:"number_of_sellers"`
	BuyBoxOwner                   string                   `json:"buy_box_owner"`
	BuyBoxOwnerSellerID           string                   `json:"buy_box_owner_seller_id"`
	DateFirstAvailable            string                   `json:"date_first_available"`
	DateFirstAvailableIsEstimated bool                     `json:"date_first_available_is_estimated"`
	Approximate30DayRevenue       float64                  `json:"approximate_30_day_revenue"`
	Approximate30DayUnitsSold     int                      `json:"approximate_30_day_units_sold"`
	SubcategoryRanks              []map[string]interface{} `json:"subcategory_ranks"`
	FeeBreakdown                  map[string]interface{}   `json:"fee_breakdown"`
	EANList                       []string                 `json:"ean_list"`
	ISBNList                      []string                 `json:"isbn_list"`
	UPCList                       []string                 `json:"upc_list"`
	GTINList                      []string                 `json:"gtin_list"`
	VariantReviews                *int                     `json:"variant_reviews"`
	UpdatedAt                     string                   `json:"updated_at"`
}

type JungleScoutProductData struct {
	ID         string                       `json:"id"`
	Type       string                       `json:"type"`
	Attributes JungleScoutProductAttributes `json:"attributes"`
}

type JungleScoutProductAPIResponse struct {
	Data []JungleScoutProductData `json:"data"`
}

// execWithRetry executes a transaction on production with 3 retries
func execTxWithRetry(prodClient *database.PostgreSQLClient, txFunc func(tx *sql.Tx) error) error {
	var prodErr error
	for attempt := 1; attempt <= 3; attempt++ {
		tx, err := prodClient.DB.Begin()
		if err != nil {
			prodErr = err
			log.Printf("Production tx begin attempt %d failed: %v", attempt, err)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
			continue
		}

		if err := txFunc(tx); err != nil {
			tx.Rollback()
			prodErr = err
			log.Printf("Production tx attempt %d failed: %v", attempt, err)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
			continue
		}

		if err := tx.Commit(); err != nil {
			prodErr = err
			log.Printf("Production tx commit attempt %d failed: %v", attempt, err)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
			continue
		}

		return nil // Success
	}
	return fmt.Errorf("production failed after 3 retries: %w", prodErr)
}

// SyncJungleScoutSalesEstimateData handles POST /admin/sync-jungle-scout
// Writes to staging first, then production with retry
func SyncJungleScoutSalesEstimateData(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get query parameters
		asin := c.Query("asin")
		marketplace := c.Query("marketplace")
		startDate := c.Query("start_date")
		endDate := c.Query("end_date")

		// Validate required parameters
		if asin == "" || marketplace == "" || startDate == "" || endDate == "" {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "asin, marketplace, start_date, and end_date are required parameters",
			})
			return
		}

		// Validate and adjust date range (max 1 year)
		parsedStartDate, err := time.Parse("2006-01-02", startDate)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid start_date format. Expected YYYY-MM-DD: %v", err),
			})
			return
		}

		parsedEndDate, err := time.Parse("2006-01-02", endDate)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid end_date format. Expected YYYY-MM-DD: %v", err),
			})
			return
		}

		// Check if date range exceeds 1 year (365 days)
		oneYearBeforeEnd := parsedEndDate.AddDate(-1, 0, 0)
		originalStartDate := startDate
		dateRangeAdjusted := false
		if parsedStartDate.Before(oneYearBeforeEnd) {
			parsedStartDate = oneYearBeforeEnd
			startDate = parsedStartDate.Format("2006-01-02")
			dateRangeAdjusted = true
		}

		// Get API key from environment
		apiKey := os.Getenv("JUNGLE_SCOUT_API_KEY")
		if apiKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "JUNGLE_SCOUT_API_KEY not configured",
			})
			return
		}

		// Build API URL
		apiURL := fmt.Sprintf(
			"https://developer.junglescout.com/api/sales_estimates_query?marketplace=%s&asin=%s&start_date=%s&end_date=%s",
			marketplace, asin, startDate, endDate,
		)

		// Create HTTP request
		req, err := http.NewRequest("GET", apiURL, nil)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to create request: %v", err),
			})
			return
		}

		// Add headers
		req.Header.Set("Authorization", apiKey)
		req.Header.Set("X_API_Type", "junglescout")
		req.Header.Set("Accept", "application/vnd.junglescout.v1+json")
		req.Header.Set("Content-Type", "application/vnd.api+json")

		// Make API request
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to fetch data from JungleScout API: %v", err),
			})
			return
		}
		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			c.JSON(resp.StatusCode, gin.H{
				"error":        "JungleScout API request failed",
				"status_code":  resp.StatusCode,
				"api_response": string(body),
			})
			return
		}

		// Parse response
		var apiResponse JungleScoutAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to parse API response: %v", err),
			})
			return
		}

		// Check if we got data
		if len(apiResponse.Data) == 0 {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "No data returned from JungleScout API for the specified ASIN",
			})
			return
		}

		// Get the first data item
		jsData := apiResponse.Data[0]
		attrs := jsData.Attributes

		// Handle parent_asin (may be empty string)
		var parentASIN *string
		if attrs.ParentASIN != "" {
			parentASIN = &attrs.ParentASIN
		}

		// Function to execute the sync on a database
		syncToDatabase := func(pgClient *database.PostgreSQLClient, dbName string) (int, int, error) {
			tableName := pgClient.TableName("jungle_scout_sales_estimate_data")

			tx, err := pgClient.DB.Begin()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to start %s transaction: %w", dbName, err)
			}
			defer tx.Rollback()

			upsertQuery := fmt.Sprintf(`
				INSERT INTO %s (asin, marketplace, is_parent, is_variant, is_standalone, parent_asin, date, estimated_units_sold, last_known_price, rank, updated_at)
				VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL, CURRENT_TIMESTAMP)
				ON CONFLICT (asin, marketplace, date)
				DO UPDATE SET
					is_parent = EXCLUDED.is_parent,
					is_variant = EXCLUDED.is_variant,
					is_standalone = EXCLUDED.is_standalone,
					parent_asin = EXCLUDED.parent_asin,
					estimated_units_sold = EXCLUDED.estimated_units_sold,
					last_known_price = EXCLUDED.last_known_price,
					updated_at = CURRENT_TIMESTAMP
				RETURNING (xmax = 0) as inserted
			`, tableName)

			stmt, err := tx.Prepare(upsertQuery)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to prepare %s statement: %w", dbName, err)
			}
			defer stmt.Close()

			var insertedCount, updatedCount int
			for _, dataPoint := range attrs.Data {
				var wasInserted bool
				err := stmt.QueryRow(
					attrs.ASIN, marketplace, attrs.IsParent, attrs.IsVariant,
					attrs.IsStandalone, parentASIN, dataPoint.Date,
					dataPoint.EstimatedUnitsSold, dataPoint.LastKnownPrice,
				).Scan(&wasInserted)

				if err != nil {
					return 0, 0, fmt.Errorf("failed to insert/update %s data for date %s: %w", dbName, dataPoint.Date, err)
				}

				if wasInserted {
					insertedCount++
				} else {
					updatedCount++
				}
			}

			if err := tx.Commit(); err != nil {
				return 0, 0, fmt.Errorf("failed to commit %s transaction: %w", dbName, err)
			}

			return insertedCount, updatedCount, nil
		}

		// Step 1: Write to STAGING first
		stagingInserted, stagingUpdated, err := syncToDatabase(stagingClient, "staging")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Staging sync failed: %v", err),
			})
			return
		}
		log.Printf("Staging sync complete: %d inserted, %d updated", stagingInserted, stagingUpdated)

		// Step 2: Write to PRODUCTION with retry (3 attempts)
		var prodInserted, prodUpdated int
		var prodErr error
		for attempt := 1; attempt <= 3; attempt++ {
			prodInserted, prodUpdated, prodErr = syncToDatabase(productionClient, "production")
			if prodErr == nil {
				break
			}
			log.Printf("Production sync attempt %d failed: %v", attempt, prodErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}

		if prodErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":           fmt.Sprintf("Production sync failed after 3 retries: %v", prodErr),
				"staging_success": true,
				"staging_inserted": stagingInserted,
				"staging_updated":  stagingUpdated,
			})
			return
		}

		response := gin.H{
			"message":             "Successfully synced data to both databases",
			"asin":                attrs.ASIN,
			"marketplace":         marketplace,
			"staging_inserted":    stagingInserted,
			"staging_updated":     stagingUpdated,
			"production_inserted": prodInserted,
			"production_updated":  prodUpdated,
			"total_records":       len(attrs.Data),
			"date_range": gin.H{
				"start": startDate,
				"end":   endDate,
			},
		}

		if dateRangeAdjusted {
			response["warning"] = fmt.Sprintf("Date range exceeded 1 year limit. Adjusted start_date from %s to %s", originalStartDate, startDate)
		}

		c.JSON(http.StatusOK, response)
	}
}

// SyncJungleScoutProductDatabaseData handles POST /admin/sync-product-database
// Writes to staging first, then production with retry
func SyncJungleScoutProductDatabaseData(stagingClient, productionClient *database.PostgreSQLClient) gin.HandlerFunc {
	return func(c *gin.Context) {
		// Parse request body
		var requestBody ProductDatabaseRequestBody
		if err := c.ShouldBindJSON(&requestBody); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": fmt.Sprintf("Invalid request body: %v", err),
			})
			return
		}

		// Validate ASIN count (max 100)
		if len(requestBody.ASINs) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "At least one ASIN is required",
			})
			return
		}

		if len(requestBody.ASINs) > 100 {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "Maximum 100 ASINs allowed per request",
			})
			return
		}

		// Get marketplace from query parameter
		marketplace := c.DefaultQuery("marketplace", "us")

		// Get API key from environment
		apiKey := os.Getenv("JUNGLE_SCOUT_API_KEY")
		if apiKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "JUNGLE_SCOUT_API_KEY not configured",
			})
			return
		}

		// Build API request body
		apiRequestBody := map[string]interface{}{
			"data": map[string]interface{}{
				"type": "product_database_query",
				"attributes": map[string]interface{}{
					"include_keywords": requestBody.ASINs,
				},
			},
		}

		jsonBody, err := json.Marshal(apiRequestBody)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to create request body: %v", err),
			})
			return
		}

		// Build API URL
		apiURL := fmt.Sprintf(
			"https://developer.junglescout.com/api/product_database_query?marketplace=%s&sort=name&page[size]=100",
			marketplace,
		)

		// Create HTTP request
		req, err := http.NewRequest("POST", apiURL, bytes.NewBuffer(jsonBody))
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to create request: %v", err),
			})
			return
		}

		// Add headers
		req.Header.Set("Authorization", apiKey)
		req.Header.Set("X_API_Type", "junglescout")
		req.Header.Set("Accept", "application/vnd.junglescout.v1+json")
		req.Header.Set("Content-Type", "application/vnd.api+json")

		// Make API request
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to fetch data from JungleScout API: %v", err),
			})
			return
		}
		defer resp.Body.Close()

		// Check response status
		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			c.JSON(resp.StatusCode, gin.H{
				"error":        "JungleScout API request failed",
				"status_code":  resp.StatusCode,
				"api_response": string(body),
			})
			return
		}

		// Parse response
		var apiResponse JungleScoutProductAPIResponse
		if err := json.NewDecoder(resp.Body).Decode(&apiResponse); err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Failed to parse API response: %v", err),
			})
			return
		}

		// Check if we got data
		if len(apiResponse.Data) == 0 {
			c.JSON(http.StatusNotFound, gin.H{
				"error": "No data returned from JungleScout API for the specified ASINs",
			})
			return
		}

		reportDate := time.Now().Format("2006-01-02")

		// Function to execute the sync on a database
		syncToDatabase := func(pgClient *database.PostgreSQLClient, dbName string) (int, int, error) {
			tableName := pgClient.TableName("jungle_scout_product_data")

			tx, err := pgClient.DB.Begin()
			if err != nil {
				return 0, 0, fmt.Errorf("failed to start %s transaction: %w", dbName, err)
			}
			defer tx.Rollback()

			upsertQuery := fmt.Sprintf(`
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
				RETURNING (xmax = 0) as inserted
			`, tableName)

			stmt, err := tx.Prepare(upsertQuery)
			if err != nil {
				return 0, 0, fmt.Errorf("failed to prepare %s statement: %w", dbName, err)
			}
			defer stmt.Close()

			var insertedCount, updatedCount int

			for _, product := range apiResponse.Data {
				attrs := product.Attributes

				// Extract ASIN from ID (format: "us/B08JYQLKXZ")
				asin := product.ID
				if strings.Contains(product.ID, "/") {
					parts := strings.Split(product.ID, "/")
					if len(parts) == 2 {
						asin = parts[1]
					}
				}

				// Convert slices and maps to JSON
				variantsJSON, _ := json.Marshal(attrs.Variants)
				subcategoryRanksJSON, _ := json.Marshal(attrs.SubcategoryRanks)
				feeBreakdownJSON, _ := json.Marshal(attrs.FeeBreakdown)
				eanListJSON, _ := json.Marshal(attrs.EANList)
				isbnListJSON, _ := json.Marshal(attrs.ISBNList)
				upcListJSON, _ := json.Marshal(attrs.UPCList)
				gtinListJSON, _ := json.Marshal(attrs.GTINList)

				// Parse date_first_available
				var dateFirstAvailable sql.NullTime
				if attrs.DateFirstAvailable != "" {
					parsedDate, err := time.Parse("2006-01-02", attrs.DateFirstAvailable)
					if err == nil {
						dateFirstAvailable = sql.NullTime{Time: parsedDate, Valid: true}
					}
				}

				// Parse updated_at
				var updatedAt sql.NullTime
				if attrs.UpdatedAt != "" {
					parsedTime, err := time.Parse(time.RFC3339, attrs.UpdatedAt)
					if err == nil {
						updatedAt = sql.NullTime{Time: parsedTime, Valid: true}
					}
				}

				var wasInserted bool
				err := stmt.QueryRow(
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
				).Scan(&wasInserted)

				if err != nil {
					return 0, 0, fmt.Errorf("failed to insert/update %s data for ASIN %s: %w", dbName, asin, err)
				}

				if wasInserted {
					insertedCount++
				} else {
					updatedCount++
				}
			}

			if err := tx.Commit(); err != nil {
				return 0, 0, fmt.Errorf("failed to commit %s transaction: %w", dbName, err)
			}

			return insertedCount, updatedCount, nil
		}

		// Step 1: Write to STAGING first
		stagingInserted, stagingUpdated, err := syncToDatabase(stagingClient, "staging")
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": fmt.Sprintf("Staging sync failed: %v", err),
			})
			return
		}
		log.Printf("Staging product sync complete: %d inserted, %d updated", stagingInserted, stagingUpdated)

		// Step 2: Write to PRODUCTION with retry (3 attempts)
		var prodInserted, prodUpdated int
		var prodErr error
		for attempt := 1; attempt <= 3; attempt++ {
			prodInserted, prodUpdated, prodErr = syncToDatabase(productionClient, "production")
			if prodErr == nil {
				break
			}
			log.Printf("Production product sync attempt %d failed: %v", attempt, prodErr)
			if attempt < 3 {
				time.Sleep(time.Duration(attempt) * time.Second)
			}
		}

		if prodErr != nil {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error":            fmt.Sprintf("Production sync failed after 3 retries: %v", prodErr),
				"staging_success":  true,
				"staging_inserted": stagingInserted,
				"staging_updated":  stagingUpdated,
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"message":             "Successfully synced product data to both databases",
			"marketplace":         marketplace,
			"report_date":         reportDate,
			"asins_requested":     len(requestBody.ASINs),
			"products_found":      len(apiResponse.Data),
			"staging_inserted":    stagingInserted,
			"staging_updated":     stagingUpdated,
			"production_inserted": prodInserted,
			"production_updated":  prodUpdated,
		})
	}
}
