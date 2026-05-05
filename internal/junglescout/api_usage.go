package junglescout

import (
	"fmt"
	"log"
	"time"

	"azaffiliates/internal/database"
)

// Endpoint key constants. These match the values written to the
// dev_az_js_api_usage.endpoint column.
const (
	EndpointProductDatabaseQuery = "product_database_query"
	EndpointSalesEstimatesQuery  = "sales_estimates_query"
)

// APIUsageRecorder records a single JungleScout API call against an endpoint.
// Implementations must be safe for concurrent use and must never panic — a
// failure to record a call must not break the API call itself.
type APIUsageRecorder interface {
	Record(endpoint string)
}

// DBAPIUsageRecorder persists API call counts to both staging and production
// databases via dual-write (staging-first, then production with 3 retries).
// Errors are logged and swallowed.
type DBAPIUsageRecorder struct {
	staging    *database.PostgreSQLClient
	production *database.PostgreSQLClient
}

func NewDBAPIUsageRecorder(staging, production *database.PostgreSQLClient) *DBAPIUsageRecorder {
	return &DBAPIUsageRecorder{
		staging:    staging,
		production: production,
	}
}

// Record upserts a +1 increment for (CURRENT_DATE, endpoint) on staging, then
// production with retry. Best-effort: any error is logged, not returned.
func (r *DBAPIUsageRecorder) Record(endpoint string) {
	if r == nil {
		return
	}

	if r.staging != nil {
		if err := upsertAPIUsage(r.staging, endpoint); err != nil {
			log.Printf("[API_USAGE] Staging upsert failed for endpoint=%s: %v", endpoint, err)
			// Skip production write if staging failed — matches the existing
			// dual-write convention in jsmatrix.go.
			return
		}
	}

	if r.production != nil {
		var lastErr error
		for attempt := 1; attempt <= 3; attempt++ {
			if err := upsertAPIUsage(r.production, endpoint); err != nil {
				lastErr = err
				time.Sleep(time.Duration(attempt) * time.Second)
				continue
			}
			lastErr = nil
			break
		}
		if lastErr != nil {
			log.Printf("[API_USAGE] Production upsert failed for endpoint=%s after 3 attempts: %v", endpoint, lastErr)
		}
	}
}

func upsertAPIUsage(pg *database.PostgreSQLClient, endpoint string) error {
	tableName := pg.TableName("js_api_usage")
	query := fmt.Sprintf(`
		INSERT INTO %s (usage_date, endpoint, call_count, created_at, updated_at)
		VALUES (CURRENT_DATE, $1, 1, NOW(), NOW())
		ON CONFLICT (usage_date, endpoint) DO UPDATE
		SET call_count = %s.call_count + 1,
		    updated_at = NOW()
	`, tableName, tableName)

	_, err := pg.DB.Exec(query, endpoint)
	return err
}
