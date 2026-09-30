package jsstore

import (
	"context"
	"database/sql"
	"fmt"
)

// HeliumWrittenTable records the Jungle Scout rows written by /helium-js jobs.
// The staging -> production publish leaves those rows out.
const HeliumWrittenTable = "helium_js_written"

// Execer is satisfied by *sql.DB and *sql.Tx.
type Execer interface {
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// MarkHeliumWritten records that dataTable rows for asin between fromDate and
// toDate were just written by a /helium-js job.
func MarkHeliumWritten(ctx context.Context, db Execer, markerTable, dataTable, asin, marketplace, fromDate, toDate string) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (table_name, asin, marketplace, from_date, to_date)
		VALUES ($1, $2, $3, $4::date, $5::date)
		ON CONFLICT (table_name, asin, marketplace, from_date, to_date) DO UPDATE
		   SET written_at = NOW(), written_at_local = LOCALTIMESTAMP`, markerTable),
		dataTable, asin, marketplace, fromDate, toDate)
	return err
}
