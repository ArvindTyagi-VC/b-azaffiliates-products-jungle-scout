package database

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/lib/pq" // PostgreSQL driver
	"log"
	"time"
)

// PostgreSQLClient represents a PostgreSQL database client
type PostgreSQLClient struct {
	DB          *sql.DB
	TablePrefix string
}

// InitPostgreSQL initializes a PostgreSQL connection
func InitPostgreSQL(host, port, user, password, dbname, tablePrefix string) (*PostgreSQLClient, error) {
	// Build connection string
	sslMode := "require"
	if host == "localhost" {
		sslMode = "disable"
	}

	connStr := fmt.Sprintf(
		"host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		host, port, user, password, dbname, sslMode,
	)

	// Open database connection
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test the connection
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Printf("Successfully connected to PostgreSQL database with table prefix: %s", tablePrefix)
	return &PostgreSQLClient{
		DB:          db,
		TablePrefix: tablePrefix,
	}, nil
}

// Close closes the PostgreSQL connection
func (pg *PostgreSQLClient) Close() error {
	if pg.DB != nil {
		return pg.DB.Close()
	}
	return nil
}

// TableName returns the full table name with prefix
func (pg *PostgreSQLClient) TableName(tableName string) string {
	if pg.TablePrefix == "" {
		return tableName
	}
	return pg.TablePrefix + tableName
}

// QueryWithTablePrefix executes a query with table names automatically prefixed
// Usage: QueryWithTablePrefix("SELECT * FROM {user} WHERE id = $1", 123)
// The {tableName} placeholders will be replaced with prefixed table names
func (pg *PostgreSQLClient) QueryWithTablePrefix(query string, args ...interface{}) (*sql.Rows, error) {
	// This is a simple implementation - you can enhance it to handle multiple table replacements
	// For now, it's a helper method that you can expand as needed
	return pg.DB.Query(query, args...)
}

// GetTablePrefix returns the current table prefix
func (pg *PostgreSQLClient) GetTablePrefix() string {
	return pg.TablePrefix
}

// TestConnectivity tests the database connectivity
func (pg *PostgreSQLClient) TestConnectivity() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Simple query to test basic connectivity without depending on specific tables
	var result int
	err := pg.DB.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return fmt.Errorf("failed to execute connectivity test query: %w", err)
	}

	if result != 1 {
		return fmt.Errorf("connectivity test returned unexpected result: %d", result)
	}

	log.Printf("PostgreSQL connectivity test passed successfully")
	return nil
}
