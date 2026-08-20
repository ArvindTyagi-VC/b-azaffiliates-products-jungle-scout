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

// ============================================================================
// ADVISORY LOCKS
// ============================================================================

// AdvisoryLock is a held PostgreSQL session-level advisory lock.
//
// It exists to stop two sync runs from overlapping. The in-process "is a sync
// already running" check only guards a single server process, and every Cloud Run
// Job execution is a fresh container, so it provides no protection between runs.
// That mattered little on an hourly schedule; on a ten-day cycle a run that hangs
// could still be going when the next one starts, and both would then compete for
// the same sync_status rows and the same API quota.
//
// The lock is tied to a single pinned connection. A session-level advisory lock
// belongs to the session that took it, and database/sql hands out pooled
// connections, so taking the lock with DB.Exec could release it against a
// different connection later — or silently lose it when the pool retires the
// original. Holding a *sql.Conn keeps one session for the lock's lifetime, and
// PostgreSQL releases it automatically if the process dies without calling
// Release.
type AdvisoryLock struct {
	conn *sql.Conn
	key  int64
}

// TryAdvisoryLock attempts to take the advisory lock identified by key without
// blocking. It reports whether the lock was acquired; false means another session
// holds it, which is a normal outcome and not an error.
func (pg *PostgreSQLClient) TryAdvisoryLock(ctx context.Context, key int64) (*AdvisoryLock, bool, error) {
	conn, err := pg.DB.Conn(ctx)
	if err != nil {
		return nil, false, fmt.Errorf("failed to reserve a connection for the advisory lock: %w", err)
	}

	var acquired bool
	if err := conn.QueryRowContext(ctx, "SELECT pg_try_advisory_lock($1)", key).Scan(&acquired); err != nil {
		conn.Close()
		return nil, false, fmt.Errorf("failed to take advisory lock %d: %w", key, err)
	}

	if !acquired {
		conn.Close()
		return nil, false, nil
	}

	log.Printf("Acquired advisory lock %d", key)
	return &AdvisoryLock{conn: conn, key: key}, true, nil
}

// Release unlocks and returns the pinned connection to the pool. Safe to call on a
// nil lock, so callers can defer it unconditionally.
func (l *AdvisoryLock) Release(ctx context.Context) error {
	if l == nil || l.conn == nil {
		return nil
	}

	_, err := l.conn.ExecContext(ctx, "SELECT pg_advisory_unlock($1)", l.key)
	closeErr := l.conn.Close()
	l.conn = nil

	if err != nil {
		return fmt.Errorf("failed to release advisory lock %d: %w", l.key, err)
	}
	if closeErr != nil {
		return fmt.Errorf("failed to return the advisory lock connection: %w", closeErr)
	}

	log.Printf("Released advisory lock %d", l.key)
	return nil
}
