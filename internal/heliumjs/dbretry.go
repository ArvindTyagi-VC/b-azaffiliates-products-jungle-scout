package heliumjs

import (
	"context"
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"syscall"
	"time"
)

// dbRetryDelays are the waits before each retry of a database call that lost
// its connection. The staging database drops outside connections now and then;
// a write is idempotent, so retrying it is safe and cheaper than stopping a job.
var dbRetryDelays = []time.Duration{2 * time.Second, 5 * time.Second, 10 * time.Second}

// withDBRetry runs fn, retrying it while it fails with a connection error.
func withDBRetry(ctx context.Context, what string, fn func() error) error {
	err := fn()
	for _, wait := range dbRetryDelays {
		if err == nil || !isConnectionError(err) || ctx.Err() != nil {
			return err
		}
		log.Printf("[HELIUM_JS] %s lost its database connection, retrying in %s: %v", what, wait, err)
		select {
		case <-time.After(wait):
		case <-ctx.Done():
			return err
		}
		err = fn()
	}
	return err
}

func isConnectionError(err error) bool {
	if errors.Is(err, driver.ErrBadConn) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, syscall.ECONNRESET) || errors.Is(err, syscall.EPIPE) || errors.Is(err, syscall.ENETUNREACH) {
		return true
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return true
	}
	msg := err.Error()
	for _, s := range []string{"connection reset", "broken pipe", "connection refused", "network is unreachable",
		"i/o timeout", "connection timed out", "bad connection", "EOF"} {
		if strings.Contains(msg, s) {
			return true
		}
	}
	return false
}
