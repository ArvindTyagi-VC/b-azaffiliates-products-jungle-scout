package heliumjs

import (
	"context"
	"errors"
	"fmt"
	"syscall"
	"testing"
	"time"
)

func TestWithDBRetry(t *testing.T) {
	saved := dbRetryDelays
	dbRetryDelays = []time.Duration{0, 0, 0}
	defer func() { dbRetryDelays = saved }()

	reset := fmt.Errorf("read tcp 1.2.3.4:5->6.7.8.9:5432: %w", syscall.ECONNRESET)
	cases := []struct {
		name      string
		errs      []error
		wantCalls int
		wantErr   bool
	}{
		{"ok", []error{nil}, 1, false},
		{"reset then ok", []error{reset, nil}, 2, false},
		{"always reset", []error{reset, reset, reset, reset}, 4, true},
		{"sql error is not retried", []error{errors.New(`pq: duplicate key value`)}, 1, true},
	}
	for _, tc := range cases {
		calls := 0
		err := withDBRetry(context.Background(), "test", func() error {
			e := tc.errs[calls]
			calls++
			return e
		})
		if calls != tc.wantCalls || (err != nil) != tc.wantErr {
			t.Errorf("%s: calls=%d err=%v", tc.name, calls, err)
		}
	}
}

func TestIsConnectionError(t *testing.T) {
	for _, msg := range []string{"read: connection reset by peer", "dial tcp: connect: network is unreachable", "read: connection timed out", "driver: bad connection"} {
		if !isConnectionError(errors.New(msg)) {
			t.Errorf("%q not treated as a connection error", msg)
		}
	}
	if isConnectionError(errors.New("pq: column \"x\" does not exist")) {
		t.Error("an SQL error was treated as a connection error")
	}
}
