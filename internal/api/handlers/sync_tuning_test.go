package handlers

import (
	"errors"
	"testing"
)

func TestEnvIntFallsBackOnBadValues(t *testing.T) {
	t.Setenv("TEST_SYNC_INT", "42")
	if got := envInt("TEST_SYNC_INT", 7); got != 42 {
		t.Errorf("envInt = %d, want 42", got)
	}

	t.Setenv("TEST_SYNC_INT", "  85000  ")
	if got := envInt("TEST_SYNC_INT", 7); got != 85000 {
		t.Errorf("envInt with padding = %d, want 85000", got)
	}

	// A typo in one tuning knob must not stop the sync: fall back, do not panic.
	t.Setenv("TEST_SYNC_INT", "ten")
	if got := envInt("TEST_SYNC_INT", 7); got != 7 {
		t.Errorf("envInt with garbage = %d, want the default 7", got)
	}

	t.Setenv("TEST_SYNC_INT", "")
	if got := envInt("TEST_SYNC_INT", 7); got != 7 {
		t.Errorf("envInt with empty value = %d, want the default 7", got)
	}
}

func TestEnvFloatFallsBackOnBadValues(t *testing.T) {
	t.Setenv("TEST_SYNC_FLOAT", "0.25")
	if got := envFloat("TEST_SYNC_FLOAT", 0.1); got != 0.25 {
		t.Errorf("envFloat = %v, want 0.25", got)
	}

	t.Setenv("TEST_SYNC_FLOAT", "lots")
	if got := envFloat("TEST_SYNC_FLOAT", 0.1); got != 0.1 {
		t.Errorf("envFloat with garbage = %v, want the default 0.1", got)
	}
}

// isTransientAPIError decides whether an ASIN gets another attempt. A false
// negative strands data for a full cycle; a false positive burns quota retrying
// something JungleScout already answered definitively.
func TestIsTransientAPIErrorRetriesTemporaryFailures(t *testing.T) {
	transient := []string{
		"max retries exceeded, last error: rate limited (429) after 3 attempt(s)",
		"429 Too Many Requests",
		"Get \"https://developer.junglescout.com\": net/http: TLS handshake timeout",
		"read tcp 10.0.0.1:443: connection reset by peer",
		"dial tcp: lookup developer.junglescout.com: no such host",
		"unexpected EOF",
		"503 Service Unavailable",
	}

	for _, msg := range transient {
		if !isTransientAPIError(errors.New(msg)) {
			t.Errorf("isTransientAPIError(%q) = false, want true", msg)
		}
	}
}

func TestIsTransientAPIErrorLeavesSettledAnswersAlone(t *testing.T) {
	// A 422 MISSING_RANK_DATA is a normal JungleScout answer for an ASIN with no
	// rank data, not a temporary fault. Retrying it cannot change the outcome.
	settled := []string{
		"API error 422: MISSING_RANK_DATA for ASIN B00TEST0001",
		"unexpected status 422",
		"400 Bad Request: Authorization header is not in a valid format",
		"401 Unauthorized",
	}

	for _, msg := range settled {
		if isTransientAPIError(errors.New(msg)) {
			t.Errorf("isTransientAPIError(%q) = true, want false", msg)
		}
	}

	if isTransientAPIError(nil) {
		t.Error("isTransientAPIError(nil) = true, want false")
	}
}

// newTestManager builds the minimum needed to exercise the accounting helpers.
func newTestManager() *HourlySyncManager {
	return &HourlySyncManager{status: &HourlySyncStatus{}}
}

// The retry pass re-runs the same sync path as the main pass, so plain counters
// would count an ASIN twice — once when it failed and again when it was retried.
// The failure-rate gate divides one counter by the other, so double counting
// directly distorts whether the job reports success.
func TestASINAccountingCountsDistinctASINs(t *testing.T) {
	m := newTestManager()

	m.markASINAttempted("A", "B", "C")
	m.markASINAttempted("A", "B") // retry pass revisits two of them

	if got := m.status.TotalASINsProcessed; got != 3 {
		t.Errorf("TotalASINsProcessed = %d, want 3 distinct ASINs", got)
	}
}

func TestASINAccountingFailureIsIdempotent(t *testing.T) {
	m := newTestManager()

	m.markASINFailed("A")
	m.markASINFailed("A")
	m.markASINFailed("B")

	if got := m.status.FailedASINs; got != 2 {
		t.Errorf("FailedASINs = %d, want 2", got)
	}
}

func TestASINAccountingRecoveryClearsFailure(t *testing.T) {
	m := newTestManager()

	m.markASINAttempted("A", "B")
	m.markASINFailed("A", "B")
	if got := m.status.FailedASINs; got != 2 {
		t.Fatalf("FailedASINs before retry = %d, want 2", got)
	}

	// The retry pass succeeds for one of them.
	m.markASINResolved("A")

	if got := m.status.FailedASINs; got != 1 {
		t.Errorf("FailedASINs after recovery = %d, want 1", got)
	}
	if got := m.status.TotalASINsProcessed; got != 2 {
		t.Errorf("TotalASINsProcessed = %d, want 2", got)
	}
}

func TestASINAccountingResolveIsSafeWhenNothingFailed(t *testing.T) {
	m := newTestManager()

	m.markASINResolved("never-seen") // must not panic on a nil failedSet

	if got := m.status.FailedASINs; got != 0 {
		t.Errorf("FailedASINs = %d, want 0", got)
	}
}

func TestProductAndSalesStoredCountDistinct(t *testing.T) {
	m := newTestManager()

	m.markProductStored("A", "B")
	m.markProductStored("A") // retry re-runs the batched product call
	if got := m.status.SuccessfulProductSync; got != 2 {
		t.Errorf("SuccessfulProductSync = %d, want 2", got)
	}

	if fresh := m.markSalesStored("A"); !fresh {
		t.Error("markSalesStored first call = false, want true")
	}
	if fresh := m.markSalesStored("A"); fresh {
		t.Error("markSalesStored repeat call = true, want false")
	}
	if got := m.status.SuccessfulSalesSync; got != 1 {
		t.Errorf("SuccessfulSalesSync = %d, want 1", got)
	}
}

// A single failed write used to stop the whole run. On a full-set run that throws
// away every ASIN not yet reached, and the next attempt is a cycle away.
func TestNoteDBFailureToleratesIsolatedFailures(t *testing.T) {
	m := newTestManager()
	err := errors.New("connection reset")

	for i := 1; i < MaxConsecutiveDBFailures; i++ {
		if stop := m.noteDBFailure("test insert", err); stop {
			t.Fatalf("run abandoned after only %d consecutive failures", i)
		}
		if m.stopRequested {
			t.Fatalf("stopRequested set after only %d consecutive failures", i)
		}
	}

	if stop := m.noteDBFailure("test insert", err); !stop {
		t.Errorf("run not abandoned after %d consecutive failures", MaxConsecutiveDBFailures)
	}
	if !m.stopRequested {
		t.Error("stopRequested = false after a sustained failure streak")
	}
}

func TestNoteDBSuccessResetsTheStreak(t *testing.T) {
	m := newTestManager()
	err := errors.New("connection reset")

	for i := 0; i < MaxConsecutiveDBFailures-1; i++ {
		m.noteDBFailure("test insert", err)
	}

	m.noteDBSuccess()

	// After a success the allowance starts over, so one more failure is harmless.
	if stop := m.noteDBFailure("test insert", err); stop {
		t.Error("run abandoned immediately after a successful write reset the streak")
	}
	if m.stopRequested {
		t.Error("stopRequested set after the streak was reset")
	}
}
