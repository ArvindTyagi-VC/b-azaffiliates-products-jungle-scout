package main

import (
	"testing"

	"azaffiliates/internal/api/handlers"
)

// The old rule was "fail only if every ASIN failed", so a run that lost half the
// catalogue exited 0 and showed as a success in Cloud Run. On a ten-day cadence
// that loss goes unnoticed for a cycle, which is what these cases guard against.

func TestExitCodeForHealthyRun(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   77000,
		SuccessfulProductSync: 77000,
		SuccessfulSalesSync:   76900,
		FailedASINs:           0,
	}

	if got := exitCodeFor(status); got != 0 {
		t.Errorf("exitCodeFor(healthy) = %d, want 0", got)
	}
}

func TestExitCodeForHalfFailedRun(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   77000,
		SuccessfulProductSync: 38500,
		FailedASINs:           38500,
	}

	if got := exitCodeFor(status); got != 1 {
		t.Errorf("exitCodeFor(half failed) = %d, want 1 — a half-failed run must not report success", got)
	}
}

func TestExitCodeForFailuresWithinThreshold(t *testing.T) {
	// 1% failures: real, logged, but not worth failing the whole run over.
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   77000,
		SuccessfulProductSync: 76230,
		FailedASINs:           770,
	}

	if got := exitCodeFor(status); got != 0 {
		t.Errorf("exitCodeFor(1%% failures) = %d, want 0", got)
	}
}

func TestExitCodeForTruncatedRun(t *testing.T) {
	// Selection hit SYNC_ASIN_LIMIT. Every ASIN it did process succeeded, so the
	// failure rate looks perfect — but an unknown number were never queued.
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   85000,
		SuccessfulProductSync: 85000,
		FailedASINs:           0,
		LimitReached:          true,
	}

	if got := exitCodeFor(status); got != 1 {
		t.Errorf("exitCodeFor(truncated) = %d, want 1 — a truncated run is not a full sync", got)
	}
}

// A run throttled by SYNC_MAX_PER_RUN is partial on purpose: the operator asked
// for a smaller run and the rest stays queued. It must NOT be reported as a
// failure, otherwise every deliberately-throttled run cries wolf and the real
// truncation alarm stops meaning anything.
func TestExitCodeForThrottledRunIsHealthy(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   500,
		SuccessfulProductSync: 500,
		SuccessfulSalesSync:   500,
		FailedASINs:           0,
		ThrottledPerRun:       true,
		LimitReached:          false,
	}

	if got := exitCodeFor(status); got != 0 {
		t.Errorf("exitCodeFor(throttled) = %d, want 0 — a deliberate partial run is not a failure", got)
	}
}

// The guard is still an incident even when a throttle is also configured.
func TestExitCodeForGuardBreachStillFails(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   150000,
		SuccessfulProductSync: 150000,
		FailedASINs:           0,
		ThrottledPerRun:       false,
		LimitReached:          true,
	}

	if got := exitCodeFor(status); got != 1 {
		t.Errorf("exitCodeFor(guard breach) = %d, want 1", got)
	}
}

func TestExitCodeForStoppedEarly(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   40000,
		SuccessfulProductSync: 40000,
		FailedASINs:           0,
		StoppedEarly:          true,
		StopReason:            "25 consecutive database failures",
	}

	if got := exitCodeFor(status); got != 1 {
		t.Errorf("exitCodeFor(stopped early) = %d, want 1", got)
	}
}

func TestExitCodeForNothingToDo(t *testing.T) {
	// Every ASIN was already fresh. Legitimate, not a failure — and it must not
	// divide by zero computing the failure rate.
	status := handlers.HourlySyncStatus{}

	if got := exitCodeFor(status); got != 0 {
		t.Errorf("exitCodeFor(no work) = %d, want 0", got)
	}
}

// A run recovered by the retry pass must count as healthy: markASINResolved clears
// the failure, so FailedASINs already excludes what was recovered.
func TestExitCodeForRunRecoveredByRetryPass(t *testing.T) {
	status := handlers.HourlySyncStatus{
		TotalASINsProcessed:   77000,
		SuccessfulProductSync: 77000,
		FailedASINs:           0,
		RetriedASINs:          9000,
		RecoveredASINs:        9000,
	}

	if got := exitCodeFor(status); got != 0 {
		t.Errorf("exitCodeFor(fully recovered) = %d, want 0", got)
	}
}
