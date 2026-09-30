// Package heliumjs fetches Jungle Scout product and sales-estimate data for
// ASINs that come out of the Helium10 P1/P2/P3 sync, and records the outcome
// per ASIN. It writes the staging database only.
package heliumjs

import (
	"encoding/json"
	"errors"
	"time"
)

// Job sources.
const (
	SourceHeliumFound   = "helium_found"   // ASINs Helium10 found in a P1/P2/P3 run
	SourceHeliumMissing = "helium_missing" // ASINs Helium10 missed, once their Helium data exists
	SourceManual        = "manual"
)

// Per-ASIN results.
const (
	ResultFound   = "found"
	ResultMissing = "missing"
	ResultFailed  = "failed"
)

// ExpectedDatabase is the only database a job will write to.
const ExpectedDatabase = "vx-3-staging"

var (
	ErrJobInProgress = errors.New("heliumjs: a Jungle Scout job is already running")
	ErrPrecondition  = errors.New("heliumjs: precondition failed")
	ErrInvalidInput  = errors.New("heliumjs: invalid input")
	ErrNothingToDo   = errors.New("heliumjs: nothing to fetch")
)

// APICounts counts one Jungle Scout API within a job.
type APICounts struct {
	Requested int `json:"requested"`
	Found     int `json:"found"`
	Missing   int `json:"missing"`
	Failed    int `json:"failed"`
	NotSent   int `json:"not_sent"`
	Calls     int `json:"calls"`
	Retries   int `json:"retries"`
}

// JobCounts is the progress of a job, stored on its row as it runs.
type JobCounts struct {
	ASINs     int       `json:"asins"`
	Product   APICounts `json:"product"`
	Sales     APICounts `json:"sales"`
	SalesDays int       `json:"sales_days"`
	StartDate string    `json:"sales_start_date"`
	EndDate   string    `json:"sales_end_date"`
}

// Job is one row of helium_js_job.
type Job struct {
	JobID       int64           `json:"job_id"`
	Source      string          `json:"source"`
	SourceRunID *int64          `json:"source_run_id,omitempty"`
	Status      string          `json:"status"`
	Phase       string          `json:"phase,omitempty"`
	Counts      json.RawMessage `json:"counts,omitempty"`
	Error       string          `json:"error,omitempty"`
	StartedAt   time.Time       `json:"started_at"`
	HeartbeatAt time.Time       `json:"heartbeat_at"`
	FinishedAt  *time.Time      `json:"finished_at,omitempty"`
}

// ASINResult is what one job did for one ASIN. An empty result means that API
// was not called for the ASIN because the job stopped first.
type ASINResult struct {
	ASIN      string `json:"asin"`
	Product   string `json:"product"`
	Sales     string `json:"sales"`
	SalesDays int    `json:"sales_days"`
}
