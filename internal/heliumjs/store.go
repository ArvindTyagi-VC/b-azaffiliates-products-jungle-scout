package heliumjs

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"azaffiliates/internal/database"
	"azaffiliates/internal/jsstore"

	"github.com/lib/pq"
)

// store persists jobs and their per-ASIN results in the staging database.
type store struct {
	pg *database.PostgreSQLClient
}

func newStore(pg *database.PostgreSQLClient) *store {
	return &store{pg: pg}
}

func (s *store) jobs() string    { return s.pg.TableName("helium_js_job") }
func (s *store) results() string { return s.pg.TableName("helium_js_asin") }

// checkEnvironment refuses any database but staging, and a missing migration.
func (s *store) checkEnvironment(ctx context.Context) error {
	var db string
	if err := s.pg.DB.QueryRowContext(ctx, `SELECT current_database()`).Scan(&db); err != nil {
		return fmt.Errorf("read current_database: %w", err)
	}
	if db != ExpectedDatabase {
		return fmt.Errorf("%w: connected to %q, jobs only run on %q", ErrPrecondition, db, ExpectedDatabase)
	}
	for _, t := range []string{s.jobs(), s.results(), s.pg.TableName(jsstore.HeliumWrittenTable)} {
		var found bool
		if err := s.pg.DB.QueryRowContext(ctx, `SELECT to_regclass($1) IS NOT NULL`, t).Scan(&found); err != nil {
			return fmt.Errorf("check table %s: %w", t, err)
		}
		if !found {
			return fmt.Errorf("%w: table %s is missing, run sql/create-helium-js-tables.sql", ErrPrecondition, t)
		}
	}
	return nil
}

// create inserts a running job. The caller holds the job lock, so any job
// still marked running belongs to a process that died mid-run.
func (s *store) create(ctx context.Context, source string, sourceRunID *int64, counts JobCounts) (int64, error) {
	if _, err := s.pg.DB.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET status = 'interrupted', finished_at = NOW() WHERE status = 'running'`, s.jobs())); err != nil {
		return 0, fmt.Errorf("mark interrupted jobs: %w", err)
	}
	b, err := json.Marshal(counts)
	if err != nil {
		return 0, err
	}
	var id int64
	err = s.pg.DB.QueryRowContext(ctx, fmt.Sprintf(
		`INSERT INTO %s (source, source_run_id, status, phase, counts) VALUES ($1, $2, 'running', 'starting', $3)
		 RETURNING job_id`, s.jobs()), source, sourceRunID, string(b)).Scan(&id)
	if err != nil {
		return 0, fmt.Errorf("insert job: %w", err)
	}
	return id, nil
}

// progress stores the phase and counts of a running job. Failures are logged:
// a lost progress marker must not fail the job.
func (s *store) progress(ctx context.Context, jobID int64, phase string, counts JobCounts) {
	b, _ := json.Marshal(counts)
	if _, err := s.pg.DB.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET phase = $2, counts = $3, heartbeat_at = NOW() WHERE job_id = $1`, s.jobs()),
		jobID, phase, string(b)); err != nil {
		log.Printf("[HELIUM_JS] job=%d progress: %v", jobID, err)
	}
}

// finish records the outcome on its own context, since the job's may already
// be past its deadline.
func (s *store) finish(jobID int64, status string, counts JobCounts, jobErr error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	b, _ := json.Marshal(counts)
	var errText interface{}
	if jobErr != nil {
		errText = jobErr.Error()
		log.Printf("[HELIUM_JS] job=%d %s: %v", jobID, status, jobErr)
	} else {
		log.Printf("[HELIUM_JS] job=%d %s", jobID, status)
	}
	if _, err := s.pg.DB.ExecContext(ctx, fmt.Sprintf(
		`UPDATE %s SET status = $2, phase = 'done', counts = $3, error = $4, finished_at = NOW(), heartbeat_at = NOW()
		  WHERE job_id = $1`, s.jobs()), jobID, status, string(b), errText); err != nil {
		log.Printf("[HELIUM_JS] job=%d record outcome %s: %v", jobID, status, err)
	}
}

// record upserts per-ASIN results. An empty result leaves the stored one alone,
// so the product and sales phases can write their halves separately.
func (s *store) record(ctx context.Context, jobID int64, rows []ASINResult) error {
	if len(rows) == 0 {
		return nil
	}
	asins := make([]string, len(rows))
	products := make([]string, len(rows))
	sales := make([]string, len(rows))
	days := make([]int64, len(rows))
	for i, r := range rows {
		asins[i], products[i], sales[i], days[i] = r.ASIN, r.Product, r.Sales, int64(r.SalesDays)
	}
	_, err := s.pg.DB.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %[1]s AS t (job_id, asin, product_result, sales_result, sales_days)
		SELECT $1, u.asin, NULLIF(u.product, ''), NULLIF(u.sales, ''), NULLIF(u.days, 0)
		  FROM unnest($2::text[], $3::text[], $4::text[], $5::int[]) AS u(asin, product, sales, days)
		ON CONFLICT (job_id, asin) DO UPDATE SET
			product_result = COALESCE(EXCLUDED.product_result, t.product_result),
			sales_result   = COALESCE(EXCLUDED.sales_result, t.sales_result),
			sales_days     = COALESCE(EXCLUDED.sales_days, t.sales_days),
			updated_at     = NOW()`, s.results()),
		jobID, pq.Array(asins), pq.Array(products), pq.Array(sales), pq.Array(days))
	return err
}

func (s *store) get(ctx context.Context, jobID int64) (*Job, error) {
	jobs, err := s.query(ctx, `WHERE job_id = $1`, jobID)
	if err != nil {
		return nil, err
	}
	if len(jobs) == 0 {
		return nil, sql.ErrNoRows
	}
	return &jobs[0], nil
}

func (s *store) list(ctx context.Context, limit int) ([]Job, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}
	return s.query(ctx, `ORDER BY job_id DESC LIMIT $1`, limit)
}

func (s *store) query(ctx context.Context, tail string, args ...interface{}) ([]Job, error) {
	rows, err := s.pg.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT job_id, source, source_run_id, status, COALESCE(phase, ''), counts, COALESCE(error, ''),
		       started_at, heartbeat_at, finished_at
		  FROM %s %s`, s.jobs(), tail), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Job
	for rows.Next() {
		var j Job
		var runID sql.NullInt64
		var counts []byte
		var finished sql.NullTime
		if err := rows.Scan(&j.JobID, &j.Source, &runID, &j.Status, &j.Phase, &counts, &j.Error,
			&j.StartedAt, &j.HeartbeatAt, &finished); err != nil {
			return nil, err
		}
		if runID.Valid {
			j.SourceRunID = &runID.Int64
		}
		if len(counts) > 0 {
			j.Counts = counts
		}
		if finished.Valid {
			j.FinishedAt = &finished.Time
		}
		out = append(out, j)
	}
	return out, rows.Err()
}

// resultsFor lists a job's per-ASIN results. api is "product" or "sales";
// result filters on that API's result, "all" keeps every ASIN.
func (s *store) resultsFor(ctx context.Context, jobID int64, api, result string) ([]ASINResult, error) {
	column := "product_result"
	if api == "sales" {
		column = "sales_result"
	}
	where, args := "TRUE", []interface{}{jobID}
	if result != "all" {
		where, args = column+" = $2", append(args, result)
	}
	rows, err := s.pg.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT asin, COALESCE(product_result, ''), COALESCE(sales_result, ''), COALESCE(sales_days, 0)
		  FROM %s WHERE job_id = $1 AND %s ORDER BY asin`, s.results(), where), args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []ASINResult
	for rows.Next() {
		var r ASINResult
		if err := rows.Scan(&r.ASIN, &r.Product, &r.Sales, &r.SalesDays); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// heliumRunASINs lists the ASINs of Helium10 P1/P2/P3 run runID that a job of
// source should fetch, skipping ASINs an earlier job of the same source for
// that run already settled for both APIs, so a rerun resumes:
//
//	helium_found    ASINs the run found
//	helium_missing  ASINs the run missed that have Helium10 data fetched since
//	                the run started (typically added by Sales)
func (s *store) heliumRunASINs(ctx context.Context, runID int64, source string) ([]string, error) {
	result, heliumDataClause := "found", "TRUE"
	switch source {
	case SourceHeliumFound:
	case SourceHeliumMissing:
		result = "missing"
		// Two EXISTS on plain equality rather than one OR over upper(), so each
		// can use its index on the research table.
		heliumDataClause = fmt.Sprintf(`(EXISTS (SELECT 1 FROM %[1]s h
		                 WHERE h.requested_asin = r.asin AND h.marketplace = 'US'
		                   AND h.fetch_date >= (SELECT started_at::date FROM %[2]s WHERE run_id = $1))
		      OR EXISTS (SELECT 1 FROM %[1]s h
		                 WHERE h.asin = r.asin AND h.marketplace = 'US'
		                   AND h.fetch_date >= (SELECT started_at::date FROM %[2]s WHERE run_id = $1)))`,
			s.pg.TableName("helium_product_research"), s.pg.TableName("helium_priority_run"))
	default:
		return nil, fmt.Errorf("%w: source %q does not come from a Helium run", ErrInvalidInput, source)
	}

	var kind string
	err := s.pg.DB.QueryRowContext(ctx, fmt.Sprintf(`SELECT kind FROM %s WHERE run_id = $1`,
		s.pg.TableName("helium_priority_run")), runID).Scan(&kind)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: Helium run %d does not exist", ErrInvalidInput, runID)
	}
	if err != nil {
		return nil, err
	}
	if kind != "priority" {
		return nil, fmt.Errorf("%w: Helium run %d is a %s run, not a P1/P2/P3 run", ErrInvalidInput, runID, kind)
	}

	rows, err := s.pg.DB.QueryContext(ctx, fmt.Sprintf(`
		SELECT r.asin
		  FROM %[1]s r
		 WHERE r.run_id = $1 AND r.result = $3
		   AND %[4]s
		   AND NOT EXISTS (SELECT 1 FROM %[2]s d JOIN %[3]s j ON j.job_id = d.job_id
		                    WHERE d.asin = r.asin AND j.source = $2 AND j.source_run_id = $1
		                      AND d.product_result IN ('found', 'missing')
		                      AND d.sales_result IN ('found', 'missing'))
		 ORDER BY r.priority NULLS LAST, r.asin`,
		s.pg.TableName("helium_run_asin"), s.results(), s.jobs(), heliumDataClause),
		runID, source, result)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}
