// Package promote copies the rows a sync run wrote in the source database
// (staging) into the same tables in the target database (production).

package promote

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"azaffiliates/internal/database"

	"github.com/lib/pq"
)


const maxBoundParams = 60000


const stateTable = "js_promote_state"


type Config struct {

	Enabled bool
	DryRun bool
	BatchRows int
	MaxRowsPerRun int
	ConflictMode string
	Overlap time.Duration
	Tables []string
}

// LoadConfig reads the publish configuration from the environment.
func LoadConfig() Config {
	cfg := Config{
		Enabled:       os.Getenv("PROMOTE_ENABLED") == "true",
		DryRun:        os.Getenv("PROMOTE_DRY_RUN") == "true",
		BatchRows:     envInt("PROMOTE_BATCH_ROWS", 500),
		MaxRowsPerRun: envInt("PROMOTE_MAX_ROWS_PER_RUN", 0),
		ConflictMode:  strings.ToLower(envStr("PROMOTE_CONFLICT", "update")),
		Overlap:       time.Duration(envInt("PROMOTE_OVERLAP_MINUTES", 5)) * time.Minute,
	}
	if raw := os.Getenv("PROMOTE_TABLES"); raw != "" {
		for _, name := range strings.Split(raw, ",") {
			if name = strings.TrimSpace(name); name != "" {
				cfg.Tables = append(cfg.Tables, name)
			}
		}
	}
	if cfg.BatchRows <= 0 {
		cfg.BatchRows = 500
	}
	if cfg.ConflictMode != "nothing" {
		cfg.ConflictMode = "update"
	}
	return cfg
}

// Log writes the resolved configuration. A publish is rare and unattended, so
// the run's own log has to be enough to reconstruct what it was told to do.
func (c Config) Log() {
	log.Printf("[PROMOTE][CONFIG] enabled=%v dry_run=%v conflict=%s batch_rows=%d max_rows_per_run=%d overlap=%s tables=%v",
		c.Enabled, c.DryRun, c.ConflictMode, c.BatchRows, c.MaxRowsPerRun, c.Overlap, c.Tables)
}

func (c Config) allows(name string) bool {
	if len(c.Tables) == 0 {
		return true
	}
	for _, allowed := range c.Tables {
		if allowed == name {
			return true
		}
	}
	return false
}

// TableSpec describes one table's publish rules.
type TableSpec struct {
	Name string
	ConflictCols []string
	ExcludeCols []string
	WatermarkExpr string
	NaiveClock bool
	TiebreakCols []string
	// FreshnessCol arms the freshness guard: on a key collision the incoming row
	// is written only when its value in this column is at least the target's. A
	// target row whose value is newer keeps its data untouched. Leave empty to
	// let the publish overwrite unconditionally.
	FreshnessCol string
}

// Specs is the set of tables the publish covers.
var Specs = []TableSpec{
	{
		Name:          "jungle_scout_product_data",
		ConflictCols:  []string{"asin", "report_date"},
		WatermarkExpr: "GREATEST(COALESCE(updated_at, created_at), COALESCE(created_at, updated_at))",
		TiebreakCols:  []string{"asin", "report_date"},
	},
	{
		Name:         "jungle_scout_sales_estimate_data",
		ConflictCols: []string{"asin", "marketplace", "date"},
		// id is SERIAL and means nothing across databases; the target owns its
		// own sequence. Copying it would collide with rows the target generated.
		ExcludeCols:   []string{"id"},
		WatermarkExpr: "GREATEST(COALESCE(updated_at, created_at), COALESCE(created_at, updated_at))",
		NaiveClock:    true,
		TiebreakCols:  []string{"id"},
		// The target is not the only writer of this table. The product page's
		// "Load graph" button re-pulls a stale ASIN straight from Jungle Scout
		// and rewrites the series in place, so a production row can be newer
		// than the staging row this publish is carrying. Guard on created_at —
		// the column that read path stamps and then reads back for its own
		// freshness window — so a publish never walks a fresher series back.
		FreshnessCol: "created_at",
	},
}

// TableReport is the outcome for one table.
type TableReport struct {
	Table       string
	RowsRead    int
	RowsSent    int
	RowsGuarded int // of RowsSent, the rows the target kept because its own copy was newer
	Duration    time.Duration
	Floor       time.Time
	Watermark   time.Time
	Throttled   bool
	Err         error
}

// Report is the outcome of a whole publish.
type Report struct {
	Tables   []TableReport
	Duration time.Duration
	Skipped  string // non-empty when the phase did not run at all
}

// RowsSent totals the rows written across every table.
func (r Report) RowsSent() int {
	total := 0
	for _, t := range r.Tables {
		total += t.RowsSent
	}
	return total
}

// Err returns the first table error, or nil when every table succeeded.
func (r Report) Err() error {
	for _, t := range r.Tables {
		if t.Err != nil {
			return fmt.Errorf("table %s: %w", t.Table, t.Err)
		}
	}
	return nil
}

// Migrator publishes from src to dst.
type Migrator struct {
	src *database.PostgreSQLClient
	dst *database.PostgreSQLClient
	cfg Config
}

// New builds a Migrator. Both clients must be non-nil; callers decide whether a
// publish is possible before constructing one.
func New(src, dst *database.PostgreSQLClient, cfg Config) *Migrator {
	return &Migrator{src: src, dst: dst, cfg: cfg}
}

// Floor is the source database's own clock, read at run start, in both of the
// conventions this schema uses. Taking it from the source (rather than the Go
// process) removes client clock skew from the comparison.
type Floor struct {
	TZ    time.Time
	Naive time.Time
}

// For returns the floor matching a table's timestamp convention.
func (f Floor) For(spec TableSpec) time.Time {
	if spec.NaiveClock {
		return f.Naive
	}
	return f.TZ
}

// CaptureFloor reads the source clock. Call it BEFORE the sync starts writing:
// it becomes the lower bound for the first publish, the one that decides "rows
// this run wrote" rather than "every row that exists".
func CaptureFloor(ctx context.Context, src *database.PostgreSQLClient) (Floor, error) {
	var f Floor
	err := src.DB.QueryRowContext(ctx, "SELECT now(), LOCALTIMESTAMP").Scan(&f.TZ, &f.Naive)
	if err != nil {
		return Floor{}, fmt.Errorf("failed to read the source clock: %w", err)
	}
	log.Printf("[PROMOTE] Run floor captured from the source clock: tz=%s naive=%s",
		f.TZ.Format(time.RFC3339Nano), f.Naive.Format("2006-01-02 15:04:05.999999"))
	return f, nil
}

// Run publishes every eligible table.
func (m *Migrator) Run(ctx context.Context, floor Floor) Report {
	started := time.Now()
	report := Report{}

	log.Println("[PROMOTE] ========== PUBLISH TO TARGET: START ==========")
	log.Printf("[PROMOTE] Source: %s prefix=%q", describe(m.src), m.src.GetTablePrefix())
	log.Printf("[PROMOTE] Target: %s prefix=%q", describe(m.dst), m.dst.GetTablePrefix())
	if m.cfg.DryRun {
		log.Println("[PROMOTE] DRY RUN — rows will be selected and counted, nothing will be written")
	}

	if err := m.ensureStateTable(ctx); err != nil {
		log.Printf("[PROMOTE] CRITICAL: %v", err)
		report.Tables = append(report.Tables, TableReport{Table: stateTable, Err: err})
		report.Duration = time.Since(started)
		return report
	}

	budget := m.cfg.MaxRowsPerRun
	for _, spec := range Specs {
		if !m.cfg.allows(spec.Name) {
			log.Printf("[PROMOTE] Skipping %s — not in PROMOTE_TABLES", spec.Name)
			continue
		}

		tr := m.publishTable(ctx, spec, floor, budget)
		report.Tables = append(report.Tables, tr)
		if budget > 0 {
			budget -= tr.RowsSent
			if budget < 0 {
				budget = 0
			}
		}

		if tr.Err != nil {
			log.Printf("[PROMOTE] CRITICAL: %s failed after %d row(s): %v", tr.Table, tr.RowsSent, tr.Err)
			continue
		}
		log.Printf("[PROMOTE] %s: read=%d sent=%d guarded=%d written=%d in %s (throttled=%v)",
			tr.Table, tr.RowsRead, tr.RowsSent, tr.RowsGuarded, tr.RowsSent-tr.RowsGuarded,
			tr.Duration.Round(time.Millisecond), tr.Throttled)
	}

	report.Duration = time.Since(started)
	log.Printf("[PROMOTE] ========== PUBLISH TO TARGET: DONE — %d row(s) in %s ==========",
		report.RowsSent(), report.Duration.Round(time.Second))
	return report
}

// publishTable copies one table's changed rows.
func (m *Migrator) publishTable(ctx context.Context, spec TableSpec, floor Floor, budget int) TableReport {
	started := time.Now()
	tr := TableReport{Table: spec.Name}

	srcTable := m.src.TableName(spec.Name)
	dstTable := m.dst.TableName(spec.Name)

	cols, err := m.sharedColumns(ctx, spec, srcTable, dstTable)
	if err != nil {
		tr.Err = err
		tr.Duration = time.Since(started)
		return tr
	}
	log.Printf("[PROMOTE] %s -> %s: %d shared column(s)", srcTable, dstTable, len(cols))

	// Where to start. A stored bookmark wins — it makes the publish self-healing
	// after a target outage. Without one this is the first publish, and the run
	// floor confines it to what this run wrote instead of the whole table.
	from, err := m.loadBookmark(ctx, spec)
	if err != nil {
		tr.Err = err
		tr.Duration = time.Since(started)
		return tr
	}
	if from.IsZero() {
		from = floor.For(spec)
		// Without a bookmark AND without a floor there is no lower bound, and the
		// selection would be the entire table — millions of rows of history the
		// target already has, upserted over the top of it. Refuse instead.
		if from.IsZero() {
			tr.Err = fmt.Errorf("no publish bookmark and no run floor for %s: refusing to publish the whole table", spec.Name)
			tr.Duration = time.Since(started)
			return tr
		}
		log.Printf("[PROMOTE] %s: no bookmark — first publish, starting at the run floor %s",
			spec.Name, from.Format(time.RFC3339Nano))
	} else {
		from = from.Add(-m.cfg.Overlap)
		log.Printf("[PROMOTE] %s: resuming from bookmark %s (overlap %s applied)",
			spec.Name, from.Format(time.RFC3339Nano), m.cfg.Overlap)
	}
	tr.Floor = from
	tr.Watermark = from

	rowsPerStatement := m.cfg.BatchRows
	if perParams := maxBoundParams / len(cols); perParams < rowsPerStatement {
		rowsPerStatement = perParams
		log.Printf("[PROMOTE] %s: batch reduced to %d row(s)/statement by the bind-parameter ceiling",
			spec.Name, rowsPerStatement)
	}

	guardCol := resolveFreshnessCol(spec, cols, m.cfg.ConflictMode)

	firstPageSQL := buildSelect(srcTable, cols, spec, rowsPerStatement, false)
	resumeSQL := buildSelect(srcTable, cols, spec, rowsPerStatement, true)
	insertSQL := buildInsert(dstTable, cols, spec, m.cfg.ConflictMode, guardCol)

	var cursor []interface{} // last (watermark, tiebreak...) seen; nil on the first page
	for {
		if budget > 0 && tr.RowsSent >= budget {
			tr.Throttled = true
			log.Printf("[PROMOTE] %s: PROMOTE_MAX_ROWS_PER_RUN reached — stopping this table at %d row(s)",
				spec.Name, tr.RowsSent)
			break
		}

		query := firstPageSQL
		args := []interface{}{from}
		if cursor != nil {
			query = resumeSQL
			args = append(args, cursor...)
		}

		values, nextCursor, lastWM, err := m.fetchPage(ctx, query, args, len(cols), len(spec.TiebreakCols))
		if err != nil {
			tr.Err = err
			break
		}
		if len(values) == 0 {
			break
		}

		rows := len(values) / len(cols)
		tr.RowsRead += rows

		if m.cfg.DryRun {
			tr.RowsSent += rows
			log.Printf("[PROMOTE][DRY_RUN] %s: would write %d row(s), watermark now %s",
				spec.Name, rows, lastWM.Format(time.RFC3339Nano))
		} else {
			written, err := m.writePage(ctx, insertSQL, values, rows, len(cols))
			if err != nil {
				tr.Err = fmt.Errorf("failed to write a %d-row page: %w", rows, err)
				break
			}
			tr.RowsSent += rows
			// With the guard armed, every row the statement did not touch is a
			// row the target held on to. Log it: this is the only place a
			// production row winning against the publish becomes visible.
			if guarded := rows - int(written); guarded > 0 && guardCol != "" {
				tr.RowsGuarded += guarded
				log.Printf("[PROMOTE] %s: %d of %d row(s) in this page kept by the target — its %s is newer",
					spec.Name, guarded, rows, guardCol)
			}
		}

		tr.Watermark = lastWM
		cursor = nextCursor

		// Checkpoint after every page. A publish that dies halfway then resumes
		// from where it stopped instead of starting over.
		if !m.cfg.DryRun {
			if err := m.saveBookmark(ctx, spec, lastWM, tr.RowsSent, "running", nil); err != nil {
				log.Printf("[PROMOTE] WARNING: %s: failed to checkpoint the bookmark: %v", spec.Name, err)
			}
		}

		if tr.RowsSent%10000 < rows {
			log.Printf("[PROMOTE] %s: %d row(s) published so far, watermark %s",
				spec.Name, tr.RowsSent, lastWM.Format(time.RFC3339Nano))
		}

		if rows < rowsPerStatement {
			break // short page — the source has nothing newer
		}
	}

	status := "ok"
	if tr.Err != nil {
		status = "failed"
	} else if tr.Throttled {
		status = "throttled"
	}
	if !m.cfg.DryRun {
		if err := m.saveBookmark(ctx, spec, tr.Watermark, tr.RowsSent, status, tr.Err); err != nil {
			log.Printf("[PROMOTE] WARNING: %s: failed to record the final bookmark: %v", spec.Name, err)
		}
	}

	tr.Duration = time.Since(started)
	return tr
}

// fetchPage reads one page and returns the flattened copy values, the cursor for
// the next page, and the highest watermark seen.
//
// The SELECT trails the watermark and the tiebreak columns after the copied
// columns, so the caller gets its next-page cursor from the same read.
func (m *Migrator) fetchPage(ctx context.Context, query string, args []interface{}, nCols, nTiebreak int) ([]interface{}, []interface{}, time.Time, error) {
	rows, err := m.src.DB.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("failed to read a page from the source: %w", err)
	}
	defer rows.Close()

	width := nCols + 1 + nTiebreak
	var (
		values []interface{}
		cursor []interface{}
		lastWM time.Time
	)

	for rows.Next() {
		cells := make([]interface{}, width)
		targets := make([]interface{}, width)
		for i := range cells {
			targets[i] = &cells[i]
		}
		if err := rows.Scan(targets...); err != nil {
			return nil, nil, time.Time{}, fmt.Errorf("failed to scan a source row: %w", err)
		}

		for i := 0; i < nCols; i++ {
			values = append(values, normalise(cells[i]))
		}

		wm, ok := cells[nCols].(time.Time)
		if !ok {
			return nil, nil, time.Time{}, fmt.Errorf("watermark came back as %T, expected a timestamp", cells[nCols])
		}
		lastWM = wm

		cursor = make([]interface{}, 0, 1+nTiebreak)
		cursor = append(cursor, wm)
		for _, cell := range cells[nCols+1:] {
			cursor = append(cursor, normalise(cell))
		}
	}
	if err := rows.Err(); err != nil {
		return nil, nil, time.Time{}, fmt.Errorf("failed while reading a page from the source: %w", err)
	}

	return values, cursor, lastWM, nil
}

// writePage upserts one page into the target inside a single transaction, so a
// failure leaves no partial page behind.
// writePage upserts one page and returns how many rows the target actually
// wrote. Without a freshness guard that is the whole page; with one it is the
// page minus the rows the target kept because its own copy was newer.
func (m *Migrator) writePage(ctx context.Context, insertSQL string, values []interface{}, rows, nCols int) (int64, error) {
	tx, err := m.dst.DB.BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to open a target transaction: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // no-op once Commit succeeds

	res, err := tx.ExecContext(ctx, withValueTuples(insertSQL, rows, nCols), values...)
	if err != nil {
		return 0, fmt.Errorf("upsert into the target failed: %w", err)
	}
	written, err := res.RowsAffected()
	if err != nil {
		// The write itself is fine, only the count is unavailable. Assume the
		// whole page landed rather than reporting a phantom guard hit.
		log.Printf("[PROMOTE] WARNING: could not read the affected-row count, assuming the full page landed: %v", err)
		written = int64(rows)
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit to the target: %w", err)
	}
	return written, nil
}


func (m *Migrator) sharedColumns(ctx context.Context, spec TableSpec, srcTable, dstTable string) ([]string, error) {
	srcCols, err := columnsOf(ctx, m.src, srcTable)
	if err != nil {
		return nil, fmt.Errorf("failed to read source columns: %w", err)
	}
	dstCols, err := columnsOf(ctx, m.dst, dstTable)
	if err != nil {
		return nil, fmt.Errorf("failed to read target columns: %w", err)
	}
	if len(srcCols) == 0 {
		return nil, fmt.Errorf("source table %s does not exist or has no columns", srcTable)
	}
	if len(dstCols) == 0 {
		return nil, fmt.Errorf("target table %s does not exist or has no columns", dstTable)
	}

	inTarget := make(map[string]bool, len(dstCols))
	for _, c := range dstCols {
		inTarget[c] = true
	}
	excluded := make(map[string]bool, len(spec.ExcludeCols))
	for _, c := range spec.ExcludeCols {
		excluded[c] = true
	}

	var shared, missing []string
	for _, c := range srcCols {
		switch {
		case excluded[c]:
			continue
		case !inTarget[c]:
			missing = append(missing, c)
		default:
			shared = append(shared, c)
		}
	}
	if len(missing) > 0 {
		log.Printf("[PROMOTE] WARNING: %s has column(s) the target lacks, they will not be copied: %v",
			srcTable, missing)
	}

	// Every conflict column has to survive the intersection, or the upsert has
	// no key to match on.
	for _, c := range spec.ConflictCols {
		if !contains(shared, c) {
			return nil, fmt.Errorf("conflict column %q is missing from the shared column set", c)
		}
	}
	return shared, nil
}

func columnsOf(ctx context.Context, pg *database.PostgreSQLClient, table string) ([]string, error) {
	rows, err := pg.DB.QueryContext(ctx, `
		SELECT column_name FROM information_schema.columns
		 WHERE table_name = $1 AND table_schema = current_schema()
		 ORDER BY ordinal_position`, table)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var cols []string
	for rows.Next() {
		var c string
		if err := rows.Scan(&c); err != nil {
			return nil, err
		}
		cols = append(cols, c)
	}
	return cols, rows.Err()
}


func (m *Migrator) stateTableName() string { return m.src.TableName(stateTable) }

func (m *Migrator) ensureStateTable(ctx context.Context) error {
	_, err := m.src.DB.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			table_name   TEXT PRIMARY KEY,
			last_wm      TEXT,
			rows_copied  BIGINT DEFAULT 0,
			status       TEXT,
			error        TEXT,
			last_run_at  TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
			updated_at   TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
		)`, pq.QuoteIdentifier(m.stateTableName())))
	if err != nil {
		return fmt.Errorf("failed to create the publish state table %s: %w", m.stateTableName(), err)
	}
	return nil
}

// loadBookmark returns the watermark the last publish reached, or the zero time
// when this table has never been published.
func (m *Migrator) loadBookmark(ctx context.Context, spec TableSpec) (time.Time, error) {
	var raw sql.NullString
	err := m.src.DB.QueryRowContext(ctx,
		fmt.Sprintf("SELECT last_wm FROM %s WHERE table_name = $1", pq.QuoteIdentifier(m.stateTableName())),
		spec.Name).Scan(&raw)
	if err == sql.ErrNoRows {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, fmt.Errorf("failed to read the publish bookmark: %w", err)
	}
	if !raw.Valid || raw.String == "" {
		return time.Time{}, nil
	}

	parsed, err := time.Parse(time.RFC3339Nano, raw.String)
	if err != nil {
		return time.Time{}, fmt.Errorf("publish bookmark %q for %s is unreadable: %w", raw.String, spec.Name, err)
	}
	return parsed, nil
}

func (m *Migrator) saveBookmark(ctx context.Context, spec TableSpec, wm time.Time, rows int, status string, cause error) error {
	var errText interface{}
	if cause != nil {
		errText = cause.Error()
	}
	_, err := m.src.DB.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (table_name, last_wm, rows_copied, status, error, last_run_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
		ON CONFLICT (table_name) DO UPDATE SET
			last_wm     = EXCLUDED.last_wm,
			rows_copied = EXCLUDED.rows_copied,
			status      = EXCLUDED.status,
			error       = EXCLUDED.error,
			updated_at  = CURRENT_TIMESTAMP`, pq.QuoteIdentifier(m.stateTableName())),
		spec.Name, wm.Format(time.RFC3339Nano), rows, status, errText)
	return err
}


func buildSelect(table string, cols []string, spec TableSpec, limit int, withCursor bool) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pq.QuoteIdentifier(c)
	}

	
	trailing := []string{spec.WatermarkExpr}
	for _, c := range spec.TiebreakCols {
		trailing = append(trailing, pq.QuoteIdentifier(c))
	}

	where := fmt.Sprintf("%s >= $1", spec.WatermarkExpr)
	if withCursor {
		slots := make([]string, len(trailing))
		for i := range trailing {
			slots[i] = "$" + strconv.Itoa(i+2)
		}
		where += fmt.Sprintf(" AND (%s) > (%s)", strings.Join(trailing, ", "), strings.Join(slots, ", "))
	}

	return fmt.Sprintf("SELECT %s, %s FROM %s WHERE %s ORDER BY %s LIMIT %d",
		strings.Join(quoted, ", "),
		strings.Join(trailing, ", "),
		pq.QuoteIdentifier(table),
		where,
		strings.Join(trailing, ", "),
		limit,
	)
}

// valuesPlaceholder marks where withValueTuples splices the VALUES tuples in.
const valuesPlaceholder = "__VALUES__"


// resolveFreshnessCol decides whether this table's freshness guard can actually
// run, and says so in the log either way — a guard that is silently off is
// worse than no guard, because the publish looks safe while it overwrites.
//
// The column has to be one of the copied ones. EXCLUDED exposes every column of
// the target table, not just the listed ones, so a column left out of the INSERT
// would come back as its DEFAULT — CURRENT_TIMESTAMP on these tables — and read
// as "always newer", disarming the guard without a single error.
func resolveFreshnessCol(spec TableSpec, cols []string, conflictMode string) string {
	if spec.FreshnessCol == "" {
		return ""
	}
	if conflictMode != "update" {
		log.Printf("[PROMOTE] %s: conflict mode is %q — the %s freshness guard is redundant, existing target rows are never rewritten",
			spec.Name, conflictMode, spec.FreshnessCol)
		return ""
	}
	if !contains(cols, spec.FreshnessCol) {
		log.Printf("[PROMOTE] WARNING: %s: freshness column %q is not in the copied column set — GUARD DISABLED, a newer target row can be overwritten",
			spec.Name, spec.FreshnessCol)
		return ""
	}
	log.Printf("[PROMOTE] %s: freshness guard ARMED on %s — a target row with a newer %s keeps its data",
		spec.Name, spec.FreshnessCol, spec.FreshnessCol)
	return spec.FreshnessCol
}

// targetAlias names the conflicting target row inside ON CONFLICT DO UPDATE, so
// the guard can compare it against EXCLUDED without repeating the full prefixed
// table name.
const targetAlias = "t"

func buildInsert(table string, cols []string, spec TableSpec, conflictMode, freshCol string) string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = pq.QuoteIdentifier(c)
	}
	conflict := make([]string, len(spec.ConflictCols))
	for i, c := range spec.ConflictCols {
		conflict[i] = pq.QuoteIdentifier(c)
	}

	action := "DO NOTHING"
	if conflictMode == "update" {
		
		skip := map[string]bool{"created_at": true}
		for _, c := range spec.ConflictCols {
			skip[c] = true
		}
		var sets []string
		for _, c := range cols {
			if skip[c] {
				continue
			}
			sets = append(sets, fmt.Sprintf("%s = EXCLUDED.%s", pq.QuoteIdentifier(c), pq.QuoteIdentifier(c)))
		}
		if len(sets) > 0 {
			action = "DO UPDATE SET " + strings.Join(sets, ", ")
			if freshCol != "" {
				// Three cases, in order:
				//   target NULL      -> nothing to protect, take the incoming row
				//   incoming NULL    -> cannot prove it is newer, leave the target
				//   both present     -> write only when incoming is at least as new
				// Equal timestamps write: the target is not ahead, so nothing is
				// lost, and it keeps a re-run of the same page idempotent.
				q := pq.QuoteIdentifier(freshCol)
				action += fmt.Sprintf(" WHERE %s.%s IS NULL OR (EXCLUDED.%s IS NOT NULL AND EXCLUDED.%s >= %s.%s)",
					targetAlias, q, q, q, targetAlias, q)
			}
		}
	}

	// The alias is what lets the guard above name the conflicting target row.
	// It is harmless when no guard is attached, so it is always emitted.
	return fmt.Sprintf("INSERT INTO %s AS %s (%s) VALUES %s ON CONFLICT (%s) %s",
		pq.QuoteIdentifier(table), targetAlias, strings.Join(quoted, ", "), valuesPlaceholder,
		strings.Join(conflict, ", "), action)
}


func withValueTuples(insertSQL string, rows, nCols int) string {
	tuples := make([]string, rows)
	n := 1
	for r := 0; r < rows; r++ {
		slots := make([]string, nCols)
		for c := 0; c < nCols; c++ {
			slots[c] = "$" + strconv.Itoa(n)
			n++
		}
		tuples[r] = "(" + strings.Join(slots, ", ") + ")"
	}
	return strings.Replace(insertSQL, valuesPlaceholder, strings.Join(tuples, ", "), 1)
}


func normalise(v interface{}) interface{} {
	if b, ok := v.([]byte); ok {
		return string(b)
	}
	return v
}

func contains(haystack []string, needle string) bool {
	for _, s := range haystack {
		if s == needle {
			return true
		}
	}
	return false
}

func describe(pg *database.PostgreSQLClient) string {
	if pg == nil {
		return "<none>"
	}
	var name string
	if err := pg.DB.QueryRow("SELECT current_database()").Scan(&name); err != nil {
		return "<unreachable>"
	}
	return name
}

func envStr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
		log.Printf("[PROMOTE][CONFIG] %s=%q is not a number — using %d", key, v, fallback)
	}
	return fallback
}
