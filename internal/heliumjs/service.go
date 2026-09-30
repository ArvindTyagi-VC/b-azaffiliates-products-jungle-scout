package heliumjs

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"azaffiliates/internal/database"
	"azaffiliates/internal/jsstore"
	"azaffiliates/internal/junglescout"
)

const (
	marketplace      = "us"
	productBatchSize = 100
	defaultWorkers   = 8
	retryDelay       = 15 * time.Second
	progressEvery    = 200
	jobLockKey       = int64(8231774500)
)

// jsAPI is the slice of the Jungle Scout client a job needs.
type jsAPI interface {
	FetchProductDataAll(asins []string, marketplace string) (*junglescout.ProductAPIResponse, error)
	FetchSalesEstimateData(asin, marketplace, startDate, endDate string) (*junglescout.SalesEstimateAPIResponse, error)
}

// jobStore is the bookkeeping a job needs.
type jobStore interface {
	checkEnvironment(ctx context.Context) error
	create(ctx context.Context, source string, sourceRunID *int64, counts JobCounts) (int64, error)
	progress(ctx context.Context, jobID int64, phase string, counts JobCounts)
	finish(jobID int64, status string, counts JobCounts, jobErr error)
	record(ctx context.Context, jobID int64, rows []ASINResult) error
}

// dataWriter stores fetched Jungle Scout data.
type dataWriter interface {
	products(ctx context.Context, products []junglescout.ProductData, reportDate string) error
	sales(ctx context.Context, series junglescout.SalesEstimateAttributes) error
}

// locker makes sure only one job runs at a time.
type locker interface {
	tryLock(ctx context.Context) (release func(), ok bool, err error)
}

// Service runs Jungle Scout jobs and serves their results.
type Service struct {
	api        jsAPI
	jobs       jobStore
	data       dataWriter
	lock       locker
	reads      *store
	workers    int
	retryDelay time.Duration
	now        func() time.Time
}

// NewService wires jobs to the staging database and the Jungle Scout client.
// workers is the number of sales-estimate calls made in parallel.
func NewService(staging *database.PostgreSQLClient, client *junglescout.Client, workers int) *Service {
	if workers <= 0 {
		workers = defaultWorkers
	}
	st := newStore(staging)
	var api jsAPI
	if client != nil {
		api = client
	}
	return &Service{
		api:        api,
		jobs:       st,
		data:       stagingWriter{pg: staging},
		lock:       advisoryLocker{pg: staging},
		reads:      st,
		workers:    workers,
		retryDelay: retryDelay,
		now:        time.Now,
	}
}

// Start registers a job for the given ASINs and runs it in the background.
// done closes when the job has finished.
func (s *Service) Start(asins []string, source string, sourceRunID *int64) (int64, <-chan struct{}, error) {
	if s.api == nil {
		return 0, nil, fmt.Errorf("%w: JUNGLE_SCOUT_API_KEY is not set", ErrPrecondition)
	}
	asins = dedupe(asins)
	if len(asins) == 0 {
		return 0, nil, fmt.Errorf("%w: no ASINs given", ErrInvalidInput)
	}

	setupCtx, cancelSetup := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelSetup()
	if err := s.jobs.checkEnvironment(setupCtx); err != nil {
		return 0, nil, err
	}
	release, ok, err := s.lock.tryLock(setupCtx)
	if err != nil {
		return 0, nil, err
	}
	if !ok {
		return 0, nil, ErrJobInProgress
	}

	end := s.now().AddDate(0, 0, -1)
	counts := JobCounts{
		ASINs:     len(asins),
		StartDate: end.AddDate(-1, 0, 0).Format("2006-01-02"),
		EndDate:   end.Format("2006-01-02"),
	}
	jobID, err := s.jobs.create(setupCtx, source, sourceRunID, counts)
	if err != nil {
		release()
		return 0, nil, err
	}

	budget := time.Hour + time.Duration(len(asins))*time.Second
	ctx, cancel := context.WithTimeout(context.Background(), budget)
	done := make(chan struct{})
	go func() {
		defer close(done)
		defer release()
		defer cancel()
		defer func() {
			if r := recover(); r != nil {
				s.jobs.finish(jobID, "failed", counts, fmt.Errorf("panic: %v", r))
			}
		}()
		if err := s.run(ctx, jobID, asins, &counts); err != nil {
			s.jobs.finish(jobID, "failed", counts, err)
			return
		}
		s.jobs.finish(jobID, "completed", counts, nil)
	}()
	return jobID, done, nil
}

// StartFromHeliumRun runs a job for the ASINs of Helium10 P1/P2/P3 run
// heliumRunID selected by source (see store.heliumRunASINs).
func (s *Service) StartFromHeliumRun(heliumRunID int64, source string) (int64, <-chan struct{}, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if err := s.jobs.checkEnvironment(ctx); err != nil {
		return 0, nil, err
	}
	asins, err := s.reads.heliumRunASINs(ctx, heliumRunID, source)
	if err != nil {
		return 0, nil, err
	}
	if len(asins) == 0 {
		return 0, nil, fmt.Errorf("%w: Helium run %d has no %s ASIN left to fetch", ErrNothingToDo, heliumRunID, source)
	}
	return s.Start(asins, source, &heliumRunID)
}

func (s *Service) run(ctx context.Context, jobID int64, asins []string, counts *JobCounts) error {
	if err := s.fetchProducts(ctx, jobID, asins, counts); err != nil {
		counts.Sales.NotSent = len(asins)
		return err
	}
	return s.fetchSales(ctx, jobID, asins, counts)
}

// fetchProducts sends the ASINs to product_database_query 100 at a time.
func (s *Service) fetchProducts(ctx context.Context, jobID int64, asins []string, counts *JobCounts) error {
	s.jobs.progress(ctx, jobID, "product", *counts)
	reportDate := s.now().Format("2006-01-02")

	for start := 0; start < len(asins); start += productBatchSize {
		end := start + productBatchSize
		if end > len(asins) {
			end = len(asins)
		}
		batch := asins[start:end]

		resp, retried, err := retryOnce(ctx, s.retryDelay, func() (*junglescout.ProductAPIResponse, error) {
			return s.api.FetchProductDataAll(batch, marketplace)
		})
		counts.Product.Calls++
		counts.Product.Requested += len(batch)
		if retried {
			counts.Product.Retries++
		}
		if err != nil {
			counts.Product.Failed += len(batch)
			counts.Product.NotSent = len(asins) - end
			_ = s.jobs.record(ctx, jobID, resultRows(batch, ResultFailed, ""))
			return fmt.Errorf("product batch %d-%d failed twice: %w", start+1, end, err)
		}

		requested := toSet(batch)
		var returned []junglescout.ProductData
		found := map[string]bool{}
		for _, p := range resp.Data {
			if a := jsstore.ProductASIN(p); requested[a] && !found[a] {
				found[a] = true
				returned = append(returned, p)
			}
		}
		if err := s.data.products(ctx, returned, reportDate); err != nil {
			counts.Product.Failed += len(batch)
			counts.Product.NotSent = len(asins) - end
			return fmt.Errorf("store products: %w", err)
		}

		rows := make([]ASINResult, 0, len(batch))
		for _, a := range batch {
			result := ResultMissing
			if found[a] {
				result = ResultFound
				counts.Product.Found++
			} else {
				counts.Product.Missing++
			}
			rows = append(rows, ASINResult{ASIN: a, Product: result})
		}
		if err := s.jobs.record(ctx, jobID, rows); err != nil {
			return fmt.Errorf("record product results: %w", err)
		}
		s.jobs.progress(ctx, jobID, "product", *counts)
	}
	return nil
}

// fetchSales sends every ASIN to sales_estimates_query, one per call, with
// s.workers calls in flight. The first call that fails twice stops the job.
func (s *Service) fetchSales(jobCtx context.Context, jobID int64, asins []string, counts *JobCounts) error {
	s.jobs.progress(jobCtx, jobID, "sales", *counts)
	// ctx stops the workers; jobCtx outlives it so the results of the calls
	// that stopped the job are still recorded.
	ctx, cancel := context.WithCancel(jobCtx)
	defer cancel()

	var (
		mu       sync.Mutex
		firstErr error
		wg       sync.WaitGroup
		queue    = make(chan string)
	)
	fail := func(err error) {
		if firstErr == nil {
			firstErr = err
			cancel()
		}
	}

	for w := 0; w < s.workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() {
				if r := recover(); r != nil {
					mu.Lock()
					fail(fmt.Errorf("sales worker panic: %v", r))
					mu.Unlock()
				}
			}()
			for asin := range queue {
				row, days, retried, err := s.fetchOne(ctx, asin, counts.StartDate, counts.EndDate)
				if errors.Is(err, context.Canceled) {
					continue // stopped by another worker, not attempted
				}
				mu.Lock()
				counts.Sales.Requested++
				counts.Sales.Calls++
				if retried {
					counts.Sales.Retries++
				}
				switch row.Sales {
				case ResultFound:
					counts.Sales.Found++
					counts.SalesDays += days
				case ResultMissing:
					counts.Sales.Missing++
				case ResultFailed:
					counts.Sales.Failed++
				}
				if err != nil {
					fail(err)
				}
				report := counts.Sales.Requested%progressEvery == 0
				snapshot := *counts
				mu.Unlock()

				if recErr := s.jobs.record(jobCtx, jobID, []ASINResult{row}); recErr != nil {
					log.Printf("[HELIUM_JS] job=%d record sales result %s: %v", jobID, asin, recErr)
				}
				if report {
					s.jobs.progress(jobCtx, jobID, "sales", snapshot)
				}
			}
		}()
	}

feed:
	for _, asin := range asins {
		select {
		case queue <- asin:
		case <-ctx.Done():
			break feed
		}
	}
	close(queue)
	wg.Wait()

	counts.Sales.NotSent = len(asins) - counts.Sales.Requested
	if firstErr == nil && jobCtx.Err() != nil {
		return fmt.Errorf("job stopped before every ASIN was sent: %w", jobCtx.Err())
	}
	return firstErr
}

// fetchOne fetches and stores one ASIN's sales series. A returned error stops
// the job; an empty series is a miss, not an error.
func (s *Service) fetchOne(ctx context.Context, asin, startDate, endDate string) (ASINResult, int, bool, error) {
	resp, retried, err := retryOnce(ctx, s.retryDelay, func() (*junglescout.SalesEstimateAPIResponse, error) {
		return s.api.FetchSalesEstimateData(asin, marketplace, startDate, endDate)
	})
	if noData(err) {
		return ASINResult{ASIN: asin, Sales: ResultMissing}, 0, retried, nil
	}
	if err != nil {
		if ctx.Err() != nil {
			return ASINResult{ASIN: asin}, 0, retried, context.Canceled
		}
		return ASINResult{ASIN: asin, Sales: ResultFailed}, 0, retried, fmt.Errorf("sales call for %s failed twice: %w", asin, err)
	}
	if resp == nil || len(resp.Data) == 0 || len(resp.Data[0].Attributes.Data) == 0 {
		return ASINResult{ASIN: asin, Sales: ResultMissing}, 0, retried, nil
	}
	series := resp.Data[0].Attributes
	if series.ASIN == "" {
		series.ASIN = asin
	}
	if err := s.data.sales(ctx, series); err != nil {
		return ASINResult{ASIN: asin, Sales: ResultFailed}, 0, retried, fmt.Errorf("store sales for %s: %w", asin, err)
	}
	days := len(series.Data)
	return ASINResult{ASIN: asin, Sales: ResultFound, SalesDays: days}, days, retried, nil
}

// Get returns one job, or sql.ErrNoRows.
func (s *Service) Get(ctx context.Context, jobID int64) (*Job, error) {
	return s.reads.get(ctx, jobID)
}

// List returns the newest jobs first.
func (s *Service) List(ctx context.Context, limit int) ([]Job, error) {
	return s.reads.list(ctx, limit)
}

// Results lists a job's per-ASIN results for one API.
func (s *Service) Results(ctx context.Context, jobID int64, api, result string) ([]ASINResult, error) {
	return s.reads.resultsFor(ctx, jobID, api, result)
}

// noData reports whether err is JungleScout saying it has no data for the
// ASIN: a miss, not a failure, and not worth a retry.
func noData(err error) bool {
	var apiErr *junglescout.APIError
	return errors.As(err, &apiErr) && apiErr.NoData()
}

func resultRows(asins []string, product, sales string) []ASINResult {
	rows := make([]ASINResult, len(asins))
	for i, a := range asins {
		rows[i] = ASINResult{ASIN: a, Product: product, Sales: sales}
	}
	return rows
}

func toSet(in []string) map[string]bool {
	out := make(map[string]bool, len(in))
	for _, a := range in {
		out[a] = true
	}
	return out
}

func dedupe(in []string) []string {
	seen := make(map[string]bool, len(in))
	out := make([]string, 0, len(in))
	for _, a := range in {
		if a != "" && !seen[a] {
			seen[a] = true
			out = append(out, a)
		}
	}
	return out
}

// retryOnce runs call, and once more after delay if it failed. retried reports
// whether the second attempt was made.
func retryOnce[T any](ctx context.Context, delay time.Duration, call func() (T, error)) (T, bool, error) {
	v, err := call()
	if err == nil || noData(err) || ctx.Err() != nil {
		return v, false, err
	}
	log.Printf("[HELIUM_JS] call failed, retrying once in %s: %v", delay, err)
	select {
	case <-time.After(delay):
	case <-ctx.Done():
		return v, false, err
	}
	v, err = call()
	return v, true, err
}
