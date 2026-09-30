package heliumjs

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"azaffiliates/internal/junglescout"
)

type fakeAPI struct {
	mu            sync.Mutex
	productKnown  map[string]bool
	salesKnown    map[string]bool
	productErrs   int // fail this many product calls before succeeding
	salesFailASIN string
	salesNoData   string
	salesCalls    int
}

func (f *fakeAPI) FetchProductDataAll(asins []string, _ string) (*junglescout.ProductAPIResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.productErrs > 0 {
		f.productErrs--
		return nil, errors.New("boom")
	}
	resp := &junglescout.ProductAPIResponse{}
	for _, a := range asins {
		if f.productKnown[a] {
			resp.Data = append(resp.Data, junglescout.ProductData{ID: "us/" + a})
		}
	}
	resp.Data = append(resp.Data, junglescout.ProductData{ID: "us/UNRELATED1"})
	return resp, nil
}

func (f *fakeAPI) FetchSalesEstimateData(asin, _, _, _ string) (*junglescout.SalesEstimateAPIResponse, error) {
	f.mu.Lock()
	f.salesCalls++
	f.mu.Unlock()
	if asin == f.salesFailASIN {
		return nil, errors.New("boom")
	}
	if asin == f.salesNoData {
		return nil, &junglescout.APIError{Status: 422, Body: `{"errors":[{"code":"MISSING_RANK_DATA"}]}`}
	}
	if !f.salesKnown[asin] {
		return &junglescout.SalesEstimateAPIResponse{}, nil
	}
	return &junglescout.SalesEstimateAPIResponse{Data: []junglescout.SalesEstimateData{{
		Attributes: junglescout.SalesEstimateAttributes{ASIN: asin, Data: []junglescout.SalesEstimateDataPoint{{Date: "2026-09-01"}, {Date: "2026-09-02"}}},
	}}}, nil
}

type fakeJobs struct {
	mu      sync.Mutex
	results map[string]ASINResult
	status  string
	counts  JobCounts
	err     error
}

func (f *fakeJobs) checkEnvironment(context.Context) error { return nil }
func (f *fakeJobs) create(context.Context, string, *int64, JobCounts) (int64, error) {
	return 1, nil
}
func (f *fakeJobs) progress(context.Context, int64, string, JobCounts) {}
func (f *fakeJobs) finish(_ int64, status string, counts JobCounts, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.status, f.counts, f.err = status, counts, err
}
func (f *fakeJobs) record(_ context.Context, _ int64, rows []ASINResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, r := range rows {
		old := f.results[r.ASIN]
		if r.Product == "" {
			r.Product = old.Product
		}
		if r.Sales == "" {
			r.Sales = old.Sales
		}
		f.results[r.ASIN] = r
	}
	return nil
}

type fakeData struct {
	mu            sync.Mutex
	storedProduct []string
	storedSales   []string
}

func (f *fakeData) products(_ context.Context, ps []junglescout.ProductData, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, p := range ps {
		f.storedProduct = append(f.storedProduct, p.ID)
	}
	return nil
}
func (f *fakeData) sales(_ context.Context, s junglescout.SalesEstimateAttributes) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.storedSales = append(f.storedSales, s.ASIN)
	return nil
}

type fakeLock struct{}

func (fakeLock) tryLock(context.Context) (func(), bool, error) { return func() {}, true, nil }

func newTestService(api *fakeAPI, workers int) (*Service, *fakeJobs, *fakeData) {
	jobs := &fakeJobs{results: map[string]ASINResult{}}
	data := &fakeData{}
	return &Service{api: api, jobs: jobs, data: data, lock: fakeLock{}, workers: workers,
		now: func() time.Time { return time.Date(2026, 9, 30, 0, 0, 0, 0, time.UTC) }}, jobs, data
}

func runJob(t *testing.T, s *Service, asins []string) {
	t.Helper()
	_, done, err := s.Start(asins, SourceHeliumFound, nil)
	if err != nil {
		t.Fatal(err)
	}
	<-done
}

func TestJobFetchesBothAPIs(t *testing.T) {
	api := &fakeAPI{
		productKnown: map[string]bool{"A1": true, "A2": true},
		salesKnown:   map[string]bool{"A1": true, "A3": true},
	}
	s, jobs, data := newTestService(api, 3)
	runJob(t, s, []string{"A1", "A2", "A3", "A1"})

	if jobs.status != "completed" || jobs.err != nil {
		t.Fatalf("status %s err %v", jobs.status, jobs.err)
	}
	c := jobs.counts
	if c.ASINs != 3 || c.Product.Found != 2 || c.Product.Missing != 1 || c.Product.Calls != 1 {
		t.Fatalf("product counts %+v", c.Product)
	}
	if c.Sales.Found != 2 || c.Sales.Missing != 1 || c.Sales.Requested != 3 || c.SalesDays != 4 || c.Sales.NotSent != 0 {
		t.Fatalf("sales counts %+v days %d", c.Sales, c.SalesDays)
	}
	if c.StartDate != "2025-09-29" || c.EndDate != "2026-09-29" {
		t.Fatalf("range %s..%s", c.StartDate, c.EndDate)
	}
	if len(data.storedProduct) != 2 || len(data.storedSales) != 2 {
		t.Fatalf("stored products %v sales %v", data.storedProduct, data.storedSales)
	}
	if r := jobs.results["A2"]; r.Product != ResultFound || r.Sales != ResultMissing {
		t.Fatalf("A2 = %+v", r)
	}
	if r := jobs.results["A3"]; r.Product != ResultMissing || r.Sales != ResultFound || r.SalesDays != 2 {
		t.Fatalf("A3 = %+v", r)
	}
}

func TestProductFailureIsRetriedOnceThenStops(t *testing.T) {
	api := &fakeAPI{productErrs: 1, productKnown: map[string]bool{"A1": true}}
	s, jobs, _ := newTestService(api, 1)
	runJob(t, s, []string{"A1"})
	if jobs.status != "completed" || jobs.counts.Product.Retries != 1 || jobs.counts.Product.Found != 1 {
		t.Fatalf("a single failure must be retried: %s %+v", jobs.status, jobs.counts.Product)
	}

	api = &fakeAPI{productErrs: 2}
	s, jobs, _ = newTestService(api, 1)
	runJob(t, s, []string{"A1", "A2"})
	if jobs.status != "failed" || jobs.counts.Product.Failed != 2 || jobs.counts.Sales.NotSent != 2 || api.salesCalls != 0 {
		t.Fatalf("two failures must stop the job before sales: %s %+v %+v calls=%d",
			jobs.status, jobs.counts.Product, jobs.counts.Sales, api.salesCalls)
	}
}

func TestSalesFailureStopsTheJob(t *testing.T) {
	var asins []string
	for i := 0; i < 20; i++ {
		asins = append(asins, fmt.Sprintf("A%02d", i))
	}
	api := &fakeAPI{salesFailASIN: "A00"}
	s, jobs, _ := newTestService(api, 1)
	runJob(t, s, asins)

	if jobs.status != "failed" || jobs.err == nil || jobs.counts.Sales.Failed != 1 {
		t.Fatalf("status %s err %v sales %+v", jobs.status, jobs.err, jobs.counts.Sales)
	}
	if jobs.counts.Sales.NotSent == 0 || jobs.counts.Sales.Requested+jobs.counts.Sales.NotSent != len(asins) {
		t.Fatalf("remaining ASINs must be reported as not sent: %+v", jobs.counts.Sales)
	}
	if jobs.results["A00"].Sales != ResultFailed {
		t.Fatalf("A00 = %+v", jobs.results["A00"])
	}
}

func TestStartRejectsAnEmptyList(t *testing.T) {
	s, _, _ := newTestService(&fakeAPI{}, 1)
	if _, _, err := s.Start([]string{"", ""}, SourceManual, nil); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("err = %v", err)
	}
}

func TestNoDataAnswerIsAMissNotAFailure(t *testing.T) {
	api := &fakeAPI{salesNoData: "A1", salesKnown: map[string]bool{"A2": true}}
	s, jobs, _ := newTestService(api, 1)
	runJob(t, s, []string{"A1", "A2"})
	if jobs.status != "completed" || jobs.counts.Sales.Missing != 1 || jobs.counts.Sales.Found != 1 ||
		jobs.counts.Sales.Retries != 0 || api.salesCalls != 2 {
		t.Fatalf("status %s sales %+v calls %d", jobs.status, jobs.counts.Sales, api.salesCalls)
	}
	if jobs.results["A1"].Sales != ResultMissing {
		t.Fatalf("A1 = %+v", jobs.results["A1"])
	}
}

func TestStartRefusesWithoutAnAPIKey(t *testing.T) {
	s, _, _ := newTestService(&fakeAPI{}, 1)
	s.api = nil
	if _, _, err := s.Start([]string{"A1"}, SourceManual, nil); !errors.Is(err, ErrPrecondition) {
		t.Fatalf("err = %v", err)
	}
}

func TestDeadlineFailsTheJob(t *testing.T) {
	api := &fakeAPI{salesKnown: map[string]bool{"A1": true}}
	s, jobs, _ := newTestService(api, 1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	counts := JobCounts{ASINs: 2}
	err := s.fetchSales(ctx, 1, []string{"A1", "A2"}, &counts)
	if err == nil || counts.Sales.NotSent == 0 {
		t.Fatalf("a job that ran out of time must fail: err=%v counts=%+v", err, counts.Sales)
	}
	_ = jobs
}
