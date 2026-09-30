package handlers

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"azaffiliates/internal/heliumjs"

	"github.com/gin-gonic/gin"
)

type fakeHeliumJS struct {
	started   [][]string
	sources   []string
	missingOf []int64
	startErr  error
}

func (f *fakeHeliumJS) Start(asins []string, source string, _ *int64) (int64, <-chan struct{}, error) {
	f.started, f.sources = append(f.started, asins), append(f.sources, source)
	if f.startErr != nil {
		return 0, nil, f.startErr
	}
	done := make(chan struct{})
	close(done)
	return 4, done, nil
}

func (f *fakeHeliumJS) StartFromHeliumRun(runID int64, source string) (int64, <-chan struct{}, error) {
	f.missingOf = append(f.missingOf, runID)
	f.sources = append(f.sources, source)
	if runID == 99 {
		return 0, nil, heliumjs.ErrNothingToDo
	}
	done := make(chan struct{})
	close(done)
	return 5, done, nil
}

func (f *fakeHeliumJS) Get(context.Context, int64) (*heliumjs.Job, error) {
	return &heliumjs.Job{JobID: 4, Status: "completed"}, nil
}

func (f *fakeHeliumJS) List(context.Context, int) ([]heliumjs.Job, error) { return nil, nil }

func (f *fakeHeliumJS) Results(context.Context, int64, string, string) ([]heliumjs.ASINResult, error) {
	return []heliumjs.ASINResult{{ASIN: "B0AAAAAAA1", Product: "found", Sales: "missing"}}, nil
}

func heliumJSRouter(svc HeliumJSService) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/helium-js/fetch", HeliumJSFetch(svc))
	r.POST("/helium-js/fetch-found", HeliumJSFetchFromRun(svc, heliumjs.SourceHeliumFound))
	r.POST("/helium-js/fetch-missing", HeliumJSFetchFromRun(svc, heliumjs.SourceHeliumMissing))
	r.GET("/helium-js/jobs/:job_id", HeliumJSJob(svc))
	r.GET("/helium-js/jobs/:job_id/asins", HeliumJSJobASINs(svc))
	return r
}

func serve(r *gin.Engine, method, url, body string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, url, bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestHeliumJSFetch(t *testing.T) {
	svc := &fakeHeliumJS{}
	w := serve(heliumJSRouter(svc), http.MethodPost, "/helium-js/fetch",
		`{"asins": ["b0aaaaaaa1", "B0AAAAAAA1", " B0AAAAAAA2 "], "source": "helium_found", "source_run_id": 12}`)
	if w.Code != http.StatusAccepted || len(svc.started) != 1 {
		t.Fatalf("status %d body %s", w.Code, w.Body.String())
	}
	if got := strings.Join(svc.started[0], ","); got != "B0AAAAAAA1,B0AAAAAAA2" || svc.sources[0] != heliumjs.SourceHeliumFound {
		t.Fatalf("started %s from %s", got, svc.sources[0])
	}

	for _, body := range []string{`{"asins": ["nope"]}`, `{"asins": ["B0AAAAAAA1"], "source": "other"}`, `not json`} {
		svc := &fakeHeliumJS{}
		if w := serve(heliumJSRouter(svc), http.MethodPost, "/helium-js/fetch", body); w.Code != http.StatusBadRequest || len(svc.started) != 0 {
			t.Errorf("%s: status %d", body, w.Code)
		}
	}

	busy := &fakeHeliumJS{startErr: heliumjs.ErrJobInProgress}
	if w := serve(heliumJSRouter(busy), http.MethodPost, "/helium-js/fetch", `{"asins": ["B0AAAAAAA1"]}`); w.Code != http.StatusConflict {
		t.Errorf("busy: status %d", w.Code)
	}
}

func TestHeliumJSFetchMissing(t *testing.T) {
	svc := &fakeHeliumJS{}
	if w := serve(heliumJSRouter(svc), http.MethodPost, "/helium-js/fetch-missing?helium_run_id=7&wait=true", ""); w.Code != http.StatusOK || svc.missingOf[0] != 7 {
		t.Fatalf("status %d missing %v", w.Code, svc.missingOf)
	}
	if w := serve(heliumJSRouter(svc), http.MethodPost, "/helium-js/fetch-missing", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("missing run id: status %d", w.Code)
	}
	if svc.sources[0] != heliumjs.SourceHeliumMissing {
		t.Fatalf("source %s", svc.sources[0])
	}
	found := &fakeHeliumJS{}
	if w := serve(heliumJSRouter(found), http.MethodPost, "/helium-js/fetch-found?helium_run_id=99", ""); w.Code != http.StatusOK ||
		!strings.Contains(w.Body.String(), "nothing_to_do") || found.sources[0] != heliumjs.SourceHeliumFound {
		t.Fatalf("fetch-found: %d %s", w.Code, w.Body.String())
	}
}

func TestHeliumJSJobASINs(t *testing.T) {
	r := heliumJSRouter(&fakeHeliumJS{})
	if w := serve(r, http.MethodGet, "/helium-js/jobs/4/asins?api=sales", ""); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "B0AAAAAAA1,found,missing,0") {
		t.Fatalf("csv: %d %s", w.Code, w.Body.String())
	}
	if w := serve(r, http.MethodGet, "/helium-js/jobs/4/asins?api=keyword", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("bad api: %d", w.Code)
	}
	if w := serve(r, http.MethodGet, "/helium-js/jobs/x", ""); w.Code != http.StatusBadRequest {
		t.Fatalf("bad id: %d", w.Code)
	}
}
