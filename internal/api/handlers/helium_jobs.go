package handlers

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"azaffiliates/internal/heliumjs"

	"github.com/gin-gonic/gin"
)

// HeliumJSService is what the /helium-js routes need.
type HeliumJSService interface {
	Start(asins []string, source string, sourceRunID *int64) (int64, <-chan struct{}, error)
	StartFromHeliumRun(heliumRunID int64, source string) (int64, <-chan struct{}, error)
	Get(ctx context.Context, jobID int64) (*heliumjs.Job, error)
	List(ctx context.Context, limit int) ([]heliumjs.Job, error)
	Results(ctx context.Context, jobID int64, api, result string) ([]heliumjs.ASINResult, error)
}

var heliumJSSources = map[string]bool{heliumjs.SourceHeliumFound: true, heliumjs.SourceHeliumMissing: true, heliumjs.SourceManual: true}

// HeliumJSFetch handles POST /helium-js/fetch: product data and sales estimates
// for the given ASINs, written to staging.
//
//	body  {"asins": [...], "source": "helium_found", "source_run_id": 12}
//	wait  true holds the request until the job finishes
func HeliumJSFetch(svc HeliumJSService) gin.HandlerFunc {
	return func(c *gin.Context) {
		var body struct {
			ASINs       []string `json:"asins"`
			Source      string   `json:"source"`
			SourceRunID *int64   `json:"source_run_id"`
		}
		if err := c.ShouldBindJSON(&body); err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": `send {"asins": [...]} as JSON`})
			return
		}
		if body.Source == "" {
			body.Source = heliumjs.SourceManual
		}
		if !heliumJSSources[body.Source] {
			c.JSON(http.StatusBadRequest, gin.H{"error": "source must be helium_found, helium_missing or manual"})
			return
		}
		asins, invalid := normalizeASINs(body.ASINs)
		if len(invalid) > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "these entries are not ASINs", "invalid": firstN(invalid, maxInvalidASINSamples), "invalid_count": len(invalid)})
			return
		}
		jobID, done, err := svc.Start(asins, body.Source, body.SourceRunID)
		respondToJobStart(c, svc, jobID, done, err)
	}
}

// HeliumJSFetchFromRun handles POST /helium-js/fetch-found and
// /helium-js/fetch-missing ?helium_run_id=N: the ASINs Helium10 P1/P2/P3 run N
// found, or missed but which now have Helium data. A rerun skips ASINs already
// settled for that run.
func HeliumJSFetchFromRun(svc HeliumJSService, source string) gin.HandlerFunc {
	return func(c *gin.Context) {
		runID, err := strconv.ParseInt(c.Query("helium_run_id"), 10, 64)
		if err != nil || runID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "helium_run_id must be a positive integer"})
			return
		}
		jobID, done, err := svc.StartFromHeliumRun(runID, source)
		respondToJobStart(c, svc, jobID, done, err)
	}
}

func respondToJobStart(c *gin.Context, svc HeliumJSService, jobID int64, done <-chan struct{}, err error) {
	switch {
	case errors.Is(err, heliumjs.ErrJobInProgress):
		c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		return
	case errors.Is(err, heliumjs.ErrPrecondition):
		c.JSON(http.StatusPreconditionFailed, gin.H{"error": err.Error()})
		return
	case errors.Is(err, heliumjs.ErrInvalidInput):
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	case errors.Is(err, heliumjs.ErrNothingToDo):
		c.JSON(http.StatusOK, gin.H{"status": "nothing_to_do", "message": err.Error()})
		return
	case err != nil:
		log.Printf("[HELIUM_JS] start: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	if c.Query("wait") != "true" {
		c.JSON(http.StatusAccepted, gin.H{"job_id": jobID, "status": "running", "status_url": fmt.Sprintf("/helium-js/jobs/%d", jobID)})
		return
	}
	select {
	case <-done:
	case <-c.Request.Context().Done():
		return // the job carries on without the caller
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	respondWithJob(ctx, c, svc, jobID)
}

// HeliumJSJob handles GET /helium-js/jobs/:job_id.
func HeliumJSJob(svc HeliumJSService) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID, ok := jobIDParam(c)
		if !ok {
			return
		}
		respondWithJob(c.Request.Context(), c, svc, jobID)
	}
}

func respondWithJob(ctx context.Context, c *gin.Context, svc HeliumJSService, jobID int64) {
	job, err := svc.Get(ctx, jobID)
	switch {
	case errors.Is(err, sql.ErrNoRows):
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found"})
	case err != nil:
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
	default:
		c.JSON(http.StatusOK, job)
	}
}

// HeliumJSJobs handles GET /helium-js/jobs?limit=20.
func HeliumJSJobs(svc HeliumJSService) gin.HandlerFunc {
	return func(c *gin.Context) {
		limit, _ := strconv.Atoi(c.DefaultQuery("limit", "20"))
		jobs, err := svc.List(c.Request.Context(), limit)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"jobs": jobs})
	}
}

// HeliumJSJobASINs handles GET /helium-js/jobs/:job_id/asins: which ASINs one
// Jungle Scout API found data for, and which it did not.
//
//	api     product (default) or sales
//	result  missing (default), found, failed or all
//	format  csv (default) or json
func HeliumJSJobASINs(svc HeliumJSService) gin.HandlerFunc {
	return func(c *gin.Context) {
		jobID, ok := jobIDParam(c)
		if !ok {
			return
		}
		api := c.DefaultQuery("api", "product")
		if api != "product" && api != "sales" {
			c.JSON(http.StatusBadRequest, gin.H{"error": "api must be product or sales"})
			return
		}
		result := c.DefaultQuery("result", heliumjs.ResultMissing)
		switch result {
		case heliumjs.ResultFound, heliumjs.ResultMissing, heliumjs.ResultFailed, "all":
		default:
			c.JSON(http.StatusBadRequest, gin.H{"error": "result must be missing, found, failed or all"})
			return
		}
		rows, err := svc.Results(c.Request.Context(), jobID, api, result)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		if c.DefaultQuery("format", "csv") == "json" {
			c.JSON(http.StatusOK, gin.H{"job_id": jobID, "api": api, "result": result, "count": len(rows), "asins": rows})
			return
		}
		c.Header("Content-Type", "text/csv; charset=utf-8")
		c.Header("Content-Disposition", fmt.Sprintf(`attachment; filename="js-job-%d-%s-%s.csv"`, jobID, api, result))
		w := csv.NewWriter(c.Writer)
		_ = w.Write([]string{"asin", "product_result", "sales_result", "sales_days"})
		for _, r := range rows {
			_ = w.Write([]string{r.ASIN, r.Product, r.Sales, strconv.Itoa(r.SalesDays)})
		}
		w.Flush()
	}
}

func jobIDParam(c *gin.Context) (int64, bool) {
	jobID, err := strconv.ParseInt(c.Param("job_id"), 10, 64)
	if err != nil || jobID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "job_id must be a positive integer"})
		return 0, false
	}
	return jobID, true
}

// normalizeASINs upper-cases and de-duplicates, splitting out anything that is
// not an ASIN.
func normalizeASINs(in []string) (valid, invalid []string) {
	seen := map[string]bool{}
	for _, raw := range in {
		a := strings.ToUpper(strings.TrimSpace(raw))
		switch {
		case a == "":
		case !asinPattern.MatchString(a):
			invalid = append(invalid, raw)
		case !seen[a]:
			seen[a] = true
			valid = append(valid, a)
		}
	}
	return valid, invalid
}

func firstN(in []string, n int) []string {
	if len(in) > n {
		return in[:n]
	}
	return in
}
