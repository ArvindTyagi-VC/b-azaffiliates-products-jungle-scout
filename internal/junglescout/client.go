package junglescout

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"
)

// maxRateLimitWait caps how long a single 429 may hold a request. A run that is
// rate limited for minutes at a time is better served by failing the ASIN and
// letting the end-of-run retry pass revisit it than by blocking a worker.
const maxRateLimitWait = 90 * time.Second

// parseRetryAfter reads an HTTP Retry-After header, which may be either a count of
// seconds or an HTTP date. Returns 0 when absent or unparseable.
func parseRetryAfter(header string) time.Duration {
	header = strings.TrimSpace(header)
	if header == "" {
		return 0
	}

	if secs, err := strconv.Atoi(header); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}

	// http.ParseTime covers the three formats the HTTP spec allows, all of which
	// spell the zone "GMT". Also try RFC1123 and RFC3339 so a server that sends
	// "UTC" or an ISO timestamp still gets its wait honoured rather than silently
	// falling through to generic backoff.
	for _, parse := range []func(string) (time.Time, error){
		http.ParseTime,
		func(s string) (time.Time, error) { return time.Parse(time.RFC1123, s) },
		func(s string) (time.Time, error) { return time.Parse(time.RFC3339, s) },
	} {
		when, err := parse(header)
		if err != nil {
			continue
		}
		if d := time.Until(when); d > 0 {
			return d
		}
		return 0
	}
	return 0
}

// parseRetryAgainAt extracts the wait implied by JungleScout's error detail, which
// reads like "retry again at 2021-03-19T00:55:19-06:00". Returns 0 when the phrase
// is missing, the timestamp will not parse, or the moment has already passed.
func parseRetryAgainAt(detail string) time.Duration {
	const marker = "retry again at"

	idx := strings.Index(strings.ToLower(detail), marker)
	if idx < 0 {
		return 0
	}

	stamp := strings.TrimSpace(detail[idx+len(marker):])
	// Trim anything trailing the timestamp (a full stop, further prose).
	if cut := strings.IndexAny(stamp, " ,;"); cut > 0 {
		stamp = stamp[:cut]
	}
	stamp = strings.Trim(stamp, ".")

	for _, layout := range []string{time.RFC3339, "2006-01-02T15:04:05Z0700", "2006-01-02 15:04:05"} {
		when, err := time.Parse(layout, stamp)
		if err != nil {
			continue
		}
		if d := time.Until(when); d > 0 {
			return d
		}
		return 0
	}
	return 0
}

// Client represents a JungleScout API client with rate limiting
type Client struct {
	apiKey      string
	httpClient  *http.Client
	rateLimiter *TokenBucketRateLimiter
	baseURL     string
	recorder    APIUsageRecorder
}

// SetUsageRecorder attaches a recorder that's invoked once per HTTP roundtrip
// to JungleScout. Pass nil to disable recording.
func (c *Client) SetUsageRecorder(r APIUsageRecorder) {
	c.recorder = r
}

// NewClient creates a new JungleScout client with rate limiting
// Default: 14 requests per second (slightly below the 15 req/sec limit for safety)
func NewClient() *Client {
	apiKey := os.Getenv("JUNGLE_SCOUT_API_KEY")
	if apiKey == "" {
		panic("JUNGLE_SCOUT_API_KEY environment variable is not set")
	}

	return &Client{
		apiKey:      apiKey,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		rateLimiter: NewTokenBucketRateLimiter(14, 14), // 14 tokens, 14 per second
		baseURL:     "https://developer.junglescout.com/api",
	}
}

// NewClientWithRateLimit creates a client with custom rate limiting
func NewClientWithRateLimit(maxRequestsPerSecond float64) *Client {
	apiKey := os.Getenv("JUNGLE_SCOUT_API_KEY")
	if apiKey == "" {
		panic("JUNGLE_SCOUT_API_KEY environment variable is not set")
	}

	return &Client{
		apiKey:      apiKey,
		httpClient:  &http.Client{Timeout: 30 * time.Second},
		rateLimiter: NewTokenBucketRateLimiter(maxRequestsPerSecond, maxRequestsPerSecond),
		baseURL:     "https://developer.junglescout.com/api",
	}
}

// doRequest performs an HTTP request with rate limiting and retry logic.
// Transport only — recording API usage is the caller's responsibility, since
// only the caller can decide whether the response qualifies as a successful
// data-bearing call.
func (c *Client) doRequest(method, endpoint string, body interface{}) (*http.Response, error) {
	maxRetries := 3
	var lastError error

	for attempt := 0; attempt < maxRetries; attempt++ {
		// Wait for rate limiter
		c.rateLimiter.Wait()

		var reqBody io.Reader
		if body != nil {
			jsonBody, err := json.Marshal(body)
			if err != nil {
				return nil, fmt.Errorf("failed to marshal request body: %w", err)
			}
			reqBody = bytes.NewBuffer(jsonBody)
		}

		req, err := http.NewRequest(method, endpoint, reqBody)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Add headers
		req.Header.Set("Authorization", c.apiKey)
		req.Header.Set("X_API_Type", "junglescout")
		req.Header.Set("Accept", "application/vnd.junglescout.v1+json")
		req.Header.Set("Content-Type", "application/vnd.api+json")

		resp, err := c.httpClient.Do(req)
		if err != nil {
			lastError = err
			time.Sleep(time.Duration(attempt+1) * time.Second) // Exponential backoff
			continue
		}

		// Handle rate limiting (429).
		//
		// JungleScout says when to come back, in two places: the standard
		// Retry-After header and a "retry again at <RFC3339>" phrase in the error
		// detail. This used to detect the phrase and then sleep a flat 5 seconds
		// regardless of what it said, so a 60-second penalty burned all three
		// attempts in 15 seconds and the ASIN failed for no reason. Honour the
		// longer of whatever the server actually told us.
		if resp.StatusCode == http.StatusTooManyRequests {
			body, _ := io.ReadAll(resp.Body)
			retryAfterHeader := resp.Header.Get("Retry-After")
			resp.Body.Close()

			wait := parseRetryAfter(retryAfterHeader)

			var errorResp struct {
				Errors []struct {
					Detail string `json:"detail"`
				} `json:"errors"`
			}
			if err := json.Unmarshal(body, &errorResp); err == nil {
				for _, e := range errorResp.Errors {
					if d := parseRetryAgainAt(e.Detail); d > wait {
						wait = d
					}
				}
			}

			// Nothing usable from the server: exponential backoff as before.
			if wait <= 0 {
				wait = time.Duration(5*(attempt+1)) * time.Second
			}

			// Cap the wait so one hostile penalty cannot hold the whole run. The
			// end-of-run retry pass picks up whatever is still failing, by which
			// point the rate window has usually moved on.
			if wait > maxRateLimitWait {
				log.Printf("[JS] 429: server asked for %s, capping at %s (attempt %d/%d)",
					wait.Round(time.Second), maxRateLimitWait, attempt+1, maxRetries)
				wait = maxRateLimitWait
			}

			lastError = fmt.Errorf("rate limited (429) after %d attempt(s)", attempt+1)
			log.Printf("[JS] 429 on %s — waiting %s before attempt %d/%d",
				endpoint, wait.Round(time.Second), attempt+2, maxRetries)
			time.Sleep(wait)
			continue
		}

		// Success or non-retryable error
		return resp, nil
	}

	return nil, fmt.Errorf("max retries exceeded, last error: %w", lastError)
}

// FetchProductData fetches product data for up to 100 ASINs
func (c *Client) FetchProductData(asins []string, marketplace string) (*ProductAPIResponse, error) {
	if len(asins) == 0 {
		return nil, fmt.Errorf("at least one ASIN is required")
	}
	if len(asins) > 100 {
		return nil, fmt.Errorf("maximum 100 ASINs allowed per request")
	}


	requestBody := map[string]interface{}{
		"data": map[string]interface{}{
			"type": "product_database_query",
			"attributes": map[string]interface{}{
				"include_keywords": asins,
			},
		},
	}

	endpoint := fmt.Sprintf("%s/product_database_query?marketplace=%s&sort=name&page[size]=100", c.baseURL, marketplace)

	resp, err := c.doRequest("POST", endpoint, requestBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()


	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	var apiResponse ProductAPIResponse
	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	if c.recorder != nil && len(apiResponse.Data) > 0 {
		c.recorder.Record(EndpointProductDatabaseQuery)
	}

	return &apiResponse, nil
}

// FetchSalesEstimateData fetches sales estimate data for a single ASIN
func (c *Client) FetchSalesEstimateData(asin, marketplace, startDate, endDate string) (*SalesEstimateAPIResponse, error) {

	// Validate date range (max 1 year)
	parsedStartDate, err := time.Parse("2006-01-02", startDate)
	if err != nil {
		return nil, fmt.Errorf("invalid start_date format: %w", err)
	}

	parsedEndDate, err := time.Parse("2006-01-02", endDate)
	if err != nil {
		return nil, fmt.Errorf("invalid end_date format: %w", err)
	}

	// Adjust if date range exceeds 1 year
	oneYearBeforeEnd := parsedEndDate.AddDate(-1, 0, 0)
	if parsedStartDate.Before(oneYearBeforeEnd) {
		parsedStartDate = oneYearBeforeEnd
		startDate = parsedStartDate.Format("2006-01-02")
	}

	endpoint := fmt.Sprintf(
		"%s/sales_estimates_query?marketplace=%s&asin=%s&start_date=%s&end_date=%s",
		c.baseURL, marketplace, asin, startDate, endDate,
	)

	resp, err := c.doRequest("GET", endpoint, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()


	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("API request failed with status %d: %s", resp.StatusCode, string(body))
	}

	bodyBytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	var apiResponse SalesEstimateAPIResponse
	if err := json.Unmarshal(bodyBytes, &apiResponse); err != nil {
		return nil, fmt.Errorf("failed to parse API response: %w", err)
	}

	if c.recorder != nil && len(apiResponse.Data) > 0 {
		c.recorder.Record(EndpointSalesEstimatesQuery)
	}

	return &apiResponse, nil
}

// GetRateLimiterStatus returns the current rate limiter status
func (c *Client) GetRateLimiterStatus() float64 {
	return c.rateLimiter.GetAvailableTokens()
}