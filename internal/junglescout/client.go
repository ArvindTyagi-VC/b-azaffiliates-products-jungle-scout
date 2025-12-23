package junglescout

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// Client represents a JungleScout API client with rate limiting
type Client struct {
	apiKey      string
	httpClient  *http.Client
	rateLimiter *TokenBucketRateLimiter
	baseURL     string
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

// doRequest performs an HTTP request with rate limiting and retry logic
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

		// Handle rate limiting (429)
		if resp.StatusCode == http.StatusTooManyRequests {
			body, _ := io.ReadAll(resp.Body)
			resp.Body.Close()

			// Parse retry time from response if available
			var errorResp struct {
				Errors []struct {
					Detail string `json:"detail"`
				} `json:"errors"`
			}

			if err := json.Unmarshal(body, &errorResp); err == nil && len(errorResp.Errors) > 0 {
				// Extract retry time from "retry again at 2021-03-19T00:55:19-06:00"
				detail := errorResp.Errors[0].Detail
				if strings.Contains(detail, "retry again at") {
					// Wait for 5 seconds by default or parse the time
					time.Sleep(5 * time.Second)
				}
			} else {
				// Default wait time for rate limiting
				time.Sleep(time.Duration(5*(attempt+1)) * time.Second)
			}
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

	return &apiResponse, nil
}

// GetRateLimiterStatus returns the current rate limiter status
func (c *Client) GetRateLimiterStatus() float64 {
	return c.rateLimiter.GetAvailableTokens()
}