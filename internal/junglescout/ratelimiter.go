package junglescout

import (
	"sync"
	"time"
)

// TokenBucketRateLimiter implements a token bucket algorithm for rate limiting
// JungleScout API allows 300 requests per minute or 15 requests per second
type TokenBucketRateLimiter struct {
	tokens        float64
	maxTokens     float64
	refillRate    float64 // tokens per second
	lastRefill    time.Time
	mu            sync.Mutex
}

// NewTokenBucketRateLimiter creates a new rate limiter with the specified parameters
func NewTokenBucketRateLimiter(maxTokens float64, refillRate float64) *TokenBucketRateLimiter {
	return &TokenBucketRateLimiter{
		tokens:     maxTokens,
		maxTokens:  maxTokens,
		refillRate: refillRate,
		lastRefill: time.Now(),
	}
}

// Wait blocks until a token is available and then consumes it
func (rl *TokenBucketRateLimiter) Wait() {
	rl.WaitN(1)
}

// WaitN blocks until n tokens are available and then consumes them
func (rl *TokenBucketRateLimiter) WaitN(n float64) {
	for {
		rl.mu.Lock()
		rl.refill()

		if rl.tokens >= n {
			rl.tokens -= n
			rl.mu.Unlock()
			return
		}

		// Calculate how long to wait for the required tokens
		tokensNeeded := n - rl.tokens
		waitTime := time.Duration(tokensNeeded/rl.refillRate*1000) * time.Millisecond
		rl.mu.Unlock()

		// Wait for tokens to refill
		time.Sleep(waitTime)
	}
}

// TryAcquire attempts to acquire a token without blocking
// Returns true if successful, false if no tokens available
func (rl *TokenBucketRateLimiter) TryAcquire() bool {
	return rl.TryAcquireN(1)
}

// TryAcquireN attempts to acquire n tokens without blocking
// Returns true if successful, false if not enough tokens available
func (rl *TokenBucketRateLimiter) TryAcquireN(n float64) bool {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refill()

	if rl.tokens >= n {
		rl.tokens -= n
		return true
	}

	return false
}

// refill adds tokens based on the time elapsed since last refill
func (rl *TokenBucketRateLimiter) refill() {
	now := time.Now()
	elapsed := now.Sub(rl.lastRefill).Seconds()

	rl.tokens = min(rl.maxTokens, rl.tokens+elapsed*rl.refillRate)
	rl.lastRefill = now
}

// GetAvailableTokens returns the current number of available tokens
func (rl *TokenBucketRateLimiter) GetAvailableTokens() float64 {
	rl.mu.Lock()
	defer rl.mu.Unlock()

	rl.refill()
	return rl.tokens
}

func min(a, b float64) float64 {
	if a < b {
		return a
	}
	return b
}