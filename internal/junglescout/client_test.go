package junglescout

import (
	"fmt"
	"net/http"
	"testing"
	"time"
)

// The 429 handler used to detect JungleScout's "retry again at" phrase and then
// sleep a flat 5 seconds regardless of what it said, so a 60-second penalty burned
// all three attempts in 15 seconds. These tests pin the parsing that replaced it.

func TestParseRetryAfterSeconds(t *testing.T) {
	tests := []struct {
		header string
		want   time.Duration
	}{
		{"30", 30 * time.Second},
		{"1", 1 * time.Second},
		{"  45  ", 45 * time.Second},
		{"", 0},
		{"0", 0},
		{"-5", 0},
		{"soon", 0},
	}

	for _, tc := range tests {
		if got := parseRetryAfter(tc.header); got != tc.want {
			t.Errorf("parseRetryAfter(%q) = %v, want %v", tc.header, got, tc.want)
		}
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	// http.TimeFormat is what a spec-compliant server sends (zone spelled "GMT").
	future := time.Now().UTC().Add(40 * time.Second).Format(http.TimeFormat)
	got := parseRetryAfter(future)
	if got < 30*time.Second || got > 45*time.Second {
		t.Errorf("parseRetryAfter(%q) = %v, want roughly 40s", future, got)
	}

	past := time.Now().UTC().Add(-time.Hour).Format(http.TimeFormat)
	if got := parseRetryAfter(past); got != 0 {
		t.Errorf("parseRetryAfter(past date) = %v, want 0", got)
	}
}

// Not every server spells the zone "GMT" or uses an HTTP date at all. A retry hint
// is worth honouring even when it arrives in a non-spec shape, so these variants
// must not fall through to generic backoff.
func TestParseRetryAfterTolerantFormats(t *testing.T) {
	variants := []string{
		time.Now().UTC().Add(40 * time.Second).Format(time.RFC1123), // zone as "UTC"
		time.Now().Add(40 * time.Second).Format(time.RFC3339),       // ISO timestamp
	}

	for _, header := range variants {
		got := parseRetryAfter(header)
		if got < 30*time.Second || got > 45*time.Second {
			t.Errorf("parseRetryAfter(%q) = %v, want roughly 40s", header, got)
		}
	}
}

func TestParseRetryAgainAt(t *testing.T) {
	future := time.Now().Add(50 * time.Second).Format(time.RFC3339)

	got := parseRetryAgainAt("rate limit exceeded, retry again at " + future)
	if got < 40*time.Second || got > 55*time.Second {
		t.Errorf("parseRetryAgainAt = %v, want roughly 50s", got)
	}

	// Trailing prose and punctuation must not defeat the parse.
	got = parseRetryAgainAt(fmt.Sprintf("retry again at %s. Please slow down.", future))
	if got < 40*time.Second || got > 55*time.Second {
		t.Errorf("parseRetryAgainAt with trailing prose = %v, want roughly 50s", got)
	}
}

func TestParseRetryAgainAtRejectsUnusable(t *testing.T) {
	past := time.Now().Add(-time.Hour).Format(time.RFC3339)

	tests := []string{
		"",
		"you are being rate limited",     // no marker
		"retry again at not-a-timestamp", // unparseable
		"retry again at " + past,         // already elapsed
	}

	for _, detail := range tests {
		if got := parseRetryAgainAt(detail); got != 0 {
			t.Errorf("parseRetryAgainAt(%q) = %v, want 0", detail, got)
		}
	}
}
