package handlers

import (
	"encoding/csv"
	"fmt"
	"io"
	"mime/multipart"
	"regexp"
	"strings"

	"github.com/gin-gonic/gin"
)

// ============================================================================
// MANUAL ASIN UPLOAD (is_manual=true)
// ============================================================================
//
// A master sync normally derives the parent ASINs to fetch from the database
// (see parentASINSourceSQL in jsparent_fanout.go). With is_manual=true the
// caller uploads a CSV instead: that database selection is skipped entirely and
// the uploaded ASINs ARE the parent set. Everything downstream — the JungleScout
// product/sales fetch, the dual-write and the parent -> child fan-out — is
// unchanged, so a CSV parent that has children in {prefix}parent_asin still
// fans out to them.
//
// CSV shape: one ASIN per row. A header row is optional; if the first row
// contains a cell named asin / parent_asin / asins, that column is used,
// otherwise the first column is. Extra columns are ignored, so an export with
// ASIN, title, brand, ... works as-is.

const (
	// maxManualCSVBytes caps the uploaded CSV. 5 MB is ~400k ASIN rows, far more
	// than any realistic manual run, and keeps a bad upload from being read into
	// memory wholesale.
	maxManualCSVBytes = 5 << 20

	// utf8BOM is the byte order mark Excel prepends to CSV exports; left in
	// place it would glue itself to the first cell of the first row.
	utf8BOM = "\ufeff"

	// maxInvalidASINSamples limits how many rejected values are echoed back in
	// the API response, so a wholly malformed file does not produce a huge body.
	maxInvalidASINSamples = 20
)

// manualCSVFormFields are the multipart field names accepted for the upload,
// tried in order.
var manualCSVFormFields = []string{"file", "csv", "asins"}

// asinPattern matches an Amazon ASIN: 10 alphanumeric characters.
var asinPattern = regexp.MustCompile(`^[A-Z0-9]{10}$`)

// asinHeaderNames are the header cells that identify the ASIN column.
var asinHeaderNames = map[string]bool{
	"asin":         true,
	"asins":        true,
	"parent_asin":  true,
	"parent asin":  true,
	"parentasin":   true,
	"parent_asins": true,
}

// ManualASINUpload is the parsed result of a manual CSV upload.
type ManualASINUpload struct {
	Filename     string   `json:"filename"`
	ASINs        []string `json:"-"`     // the parent ASINs to sync, de-duplicated
	Valid        int      `json:"valid"` // len(ASINs)
	Duplicates   int      `json:"duplicates"`
	Invalid      []string `json:"invalid,omitempty"` // up to maxInvalidASINSamples samples
	InvalidCount int      `json:"invalid_count"`
	DataRows     int      `json:"data_rows"` // non-empty rows read, excluding the header
}

// isManualRequested reports whether the caller asked for a manual (CSV-driven)
// sync. The flag is accepted as a query parameter or as a form field, so it can
// travel in the same multipart body as the CSV.
func isManualRequested(c *gin.Context) bool {
	value := c.Query("is_manual")
	if value == "" {
		value = c.PostForm("is_manual")
	}
	return strings.EqualFold(value, "true") || value == "1"
}

// paramOrDefault reads a parameter from the query string, falling back to the
// form body. A manual sync arrives as multipart/form-data, so callers may send
// the other sync parameters as form fields alongside the CSV.
func paramOrDefault(c *gin.Context, key, defaultValue string) string {
	if value := c.Query(key); value != "" {
		return value
	}
	if value := c.PostForm(key); value != "" {
		return value
	}
	return defaultValue
}

// readManualASINUpload pulls the CSV out of the request and parses it into the
// parent ASIN list. Every error it returns is a client error (HTTP 400).
func readManualASINUpload(c *gin.Context) (*ManualASINUpload, error) {
	var fileHeader *multipart.FileHeader
	for _, field := range manualCSVFormFields {
		if fh, err := c.FormFile(field); err == nil {
			fileHeader = fh
			break
		}
	}
	if fileHeader == nil {
		return nil, fmt.Errorf("is_manual=true requires a CSV upload: send the file as multipart/form-data in field %q", manualCSVFormFields[0])
	}

	if fileHeader.Size > maxManualCSVBytes {
		return nil, fmt.Errorf("CSV %s is %d bytes, which exceeds the %d byte limit", fileHeader.Filename, fileHeader.Size, maxManualCSVBytes)
	}

	file, err := fileHeader.Open()
	if err != nil {
		return nil, fmt.Errorf("failed to open uploaded CSV %s: %w", fileHeader.Filename, err)
	}
	defer file.Close()

	upload, err := parseManualASINCSV(io.LimitReader(file, maxManualCSVBytes))
	if err != nil {
		return nil, fmt.Errorf("failed to parse CSV %s: %w", fileHeader.Filename, err)
	}

	upload.Filename = fileHeader.Filename
	if len(upload.ASINs) == 0 {
		return nil, fmt.Errorf("no valid ASINs found in %s (%d rows read, %d rejected)", fileHeader.Filename, upload.DataRows, upload.InvalidCount)
	}

	return upload, nil
}

// parseManualASINCSV reads ASINs from a CSV stream. Blank rows and rows whose
// ASIN column is empty are skipped; malformed values are collected in Invalid
// rather than failing the whole upload, so one bad row does not cost the caller
// the run. Duplicates are dropped, keeping first-seen order.
func parseManualASINCSV(r io.Reader) (*ManualASINUpload, error) {
	reader := csv.NewReader(r)
	reader.FieldsPerRecord = -1 // rows may be ragged; only the ASIN column matters
	reader.TrimLeadingSpace = true

	upload := &ManualASINUpload{}
	seen := make(map[string]bool)
	column := 0
	firstRecord := true

	for {
		record, err := reader.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		if firstRecord {
			firstRecord = false
			// Strip a UTF-8 BOM left by Excel exports.
			if len(record) > 0 {
				record[0] = strings.TrimPrefix(record[0], utf8BOM)
			}
			if idx := asinHeaderColumn(record); idx >= 0 {
				column = idx
				continue // header row, not data
			}
		}

		if column >= len(record) {
			continue
		}

		raw := strings.TrimSpace(record[column])
		if raw == "" {
			continue
		}
		upload.DataRows++

		asin := strings.ToUpper(raw)
		if !asinPattern.MatchString(asin) {
			upload.InvalidCount++
			if len(upload.Invalid) < maxInvalidASINSamples {
				upload.Invalid = append(upload.Invalid, raw)
			}
			continue
		}

		if seen[asin] {
			upload.Duplicates++
			continue
		}
		seen[asin] = true
		upload.ASINs = append(upload.ASINs, asin)
	}

	upload.Valid = len(upload.ASINs)
	return upload, nil
}

// asinSourceLabel describes where a run's ASINs came from, for logs, status
// payloads and the Discord report.
func asinSourceLabel(isManual bool, filename string) string {
	if !isManual {
		return "Database"
	}
	if filename != "" {
		return fmt.Sprintf("Manual CSV (%s)", filename)
	}
	return "Manual CSV"
}

// asinHeaderColumn returns the index of the ASIN column if the record looks like
// a header row, or -1 if it does not.
func asinHeaderColumn(record []string) int {
	for i, cell := range record {
		if asinHeaderNames[strings.ToLower(strings.TrimSpace(cell))] {
			return i
		}
	}
	return -1
}
