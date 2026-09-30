package jsstore

import (
	"strings"
	"testing"

	"azaffiliates/internal/junglescout"
)

func TestProductUpsertManySQL(t *testing.T) {
	q := ProductUpsertManySQL("dev_az_jungle_scout_product_data", 3)
	if strings.Count(q, "INSERT INTO") != 1 || strings.Count(q, "ON CONFLICT") != 1 {
		t.Fatalf("want one statement:\n%s", q)
	}
	if !strings.Contains(q, "$1, $2") || !strings.Contains(q, "$126)") || strings.Contains(q, "$127") {
		t.Fatalf("placeholders for 3 x 42 columns are wrong:\n%s", q)
	}
	if got := len(ProductArgs(junglescout.ProductData{ID: "us/B0A"}, "2026-09-30")); got != productColumns {
		t.Fatalf("ProductArgs has %d values, want %d", got, productColumns)
	}
	single := ProductUpsertSQL("t")
	many := ProductUpsertManySQL("t", 1)
	norm := func(s string) string {
		s = strings.Join(strings.Fields(s), " ")
		return strings.NewReplacer("( ", "(", " )", ")").Replace(s)
	}
	if norm(single) != norm(many) {
		t.Fatalf("one-row statement differs from the single upsert:\n%s\n---\n%s", norm(single), norm(many))
	}
}
