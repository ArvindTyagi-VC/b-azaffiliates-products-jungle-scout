package jsstore

import (
	"context"
	"fmt"

	"azaffiliates/internal/junglescout"

	"github.com/lib/pq"
)

// UpsertSales writes one sales_estimates_query series into the
// jungle_scout_sales_estimate_data table named table, in a single statement.
// Existing days are updated and days outside the series are left alone.
func UpsertSales(ctx context.Context, db Execer, table, marketplace string, a junglescout.SalesEstimateAttributes) (int, error) {
	if len(a.Data) == 0 {
		return 0, nil
	}
	res, err := db.ExecContext(ctx, salesUpsertSQL(table), salesArgs(marketplace, a)...)
	if err != nil {
		return 0, err
	}
	n, _ := res.RowsAffected()
	return int(n), nil
}

// UpsertSalesMarked is UpsertSales plus its helium_js_written marker, in one
// statement: one round trip, and the rows and the marker land together.
func UpsertSalesMarked(ctx context.Context, db Execer, table, markerTable, dataTable, marketplace string, a junglescout.SalesEstimateAttributes) error {
	if len(a.Data) == 0 {
		return nil
	}
	query := fmt.Sprintf(`
		WITH written AS (%s RETURNING 1)
		INSERT INTO %s (table_name, asin, marketplace, from_date, to_date)
		SELECT $10, $1, $2, MIN(d)::date, MAX(d)::date FROM unnest($7::text[]) AS d
		ON CONFLICT (table_name, asin, marketplace, from_date, to_date) DO UPDATE
		   SET written_at = NOW(), written_at_local = LOCALTIMESTAMP`, salesUpsertSQL(table), markerTable)
	_, err := db.ExecContext(ctx, query, append(salesArgs(marketplace, a), dataTable)...)
	return err
}

func salesUpsertSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (asin, marketplace, is_parent, is_variant, is_standalone,
		                parent_asin, date, estimated_units_sold, last_known_price)
		SELECT $1, $2, $3, $4, $5, NULLIF($6, ''), u.date::date, u.units, u.price
		  FROM unnest($7::text[], $8::int[], $9::float8[]) AS u(date, units, price)
		ON CONFLICT (asin, marketplace, date) DO UPDATE SET
			is_parent = EXCLUDED.is_parent,
			is_variant = EXCLUDED.is_variant,
			is_standalone = EXCLUDED.is_standalone,
			parent_asin = EXCLUDED.parent_asin,
			estimated_units_sold = EXCLUDED.estimated_units_sold,
			last_known_price = EXCLUDED.last_known_price,
			updated_at = CURRENT_TIMESTAMP`, table)
}

// salesArgs are the salesUpsertSQL arguments: one row per date, the last value
// winning, since one INSERT ... ON CONFLICT cannot touch a row twice.
func salesArgs(marketplace string, a junglescout.SalesEstimateAttributes) []interface{} {
	index := make(map[string]int, len(a.Data))
	var dates []string
	var units []int64
	var prices []float64
	for _, d := range a.Data {
		if i, ok := index[d.Date]; ok {
			units[i], prices[i] = int64(d.EstimatedUnitsSold), d.LastKnownPrice
			continue
		}
		index[d.Date] = len(dates)
		dates = append(dates, d.Date)
		units = append(units, int64(d.EstimatedUnitsSold))
		prices = append(prices, d.LastKnownPrice)
	}
	return []interface{}{a.ASIN, marketplace, a.IsParent, a.IsVariant, a.IsStandalone, a.ParentASIN,
		pq.Array(dates), pq.Array(units), pq.Array(prices)}
}
