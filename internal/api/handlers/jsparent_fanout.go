package handlers

import (
	"fmt"
	"strings"

	"azaffiliates/internal/database"
	"azaffiliates/internal/utils"
)

// ============================================================================
// PARENT-ONLY SYNC + CHILD FAN-OUT
// ============================================================================
//
// The JungleScout sync fetches data for PARENT ASINs only. Every parent row that
// lands in jungle_scout_product_data / jungle_scout_sales_estimate_data is then
// copied verbatim onto each of that parent's visible children, as listed in the
// {prefix}parent_asin mapping table.
//
// Mapping table shape ({prefix}parent_asin):
//   geo_id, parent_asin, child_asin, is_primary, effdtm, is_available, is_fod
//   UNIQUE (parent_asin, child_asin) and UNIQUE (child_asin, geo_id)
//   => a child belongs to exactly one parent per geo.
//
// Rows where parent_asin = child_asin are standalone products mapped to
// themselves. They are excluded from the fan-out (a row would be copied onto
// itself); the parent fetch already wrote that ASIN's own row.
//
// ALL data columns are copied, including the ones that genuinely differ between
// variants (price, reviews, rating, product_rank, buy_box_owner, number_of_sellers,
// approximate_30_day_revenue/units, estimated_units_sold, last_known_price). This
// is intentional and was explicitly requested. Consequences to be aware of:
//   - Child price/reviews/rating reflect the PARENT listing, not the variant.
//   - A parent's estimated_units_sold is the aggregate across all its variants,
//     so summing units across children multiplies the true total by the number
//     of children.
//
// Only five columns are NOT copied verbatim, because they identify the row rather
// than describe the product:
//   asin        -> the child ASIN (primary key component)
//   parent_asin -> the parent the row was copied from (lineage)
//   is_variant  -> true   (the row now describes a variant)
//   is_parent   -> false
//   created_at  -> CURRENT_TIMESTAMP
//
// NOTE ON GEO: the mapping table carries geo_id, but the sync as a whole has no
// geo concept (fetchParentASINsToSync, cleanupSyncStatus and the fan-out all
// ignore it), matching the pre-existing behaviour of this package. Today the
// mapping is 100% geo_id='US'. When a second geo is onboarded, every query in
// this file plus the ASIN selection needs a geo_id filter, otherwise children
// from one geo will be fed from another geo's parent.

// parentMappingTable is the unprefixed name of the parent -> child mapping table.
const parentMappingTable = "parent_asin"

// productCopyColumns lists the jungle_scout_product_data columns copied verbatim
// from the parent row onto each child row. asin, parent_asin, is_variant,
// is_parent, report_date and created_at are handled separately (see file header).
var productCopyColumns = []string{
	"id", "title", "price", "reviews", "category", "rating", "image_url",
	"seller_type", "variants", "breadcrumb_path", "is_standalone", "is_available",
	"brand", "product_rank", "weight_value", "weight_unit", "length_value",
	"width_value", "height_value", "dimensions_unit", "listing_quality_score",
	"number_of_sellers", "buy_box_owner", "buy_box_owner_seller_id",
	"date_first_available", "date_first_available_is_estimated",
	"approximate_30_day_revenue", "approximate_30_day_units_sold",
	"subcategory_ranks", "fee_breakdown", "ean_list", "isbn_list", "upc_list",
	"gtin_list", "variant_reviews", "updated_at",
}

// salesCopyColumns lists the jungle_scout_sales_estimate_data columns copied
// verbatim. marketplace and date are carried over from the parent row as part of
// the key; asin, parent_asin, is_parent, is_variant and the timestamps are set
// explicitly.
var salesCopyColumns = []string{
	"is_standalone", "estimated_units_sold", "last_known_price",
}

// syncTables holds the resolved table names needed by the parent-only sync.
// Resolve once per batch (asin table name costs a round-trip) and reuse.
type syncTables struct {
	product string
	sales   string
	mapping string
	asin    string // active ASIN table, resolved via {prefix}frontend_asin
}

// resolveSyncTables looks up every table name the fan-out needs for one database.
func resolveSyncTables(pg *database.PostgreSQLClient) (syncTables, error) {
	asinTable, err := utils.GetTableName(pg, "asin")
	if err != nil {
		return syncTables{}, fmt.Errorf("failed to resolve active ASIN table: %w", err)
	}
	return syncTables{
		product: pg.TableName("jungle_scout_product_data"),
		sales:   pg.TableName("jungle_scout_sales_estimate_data"),
		mapping: pg.TableName(parentMappingTable),
		asin:    asinTable,
	}, nil
}

// prefixed returns "alias.col, alias.col, ..." for a SELECT list.
func prefixed(alias string, cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = alias + "." + c
	}
	return strings.Join(out, ", ")
}

// excluded returns "col = EXCLUDED.col, ..." for an ON CONFLICT DO UPDATE list.
func excluded(cols []string) string {
	out := make([]string, len(cols))
	for i, c := range cols {
		out[i] = c + " = EXCLUDED." + c
	}
	return strings.Join(out, ", ")
}

// parentASINSourceSQL returns a SELECT yielding the distinct set of ASINs the
// sync must fetch from JungleScout, as a single "asin" column:
//
//  1. every parent that has at least one visible child, plus
//  2. every visible ASIN absent from the mapping table, treated as its own
//     parent so that coverage is not lost.
//
// Branch 2 matters: ~2k visible ASINs have no mapping row, and without it they
// would never be fetched. Note that branch 1 deliberately includes parents whose
// own asin_visibility is false — a hidden parent still has to be fetched to feed
// its visible children.
func parentASINSourceSQL(t syncTables) string {
	return fmt.Sprintf(`
		SELECT DISTINCT m.parent_asin AS asin
		FROM %[1]s m
		JOIN %[2]s a ON a.asin = m.child_asin
		WHERE a.asin_visibility = true
		  AND m.parent_asin IS NOT NULL
		  AND m.parent_asin != ''
		UNION
		SELECT a.asin
		FROM %[2]s a
		WHERE a.asin_visibility = true
		  AND a.asin IS NOT NULL
		  AND a.asin != ''
		  AND NOT EXISTS (
			  SELECT 1 FROM %[1]s m2 WHERE m2.child_asin = a.asin
		  )
	`, t.mapping, t.asin)
}

// productFanOutQuery builds the parent -> children copy for product data.
// Params: $1 = parent ASIN, $2 = report_date.
func productFanOutQuery(t syncTables) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]s (
			asin, report_date, parent_asin, is_variant, is_parent, created_at, %[2]s
		)
		SELECT m.child_asin, p.report_date, p.asin, true, false, CURRENT_TIMESTAMP, %[3]s
		FROM %[1]s p
		JOIN %[4]s m ON m.parent_asin = p.asin
		JOIN %[5]s a ON a.asin = m.child_asin AND a.asin_visibility = true
		WHERE p.asin = $1
		  AND p.report_date = $2
		  AND m.child_asin <> p.asin
		  AND %[7]s
		ON CONFLICT (asin, report_date) DO UPDATE SET
			parent_asin = EXCLUDED.parent_asin,
			is_variant = EXCLUDED.is_variant,
			is_parent = EXCLUDED.is_parent,
			%[6]s
	`,
		t.product,
		strings.Join(productCopyColumns, ", "),
		prefixed("p", productCopyColumns),
		t.mapping,
		t.asin,
		excluded(productCopyColumns),
		notItselfAParent(t, "m.child_asin"),
	)
}

// syncStatusCleanupQuery removes sync_status rows that are no longer in the parent
// set. It must be the parent set and NOT "visible ASINs": a parent may itself be
// asin_visibility=false while still needing to be fetched to feed its visible
// children, and child ASINs must never be queued for their own API call.
func syncStatusCleanupQuery(syncTable string, t syncTables) string {
	return fmt.Sprintf(`
		DELETE FROM %s
		WHERE asin NOT IN (%s)
	`, syncTable, parentASINSourceSQL(t))
}

// syncStatusAddNewQuery queues parent ASINs that are not in sync_status yet.
// Children are deliberately never queued — they are filled in by the fan-out.
func syncStatusAddNewQuery(syncTable string, t syncTables) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]s (asin, has_product_data, has_sales_data, updated_at)
		SELECT src.asin, false, false, CURRENT_TIMESTAMP
		FROM (%[2]s) src
		WHERE src.asin NOT IN (SELECT asin FROM %[1]s)
	`, syncTable, parentASINSourceSQL(t))
}

// notItselfAParent returns a SQL predicate that excludes a candidate child ASIN
// which is ITSELF a parent of at least one visible child.
//
// The mapping is not a clean two-level tree: ~484 ASINs appear both as a parent
// (of visible children) and as a child of some other parent. Such an ASIN is
// fetched from JungleScout directly, because its own children depend on it. If
// the fan-out were also allowed to target it, the other parent's copied data
// would overwrite its real fetched data, and which value won would depend on
// batch ordering. Directly-fetched data always wins, so these are skipped.
//
// childCol is the qualified column holding the candidate child ASIN, e.g. "m.child_asin".
func notItselfAParent(t syncTables, childCol string) string {
	return fmt.Sprintf(`
		NOT EXISTS (
			SELECT 1
			FROM %[1]s m2
			JOIN %[2]s a2 ON a2.asin = m2.child_asin AND a2.asin_visibility = true
			WHERE m2.parent_asin = %[3]s
			  AND m2.child_asin <> m2.parent_asin
		)`, t.mapping, t.asin, childCol)
}

// fanOutProductRow copies a freshly written parent row in jungle_scout_product_data
// onto every visible child of that parent, for the given report_date.
// Returns the number of child rows written.
func fanOutProductRow(pg *database.PostgreSQLClient, t syncTables, parentASIN, reportDate string) (int64, error) {
	res, err := pg.DB.Exec(productFanOutQuery(t), parentASIN, reportDate)
	if err != nil {
		return 0, fmt.Errorf("product fan-out for parent %s failed: %w", parentASIN, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}

// salesFanOutQuery builds the parent -> children copy for sales estimate data.
// Params: $1 = parent ASIN, $2 = marketplace.
func salesFanOutQuery(t syncTables) string {
	return fmt.Sprintf(`
		INSERT INTO %[1]s (
			asin, marketplace, date, parent_asin, is_parent, is_variant, created_at, %[2]s
		)
		SELECT m.child_asin, s.marketplace, s.date, s.asin, false, true, CURRENT_TIMESTAMP, %[3]s
		FROM %[1]s s
		JOIN %[4]s m ON m.parent_asin = s.asin
		JOIN %[5]s a ON a.asin = m.child_asin AND a.asin_visibility = true
		WHERE s.asin = $1
		  AND s.marketplace = $2
		  AND m.child_asin <> s.asin
		  AND %[7]s
		ON CONFLICT (asin, marketplace, date) DO UPDATE SET
			parent_asin = EXCLUDED.parent_asin,
			is_parent = EXCLUDED.is_parent,
			is_variant = EXCLUDED.is_variant,
			updated_at = CURRENT_TIMESTAMP,
			%[6]s
	`,
		t.sales,
		strings.Join(salesCopyColumns, ", "),
		prefixed("s", salesCopyColumns),
		t.mapping,
		t.asin,
		excluded(salesCopyColumns),
		notItselfAParent(t, "m.child_asin"),
	)
}

// fanOutSalesRows copies every sales-estimate row of a parent onto each of its
// visible children, for the given marketplace. Returns rows written.
func fanOutSalesRows(pg *database.PostgreSQLClient, t syncTables, parentASIN, marketplace string) (int64, error) {
	res, err := pg.DB.Exec(salesFanOutQuery(t), parentASIN, marketplace)
	if err != nil {
		return 0, fmt.Errorf("sales fan-out for parent %s failed: %w", parentASIN, err)
	}
	n, _ := res.RowsAffected()
	return n, nil
}
