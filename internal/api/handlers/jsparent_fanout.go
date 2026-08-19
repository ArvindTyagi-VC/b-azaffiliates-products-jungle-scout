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

// salesFanOutMode selects which slice of a parent's sales history is copied onto
// its children.
//
// Background: this query originally had no date bound at all, so every run
// re-copied each parent's entire sales history onto every child — roughly a
// full-table rewrite (~34M upserts in production) to deliver whatever the API had
// just returned, which for an incremental fetch is about ten days. The cost also
// grew with every month of accumulated history.
//
// Bounding it by date fixes that, but the unbounded copy had one accidental
// virtue: it repaired gaps. A child added to the mapping table after its parent
// was last fetched, or one whose earlier fan-out failed, got its full history
// filled in on the next run. A date bound alone would silently strand those
// children with only the newest window, permanently. Hence two modes: the bounded
// copy for the normal case, and an explicit backfill for children that have no
// rows yet.
//
// Note the asymmetry with productFanOutQuery, which needs no equivalent: product
// rows are point-in-time snapshots keyed by report_date, so a new child correctly
// starts from the current run's snapshot and has no history to recover.
type salesFanOutMode int

const (
	// salesFanOutIncremental copies rows dated at or after a lower bound onto all
	// visible children. Params: $1 = parent ASIN, $2 = marketplace, $3 = min date.
	salesFanOutIncremental salesFanOutMode = iota

	// salesFanOutBackfill copies the parent's full history, but only onto children
	// that currently have no sales rows at all. Params: $1 = parent, $2 = marketplace.
	salesFanOutBackfill

	// salesFanOutAll copies the parent's full history onto every visible child —
	// the original unbounded behaviour. Retained for the master/manual sync paths,
	// which do a deliberate full resync and do not track a per-ASIN fetch window,
	// so narrowing them here would change what those endpoints do.
	// Params: $1 = parent, $2 = marketplace.
	salesFanOutAll
)

// salesFanOutQuery builds the parent -> children copy for sales estimate data.
func salesFanOutQuery(t syncTables, mode salesFanOutMode) string {
	// Extra predicate per mode. Both keep the row set proportional to real work:
	// the incremental bound to the window just fetched, the backfill to children
	// that have nothing.
	var scope string
	switch mode {
	case salesFanOutBackfill:
		scope = fmt.Sprintf(`
		  AND NOT EXISTS (
			  SELECT 1 FROM %[1]s existing
			  WHERE existing.asin = m.child_asin
				AND existing.marketplace = s.marketplace
		  )`, t.sales)
	case salesFanOutAll:
		scope = "" // no extra bound: the parent's whole history, every child
	default:
		scope = `
		  AND s.date >= $3`
	}

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
		  AND %[7]s%[8]s
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
		scope,
	)
}

// childrenMissingSalesQuery counts visible children of a parent that have no sales
// rows at all, i.e. those a date-bounded fan-out would leave stranded.
func childrenMissingSalesQuery(t syncTables) string {
	return fmt.Sprintf(`
		SELECT COUNT(*)
		FROM %[1]s m
		JOIN %[2]s a ON a.asin = m.child_asin AND a.asin_visibility = true
		WHERE m.parent_asin = $1
		  AND m.child_asin <> m.parent_asin
		  AND %[4]s
		  AND NOT EXISTS (
			  SELECT 1 FROM %[3]s s
			  WHERE s.asin = m.child_asin AND s.marketplace = $2
		  )
	`, t.mapping, t.asin, t.sales, notItselfAParent(t, "m.child_asin"))
}

// fanOutSalesRows copies a parent's sales rows onto its visible children.
//
// minDate bounds the copy to the window just fetched. Pass an empty string for the
// original unbounded copy onto every child — what the master and manual sync paths
// do, since they are deliberate full resyncs and do not track a per-ASIN window.
//
// Children that have no sales rows yet are handled separately: they get the
// parent's full history regardless of minDate, because for them there is no
// earlier run to have filled it in. That second statement only runs when such a
// child actually exists, so the common case stays a single bounded insert.
func fanOutSalesRows(pg *database.PostgreSQLClient, t syncTables, parentASIN, marketplace, minDate string) (int64, error) {
	var total int64

	if minDate == "" {
		res, err := pg.DB.Exec(salesFanOutQuery(t, salesFanOutAll), parentASIN, marketplace)
		if err != nil {
			return 0, fmt.Errorf("sales fan-out (full history) for parent %s failed: %w", parentASIN, err)
		}
		n, _ := res.RowsAffected()
		return n, nil
	}

	res, err := pg.DB.Exec(salesFanOutQuery(t, salesFanOutIncremental), parentASIN, marketplace, minDate)
	if err != nil {
		return 0, fmt.Errorf("sales fan-out for parent %s failed: %w", parentASIN, err)
	}
	n, _ := res.RowsAffected()
	total += n

	// Only pay for the backfill statement when a child genuinely has nothing.
	var missing int
	if err := pg.DB.QueryRow(childrenMissingSalesQuery(t), parentASIN, marketplace).Scan(&missing); err != nil {
		// Not fatal: the bounded copy above already succeeded. Report it so a
		// persistent failure here is visible rather than quietly skipping backfills.
		return total, fmt.Errorf("could not check for children missing sales history under parent %s: %w", parentASIN, err)
	}
	if missing == 0 {
		return total, nil
	}

	backfillRes, err := pg.DB.Exec(salesFanOutQuery(t, salesFanOutBackfill), parentASIN, marketplace)
	if err != nil {
		return total, fmt.Errorf("sales backfill fan-out for parent %s (%d children with no history) failed: %w",
			parentASIN, missing, err)
	}
	bn, _ := backfillRes.RowsAffected()
	return total + bn, nil
}
