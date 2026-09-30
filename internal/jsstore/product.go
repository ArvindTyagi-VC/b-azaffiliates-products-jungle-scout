package jsstore

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"azaffiliates/internal/junglescout"
)

// ProductUpsertSQL is the upsert of one product_database_query row into the
// jungle_scout_product_data table named table.
func ProductUpsertSQL(table string) string {
	return fmt.Sprintf(`
		INSERT INTO %s (
			asin, report_date, id, title, price, reviews, category, rating,
			image_url, parent_asin, is_variant, seller_type, variants,
			breadcrumb_path, is_standalone, is_parent, is_available, brand,
			product_rank, weight_value, weight_unit, length_value, width_value,
			height_value, dimensions_unit, listing_quality_score,
			number_of_sellers, buy_box_owner, buy_box_owner_seller_id,
			date_first_available, date_first_available_is_estimated,
			approximate_30_day_revenue, approximate_30_day_units_sold,
			subcategory_ranks, fee_breakdown, ean_list, isbn_list, upc_list,
			gtin_list, variant_reviews, updated_at, created_at
		)
		VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28,
			$29, $30, $31, $32, $33, $34, $35, $36, $37, $38, $39, $40, $41, $42
		)
		ON CONFLICT (asin, report_date) DO UPDATE SET
			title = EXCLUDED.title,
			price = EXCLUDED.price,
			reviews = EXCLUDED.reviews,
			category = EXCLUDED.category,
			rating = EXCLUDED.rating,
			image_url = EXCLUDED.image_url,
			parent_asin = EXCLUDED.parent_asin,
			is_variant = EXCLUDED.is_variant,
			seller_type = EXCLUDED.seller_type,
			variants = EXCLUDED.variants,
			breadcrumb_path = EXCLUDED.breadcrumb_path,
			is_standalone = EXCLUDED.is_standalone,
			is_parent = EXCLUDED.is_parent,
			is_available = EXCLUDED.is_available,
			brand = EXCLUDED.brand,
			product_rank = EXCLUDED.product_rank,
			weight_value = EXCLUDED.weight_value,
			weight_unit = EXCLUDED.weight_unit,
			length_value = EXCLUDED.length_value,
			width_value = EXCLUDED.width_value,
			height_value = EXCLUDED.height_value,
			dimensions_unit = EXCLUDED.dimensions_unit,
			listing_quality_score = EXCLUDED.listing_quality_score,
			number_of_sellers = EXCLUDED.number_of_sellers,
			buy_box_owner = EXCLUDED.buy_box_owner,
			buy_box_owner_seller_id = EXCLUDED.buy_box_owner_seller_id,
			date_first_available = EXCLUDED.date_first_available,
			date_first_available_is_estimated = EXCLUDED.date_first_available_is_estimated,
			approximate_30_day_revenue = EXCLUDED.approximate_30_day_revenue,
			approximate_30_day_units_sold = EXCLUDED.approximate_30_day_units_sold,
			subcategory_ranks = EXCLUDED.subcategory_ranks,
			fee_breakdown = EXCLUDED.fee_breakdown,
			ean_list = EXCLUDED.ean_list,
			isbn_list = EXCLUDED.isbn_list,
			upc_list = EXCLUDED.upc_list,
			gtin_list = EXCLUDED.gtin_list,
			variant_reviews = EXCLUDED.variant_reviews,
			updated_at = EXCLUDED.updated_at
	`, table)
}

// ProductASIN is the ASIN a product row is stored under: the ID, without the
// "us/" style marketplace prefix JungleScout puts on it.
func ProductASIN(p junglescout.ProductData) string {
	if parts := strings.Split(p.ID, "/"); len(parts) == 2 {
		return parts[1]
	}
	return p.ID
}

// ProductArgs are the ProductUpsertSQL arguments for one product.
func ProductArgs(p junglescout.ProductData, reportDate string) []interface{} {
	attrs := p.Attributes
	variantsJSON, _ := json.Marshal(attrs.Variants)
	subcategoryRanksJSON, _ := json.Marshal(attrs.SubcategoryRanks)
	feeBreakdownJSON, _ := json.Marshal(attrs.FeeBreakdown)
	eanListJSON, _ := json.Marshal(attrs.EANList)
	isbnListJSON, _ := json.Marshal(attrs.ISBNList)
	upcListJSON, _ := json.Marshal(attrs.UPCList)
	gtinListJSON, _ := json.Marshal(attrs.GTINList)

	var dateFirstAvailable sql.NullTime
	if attrs.DateFirstAvailable != "" {
		if parsed, err := time.Parse("2006-01-02", attrs.DateFirstAvailable); err == nil {
			dateFirstAvailable = sql.NullTime{Time: parsed, Valid: true}
		}
	}
	var updatedAt sql.NullTime
	if attrs.UpdatedAt != "" {
		if parsed, err := time.Parse(time.RFC3339, attrs.UpdatedAt); err == nil {
			updatedAt = sql.NullTime{Time: parsed, Valid: true}
		}
	}

	return []interface{}{
		ProductASIN(p), reportDate, p.ID, attrs.Title, attrs.Price, attrs.Reviews,
		attrs.Category, attrs.Rating, attrs.ImageURL, attrs.ParentASIN,
		attrs.IsVariant, attrs.SellerType, variantsJSON, attrs.BreadcrumbPath,
		attrs.IsStandalone, attrs.IsParent, attrs.IsAvailable, attrs.Brand,
		attrs.ProductRank, attrs.WeightValue, attrs.WeightUnit, attrs.LengthValue,
		attrs.WidthValue, attrs.HeightValue, attrs.DimensionsUnit,
		attrs.ListingQualityScore, attrs.NumberOfSellers, attrs.BuyBoxOwner,
		attrs.BuyBoxOwnerSellerID, dateFirstAvailable,
		attrs.DateFirstAvailableIsEstimated, attrs.Approximate30DayRevenue,
		attrs.Approximate30DayUnitsSold, subcategoryRanksJSON, feeBreakdownJSON,
		eanListJSON, isbnListJSON, upcListJSON, gtinListJSON,
		attrs.VariantReviews, updatedAt, time.Now(),
	}
}
