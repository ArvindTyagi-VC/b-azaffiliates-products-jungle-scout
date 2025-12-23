package junglescout

// ProductAttributes represents the attributes of a product from JungleScout API
type ProductAttributes struct {
	Title                         string                   `json:"title"`
	Price                         float64                  `json:"price"`
	Reviews                       int                      `json:"reviews"`
	Category                      string                   `json:"category"`
	Rating                        float64                  `json:"rating"`
	ImageURL                      string                   `json:"image_url"`
	ParentASIN                    string                   `json:"parent_asin"`
	IsVariant                     bool                     `json:"is_variant"`
	SellerType                    string                   `json:"seller_type"`
	Variants                      []string                 `json:"variants"`
	BreadcrumbPath                string                   `json:"breadcrumb_path"`
	IsStandalone                  bool                     `json:"is_standalone"`
	IsParent                      bool                     `json:"is_parent"`
	IsAvailable                   bool                     `json:"is_available"`
	Brand                         string                   `json:"brand"`
	ProductRank                   *int                     `json:"product_rank"`
	WeightValue                   float64                  `json:"weight_value"`
	WeightUnit                    string                   `json:"weight_unit"`
	LengthValue                   float64                  `json:"length_value"`
	WidthValue                    float64                  `json:"width_value"`
	HeightValue                   float64                  `json:"height_value"`
	DimensionsUnit                string                   `json:"dimensions_unit"`
	ListingQualityScore           *int                     `json:"listing_quality_score"`
	NumberOfSellers               *int                     `json:"number_of_sellers"`
	BuyBoxOwner                   string                   `json:"buy_box_owner"`
	BuyBoxOwnerSellerID           string                   `json:"buy_box_owner_seller_id"`
	DateFirstAvailable            string                   `json:"date_first_available"`
	DateFirstAvailableIsEstimated bool                     `json:"date_first_available_is_estimated"`
	Approximate30DayRevenue       float64                  `json:"approximate_30_day_revenue"`
	Approximate30DayUnitsSold     int                      `json:"approximate_30_day_units_sold"`
	SubcategoryRanks              []map[string]interface{} `json:"subcategory_ranks"`
	FeeBreakdown                  map[string]interface{}   `json:"fee_breakdown"`
	EANList                       []string                 `json:"ean_list"`
	ISBNList                      []string                 `json:"isbn_list"`
	UPCList                       []string                 `json:"upc_list"`
	GTINList                      []string                 `json:"gtin_list"`
	VariantReviews                *int                     `json:"variant_reviews"`
	UpdatedAt                     string                   `json:"updated_at"`
}

// ProductData represents a single product from the API response
type ProductData struct {
	ID         string            `json:"id"`
	Type       string            `json:"type"`
	Attributes ProductAttributes `json:"attributes"`
}

// ProductAPIResponse represents the response from the product database API
type ProductAPIResponse struct {
	Data []ProductData `json:"data"`
}

// SalesEstimateDataPoint represents a single day's sales data
type SalesEstimateDataPoint struct {
	Date               string  `json:"date"`
	EstimatedUnitsSold int     `json:"estimated_units_sold"`
	LastKnownPrice     float64 `json:"last_known_price"`
}

// SalesEstimateAttributes represents the attributes of sales estimate data
type SalesEstimateAttributes struct {
	ASIN         string                   `json:"asin"`
	IsParent     bool                     `json:"is_parent"`
	IsVariant    bool                     `json:"is_variant"`
	IsStandalone bool                     `json:"is_standalone"`
	ParentASIN   string                   `json:"parent_asin"`
	Variants     []string                 `json:"variants"`
	Data         []SalesEstimateDataPoint `json:"data"`
}

// SalesEstimateData represents sales estimate data from the API
type SalesEstimateData struct {
	ID         string                  `json:"id"`
	Type       string                  `json:"type"`
	Attributes SalesEstimateAttributes `json:"attributes"`
}

// SalesEstimateAPIResponse represents the response from the sales estimates API
type SalesEstimateAPIResponse struct {
	Data []SalesEstimateData `json:"data"`
}