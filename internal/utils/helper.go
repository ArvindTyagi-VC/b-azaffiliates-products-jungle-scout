package utils

import (
	"database/sql"
	"fmt"
	"strconv"
)

func GetGroupColumns(groupBy string) []string {
	switch groupBy {
	case "asin":
		return []string{"group_value", "product_title", "product_image"}
	case "store_id":
		return []string{"group_value", "store_id"}
	default:
		return []string{"group_value"}
	}
}

func NullIntToStr(ni sql.NullInt64) string {
	if ni.Valid {
		return strconv.FormatInt(ni.Int64, 10)
	}
	return "0"
}

func NullFloatToStr(nf sql.NullFloat64) string {
	if nf.Valid {
		return fmt.Sprintf("%.2f", nf.Float64)
	}
	return "0.00"
}

func NullToStr(ns sql.NullString) string {
	if ns.Valid {
		return ns.String
	}
	return ""
}