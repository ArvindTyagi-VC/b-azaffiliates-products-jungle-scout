package utils

// RoleDesignationMap maps roles to their designations
var RoleDesignationMap = map[string]string{
	"afm":        "Affiliate Manager",
	"subadmin":   "Sub Admin",
	"admin":      "Admin",
	"publisher":  "Affiliate",
	"advertiser": "Advertiser",
}

// GetDesignation returns the designation for a given role,
// or "User" if the role is not found in the map.
func GetDesignation(role string) string {
	if designation, ok := RoleDesignationMap[role]; ok {
		return designation
	}
	return "User"
}
