// internal/utils/id_generator.go
package utils

import (
	"crypto/md5"
	"fmt"
	"math/rand"
	"time"
)

func GenerateCreativeID() string {
	// Define allowed characters (only alphanumeric)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Create a hash of the current time + random number for uniqueness
	now := time.Now().UnixNano()
	source := rand.NewSource(now)
	random := rand.New(source)
	randNum := random.Int63()

	// Generate a hash using MD5
	hashInput := fmt.Sprintf("%d%d", now, randNum)
	hash := md5.Sum([]byte(hashInput))

	// Create the 32-character string using the hash bytes to select from charset
	result := make([]byte, 32)

	// Use hash bytes for the first 16 characters
	for i := 0; i < 16 && i < len(hash); i++ {
		charIndex := int(hash[i]) % len(charset)
		result[i] = charset[charIndex]
	}

	// Use additional randomness for the remaining characters
	for i := 16; i < 32; i++ {
		result[i] = charset[random.Intn(len(charset))]
	}

	return string(result)
}

// GenerateCreativeID generates a creative ID
/*
func GenerateCreativeID() string {
	// Generate a globally unique ID and strip any hyphens
	guid := xid.New().String()
	return strings.ReplaceAll(guid, "-", "") + strconv.FormatInt(time.Now().UnixNano()%100000, 10)
}
*/
// GenerateEncodedID generates a unique alphanumeric encoded ID
/*
func GenerateEncodedID(prefix string) string {
	// Create a hash of the current time + random number
	now := time.Now().UnixNano()
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	randNum := random.Int63()
	// Generate a hash using MD5
	hashInput := fmt.Sprintf("%d%d", now, randNum)
	hash := md5.Sum([]byte(hashInput))

	// Convert first 8 bytes to base64 and make URL-safe
	encoded := base64.URLEncoding.EncodeToString(hash[:8])
	encoded = strings.ReplaceAll(encoded, "-", "a")
	encoded = strings.ReplaceAll(encoded, "_", "b")

	// Prefix + first 12 chars of encoded string
	if len(encoded) > 12 {
		encoded = encoded[:12]
	}

	return prefix + encoded
}
*/

// GenerateEncodedID generates a unique alphanumeric encoded ID with the specified length
// prefix will be the first character, and the total length will be (1 + length)
// If length is 0 or negative, it defaults to 8 (resulting in 9 characters total)
func GenerateEncodedID(prefix string, length int) string {
	// Default length if invalid
	if length <= 0 {
		length = 8 // Default to 8 chars + 1 prefix = 9 total
	}

	// Create a hash of the current time + random number
	now := time.Now().UnixNano()
	source := rand.NewSource(time.Now().UnixNano())
	random := rand.New(source)
	randNum := random.Int63()

	// Generate a hash using MD5
	hashInput := fmt.Sprintf("%d%d", now, randNum)
	hash := md5.Sum([]byte(hashInput))

	// Define allowed characters (only alphanumeric)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Create a string of the specified length using the hash bytes to select from charset
	result := make([]byte, length)

	// Use hash bytes first
	for i := 0; i < length && i < len(hash); i++ {
		charIndex := int(hash[i]) % len(charset)
		result[i] = charset[charIndex]
	}

	// If we need more characters than hash provides, use random generation
	for i := len(hash); i < length; i++ {
		result[i] = charset[random.Intn(len(charset))]
	}

	// Ensure prefix is only one character
	if len(prefix) != 1 {
		prefix = "x"
	}

	return prefix + string(result)
}

func GenerateAdGroupID(affiliateID string, asin string, trafficSourceID int) string {
	// We'll use a more direct, reliable approach that ensures the ID is both unique and decodable

	// Create a short identifier for each component
	// Limit lengths to ensure we have space for everything
	const maxAffiliateLen = 10 // Max length for affiliate ID portion
	const maxAsinLen = 10      // Max length for ASIN portion

	// Trim inputs if needed
	if len(affiliateID) > maxAffiliateLen {
		affiliateID = affiliateID[:maxAffiliateLen]
	}

	if len(asin) > maxAsinLen {
		asin = asin[:maxAsinLen]
	}

	// Generate a timestamp-based unique component
	now := time.Now().UnixNano()

	// Create a hash to add randomness
	hashInput := fmt.Sprintf("%s%s%d%d", affiliateID, asin, trafficSourceID, now)
	hash := md5.Sum([]byte(hashInput))

	// Define allowed characters (only alphanumeric)
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"

	// Generate a randomized string from the hash (15 chars)
	randomPart := make([]byte, 15)
	for i := 0; i < 15 && i < len(hash); i++ {
		charIndex := int(hash[i]) % len(charset)
		randomPart[i] = charset[charIndex]
	}

	// Format the components in a way that's both human-readable and decodable
	// Pattern: ag-{affiliateID}-{asin}-{trafficSourceID}-{randomPart}
	// Replace "-" with "X" to keep it alphanumeric
	idFormat := fmt.Sprintf("ag%sX%sX%dX%s",
		affiliateID,
		asin,
		trafficSourceID,
		string(randomPart))

	// Ensure ID is exactly 45 characters
	if len(idFormat) > 45 {
		return idFormat[:45]
	} else if len(idFormat) < 45 {
		// Pad with random alphanumeric characters if needed
		extraPad := GenerateEncodedID("x", 45-len(idFormat))[1:] // Remove the prefix
		idFormat += extraPad
	}

	return idFormat
}
