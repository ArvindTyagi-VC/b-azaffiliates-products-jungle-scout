// internal/auth/jwt.go
package auth

import (
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/golang-jwt/jwt/v4"
)

// JWT secret key - in production, store this in environment variables or secret management
var jwtSecret = []byte(os.Getenv("JWT_SECRET"))

// Ensure JWT_SECRET is set
func init() {
	if len(jwtSecret) == 0 {
		panic("JWT_SECRET environment variable is not set")
	} else {
		//fmt.println("Using JWT secret from environment variable, value:", string(jwtSecret[:10]), "...") // Log only the first 10 characters for security
	}
}

// Claims represents the JWT claims
type Claims struct {
	UserID    string `json:"user_id"`
	Role      string `json:"role"`
	VisibleID string `json:"visible_id"`
	jwt.RegisteredClaims
}

// GenerateToken creates a new JWT token for a user
// internal/auth/jwt.go
// Modify the GenerateToken function to reduce token lifetime for testing

func GenerateToken(userID, role, visibleID string) (string, error) {
	// Set expiration time to 12 hours for local development
	// IMPORTANT: Change this back to a shorter duration for production!
	expirationTime := time.Now().Add(12 * time.Hour)

	// Create claims
	claims := &Claims{
		UserID:    userID,
		Role:      role,
		VisibleID: visibleID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "amazon-affiliate-backend",
		},
	}

	// Create token with claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Generate signed token
	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

// GenerateServiceToken creates a long-lived JWT token for service accounts
// This token is intended for machine-to-machine communication like Zoho Analytics integration
func GenerateServiceToken(userID, role, visibleID string, validityDays int) (string, error) {
	// Set long expiration time (default 365 days if not specified)
	if validityDays <= 0 {
		validityDays = 365
	}
	expirationTime := time.Now().Add(time.Duration(validityDays) * 24 * time.Hour)

	// Create claims with service indicator
	claims := &Claims{
		UserID:    userID,
		Role:      role,
		VisibleID: visibleID,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expirationTime),
			IssuedAt:  jwt.NewNumericDate(time.Now()),
			NotBefore: jwt.NewNumericDate(time.Now()),
			Issuer:    "amazon-affiliate-backend",
			Subject:   "service-account", // Indicates this is a service account token
		},
	}

	// Create token with claims
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)

	// Generate signed token
	tokenString, err := token.SignedString(jwtSecret)
	if err != nil {
		return "", err
	}

	return tokenString, nil
}

// JWTAuth is a middleware for JWT authentication
func JWTAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get token from Authorization header
		authHeader := c.GetHeader("Authorization")
		if authHeader == "" {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization header required"})
			c.Abort()
			return
		}

		// Check if format is Bearer
		if !strings.HasPrefix(authHeader, "Bearer ") {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Authorization header must be Bearer token"})
			c.Abort()
			return
		}

		tokenString := strings.Replace(authHeader, "Bearer ", "", 1)

		// Parse and validate token
		claims := &Claims{}
		token, err := jwt.ParseWithClaims(tokenString, claims, func(token *jwt.Token) (interface{}, error) {
			// Validate signing method
			if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
				return nil, fmt.Errorf("unexpected signing method: %v", token.Header["alg"])
			}

			// Return the secret key
			return jwtSecret, nil
		})

		if err != nil {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token: " + err.Error()})
			c.Abort()
			return
		}

		// Check if token is valid
		if !token.Valid {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Invalid token"})
			c.Abort()
			return
		}

		// Add claims to context
		c.Set("userID", claims.UserID)
		c.Set("role", claims.Role)
		c.Set("visibleID", claims.VisibleID)

		c.Next()
	}
}

func ManagerOrAdminRoleCheck() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get role from context (set by JWTAuth middleware)
		role, exists := c.Get("role")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}

		// Check if role is manager or subadmin
		if role != "afm" && role != "subadmin" && role != "admin" {
			c.JSON(http.StatusOK, gin.H{"affiliate_id": c.GetString("visibleID")})
			c.Abort()
			return
		}

		c.Next()
	}
}

// AdminRoleCheck checks if the user has admin role
func AdminRoleCheck() gin.HandlerFunc {
	return func(c *gin.Context) {
		// Get role from context (set by JWTAuth middleware)
		role, exists := c.Get("role")
		if !exists {
			c.JSON(http.StatusUnauthorized, gin.H{"error": "Unauthorized"})
			c.Abort()
			return
		}

		// Check if role is admin
		if role != "subadmin" && role != "admin" {
			c.JSON(http.StatusForbidden, gin.H{"error": "Admin privileges required"})
			c.Abort()
			return
		}

		c.Next()
	}
}
