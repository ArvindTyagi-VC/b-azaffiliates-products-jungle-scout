// internal/auth/apikey.go
package auth

import (
	"net/http"
	"os"

	"github.com/gin-gonic/gin"
)

// APIKeyAuth middleware validates requests using X-API-KEY header
// against the SYNC_API_KEY environment variable
func APIKeyAuth() gin.HandlerFunc {
	return func(c *gin.Context) {
		apiKey := c.GetHeader("X-API-KEY")
		if apiKey == "" {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "X-API-KEY header required",
			})
			c.Abort()
			return
		}

		expectedKey := os.Getenv("SYNC_API_KEY")
		if expectedKey == "" {
			c.JSON(http.StatusInternalServerError, gin.H{
				"error": "SYNC_API_KEY not configured on server",
			})
			c.Abort()
			return
		}

		if apiKey != expectedKey {
			c.JSON(http.StatusUnauthorized, gin.H{
				"error": "Invalid API key",
			})
			c.Abort()
			return
		}

		c.Next()
	}
}
