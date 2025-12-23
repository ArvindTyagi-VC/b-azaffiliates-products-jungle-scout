package utils

import (
	"azaffiliates/internal/database"
	"fmt"

	_ "github.com/lib/pq"
)

func GetTableName(pgClient *database.PostgreSQLClient, metaTable string) (string, error) {
	var tableName string
	metaTable = "frontend_" + metaTable
	metaTable = pgClient.TableName(metaTable)
	query := fmt.Sprintf("SELECT table_name FROM %s LIMIT 1", metaTable)

	err := pgClient.DB.QueryRow(query).Scan(&tableName)
	if err != nil {
		return "", err
	}
	return tableName, nil
}
