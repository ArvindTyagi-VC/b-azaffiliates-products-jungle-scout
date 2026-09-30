package heliumjs

import (
	"context"
	"fmt"

	"azaffiliates/internal/database"
	"azaffiliates/internal/jsstore"
	"azaffiliates/internal/junglescout"
)

// stagingWriter stores Jungle Scout data in the staging database only; it has
// no production client, so a job can never write production.
type stagingWriter struct {
	pg *database.PostgreSQLClient
}

func (w stagingWriter) products(ctx context.Context, products []junglescout.ProductData, reportDate string) error {
	query := jsstore.ProductUpsertSQL(w.pg.TableName("jungle_scout_product_data"))
	for _, p := range products {
		if _, err := w.pg.DB.ExecContext(ctx, query, jsstore.ProductArgs(p, reportDate)...); err != nil {
			return fmt.Errorf("product %s: %w", jsstore.ProductASIN(p), err)
		}
	}
	return nil
}

func (w stagingWriter) sales(ctx context.Context, series junglescout.SalesEstimateAttributes) error {
	_, err := jsstore.UpsertSales(ctx, w.pg.DB, w.pg.TableName("jungle_scout_sales_estimate_data"), marketplace, series)
	return err
}

// advisoryLocker holds a session advisory lock on the staging database for the
// life of a job.
type advisoryLocker struct {
	pg *database.PostgreSQLClient
}

func (l advisoryLocker) tryLock(ctx context.Context) (func(), bool, error) {
	lock, ok, err := l.pg.TryAdvisoryLock(ctx, jobLockKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	return func() { _ = lock.Release(context.Background()) }, true, nil
}
