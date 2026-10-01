package heliumjs

import (
	"context"
	"database/sql"
	"fmt"

	"azaffiliates/internal/database"
	"azaffiliates/internal/jsstore"
	"azaffiliates/internal/junglescout"
)

const (
	productTable = "jungle_scout_product_data"
	salesTable   = "jungle_scout_sales_estimate_data"
)

// stagingWriter stores Jungle Scout data in the staging database only; it has
// no production client, so a job can never write production. Each write also
// marks its rows in helium_js_written, in the same transaction, so the
// staging -> production publish never picks them up.
type stagingWriter struct {
	pg *database.PostgreSQLClient
}

func (w stagingWriter) products(ctx context.Context, products []junglescout.ProductData, reportDate string) error {
	if len(products) == 0 {
		return nil
	}
	// One statement for the rows and one for their markers, rather than two
	// round trips per product: over the network from Cloud Run that was most
	// of a batch's time.
	seen := make(map[string]bool, len(products))
	var asins []string
	var args []interface{}
	for _, p := range products {
		asin := jsstore.ProductASIN(p)
		if seen[asin] {
			continue
		}
		seen[asin] = true
		asins = append(asins, asin)
		args = append(args, jsstore.ProductArgs(p, reportDate)...)
	}
	return w.inTx(ctx, func(tx *sql.Tx) error {
		query := jsstore.ProductUpsertManySQL(w.pg.TableName(productTable), len(asins))
		if _, err := tx.ExecContext(ctx, query, args...); err != nil {
			return fmt.Errorf("store %d products: %w", len(asins), err)
		}
		if err := jsstore.MarkHeliumWrittenMany(ctx, tx, w.marker(), productTable, asins, "", reportDate, reportDate); err != nil {
			return fmt.Errorf("mark %d products: %w", len(asins), err)
		}
		return nil
	})
}

func (w stagingWriter) sales(ctx context.Context, series junglescout.SalesEstimateAttributes) error {
	return jsstore.UpsertSalesMarked(ctx, w.pg.DB, w.pg.TableName(salesTable), w.marker(), salesTable, marketplace, series)
}

func (w stagingWriter) marker() string { return w.pg.TableName(jsstore.HeliumWrittenTable) }

func (w stagingWriter) inTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := w.pg.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
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
