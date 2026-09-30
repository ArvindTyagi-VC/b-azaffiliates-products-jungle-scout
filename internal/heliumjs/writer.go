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
	return w.inTx(ctx, func(tx *sql.Tx) error {
		query := jsstore.ProductUpsertSQL(w.pg.TableName(productTable))
		for _, p := range products {
			asin := jsstore.ProductASIN(p)
			if _, err := tx.ExecContext(ctx, query, jsstore.ProductArgs(p, reportDate)...); err != nil {
				return fmt.Errorf("product %s: %w", asin, err)
			}
			if err := jsstore.MarkHeliumWritten(ctx, tx, w.marker(), productTable, asin, "", reportDate, reportDate); err != nil {
				return fmt.Errorf("mark product %s: %w", asin, err)
			}
		}
		return nil
	})
}

func (w stagingWriter) sales(ctx context.Context, series junglescout.SalesEstimateAttributes) error {
	if len(series.Data) == 0 {
		return nil
	}
	from, to := series.Data[0].Date, series.Data[0].Date
	for _, d := range series.Data {
		if d.Date < from {
			from = d.Date
		}
		if d.Date > to {
			to = d.Date
		}
	}
	return w.inTx(ctx, func(tx *sql.Tx) error {
		if _, err := jsstore.UpsertSales(ctx, tx, w.pg.TableName(salesTable), marketplace, series); err != nil {
			return err
		}
		return jsstore.MarkHeliumWritten(ctx, tx, w.marker(), salesTable, series.ASIN, marketplace, from, to)
	})
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
