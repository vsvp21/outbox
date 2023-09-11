package outbox

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

func NewPgxPersister(db *pgxpool.Pool) *PgxPersister {
	return &PgxPersister{db: db}
}

type PgxPersister struct {
	db *pgxpool.Pool
}

func (r *PgxPersister) PersistInTx(ctx context.Context, fn func(tx pgx.Tx) ([]Message, error)) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("%w: transaction begin failed", err)
	}

	messages, err := fn(tx)
	if err != nil {
		if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
			return fmt.Errorf("%w: transaction rollback while fn exec", rollbackErr)
		}

		return err
	}

	query := fmt.Sprintf(`
INSERT INTO %s (event_id, event_type, payload, exchange, routing_key, partition_key)
VALUES($1, $2, $3, $4, $5, $6)
`, TableName)

	for _, event := range messages {
		_, err = tx.Exec(
			ctx,
			query,
			event.ID,
			event.EventType,
			event.Payload,
			event.Exchange,
			event.RoutingKey,
			event.PartitionKey,
		)

		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return fmt.Errorf("%w: transaction rollback failed while of query exec", rollbackErr)
			}

			return fmt.Errorf("%w: messages persist failed", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: transaction commit failed", err)
	}

	return nil
}

func NewGormPersister(db *gorm.DB) *GormPersister {
	return &GormPersister{db: db}
}

type GormPersister struct {
	db *gorm.DB
}

func (r *GormPersister) PersistInTx(fn func(tx *gorm.DB) ([]Message, error)) error {
	return r.db.Transaction(func(tx *gorm.DB) error {
		messages, err := fn(tx)
		if err != nil {
			return err
		}

		query := fmt.Sprintf(`
INSERT INTO %s (event_id, event_type, payload, exchange, routing_key, partition_key)
VALUES(?, ?, ?, ?, ?, ?)
`, TableName)

		for _, event := range messages {
			err := tx.Exec(
				query,
				event.ID,
				event.EventType,
				event.Payload,
				event.Exchange,
				event.RoutingKey,
				event.PartitionKey,
			).Error

			if err != nil {
				return fmt.Errorf("%w: messages persist failed", err)
			}
		}

		return nil
	})
}
