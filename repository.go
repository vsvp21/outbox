package outbox

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func NewPgxOutboxRepository(db *pgxpool.Pool) *PgxRepository {
	return &PgxRepository{db: db}
}

type PgxRepository struct {
	db *pgxpool.Pool
}

func (r *PgxRepository) Persist(ctx context.Context, messages []*Message) error {
	tx, err := r.db.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("%w: transaction begin failed", err)
	}

	query := fmt.Sprintf(`
INSERT INTO %s (id, event_type, payload, exchange, routing_key)
VALUES($1, $2, $3, $4, $5)
`, OutboxTableName)

	for _, event := range messages {
		_, err = tx.Exec(
			ctx,
			query,
			uuid.New().String(),
			event.EventType,
			event.Payload,
			event.Exchange,
			event.RoutingKey,
		)

		if err != nil {
			if rollbackErr := tx.Rollback(ctx); rollbackErr != nil {
				return fmt.Errorf("%w: transaction rollback failed", rollbackErr)
			}

			return fmt.Errorf("%w: messages persist failed", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: transaction commit failed", err)
	}

	return nil
}

func (r *PgxRepository) Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error) {
	query := fmt.Sprintf(`
SELECT id, event_type, exchange, routing_key, payload, consumed, created_at
FROM %s
WHERE consumed = $1 ORDER BY created_at DESC LIMIT $2
`, OutboxTableName)

	rows, err := r.db.Query(ctx, query, statusNotConsumed, batchSize)
	if err != nil {
		return nil, fmt.Errorf("%w: quering messages failed", err)
	}

	messages := make([]*Message, 0, batchSize)
	for rows.Next() {
		message := &Message{}

		err = rows.Scan(&message.ID, &message.EventType, &message.Exchange, &message.RoutingKey, &message.Payload, &message.Consumed, &message.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("%w: scan messages failed", err)
		}

		messages = append(messages, message)
	}

	return messages, nil
}

func (r *PgxRepository) MarkConsumed(ctx context.Context, messages []*Message) error {
	if len(messages) == 0 {
		return nil
	}

	ids := make([]string, len(messages))
	for i, msg := range messages {
		ids[i] = msg.ID
	}

	query := fmt.Sprintf("UPDATE %s SET consumed=$1 WHERE id=ANY($2)", OutboxTableName)
	if _, err := r.db.Exec(ctx, query, statusConsumed, ids); err != nil {
		return fmt.Errorf("%w: update consumed status failed", err)
	}

	return nil
}
