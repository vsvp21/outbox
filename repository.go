package outbox

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	"gorm.io/gorm"
)

func NewPgxOutboxRepository(db *pgxpool.Pool) *PgxRepository {
	return &PgxRepository{db: db}
}

type PgxRepository struct {
	db *pgxpool.Pool
}

func (r *PgxRepository) Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error) {
	query := fmt.Sprintf(`
SELECT id, event_type, exchange, routing_key, payload, consumed, created_at
FROM %s
WHERE consumed = $1 ORDER BY created_at DESC LIMIT $2
`, TableName)

	rows, err := r.db.Query(ctx, query, statusNotConsumed, batchSize)
	if err != nil {
		return nil, fmt.Errorf("%w: quering messages failed", err)
	}
	defer rows.Close()

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

	query := fmt.Sprintf("UPDATE %s SET consumed=$1 WHERE id=ANY($2)", TableName)
	if _, err := r.db.Exec(ctx, query, statusConsumed, ids); err != nil {
		return fmt.Errorf("%w: update consumed status failed", err)
	}

	return nil
}

func NewGormRepository(db *gorm.DB) *GormRepository {
	return &GormRepository{
		db: db,
	}
}

type GormRepository struct {
	db *gorm.DB
}

func (r *GormRepository) Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error) {
	query := fmt.Sprintf(`
SELECT id, event_type, exchange, routing_key, payload, consumed, created_at
FROM %s
WHERE consumed = ? ORDER BY created_at DESC LIMIT ?
`, TableName)

	result := r.db.Raw(query, statusNotConsumed, batchSize)
	if result.Error != nil {
		return nil, fmt.Errorf("%w: quering messages failed", result.Error)
	}

	messages := make([]*Message, 0, batchSize)
	rows, err := result.Rows()
	if err != nil {
		return nil, fmt.Errorf("%w: fetching rows failed", err)
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			log.Error().Err(err).Send()
		}
	}(rows)

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

func (r *GormRepository) MarkConsumed(ctx context.Context, messages []*Message) error {
	if len(messages) == 0 {
		return nil
	}

	ids := make([]string, len(messages))
	for i, msg := range messages {
		ids[i] = msg.ID
	}

	query := fmt.Sprintf("UPDATE %s SET consumed=$1 WHERE id=ANY($2)", TableName)
	if err := r.db.Exec(query, statusConsumed, ids).Error; err != nil {
		return fmt.Errorf("%w: update consumed status failed", err)
	}

	return nil
}
