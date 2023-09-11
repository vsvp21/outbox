package outbox

import (
	"context"
	"fmt"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
	concurrency "github.com/vsvp21/go-concurrency"
	"gorm.io/gorm"
	"time"
)

func NewPgxOutboxRepository(db *pgxpool.Pool) *PgxRepository {
	return &PgxRepository{db: db}
}

type PgxRepository struct {
	db *pgxpool.Pool
}

func (r *PgxRepository) Fetch(ctx context.Context, delay time.Duration, batchSize BatchSize) <-chan Message {
	stream := make(chan Message, batchSize)

	query := fmt.Sprintf(`
SELECT event_id, event_type, exchange, routing_key, partition_key, payload, consumed, created_at
FROM %s
WHERE consumed = $1 ORDER BY id ASC LIMIT $2
`, TableName)

	d := time.NewTicker(delay)
	go func() {
		defer func() {
			close(stream)
			d.Stop()
		}()

		for range concurrency.OrDone[time.Time](ctx, d.C) {
			rows, err := r.db.Query(ctx, query, statusNotConsumed, batchSize)
			if err != nil {
				log.Error().Err(err).Msg("while quering messages")
				continue
			}

			for rows.Next() {
				message := Message{}

				err = rows.Scan(&message.ID, &message.EventType, &message.Exchange, &message.RoutingKey, &message.PartitionKey, &message.Payload, &message.Consumed, &message.CreatedAt)
				if err != nil {
					log.Error().Err(err).Msg("while scan messages")
					continue
				}

				stream <- message
			}

			rows.Close()
		}
	}()

	return stream
}

func (r *PgxRepository) MarkConsumed(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s SET consumed=$1 WHERE event_id=$2", TableName)
	if _, err := r.db.Exec(ctx, query, statusConsumed, msg.ID); err != nil {
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

func (r *GormRepository) Fetch(ctx context.Context, delay time.Duration, batchSize BatchSize) <-chan Message {
	stream := make(chan Message, batchSize)

	query := fmt.Sprintf(`
SELECT event_id, event_type, exchange, routing_key, partition_key, payload, consumed, created_at
FROM %s
WHERE consumed = ? ORDER BY id ASC LIMIT ?
`, TableName)

	d := time.NewTicker(delay)
	go func() {
		defer func() {
			close(stream)
			d.Stop()
		}()

		for range concurrency.OrDone[time.Time](ctx, d.C) {
			result := r.db.Raw(query, statusNotConsumed, batchSize)
			if result.Error != nil {
				log.Error().Err(result.Error).Msg("[gorm] while quering messages")
				continue
			}

			rows, err := result.Rows()
			if err != nil {
				log.Error().Err(result.Error).Msg("[gorm] fetching rows failed")
				continue
			}

			for rows.Next() {
				message := Message{}

				var payload string
				err = rows.Scan(&message.ID, &message.EventType, &message.Exchange, &message.RoutingKey, &message.PartitionKey, &payload, &message.Consumed, &message.CreatedAt)
				if err != nil {
					log.Error().Err(err).Msg("[gorm] while scanning message")
					continue
				}

				message.Payload = payload

				stream <- message
			}

			if err := rows.Close(); err != nil {
				log.Error().Err(err).Msg("[gorm] while closing rows")
			}
		}
	}()

	return stream
}

func (r *GormRepository) MarkConsumed(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		return nil
	}

	query := fmt.Sprintf("UPDATE %s SET consumed=$1 WHERE event_id=$2", TableName)
	if err := r.db.Exec(query, statusConsumed, msg.ID).Error; err != nil {
		return fmt.Errorf("[gorm] %w: update consumed status failed", err)
	}

	return nil
}
