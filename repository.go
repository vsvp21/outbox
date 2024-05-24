package outbox

import (
	"context"
	"fmt"

	"github.com/rs/zerolog/log"
)

type Repository struct {
	db DBAdapter
}

func NewRepository(db DBAdapter) *Repository {
	return &Repository{db: db}
}

func (r *Repository) Fetch(ctx context.Context, batchSize BatchSize) <-chan Message {
	stream := make(chan Message, batchSize)

	query := fmt.Sprintf(`
SELECT event_id, event_type, exchange, routing_key, partition_key, payload, consumed, created_at
FROM %s
WHERE consumed = $1 ORDER BY created_at ASC LIMIT $2
`, TableName)

	go func() {
		defer close(stream)

		rows, err := r.db.Query(ctx, query, statusNotConsumed, batchSize)
		if err != nil {
			log.Error().Err(err).Msg("while quering messages")
			return
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

		if err := rows.Close(); err != nil {
			log.Error().Err(err).Msg("while closing rows")
		}
	}()

	return stream
}

func (r *Repository) MarkConsumed(ctx context.Context, msgs []Message) error {
	if len(msgs) == 0 {
		return nil
	}

	ids := make([]string, 0, 1000)
	for i, msg := range msgs {
		if msg.ID != "" {
			ids = append(ids, msg.ID)
		}

		if len(ids) == 1000 || i == len(msgs)-1 {
			query := fmt.Sprintf("UPDATE %s SET consumed=? WHERE event_id IN ?", TableName)
			if err := r.db.Exec(ctx, query, statusConsumed, ids); err != nil {
				return fmt.Errorf("while update consumed status failed: %w", err)
			}

			ids = ids[:0]
		}
	}

	return nil
}
