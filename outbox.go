package outbox

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"time"
)

const (
	statusConsumed    = true
	statusNotConsumed = false

	maxBatchSize = 1000
)

var (
	ErrBatchSizeOutOfRange = errors.New("invalid batch size")

	TableName                 = "outbox_messages"
	PublishRetryDelay         = time.Second
	PublishRetryAttempts uint = 3
)

type BatchSize uint

func (b BatchSize) Valid() error {
	if b == 0 || b > maxBatchSize {
		return ErrBatchSizeOutOfRange
	}

	return nil
}

func NewMessage(eventType string, payload interface{}, exchange, routingKey string) *Message {
	return &Message{
		ID:         uuid.New().String(),
		EventType:  eventType,
		Payload:    payload,
		Exchange:   exchange,
		RoutingKey: routingKey,
		CreatedAt:  time.Now(),
	}
}

type Message struct {
	ID         string
	EventType  string
	Payload    interface{}
	Exchange   string
	RoutingKey string
	Consumed   bool
	CreatedAt  time.Time
}

type EventRepository interface {
	Persist(ctx context.Context, messages []*Message) error
	Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error)
	MarkConsumed(ctx context.Context, messages []*Message) error
}
