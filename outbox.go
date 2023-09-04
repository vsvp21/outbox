package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"hash/fnv"
	"time"
)

const (
	statusConsumed    = true
	statusNotConsumed = false

	maxBatchSize = 10000
)

var (
	ErrBatchSizeOutOfRange = errors.New("invalid batch size")

	TableName                  = "outbox_messages"
	PublishRetryDelay          = time.Second
	PublishRetryAttempts  uint = 3
	PartitionKeyAlgorithm      = partitionKey
)

type BatchSize uint
type PartitionKeyAlg func(s string) int

func (b BatchSize) Valid() error {
	if b == 0 || b > maxBatchSize {
		return ErrBatchSizeOutOfRange
	}

	return nil
}

type Publisher interface {
	Publish(exchange, topic string, message Message) error
}

func NewMessage(id string, eventType string, payload interface{}, exchange, partition, routingKey string) Message {
	return Message{
		ID:           id,
		EventType:    eventType,
		Payload:      payload,
		Exchange:     exchange,
		RoutingKey:   routingKey,
		CreatedAt:    time.Now(),
		PartitionKey: PartitionKeyAlgorithm(partition),
	}
}

type Message struct {
	ID           string
	EventType    string
	Payload      interface{}
	PartitionKey int
	Exchange     string
	RoutingKey   string
	Consumed     bool
	CreatedAt    time.Time
}

func (m *Message) BytePayload() ([]byte, error) {
	switch p := m.Payload.(type) {
	case string:
		return []byte(p), nil
	default:
		return json.Marshal(m.Payload)
	}
}

type EventRepository interface {
	Fetch(ctx context.Context, delay time.Duration, batchSize BatchSize) <-chan Message
	MarkConsumed(ctx context.Context, ids []string) error
}

func partitionKey(s string) int {
	// Create an FNV-1a hash of the input string
	h := fnv.New32a()
	h.Write([]byte(s))
	hashValue := h.Sum32()

	// Map the hash value to a partition within the specified range
	return int(hashValue)
}
