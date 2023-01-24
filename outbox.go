package outbox

import (
	"context"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v4"
	go_concurrency "github.com/vsvp21/go-concurrency"
	"sync"
	"time"
)

const (
	statusConsumed    = true
	statusNotConsumed = false

	maxBatchSize = 10000
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

type Publisher interface {
	Publish(exchange, topic string, message *Message) error
}

func NewMessage(id string, eventType string, payload interface{}, exchange, routingKey string) *Message {
	return &Message{
		ID:         id,
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

type PersistFunc func(tx pgx.Tx) ([]*Message, error)

type EventRepository interface {
	PersistInTx(ctx context.Context, f PersistFunc) error
	Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error)
	MarkConsumed(ctx context.Context, messages []*Message) error
}

func NewAwaitingPool[T any](poolSize int, ch <-chan T) *WorkerPool[T] {
	return &WorkerPool[T]{
		poolSize: poolSize,
		ch:       ch,
		wg:       sync.WaitGroup{},
		await:    true,
	}
}

func NewPool[T any](poolSize int, ch <-chan T) *WorkerPool[T] {
	return &WorkerPool[T]{
		poolSize: poolSize,
		ch:       ch,
		wg:       sync.WaitGroup{},
		await:    false,
	}
}

type WorkerPool[T any] struct {
	poolSize int
	ch       <-chan T
	wg       sync.WaitGroup
	await    bool
}

func (p *WorkerPool[T]) Go(ctx context.Context, fn func(ctx context.Context, v T)) {
	p.add()
	for i := 0; i < p.poolSize; i++ {
		go func() {
			defer p.done()
			for v := range go_concurrency.OrDone[T](ctx, p.ch) {
				fn(ctx, v)
			}
		}()
	}
	p.wait()
	fmt.Println(1245315)
}

func (p *WorkerPool[T]) add() {
	if p.await {
		p.wg.Add(p.poolSize)
	}
}

func (p *WorkerPool[T]) done() {
	if p.await {
		p.wg.Done()
	}
}

func (p *WorkerPool[T]) wait() {
	if p.await {
		p.wg.Wait()
	}
}
