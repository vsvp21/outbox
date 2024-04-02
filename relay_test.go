package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
)

// publisherMock mocks message publishing
type publisherMock struct{}

func (p publisherMock) Publish(exchange, topic string, message Message) error {
	payload, err := json.Marshal(message.Payload)
	if err != nil {
		return err
	}

	fmt.Printf("published message to topic: %s, payload: %s", topic, string(payload))

	return nil
}

func ExampleRelay_Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := pgxpool.New(ctx, "postgres://root:root@127.0.0.1:5432/db_name")
	if err != nil {
		log.Fatal(err)
	}

	p := NewPgxPersister(c)
	r := NewRepository(NewPGXAdapter(c))
	if err = p.PersistInTx(ctx, func(tx pgx.Tx) ([]Message, error) {
		return GenerateMessages(1000), nil
	}); err != nil {
		log.Fatal(err)
	}

	relay := NewRelay(r, publisherMock{}, runtime.NumCPU(), time.Second)
	if err = relay.Run(ctx, BatchSize(100)); err != nil {
		log.Fatal(err)
	}
}

func TestRelay_Run(t *testing.T) {
	t.Run("Test run relay", func(t *testing.T) {
		t.Parallel()
		ctx, cancel := context.WithTimeout(context.TODO(), time.Millisecond*500)
		defer cancel()

		n := 10
		r := &RepositoryMock{
			Messages: GenerateMessages(n),
		}

		p := &PublisherMock{Published: make([]Message, 0, n)}

		relay := &Relay{
			eventRepository: r,
			publisher:       p,
			delay:           time.Millisecond,
			partitions:      runtime.NumCPU(),
		}

		if err := relay.Run(ctx, BatchSize(10)); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, n, len(p.Published))
		assert.Equal(t, n, len(r.Consumed))
	})
}
