package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"log"
	"runtime"
	"testing"
	"time"
)

// publisherMock mocks message publishing
type publisherMock struct{}

func (p publisherMock) Publish(exchange, topic string, message *Message) error {
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
	r := NewPgxOutboxRepository(c)
	if err = p.PersistInTx(ctx, func(tx pgx.Tx) ([]*Message, error) {
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

		n := 100
		r := &RepositoryMock{
			Messages: GenerateMessages(n),
		}

		p := &PublisherMock{Published: make([]*Message, 0, n)}

		relay := &Relay{
			eventRepository: r,
			publisher:       p,
			gen: messagesGenerator{
				eventRepository: r,
				t:               time.NewTicker(time.Millisecond),
			},
			publishWorkerPoolSize: runtime.NumCPU(),
		}

		if err := relay.Run(ctx, BatchSize(10)); err != nil {
			log.Fatal(err)
		}

		assert.Equal(t, n, len(p.Published))
		assert.Equal(t, n, len(r.Consumed))
	})
}

func TestGetMessages(t *testing.T) {
	n := 10
	r := &RepositoryMock{
		Messages: GenerateMessages(n),
	}

	g := messagesGenerator{
		eventRepository: r,
		t:               time.NewTicker(time.Millisecond),
	}

	t.Run("Test receive messages", func(t *testing.T) {
		t.Parallel()

		msgCh := g.getMessages(context.Background(), 10)

		msg := <-msgCh

		assert.Equal(t, 10, len(msg.messages))
		assert.Equal(t, nil, msg.err)
	})
}

func TestGetMessagesError(t *testing.T) {
	n := 10
	r := &RepositoryMock{
		Messages: GenerateMessages(n),
		FetchErr: true,
	}

	g := messagesGenerator{
		eventRepository: r,
		t:               time.NewTicker(time.Millisecond),
	}

	t.Run("Test receive error envelope if error occurred", func(t *testing.T) {
		t.Parallel()

		msgCh := g.getMessages(context.Background(), 10)

		msg := <-msgCh

		assert.Equal(t, 0, len(msg.messages))
		assert.True(t, msg.err != nil)
	})
}

func TestMessageEnvelope(t *testing.T) {
	t.Run("Test create error message", func(t *testing.T) {
		t.Parallel()
		err := errors.New("err")

		e := newErrorMessagesEnvelope(err)

		assert.True(t, errors.Is(e.err, err))
	})

	t.Run("Test create success message", func(t *testing.T) {
		t.Parallel()
		m := []*Message{{}}

		e := newMessagesEnvelope(m)

		assert.Nil(t, e.err)
		assert.Equal(t, m, e.messages)
	})
}

func TestMessageContainer(t *testing.T) {
	t.Run("Test message container is not empty", func(t *testing.T) {
		c := newConsumedMessagesContainer(1)

		c.addMessage(&Message{})

		assert.False(t, c.empty())
	})

	t.Run("Test message container is empty", func(t *testing.T) {
		c := newConsumedMessagesContainer(1)

		assert.True(t, c.empty())
	})
}
