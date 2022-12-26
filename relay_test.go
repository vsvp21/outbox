package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/jackc/pgx/v4/pgxpool"
	"log"
	"runtime"
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

var generateMessages = func(n int) []*Message {
	ms := make([]*Message, n)
	for i := 0; i < n; i++ {
		ms[i] = NewMessage("1", "Test", map[string]int{"num": i}, "test1", "test2")
	}

	return ms
}

func ExampleRelay_Run() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := pgxpool.Connect(ctx, "postgres://root:root@127.0.0.1:5432/db_name")
	if err != nil {
		log.Fatal(err)
	}

	r := NewPgxOutboxRepository(c)
	if err = r.Persist(ctx, generateMessages(1000)); err != nil {
		log.Fatal(err)
	}

	relay := NewRelay(r, publisherMock{}, runtime.NumCPU(), time.Second)
	if err = relay.Run(ctx, BatchSize(100)); err != nil {
		log.Fatal(err)
	}
}
