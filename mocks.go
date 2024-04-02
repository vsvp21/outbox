package outbox

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
)

type RepositoryMock struct {
	Messages []Message
	Consumed []string
	Cursor   int
	mu       sync.Mutex
}

func (m *RepositoryMock) Fetch(ctx context.Context, batchSize BatchSize) <-chan Message {
	ch := make(chan Message)

	go func() {
		defer close(ch)

		for _, m := range m.Messages {
			ch <- m
		}

		m.Messages = m.Messages[:0]
	}()

	return ch
}

func (m *RepositoryMock) MarkConsumed(ctx context.Context, msg []Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, msg := range msg {
		m.Consumed = append(m.Consumed, msg.ID)
	}

	return nil
}

type PublisherMock struct {
	Published []Message
	mu        sync.Mutex
}

func (p *PublisherMock) Publish(exchange, topic string, message Message) error {
	p.mu.Lock()
	p.Published = append(p.Published, message)
	p.mu.Unlock()

	return nil
}

func GenerateMessages(n int) []Message {
	ms := make([]Message, n)
	for i := 0; i < n; i++ {
		ms[i] = NewMessage(strconv.Itoa(rand.Int()), "Test", map[string]int{"num": i}, "test1", "test1", strconv.Itoa(i))
	}

	return ms
}
