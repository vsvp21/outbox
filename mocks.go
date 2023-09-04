package outbox

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

type RepositoryMock struct {
	Messages []Message
	Consumed []string
	Cursor   int
	mu       sync.Mutex
}

func (m *RepositoryMock) Fetch(ctx context.Context, delay time.Duration, batchSize BatchSize) <-chan Message {
	ch := make(chan Message)

	go func() {
		defer close(ch)

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}

			bs := int(batchSize)
			if m.Cursor > cap(m.Messages) {
				return
			}

			end := m.Cursor + bs
			if end > cap(m.Messages) {
				end = cap(m.Messages)
			}

			for _, m := range m.Messages[m.Cursor:end] {
				ch <- m
			}

			m.Cursor = end

			time.Sleep(delay)
		}
	}()

	return ch
}

func (m *RepositoryMock) MarkConsumed(ctx context.Context, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Consumed = append(m.Consumed, msg.ID)

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
