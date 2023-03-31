package outbox

import (
	"context"
	"errors"
	"sync"
	"time"
)

type RepositoryMock struct {
	Messages []*Message
	Consumed []*Message
	Cursor   int
	FetchErr bool
	mu       sync.Mutex
}

func (m *RepositoryMock) Fetch(ctx context.Context, batchSize BatchSize) ([]*Message, error) {
	if m.FetchErr {
		return nil, errors.New("error")
	}

	bs := int(batchSize)

	if m.Cursor > cap(m.Messages) {
		return nil, nil
	}

	end := m.Cursor + bs
	if end > cap(m.Messages) {
		end = cap(m.Messages)
	}

	ms := m.Messages[m.Cursor:end]
	m.Cursor = end

	return ms, nil
}

func (m *RepositoryMock) MarkConsumed(ctx context.Context, messages []*Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.Consumed = append(m.Consumed, messages...)

	return nil
}

type PublisherMock struct {
	Published []*Message
	mu        sync.Mutex
}

func (p *PublisherMock) Publish(exchange, topic string, message *Message) error {
	p.mu.Lock()
	p.Published = append(p.Published, message)
	p.mu.Unlock()

	t := time.NewTicker(time.Millisecond)
	defer t.Stop()

	<-t.C

	return nil
}

func GenerateMessages(n int) []*Message {
	ms := make([]*Message, n)
	for i := 0; i < n; i++ {
		ms[i] = NewMessage("1", "Test", map[string]int{"num": i}, "test1", "test2")
	}

	return ms
}
