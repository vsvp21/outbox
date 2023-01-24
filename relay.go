package outbox

import (
	"context"
	"fmt"
	"github.com/avast/retry-go"
	"github.com/rs/zerolog/log"
	concurrency "github.com/vsvp21/go-concurrency"
	"sync"
	"time"
)

func NewRelay(repo EventRepository, publisher Publisher, publishWorkerPoolSize int, publishDelay time.Duration) *Relay {
	return &Relay{
		eventRepository: repo,
		publisher:       publisher,
		gen: messagesGenerator{
			eventRepository: repo,
			t:               time.NewTicker(publishDelay),
		},
		publishWorkerPoolSize: publishWorkerPoolSize,
	}
}

type Relay struct {
	eventRepository       EventRepository
	publisher             Publisher
	gen                   messagesGenerator
	publishWorkerPoolSize int
}

func (r *Relay) Run(ctx context.Context, batchSize BatchSize) error {
	if err := batchSize.Valid(); err != nil {
		return err
	}

	for m := range concurrency.OrDone[*messagesEnvelope](ctx, r.gen.getMessages(ctx, batchSize)) {
		if m.err != nil {
			log.Error().Err(m.err).Msg("while messages receive")
			continue
		}

		msgCh := make(chan *Message, batchSize)
		pool := NewAwaitingPool[*Message](r.publishWorkerPoolSize, msgCh)

		go func() {
			defer close(msgCh)
			for _, msg := range m.messages {
				msgCh <- msg
			}
		}()

		consumed := newConsumedMessagesContainer(batchSize)
		pool.Go(ctx, func(ctx context.Context, msg *Message) {
			publish := func() error {
				return r.publisher.Publish(msg.Exchange, msg.RoutingKey, msg)
			}

			err := retry.Do(publish, retry.Delay(PublishRetryDelay), retry.Attempts(PublishRetryAttempts), retry.Context(ctx))
			if err != nil {
				log.Error().Err(err).Msg("while publishing message")
				return
			}

			consumed.addMessage(msg)

			log.Info().
				Str("routing_key", msg.RoutingKey).
				Str("id", msg.ID).
				Interface("payload", msg.Payload).
				Msg("Message published")
		})

		if !consumed.empty() {
			if err := r.eventRepository.MarkConsumed(ctx, consumed.messages); err != nil {
				log.Error().Err(err).Msg("while updating consumed status")
				continue
			}

			log.Info().Msgf("published %d messages", len(consumed.messages))
		}
	}

	return nil
}

type messagesGenerator struct {
	eventRepository EventRepository
	t               *time.Ticker
}

func (g messagesGenerator) getMessages(ctx context.Context, size BatchSize) <-chan *messagesEnvelope {
	ch := make(chan *messagesEnvelope)

	go func() {
		defer func() {
			close(ch)
			g.t.Stop()
			log.Info().Msg("stream finished")
		}()

		log.Info().Msg("stream started")
		for {
			select {
			case <-ctx.Done():
				return
			case <-g.t.C:
				messages, err := g.eventRepository.Fetch(ctx, size)
				if err != nil {
					ch <- newErrorMessagesEnvelope(fmt.Errorf("%w: fetching messages failed", err))
					continue
				}

				log.Info().Msgf("received %d messages from event store", len(messages))

				if len(messages) > 0 {
					ch <- newMessagesEnvelope(messages)
				}
			}
		}
	}()

	return ch
}

func newErrorMessagesEnvelope(err error) *messagesEnvelope {
	return &messagesEnvelope{
		err: err,
	}
}

func newMessagesEnvelope(messages []*Message) *messagesEnvelope {
	return &messagesEnvelope{
		messages: messages,
	}
}

type messagesEnvelope struct {
	messages []*Message
	err      error
}

func newConsumedMessagesContainer(size BatchSize) *consumedMessagesContainer {
	return &consumedMessagesContainer{
		messages: make([]*Message, 0, size),
	}
}

type consumedMessagesContainer struct {
	messages []*Message
	m        sync.Mutex
}

func (c *consumedMessagesContainer) addMessage(msg *Message) {
	c.m.Lock()
	defer c.m.Unlock()

	c.messages = append(c.messages, msg)
}

func (c *consumedMessagesContainer) empty() bool {
	return len(c.messages) == 0
}
