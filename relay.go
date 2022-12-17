package outbox

import (
	"context"
	"encoding/json"
	"fmt"
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
			delay:           publishDelay,
		},
		pool: concurrency.NewWorkerPool(publishWorkerPoolSize),
	}
}

type Relay struct {
	eventRepository EventRepository
	publisher       Publisher
	gen             messagesGenerator
	pool            *concurrency.WorkerPool
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
		go func() {
			defer close(msgCh)
			for _, msg := range m.messages {
				msgCh <- msg
			}
		}()

		consumed := newConsumedMessagesContainer(batchSize)
		r.pool.Go(ctx, func(ctx context.Context) {
			for msg := range concurrency.OrDone[*Message](ctx, msgCh) {
				p, err := json.Marshal(msg.Payload)
				if err != nil {
					log.Error().Err(err).Msg("while payload unmarshall")
					continue
				}

				err = r.publisher.Publish(msg.RoutingKey, newPayload(msg.ID, p))
				if err != nil {
					log.Error().Err(err).Msg("while publishing message")
					continue
				}

				consumed.addMessage(msg)

				log.Info().
					Str("routing_key", msg.RoutingKey).
					Str("id", msg.ID).
					Interface("payload", msg.Payload).
					Msg("Message published")
			}
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

type messagesGenerator struct {
	eventRepository EventRepository
	delay           time.Duration
}

func (g messagesGenerator) getMessages(ctx context.Context, size BatchSize) <-chan *messagesEnvelope {
	ch := make(chan *messagesEnvelope)

	go func() {
		defer func() {
			close(ch)
			log.Info().Msg("stream finished")
		}()

		log.Info().Msg("stream started")
		for {
			select {
			case <-ctx.Done():
				return
			default:
				messages, err := g.eventRepository.Fetch(ctx, size)
				if err != nil {
					ch <- newErrorMessagesEnvelope(fmt.Errorf("%w: fetching messages failed", err))
					continue
				}

				log.Info().Msgf("received %d messages from event store", len(messages))

				if len(messages) > 0 {
					ch <- newMessagesEnvelope(messages)
				}

				time.Sleep(g.delay)
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
