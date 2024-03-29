package outbox

import (
	"context"
	"github.com/avast/retry-go"
	"github.com/rs/zerolog/log"
	concurrency "github.com/vsvp21/go-concurrency"
	"sync"
	"time"
)

func NewRelay(repo EventRepository, publisher Publisher, partitions int, publishDelay time.Duration) *Relay {
	return &Relay{
		eventRepository: repo,
		publisher:       publisher,
		delay:           publishDelay,
		partitions:      partitions,
	}
}

type Relay struct {
	eventRepository EventRepository
	publisher       Publisher
	delay           time.Duration
	partitions      int
}

func (r *Relay) Run(ctx context.Context, batchSize BatchSize) error {
	if err := batchSize.Valid(); err != nil {
		return err
	}

	messagesStream := r.eventRepository.Fetch(ctx, r.delay, batchSize)
	partitionedMessagesStreams := partitionedFanOut(ctx, messagesStream, r.partitions)
	publishStream := fanInPublish(ctx, r.publisher, partitionedMessagesStreams)

	markConsumed(ctx, r.eventRepository, publishStream, batchSize)

	return nil
}

func partitionedFanOut(ctx context.Context, ch <-chan Message, n int) []chan Message {
	cs := make([]chan Message, n)
	for i := 0; i < n; i++ {
		cs[i] = make(chan Message)
	}

	go func() {
		defer func() {
			for _, c := range cs {
				close(c)
			}
		}()

		for msg := range concurrency.OrDone[Message](ctx, ch) {
			a := int(msg.PartitionKey.Int64) % len(cs)
			cs[a] <- msg
		}
	}()

	return cs
}

func fanInPublish(ctx context.Context, publisher Publisher, cs []chan Message) <-chan Message {
	fanInCh := make(chan Message)

	go func() {
		defer close(fanInCh)
		wg := sync.WaitGroup{}
		wg.Add(len(cs))

		for _, ch := range cs {
			go func(ch <-chan Message) {
				defer wg.Done()
				for msg := range concurrency.OrDone[Message](ctx, ch) {
					publish := func() error {
						return publisher.Publish(msg.Exchange, msg.RoutingKey, msg)
					}

					err := retry.Do(publish, retry.Delay(PublishRetryDelay), retry.Attempts(PublishRetryAttempts), retry.Context(ctx))
					if err != nil {
						log.Error().Err(err).Msg("while publishing message")
						return
					}

					fanInCh <- msg

					log.Info().
						Str("routing_key", msg.RoutingKey).
						Str("id", msg.ID).
						Msg("Message published")
				}
			}(ch)
		}

		wg.Wait()
	}()

	return fanInCh
}

func markConsumed(ctx context.Context, eventRepository EventRepository, ch <-chan Message, batchSize BatchSize) {
	for msg := range concurrency.OrDone[Message](ctx, ch) {
		if err := eventRepository.MarkConsumed(ctx, msg); err != nil {
			log.Error().Err(err).Msg("while mark consumed")
			continue
		}
	}
}
