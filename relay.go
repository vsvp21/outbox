package outbox

import (
	"context"
	"sync"
	"time"

	"github.com/avast/retry-go"
	"github.com/rs/zerolog/log"
	concurrency "github.com/vsvp21/go-concurrency"
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

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		batchCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		func() {
			defer cancel()
			messagesStream := r.eventRepository.Fetch(batchCtx, batchSize)
			partitionedMessagesStreams := partitionedFanOut(batchCtx, messagesStream, r.partitions)
			publishStream := fanInPublish(batchCtx, r.publisher, partitionedMessagesStreams)
			markConsumed(batchCtx, r.eventRepository, publishStream, batchSize)
		}()

		select {
		case <-ctx.Done():
			return nil
		case <-time.After(r.delay):
		}
	}
}

func partitionedFanOut(ctx context.Context, ch <-chan Message, n int) []chan Message {
	cs := make([]chan Message, n)
	for i := 0; i < n; i++ {
		cs[i] = make(chan Message, 1000)
	}

	go func() {
		defer func() {
			for _, c := range cs {
				close(c)
			}
		}()

		for msg := range concurrency.OrDone[Message](ctx, ch) {
			partitionIdx := int(msg.PartitionKey.Int64) % len(cs)

			select {
			case cs[partitionIdx] <- msg:
			case <-ctx.Done():
				log.Info().Msg("context cancelled while partitioning messages")
				return
			default:
				log.Info().
					Int("partition", partitionIdx).
					Str("message_id", msg.ID).
					Msg("partition channel full, dropping message")
			}
		}
	}()

	return cs
}

func fanInPublish(ctx context.Context, publisher Publisher, cs []chan Message) <-chan Message {
	fanInCh := make(chan Message, 1000)

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

					select {
					case fanInCh <- msg:
					case <-ctx.Done():
						log.Warn().Msg("context cancelled while trying to send published message")
						return
					}
				}
			}(ch)
		}

		wg.Wait()
	}()

	return fanInCh
}

func markConsumed(ctx context.Context, eventRepository EventRepository, ch <-chan Message, batchSize BatchSize) {
	msgs := make([]Message, 0, batchSize)

	for msg := range concurrency.OrDone[Message](ctx, ch) {
		msgs = append(msgs, msg)
	}

	if len(msgs) == 0 {
		return
	}

	if err := eventRepository.MarkConsumed(ctx, msgs); err != nil {
		log.Error().Err(err).
			Int("message_count", len(msgs)).
			Msg("failed to mark messages as consumed - messages will be reprocessed")
	}
}
