package outbox

import (
	"encoding/json"
	"github.com/ThreeDotsLabs/watermill"
	"github.com/ThreeDotsLabs/watermill-amqp/v2/pkg/amqp"
	"github.com/ThreeDotsLabs/watermill/message"
)

type Publisher interface {
	Publish(topic string, message *Message) error
}

func NewWatermillAmqpPublisher(logger watermill.LoggerAdapter, cfg amqp.Config) *WatermillAmqpPublisher {
	return &WatermillAmqpPublisher{
		logger: logger,
		cfg:    cfg,
	}
}

type WatermillAmqpPublisher struct {
	logger watermill.LoggerAdapter
	cfg    amqp.Config
}

func (p *WatermillAmqpPublisher) Publish(topic string, msg *Message) error {
	publisher, err := amqp.NewPublisher(p.cfg, p.logger)
	if err != nil {
		return err
	}

	payload, err := json.Marshal(msg.Payload)
	if err != nil {
		return err
	}

	if err = publisher.Publish(topic, message.NewMessage(msg.ID, payload)); err != nil {
		return err
	}

	return nil
}
