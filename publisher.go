package outbox

type Publisher interface {
	Publish(exchange, topic string, message *Message) error
}
