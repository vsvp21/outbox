package outbox

func newPayload(id string, data []byte) Payload {
	return Payload{
		ID:   id,
		Data: data,
	}
}

type Payload struct {
	ID   string
	Data []byte
}

type Publisher interface {
	Publish(topic string, payload Payload) error
}
