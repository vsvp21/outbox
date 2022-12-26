# Golang simple transactional outbox

Transactional outbox based on polling publisher for PostgreSQL.

## Features:

* Persist messages
* Publish message batch
* Publish message in worker pool
* Manage delay between batch publishing
* Create custom publisher
* Create custom repository
* Use custom outbox table

## Basic initialization with pgx Repository

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/vsvp21/outbox"
)

type publisherMock struct{}
func (p publisherMock) Publish(exchange, topic string, message *outbox.Message) error {
	payload, err := json.Marshal(message.Payload)
	if err != nil {
		return err
	}

	fmt.Printf("published message to topic: %s, payload: %s", topic, string(payload))

	return nil
}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := pgxpool.Connect(ctx, "postgres://root:root@127.0.0.1:5432/db_name")
	if err != nil {
		log.Fatal(err)
	}

	r := outbox.NewPgxOutboxRepository(c)

	relay := outbox.NewRelay(r, publisherMock{}, runtime.NumCPU(), time.Second)
	if err = relay.Run(ctx, outbox.BatchSize(100)); err != nil {
		log.Fatal(err)
	}
}
```
## Custom outbox table:

```go
package main

import "github.com/vsvp21/outbox"

func main() {
	// Your code ...
	outbox.TableName = "custom"
	// Your code ...
}
```

## TODO:

* Tests
