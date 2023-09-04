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
* Publish in partitions

## Drivers:
* pgx
* gorm

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

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vsvp21/outbox/v2"
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

	c, err := pgxpool.New(ctx, "postgres://root:root@127.0.0.1:5432/db_name")
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

## Basic initialization with gorm Repository

```go
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"runtime"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"

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

	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=127.0.0.1 user=db_user password=secretsecret dbname=test_db port=5432 sslmode=disable",
	}), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}

	r := outbox.NewGormRepository(c)

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

## Pgx Persister

```go
package main

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/vsvp21/outbox/v2"
	"log"
)

func main() {
	db, err := pgxpool.New(context.TODO(), "postgres://root:root@127.0.0.1:5432/db_name")
	if err != nil {
		log.Fatal(err)
	}

	p := outbox.NewPgxPersister(db)
	p.PersistInTx(context.TODO(), func(tx pgx.Tx) ([]outbox.Message, error) {
		// SQL Queries
		return []outbox.Message{}, nil
	})
}
```


## Gorm Persister

```go
package main

import (
	"github.com/vsvp21/outbox/v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"log"
)

func main() {
	c := postgres.Config{
		DSN: "host=127.0.0.1 user=db_user password=secretsecret dbname=test_db port=5432 sslmode=disable",
	}

	db, err := gorm.Open(postgres.New(c))
	if err != nil {
		log.Fatal(err)
	}

	p := outbox.NewGormPersister(db)
	p.PersistInTx(func(tx *gorm.DB) ([]outbox.Message, error) {
		// SQL Queries
		return []outbox.Message{}, nil
	})
}
```