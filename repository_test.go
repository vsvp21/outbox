package outbox

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	_ "github.com/lib/pq"
	"github.com/romanyx/polluter"
	"github.com/stretchr/testify/suite"
	"strings"
	"testing"
)

const input = `
outbox_messages:
- id: f53ec986-345f-48a4-b248-430a7d7f342a
  consumed: false
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
- id: f53ec986-345f-48a4-b248-430a7d7f342b
  consumed: true
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
- id: f53ec986-345f-48a4-b248-430a7d7f342c
  consumed: false
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
`

type PgxRepositoryTestSuite struct {
	suite.Suite
	pgx *pgxpool.Pool
	db  *sql.DB
	r   *PgxRepository
}

func (suite *PgxRepositoryTestSuite) SetupTest() {
	conn, err := pgxpool.Connect(context.Background(), "postgres://postgres:postgres@localhost:5432/test_db")
	if err != nil {
		suite.Failf("failed to connect to pgx: %s", "", err)
	}
	suite.pgx = conn

	dsn := "postgres://postgres:postgres@localhost:5432/test_db?sslmode=disable"

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		suite.Failf("failed to connect to postgres driver: %s", "", err)
	}

	suite.db = db

	suite.r = &PgxRepository{db: conn}
}

func (suite *PgxRepositoryTestSuite) cleanDB() {
	_, err := suite.db.Exec("DELETE FROM outbox_messages")
	if err != nil {
		suite.Failf("cleaning database: %s", "", err)
	}
}

func (suite *PgxRepositoryTestSuite) pollute() {
	p := polluter.New(polluter.PostgresEngine(suite.db))
	if err := p.Pollute(strings.NewReader(input)); err != nil {
		suite.Failf("failed to pollute db: %s", "", err)
	}
}

func (suite *PgxRepositoryTestSuite) TearDownSuite() {
	suite.pgx.Close()
	suite.db.Close()
}

func (suite *PgxRepositoryTestSuite) TestFetch() {
	suite.cleanDB()
	suite.pollute()
	messages, err := suite.r.Fetch(context.Background(), 100)
	if err != nil {
		return
	}

	suite.Equal(2, len(messages))
}

func (suite *PgxRepositoryTestSuite) TestMarkConsumed() {
	suite.cleanDB()
	suite.pollute()
	err := suite.r.MarkConsumed(context.Background(), []*Message{
		{
			ID: "f53ec986-345f-48a4-b248-430a7d7f342a",
		},
	})
	if err != nil {
		return
	}

	messages, err := suite.r.Fetch(context.Background(), 100)
	if err != nil {
		suite.Failf("Receiving messages from repo %s", "", err)
	}

	suite.Equal(1, len(messages))
}

func (suite *PgxRepositoryTestSuite) TestPersistInTx() {
	suite.cleanDB()
	suite.pollute()
	err := suite.r.PersistInTx(context.Background(), func(tx pgx.Tx) ([]*Message, error) {
		return []*Message{
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342f", Payload: map[string]string{}},
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342e", Payload: map[string]string{}},
		}, nil
	})
	if err != nil {
		suite.Failf("Cannot persist messages", "%s", err)
	}

	messages, err := suite.r.Fetch(context.Background(), 100)
	if err != nil {
		suite.Failf("Receiving messages from repo %s", "", err)
	}

	suite.Equal(4, len(messages))
}

// In order for 'go test' to run this suite, we need to create
// a normal test function and pass our suite to suite.Run
func TestPgxRepository(t *testing.T) {
	suite.Run(t, new(PgxRepositoryTestSuite))
}
