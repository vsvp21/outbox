package outbox

import (
	"context"
	"database/sql"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/romanyx/polluter"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

const createTableQuery = `
create table if not exists outbox_messages
(
    id         bigserial primary key, 
    event_id          uuid                                   not null,
    consumed    boolean      default false             not null,
    event_type  varchar(255)                           not null,
    payload     jsonb                                  not null,
    exchange    varchar(255)                           not null,
    routing_key varchar(255)                           not null,
    partition_key bigint,
    created_at  timestamp(0) default CURRENT_TIMESTAMP not null
)
`

const input = `
outbox_messages:
- event_id: f53ec986-345f-48a4-b248-430a7d7f342a
  consumed: false
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
  partition_key: 1
- event_id: f53ec986-345f-48a4-b248-430a7d7f342b
  consumed: true
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
  partition_key: 2
- event_id: f53ec986-345f-48a4-b248-430a7d7f342c
  consumed: false
  event_type: TestEvent
  payload: "{}"
  exchange: test
  routing_key: test
  partition_key: 3
`

// TestSuite base test suite
type TestSuite struct {
	suite.Suite
	db           *sql.DB
	pgxDB        *pgxpool.Pool
	gormDB       *gorm.DB
	tableCreated bool
}

func (suite *TestSuite) pollute() {
	p := polluter.New(polluter.PostgresEngine(suite.db))
	if err := p.Pollute(strings.NewReader(input)); err != nil {
		suite.Failf("failed to pollute db: %s", "", err)
	}
}

func (suite *TestSuite) cleanDB() {
	_, err := suite.db.Exec("DELETE FROM outbox_messages")
	if err != nil {
		suite.Failf("cleaning database: %s", "", err)
	}
}

func (suite *TestSuite) SetupTest() {
	dsn := "postgres://db_user:secretsecret@localhost:5432/outbox_test?sslmode=disable"
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		suite.Failf("failed to connect to postgres driver: %s", "", err)
	}

	suite.db = db
	if !suite.tableCreated {
		_, err := db.Exec(createTableQuery)
		if err != nil {
			suite.Failf("failed to create table: %s", "", err)
		}
		suite.tableCreated = true
	}

	gormDB, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=127.0.0.1 user=db_user password=secretsecret dbname=outbox_test port=5432 sslmode=disable",
	}), &gorm.Config{})
	if err != nil {
		suite.Failf("failed to connect to pgx: %s", "", err)
	}

	suite.gormDB = gormDB

	conn, err := pgxpool.New(context.Background(), "postgres://db_user:secretsecret@localhost:5432/outbox_test")
	if err != nil {
		suite.Failf("failed to connect to pgx: %s", "", err)
	}

	suite.pgxDB = conn
}

func (suite *TestSuite) TearDownSuite() {
	suite.db.Close() //nolint
	suite.pgxDB.Close()
}
