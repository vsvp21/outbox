package outbox

import (
	"context"
	"database/sql"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/romanyx/polluter"
	"github.com/stretchr/testify/suite"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"strings"
)

const createTableQuery = `
create table if not exists outbox_messages
(
    id          uuid                                   not null
        primary key,
    consumed    boolean      default false             not null,
    event_type  varchar(255)                           not null,
    payload     jsonb                                  not null,
    exchange    varchar(255)                           not null,
    routing_key varchar(255)                           not null,
    created_at  timestamp(0) default CURRENT_TIMESTAMP not null
)
`

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

// TestSuite base test suite
type TestSuite struct {
	suite.Suite
	db           *sql.DB
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
	dsn := "postgres://db_user:secretsecret@localhost:5432/test_db?sslmode=disable"
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
}

func (suite *TestSuite) TearDownSuite() {
	suite.db.Close()
}

// PgxTestSuite base test suite for pgx tests
type PgxTestSuite struct {
	TestSuite
	pgx *pgxpool.Pool
}

func (suite *PgxTestSuite) SetupTest() {
	conn, err := pgxpool.New(context.Background(), "postgres://db_user:secretsecret@localhost:5432/test_db")
	if err != nil {
		suite.Failf("failed to connect to pgx: %s", "", err)
	}
	suite.pgx = conn
	suite.TestSuite.SetupTest()
}

func (suite *PgxTestSuite) TearDownSuite() {
	suite.TestSuite.TearDownSuite()
	suite.pgx.Close()
}

// GormTestSuite base test suite for gorm tests
type GormTestSuite struct {
	TestSuite
	gorm *gorm.DB
}

func (suite *GormTestSuite) SetupTest() {
	db, err := gorm.Open(postgres.New(postgres.Config{
		DSN: "host=127.0.0.1 user=db_user password=secretsecret dbname=test_db port=5432 sslmode=disable",
	}), &gorm.Config{})
	if err != nil {
		suite.Failf("failed to connect to pgx: %s", "", err)
	}

	suite.gorm = db
	suite.TestSuite.SetupTest()
}
