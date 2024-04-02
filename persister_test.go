package outbox

import (
	"context"
	"database/sql"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
)

// PgxPersisterTestSuite tests for pgx persister
type PgxPersisterTestSuite struct {
	TestSuite
	p *PgxPersister
	r *Repository
}

func (suite *PgxPersisterTestSuite) SetupTest() {
	suite.TestSuite.SetupTest()
	suite.p = &PgxPersister{db: suite.pgxDB}
	suite.r = &Repository{db: NewPGXAdapter(suite.pgxDB)}
}

func (suite *PgxPersisterTestSuite) TestPersistInTx() {
	suite.pollute()
	defer suite.cleanDB()

	err := suite.p.PersistInTx(context.Background(), func(tx pgx.Tx) ([]Message, error) {
		return []Message{
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342f", Payload: map[string]string{}, PartitionKey: sql.NullInt64{
				Int64: 1,
				Valid: true,
			}},
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342e", Payload: map[string]string{}, PartitionKey: sql.NullInt64{
				Int64: 2,
				Valid: true,
			}},
		}, nil
	})

	if err != nil {
		suite.Failf("Cannot persist messages", "%s", err)
	}

	c := map[string]struct{}{}
	ch := suite.r.Fetch(context.TODO(), 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(4, len(c))
}

func TestPgxPersister(t *testing.T) {
	suite.Run(t, new(PgxPersisterTestSuite))
}

// GormPersisterTestSuite tests for gorm persister
type GormPersisterTestSuite struct {
	TestSuite
	p *GormPersister
	r *Repository
}

func (suite *GormPersisterTestSuite) SetupTest() {
	suite.TestSuite.SetupTest()
	suite.p = &GormPersister{db: suite.gormDB}
	suite.r = &Repository{db: NewGORMAdapter(suite.gormDB)}
}

func (suite *GormPersisterTestSuite) TestPersistInTx() {
	suite.pollute()
	defer suite.cleanDB()

	err := suite.p.PersistInTx(func(tx *gorm.DB) ([]Message, error) {
		return []Message{
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342f", Payload: map[string]string{}},
			{ID: "f53ec986-345f-48a4-b248-430a7d7f342e", Payload: map[string]string{}},
		}, nil
	})

	if err != nil {
		suite.Failf("Cannot persist messages", "%s", err)
	}

	c := map[string]struct{}{}
	ch := suite.r.Fetch(context.TODO(), 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(4, len(c))
}

func TestGormPersister(t *testing.T) {
	suite.Run(t, new(GormPersisterTestSuite))
}
