package outbox

import (
	"context"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/suite"
	"gorm.io/gorm"
	"testing"
)

// PgxPersisterTestSuite tests for pgx persister
type PgxPersisterTestSuite struct {
	PgxTestSuite
	p *PgxPersister
	r *PgxRepository
}

func (suite *PgxPersisterTestSuite) SetupTest() {
	suite.PgxTestSuite.SetupTest()
	suite.p = &PgxPersister{db: suite.pgx}
	suite.r = &PgxRepository{db: suite.pgx}
}

func (suite *PgxPersisterTestSuite) TestPersistInTx() {
	suite.pollute()
	defer suite.cleanDB()
	err := suite.p.PersistInTx(context.Background(), func(tx pgx.Tx) ([]*Message, error) {
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

func TestPgxPersister(t *testing.T) {
	suite.Run(t, new(PgxPersisterTestSuite))
}

// GormPersisterTestSuite tests for gorm persister
type GormPersisterTestSuite struct {
	GormTestSuite
	p *GormPersister
	r *GormRepository
}

func (suite *GormPersisterTestSuite) SetupTest() {
	suite.GormTestSuite.SetupTest()
	suite.p = &GormPersister{db: suite.gorm}
	suite.r = &GormRepository{db: suite.gorm}
}

func (suite *GormPersisterTestSuite) TestPersistInTx() {
	suite.pollute()
	defer suite.cleanDB()
	err := suite.p.PersistInTx(func(tx *gorm.DB) ([]*Message, error) {
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

func TestGormPersister(t *testing.T) {
	suite.Run(t, new(GormPersisterTestSuite))
}
