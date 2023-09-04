package outbox

import (
	"context"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/suite"
	"testing"
	"time"
)

// PgxRepositoryTestSuite pgx repository tests
type PgxRepositoryTestSuite struct {
	PgxTestSuite
	r *PgxRepository
}

func (suite *PgxRepositoryTestSuite) SetupTest() {
	suite.PgxTestSuite.SetupTest()
	suite.r = &PgxRepository{db: suite.pgx}
}

func (suite *PgxRepositoryTestSuite) TestFetch() {
	suite.pollute()
	defer suite.cleanDB()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	c := map[string]struct{}{}
	ch := suite.r.Fetch(ctx, time.Millisecond, 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(2, len(c))
}

func (suite *PgxRepositoryTestSuite) TestMarkConsumed() {
	suite.pollute()
	defer suite.cleanDB()
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	err := suite.r.MarkConsumed(context.Background(), Message{ID: "f53ec986-345f-48a4-b248-430a7d7f342a"})
	if err != nil {
		return
	}

	c := map[string]struct{}{}
	ch := suite.r.Fetch(ctx, time.Millisecond, 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(1, len(c))
}

func TestPgxRepository(t *testing.T) {
	suite.Run(t, new(PgxRepositoryTestSuite))
}

// GormRepositoryTestSuite gorm repository tests
type GormRepositoryTestSuite struct {
	GormTestSuite
	r *GormRepository
}

func (suite *GormRepositoryTestSuite) SetupTest() {
	suite.GormTestSuite.SetupTest()
	suite.r = &GormRepository{db: suite.gorm}
}

func (suite *GormRepositoryTestSuite) TestFetch() {
	suite.pollute()
	defer suite.cleanDB()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	c := map[string]struct{}{}
	ch := suite.r.Fetch(ctx, time.Millisecond, 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(2, len(c))
}

func (suite *GormRepositoryTestSuite) TestMarkConsumed() {
	suite.pollute()
	defer suite.cleanDB()

	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond*100)
	defer cancel()

	err := suite.r.MarkConsumed(context.Background(), Message{ID: "f53ec986-345f-48a4-b248-430a7d7f342a"})
	if err != nil {
		return
	}

	c := map[string]struct{}{}
	ch := suite.r.Fetch(ctx, time.Millisecond, 100)
	for m := range ch {
		c[m.ID] = struct{}{}
	}

	suite.Equal(1, len(c))
}

func TestGormRepository(t *testing.T) {
	suite.Run(t, new(GormRepositoryTestSuite))
}

//
//func ExampleGormRepository_PersistInTx() {
//	db, err := gorm.Open(postgres.New(postgres.Config{
//		DSN: "host=127.0.0.1 user=db_user password=secretsecret dbname=test_db port=5432 sslmode=disable",
//	}), &gorm.Config{})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	r := &GormPersister{
//		db: db,
//	}
//
//	err = r.PersistInTx(func(tx *gorm.DB) ([]*Message, error) {
//		tx.Exec("SELECT 1")
//
//		return []*Message{{ID: "f53ec986-345f-48a4-b248-430a7d7f342a", EventType: "asdasd", Payload: []byte("{}"), Exchange: "asdad", RoutingKey: "Asdasda"}}, nil
//	})
//	if err != nil {
//		log.Fatal(err)
//	}
//
//	//Output: 1
//}
