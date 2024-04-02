package outbox

import (
	"context"
	"testing"

	_ "github.com/lib/pq"
	"github.com/stretchr/testify/suite"
)

// RepositoryTestSuite pgx repository tests
type RepositoryTestSuite struct {
	TestSuite
}

func (suite *RepositoryTestSuite) SetupTest() {
	suite.TestSuite.SetupTest()
}

func (suite *RepositoryTestSuite) TestFetch() {
	tt := []struct {
		message string
		adapter DBAdapter
		equals  int
	}{
		{
			message: "Test with pgx adapter",
			adapter: NewPGXAdapter(suite.pgxDB),
			equals:  2,
		},
		{
			message: "Test with gorm adapter",
			adapter: NewGORMAdapter(suite.gormDB),
			equals:  2,
		},
	}

	for _, tc := range tt {
		suite.Run(tc.message, func() {
			r := NewRepository(tc.adapter)
			suite.pollute()
			defer suite.cleanDB()

			c := map[string]struct{}{}
			ch := r.Fetch(context.Background(), 100)
			for m := range ch {
				c[m.ID] = struct{}{}
			}

			suite.Equal(tc.equals, len(c))
		})
	}
}

func (suite *RepositoryTestSuite) TestMarkConsumed() {
	tt := []struct {
		message string
		adapter DBAdapter
		equals  int
	}{
		{
			message: "Test with pgx adapter",
			adapter: NewPGXAdapter(suite.pgxDB),
			equals:  1,
		},
		{
			message: "Test with gorm adapter",
			adapter: NewGORMAdapter(suite.gormDB),
			equals:  1,
		},
	}

	for _, tc := range tt {
		suite.Run(tc.message, func() {
			r := NewRepository(tc.adapter)
			suite.pollute()
			defer suite.cleanDB()

			err := r.MarkConsumed(context.Background(), []Message{{ID: "f53ec986-345f-48a4-b248-430a7d7f342a"}})
			if err != nil {
				return
			}

			c := map[string]struct{}{}
			ch := r.Fetch(context.Background(), 100)
			for m := range ch {
				c[m.ID] = struct{}{}
			}

			suite.Equal(tc.equals, len(c))
		})
	}
}

func TestRepositoryTestSuite(t *testing.T) {
	suite.Run(t, new(RepositoryTestSuite))
}
