package outbox

import (
	"context"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gorm.io/gorm"
)

type DBAdapter interface {
	Query(ctx context.Context, query string, args ...any) (Rows, error)
	Exec(ctx context.Context, query string, args ...any) error
}

type Rows interface {
	Next() bool
	Scan(dest ...any) error
	Close() error
}

// PGXRowsWrapper is a wrapper for pgx.Rows
// to implement Rows interface
type PGXRowsWrapper struct {
	rows pgx.Rows
}

func (r PGXRowsWrapper) Next() bool {
	return r.rows.Next()
}

func (r PGXRowsWrapper) Scan(dest ...any) error {
	return r.rows.Scan(dest...)
}

func (r PGXRowsWrapper) Close() error {
	r.rows.Close()
	return nil
}

type PGXAdapter struct {
	conn *pgxpool.Pool
}

func NewPGXAdapter(conn *pgxpool.Pool) *PGXAdapter {
	return &PGXAdapter{conn: conn}
}

func (a *PGXAdapter) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	rows, err := a.conn.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}

	return PGXRowsWrapper{rows: rows}, nil
}

func (a *PGXAdapter) Exec(ctx context.Context, query string, args ...any) error {
	_, err := a.conn.Exec(ctx, query, args)
	if err != nil {
		return err
	}

	return nil
}

type GORMAdapter struct {
	db *gorm.DB
}

func NewGORMAdapter(db *gorm.DB) *GORMAdapter {
	return &GORMAdapter{db: db}
}

func (a *GORMAdapter) Query(ctx context.Context, query string, args ...any) (Rows, error) {
	return a.db.Raw(query, args...).Rows()
}

func (a *GORMAdapter) Exec(ctx context.Context, query string, args ...any) error {
	return a.db.Exec(query, args...).Error
}
