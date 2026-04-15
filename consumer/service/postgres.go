package service

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	db *pgxpool.Pool
}

func NewPostgres(connStr string) (*Postgres, error) {
	db, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, err
	}
	return &Postgres{db: db}, nil
}

func (p *Postgres) InsertEvent(ctx context.Context, eventID, key, eventType string, payload []byte, ts int64) error {
	_, err := p.db.Exec(ctx,
		`INSERT INTO events (id, key, type, payload, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO NOTHING`,
		eventID, key, eventType, payload, ts,
	)
	return err
}

func (p *Postgres) Close() {
	p.db.Close()
}
