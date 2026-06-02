package service

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type Postgres struct {
	db *pgxpool.Pool
}

// EventRow holds the fields for a single event to be inserted into the DB.
// Defined here (alongside the DB layer) so the batcher and worker don't need to reach into each other's packages for this type.
type EventRow struct {
	ID           string
	Key          string
	Type         string
	PayloadBytes []byte
	Timestamp    int64
}

func NewPostgres(connStr string) (*Postgres, error) {
	db, err := pgxpool.New(context.Background(), connStr)
	if err != nil {
		return nil, err
	}
	return &Postgres{db: db}, nil
}

// InsertEventBatch inserts multiple events in a single DB round-trip using a dynamically built multi-row INSERT. ON CONFLICT DO NOTHING handles duplicate event IDs gracefully — the insert skips them without returning an error.
// If the batch fails, the caller is responsible for retrying each event individually via the retry queue.
func (p *Postgres) InsertEventBatch(ctx context.Context, rows []EventRow) error {
	if len(rows) == 0 {
		return nil
	}

	var sb strings.Builder
	args := make([]any, 0, len(rows)*5)

	sb.WriteString("INSERT INTO events (id, key, type, payload, created_at) VALUES")
	for i, r := range rows {
		if i > 0 {
			sb.WriteString(", ")
		}
		base := i * 5
		fmt.Fprintf(&sb, "($%d, $%d, $%d, $%d, $%d)", base+1, base+2, base+3, base+4, base+5)
		args = append(args, r.ID, r.Key, r.Type, r.PayloadBytes, r.Timestamp)
	}
	sb.WriteString(" ON CONFLICT (id) DO NOTHING")

	_, err := p.db.Exec(ctx, sb.String(), args...)
	return err
}

// InsertEvent inserts a single event. Kept for use in tests and one-off cases where batching is not appropriate.
func (p *Postgres) InsertEvent(ctx context.Context, eventID, key, eventType string, payload []byte, ts int64) error {
	_, err := p.db.Exec(ctx,
		`INSERT INTO events (id, key, type, payload, created_at)
		 VALUES ($1, $2, $3, $4, $5)
		 ON CONFLICT (id) DO NOTHING`,
		eventID, key, eventType, payload, ts,
	)
	return err
}

// Ping checks Postgres connectivity. Used by the readiness health check.
func (p *Postgres) Ping(ctx context.Context) error {
	return p.db.Ping(ctx)
}

func (p *Postgres) Close() {
	p.db.Close()
}
