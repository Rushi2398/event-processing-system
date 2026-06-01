package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
)

// submission pairs an event row with a result channel the submitting goroutine
// blocks on. When the batch is flushed, every submission in it receives the
// same error (nil on success, the insert error on failure).
type submission struct {
	row    service.EventRow
	result chan error
}

// BatchInserter collects individual event inserts and flushes them to
// PostgreSQL in batches, reducing DB round-trips from O(N) to O(N/batchSize).
// A flush is triggered by whichever condition comes first:
//   - the pending batch reaches batchSize, or
//   - flushInterval elapses without a size-triggered flush
//
// This ensures low-volume periods still get timely flushes, while high-volume
// periods batch aggressively.
type BatchInserter struct {
	input         chan submission
	pg            *service.Postgres
	batchSize     int
	flushInterval time.Duration
}

func NewBatcher(pg *service.Postgres, batchSize int, flushInterval time.Duration) *BatchInserter {
	return &BatchInserter{
		// Buffer is 4x batchSize so fast producers don't block on Submit while the batcher is mid-flush.
		input:         make(chan submission, batchSize*4),
		pg:            pg,
		batchSize:     batchSize,
		flushInterval: flushInterval,
	}
}

// Submit sends an event row to the batcher and blocks until the batch it
// belongs to has been flushed to the DB. Returns the insert error, if any.
// All events in the same batch share the same error — if the batch fails,
// every caller gets the error and can route their event to the retry queue.
func (b *BatchInserter) Submit(ctx context.Context, row service.EventRow) error {
	// Send the submission — blocks only if the input buffer is full (i.e. the batcher can't keep up). Under normal load this is instant.
	result := make(chan error, 1)
	select {
	case b.input <- submission{row: row, result: result}:
	case <-ctx.Done():
		return ctx.Err()
	}
	// Wait for the batch to be flushed.
	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Start launches the background goroutine that accumulates and flushes batches.
// It uses a separate WaitGroup (batcherWg) from the main worker pool so the caller can stop workers first, then stop the batcher last.
func (b *BatchInserter) Start(batcherWg *sync.WaitGroup) {
	batcherWg.Add(1)
	go func() {
		defer batcherWg.Done()
		pending := make([]submission, 0, b.batchSize)
		ticker := time.NewTicker(b.flushInterval)
		defer ticker.Stop()

		flush := func() {
			if len(pending) == 0 {
				return
			}
			rows := make([]service.EventRow, len(pending))
			for i, s := range pending {
				rows[i] = s.row
			}
			// Use a background context here — by the time Stop() is called
			// and the channel is closed, the main ctx may already be cancelled.
			// We still want to commit the final batch.
			err := b.pg.InsertEventBatch(context.Background(), rows)
			if err != nil {
				log.Printf("[batcher] batch insert failed (%d events): %v", len(pending), err)
			} else {
				log.Printf("[batcher] flushed %d event(s) to DB", len(pending))
			}

			for _, s := range pending {
				s.result <- err
			}
			// Reslice to zero but keep the allocated capacity for the next batch.
			pending = pending[:0]
		}

		log.Printf("[batcher] started")
		for {
			select {
			case s, ok := <-b.input:
				if !ok {
					// Stop() was called — drain and flush whatever remains.
					flush()
					log.Println("[batcher] stopped")
					return
				}
				pending = append(pending, s)
				if len(pending) >= b.batchSize {
					flush()
				}
			case <-ticker.C:
				flush()
			}
		}
	}()
}

// Stop signals the batcher to flush remaining events and exit.
// Must only be called after all goroutines that call Submit have returned; closing the channel while a Submit is in-flight would panic.
func (b *BatchInserter) Stop() {
	close(b.input)
}
