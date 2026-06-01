package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
)

const (
	// schedulerInterval is how often the scheduler checks for due retries.
	schedulerInterval = 200 * time.Millisecond

	// brpopTimeout is how long BRPop blocks before returning to check ctx.
	// Keeping this short (2s) means graceful shutdown is responsive.
	brpopTimeout = 2 * time.Second
)

// StartRetryScheduler runs a single goroutine that periodically moves events
// whose scheduled retry time has passed from the retry_schedule sorted set
// into the retry_queue list.
func StartRetryScheduler(ctx context.Context, redisClient *service.RedisClient, wg *sync.WaitGroup) {
	wg.Add(1)
	go func() {
		defer wg.Done()

		ticker := time.NewTicker(schedulerInterval)
		defer ticker.Stop()

		log.Println("[retry-scheduler] started")
		for {
			select {
			case <-ctx.Done():
				log.Println("[retry-scheduler] stopping")
				return
			case <-ticker.C:
				n, err := redisClient.MoveDueRetries(ctx)
				if err != nil && ctx.Err() == nil {
					log.Printf("[retry-scheduler] error moving due retries: %v", err)
					continue
				}
				if n > 0 {
					log.Printf("[retry-scheduler] moved %d event(s) to retry_queue", n)
				}
			}
		}
	}()
}

// StartRetryWorkers launches a pool of workerCount goroutines. Each worker
// uses BRPop to block until a message appears in retry_queue, then processes
// it. On failure the event is passed back to HandleRetry, which schedules
// another attempt or moves it to DLQ.
//
// The retry worker pool is intentionally separate from the main Kafka worker
// pool so retry storms don't starve fresh events.
func StartRetryWorkers(ctx context.Context, redisClient *service.RedisClient,
	batcher *BatchInserter, wg *sync.WaitGroup, retryLimit int, workerCount int) {
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			log.Printf("[retry-worker-%d] started", id)
			for {
				// BRPop blocks for brpopTimeout, then returns a timeout error.
				// We loop so we can check ctx.Done() between polls.
				msg, err := redisClient.PopRetryMessage(ctx, brpopTimeout)
				if err != nil {
					if ctx.Err() != nil {
						log.Printf("[retry-worker-%d] stopping", id)
						return
					}
					// Timeout or transient Redis error — loop and try again.
					log.Printf("[retry-worker-%d] error popping retry message: %v", id, err)
					continue
				}
				// BRPop returns [key, value].
				if msg == nil {
					// Timeout — nothing ready yet, loop to check ctx.
					continue
				}

				log.Printf("[retry-worker-%d] processing retry message", id)
				if err := ProcessEvent(ctx, msg, redisClient, batcher); err != nil {
					log.Printf("[retry-worker-%d] processing failed: %v", id, err)

					if hErr := HandleRetry(ctx, msg, redisClient, retryLimit); hErr != nil {
						// If HandleRetry itself fails (e.g. Redis down), log loudly.
						// In Phase 3 this becomes a Prometheus counter + alert.
						log.Printf("[retry-worker-%d] CRITICAL: failed to handle retry: %v", id, hErr)
					}
				}
			}
		}(i)
	}
}
