package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

const (
	baseRetryDelay = time.Second
	maxRetryDelay  = 5 * time.Minute
)

// HandleRetry increments the retry counter on an event and either schedules
// it for a future retry attempt (with exponential backoff + jitter) or moves
// it to the dead-letter queue when the retry limit is exceeded.
//
// It always returns an error so callers can log/alert if the retry or DLQ
func HandleRetry(ctx context.Context, msg []byte, redisClient *service.RedisClient, retryLimit int) error {

	var event model.Event
	if err := json.Unmarshal(msg, &event); err != nil {
		return fmt.Errorf("HandleRetry: failed to unmarshal event: %w", err)
	}

	event.Retry++

	updated, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("HandleRetry: failed to marshal event %s: %w", event.ID, err)
	}
	if event.Retry > retryLimit {
		log.Printf("[retry] event %s exceeded retry limit (%d/%d), sending to DLQ", event.ID, event.Retry-1, retryLimit)
		if err := redisClient.PushToDLQ(ctx, updated); err != nil {
			return fmt.Errorf("HandleRetry: failed to push event %s to DLQ: %w", event.ID, err)
		}
		return nil
	}

	delay := retryDelay(event.Retry)
	log.Printf("[retry] scheduling retry %d/%d for event %s in %s", event.Retry, retryLimit, event.ID, delay.Round(time.Millisecond))

	if err := redisClient.ScheduleRetry(ctx, updated, delay); err != nil {
		return fmt.Errorf("HandleRetry: failed to schedule retry for event %s: %w", event.ID, err)
	}
	return nil

}

// retryDelay computes the backoff for the given attempt number using
// exponential backoff with full jitter:
//
//	delay = random(0, min(maxRetryDelay, baseDelay * 2^(attempt-1)))
//
// Full jitter is preferred over pure exponential because it spreads retried
// events evenly over the backoff window, reducing thundering-herd spikes.
func retryDelay(attempt int) time.Duration {
	exp := 1 << (attempt - 1) // 1, 2, 4, 8, ...
	cap := time.Duration(exp) * baseRetryDelay
	if cap > maxRetryDelay {
		cap = maxRetryDelay
	}
	// Full jitter: uniform random in [0, cap]
	return time.Duration(rand.Int63n(int64(cap) + 1))
}
