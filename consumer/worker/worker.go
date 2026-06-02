package worker

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/observability"
	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

// ProcessEvent handles a single raw event message from Kafka or the retry queue.
// Steps:
//  1. Unmarshal the event
//  2. Acquire a Redis lock — skips silently if another worker is already on it
//  3. Submit to the BatchInserter — blocks until the batch is flushed to DB
//  4. Mark the event as processed in Redis (idempotency record)
//
// ctx is the main application context. Cancellation (shutdown) causes Submit to return immediately so workers don't get stuck waiting on a final flush.
func ProcessEvent(ctx context.Context, msg []byte, redisClient *service.RedisClient, batcher *BatchInserter) error {
	start := time.Now()
	var event model.Event

	if err := json.Unmarshal(msg, &event); err != nil {
		slog.Error("[worker] failed to parse event", "error", err)
		observability.EventsProcessedTotal.WithLabelValues("failed").Inc()
		return err
	}

	//Idempotency Check
	locked, err := redisClient.TryLock(ctx, event.ID)
	if err != nil {
		observability.EventsProcessedTotal.WithLabelValues("failed").Inc()
		return err
	}

	if !locked {
		slog.Info("[worker] event already being processed, skipping", "event_id", event.ID)
		observability.EventsProcessedTotal.WithLabelValues("duplicate").Inc()
		return nil
	}

	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		observability.EventsProcessedTotal.WithLabelValues("failed").Inc()
		return err
	}

	if err := batcher.Submit(ctx, service.EventRow{
		ID:           event.ID,
		Key:          event.Key,
		Type:         event.Type,
		PayloadBytes: payloadBytes,
		Timestamp:    event.Timestamp,
	}); err != nil {
		observability.EventsProcessedTotal.WithLabelValues("failed").Inc()
		observability.EventProcessingDuration.WithLabelValues("failed").Observe(time.Since(start).Seconds())
		return err
	}

	if err := redisClient.MarkProcessed(ctx, event.ID); err != nil {
		// Non-fatal: the event was inserted. Log and continue — worst case
		// a duplicate is skipped next time via TryLock.
		slog.Warn("failed to mark event as processed", "event_id", event.ID, "error", err)
	}

	duration := time.Since(start)
	observability.EventsProcessedTotal.WithLabelValues("success").Inc()
	observability.EventProcessingDuration.WithLabelValues("success").Observe(duration.Seconds())
	slog.Info("event processed", "event_id", event.ID, "duration_ms", duration.Milliseconds(), "retry", event.Retry)

	return nil
}
