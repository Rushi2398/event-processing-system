package worker

import (
	"context"
	"encoding/json"
	"log"

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
	var event model.Event

	if err := json.Unmarshal(msg, &event); err != nil {
		log.Printf("[worker] failed to parse event: %v", err)
		return err
	}

	//Idempotency Check
	locked, err := redisClient.TryLock(ctx, event.ID)
	if err != nil {
		return err
	}

	if !locked {
		log.Printf("[worker] event %s already being processed, skipping", event.ID)
		return nil
	}

	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	if err := batcher.Submit(ctx, service.EventRow{
		ID:           event.ID,
		Key:          event.Key,
		Type:         event.Type,
		PayloadBytes: payloadBytes,
		Timestamp:    event.Timestamp,
	}); err != nil {
		return err
	}

	return redisClient.MarkProcessed(ctx, event.ID)
}
