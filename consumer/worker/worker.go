package worker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

func ProcessEvent(msg []byte, redisClient *service.RedisClient, pg *service.Postgres) error {
	var event model.Event

	if err := json.Unmarshal(msg, &event); err != nil {
		log.Println("failed to parse event:", err)
		return err
	}

	ctx := context.Background()

	//Idempotency Check
	processed, err := redisClient.IsProcessed(ctx, event.ID)
	if err != nil {
		return err
	}
	if processed {
		log.Println("event already processed:", event.ID)
		return nil
	}

	// log.Printf("Processing event: ID=%s Type=%s Key=%s\n", event.ID, event.Type, event.Key)

	if err := processBusinessLogic(event, pg); err != nil {
		return err
	}

	return redisClient.MarkProcessed(ctx, event.ID)
}

func processBusinessLogic(event model.Event, pg *service.Postgres) error {
	// Simulate real work
	log.Printf("Processing business logic for event: %s\n", event.ID)

	payloadBytes, err := json.Marshal(event.Payload)
	if err != nil {
		return err
	}

	// TODO: Replace with real logic
	// e.g. DB insert, API call, etc.
	return pg.InsertEvent(
		context.Background(),
		event.ID,
		event.Key,
		event.Type,
		payloadBytes,
		event.Timestamp,
	)
}
