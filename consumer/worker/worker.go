package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

func ProcessEvent(msg []byte, redisClient *service.RedisClient) error {
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

	if err := processBusinessLogic(event); err != nil {
		return err
	}

	return redisClient.MarkProcessed(ctx, event.ID)
}

func processBusinessLogic(event model.Event) error {
	// Simulate real work
	log.Printf("Processing business logic for event: %s\n", event.ID)

	// Simulate failure (for testing retry)
	if event.Type == "fail_event" {
		return fmt.Errorf("simulated failure")
	}

	// TODO: Replace with real logic
	// e.g. DB insert, API call, etc.

	return nil
}
