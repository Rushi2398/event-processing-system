package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

func ProcessEvent(msg []byte, redisClient *service.RedisClient) {
	var event model.Event

	if err := json.Unmarshal(msg, &event); err != nil {
		log.Println("failed to parse event:", err)
		return
	}

	ctx := context.Background()

	//Idempotency Check
	processed, err := redisClient.IsProcessed(ctx, event.ID)
	if err != nil {
		log.Println("redis error:", err)
		return
	}
	if processed {
		log.Println("event already processed:", event.ID)
		return
	}

	// log.Printf("Processing event: ID=%s Type=%s Key=%s\n", event.ID, event.Type, event.Key)

	if err := processBusinessLogic(event); err != nil {
		log.Println("processing failed, pushing to retry:", err)

		//Push to Retry Queue
		err := redisClient.Client().LPush(ctx, "retry_queue", msg).Err()
		if err != nil {
			log.Println("failed to push retry:", err)
		}
		return
	}

	if err := redisClient.MarkProcessed(ctx, event.ID); err != nil {
		log.Println("failed to mark processed:", err)
	}
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
