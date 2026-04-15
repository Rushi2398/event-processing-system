package worker

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

func HandleRetry(msg []byte, redisClient *service.RedisClient, retryLimit int) {
	ctx := context.Background()

	var event model.Event
	if err := json.Unmarshal(msg, &event); err != nil {
		log.Println("failed to unmarshal event:", err)
		return
	}

	event.Retry++

	if event.Retry > retryLimit {
		log.Println("sending to DLQ:", event.ID)

		updated, _ := json.Marshal(event)
		redisClient.Client().LPush(ctx, "dlq", updated)
		return
	}

	updated, _ := json.Marshal(event)
	redisClient.Client().LPush(ctx, "retry_queue", updated)
}
