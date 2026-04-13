package worker

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/producer/model"
)

func StartRetryWorker(redisClient *service.RedisClient, pg *service.Postgres) {
	for {
		ctx := context.Background()

		msg, err := redisClient.Client().RPop(ctx, "retry_queue").Result()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("Retrying message")
		err = ProcessEvent([]byte(msg), redisClient, pg)
		if err != nil {
			log.Println("retry failed again:", err)

			var event model.Event
			json.Unmarshal([]byte(msg), &event)

			event.Retry++

			if event.Retry > 3 {
				log.Println("sending to DLQ from retry worker:", event.ID)

				updated, _ := json.Marshal(event)
				redisClient.Client().LPush(ctx, "dlq", updated)
				continue
			}

			updated, _ := json.Marshal(event)
			redisClient.Client().LPush(ctx, "retry_queue", updated)
		}
	}
}
