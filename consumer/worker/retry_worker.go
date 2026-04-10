package worker

import (
	"context"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
)

func StartRetryWorker(redisClient *service.RedisClient) {
	for {
		ctx := context.Background()

		msg, err := redisClient.Client().RPop(ctx, "retry_queue").Result()
		if err != nil {
			time.Sleep(2 * time.Second)
			continue
		}

		ProcessEvent([]byte(msg), redisClient)
	}
}
