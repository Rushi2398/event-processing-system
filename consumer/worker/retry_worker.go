package worker

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/Rushi2398/event-processing-system/consumer/service"
)

func StartRetryWorker(ctx context.Context, redisClient *service.RedisClient, pg *service.Postgres, wg *sync.WaitGroup, retryLimit int) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				log.Println("Stopping retry worker...")
				return
			default:
				msg, err := redisClient.Client().RPop(ctx, "retry_queue").Result()
				if err != nil {
					time.Sleep(2 * time.Second)
					continue
				}

				log.Println("Retrying message")
				err = ProcessEvent([]byte(msg), redisClient, pg)
				if err != nil {
					log.Println("retry failed again:", err)

					HandleRetry([]byte(msg), redisClient, retryLimit)
				}
			}
		}
	}()
}
