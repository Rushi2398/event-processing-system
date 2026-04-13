package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"strconv"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
	"github.com/Rushi2398/event-processing-system/producer/model"
	"github.com/joho/godotenv"
	"github.com/segmentio/kafka-go"
)

func main() {
	err := godotenv.Load("../.env")
	if err != nil {
		log.Println("No .env file found")
	}
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	topic := os.Getenv("KAFKA_TOPIC")
	groupID := os.Getenv("KAFKA_GROUP_ID")

	consumer := service.NewConsumer(brokers, topic, groupID)
	redisClient := service.NewRedisClient(os.Getenv("REDIS_ADDR"))
	pg, err := service.NewPostgres(os.Getenv("POSTGRES_URL"))
	if err != nil {
		log.Fatal("failed to connect postgres:", err)

	}
	go worker.StartRetryWorker(redisClient, pg)

	//Worker Pool
	workerCountStr := os.Getenv("WORKER_COUNT")
	workerCount, _ := strconv.Atoi(workerCountStr)

	retryLimitStr := os.Getenv("RETRY_LIMIT")
	retryLimit, _ := strconv.Atoi(retryLimitStr)
	jobs := make(chan []byte, 100)

	for i := 0; i < workerCount; i++ {
		go func(id int) {
			for msg := range jobs {
				log.Printf("Worker %d processing message\n", id)
				worker.ProcessEvent(msg, redisClient, pg)
			}
		}(i)
	}
	log.Println("Consumer started...")

	//Consume Messages
	for {
		msg, err := consumer.FetchMessage(context.Background())
		if err != nil {
			log.Println("error fetching message:", err)
			continue
		}

		go func(m kafka.Message) {
			err := worker.ProcessEvent(m.Value, redisClient, pg)

			if err != nil {
				log.Println("processing failed:", err)

				// Retry logic
				ctx := context.Background()

				var event model.Event
				json.Unmarshal(m.Value, &event)

				event.Retry++
				if event.Retry > retryLimit {
					log.Println("sending to DLQ:", event.ID)
					updated, _ := json.Marshal(event)
					redisClient.Client().LPush(ctx, "dlq", updated)
					return
				}
				updated, _ := json.Marshal(event)
				redisClient.Client().LPush(ctx, "retry_queue", updated)

				return

			}
			if err := consumer.CommitMessage(context.Background(), m); err != nil {
				log.Println("failed to commit:", err)
			}
		}(msg)
	}
}
