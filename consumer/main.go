package main

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
	"github.com/Rushi2398/event-processing-system/producer/model"
	"github.com/segmentio/kafka-go"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "events"
	groupID := "event-consumer-group"

	consumer := service.NewConsumer(brokers, topic, groupID)
	redisClient := service.NewRedisClient("localhost:6379")

	go worker.StartRetryWorker(redisClient)

	//Worker Pool
	workerCount := 5
	jobs := make(chan []byte, 100)

	for i := 0; i < workerCount; i++ {
		go func(id int) {
			for msg := range jobs {
				log.Printf("Worker %d processing message\n", id)
				worker.ProcessEvent(msg, redisClient)
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
			err := worker.ProcessEvent(m.Value, redisClient)

			if err != nil {
				log.Println("processing failed:", err)

				// Retry logic
				ctx := context.Background()

				var event model.Event
				json.Unmarshal(m.Value, &event)

				event.Retry++
				if event.Retry > 3 {
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
