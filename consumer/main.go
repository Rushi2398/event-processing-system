package main

import (
	"context"
	"log"

	"github.com/Rushi2398/event-processing-system/consumer/service"
	"github.com/Rushi2398/event-processing-system/consumer/worker"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "events"
	groupID := "event-consumer-group"

	consumer := service.NewConsumer(brokers, topic, groupID)

	//Worker Pool
	workerCount := 5
	jobs := make(chan []byte, 100)

	for i := range workerCount {
		go func(id int) {
			for msg := range jobs {
				log.Printf("Worker %d processing message\n", id)
				worker.ProcessEvent(msg)
			}
		}(i)
	}
	log.Println("Consumer started...")

	//Consume Messages
	for {
		msg, err := consumer.ReadMessage(context.Background())
		if err != nil {
			log.Println("error reading message:", err)
			continue
		}
		jobs <- msg.Value
	}
}
