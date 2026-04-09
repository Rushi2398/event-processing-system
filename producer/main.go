package main

import (
	"log"

	"github.com/Rushi2398/event-processing-system/producer/handler"
	"github.com/Rushi2398/event-processing-system/producer/service"
	"github.com/gin-gonic/gin"
)

func main() {
	brokers := []string{"localhost:9092"}
	topic := "events"

	producer := service.NewProducer(brokers, topic)
	handler := handler.NewEventHandler(producer)

	r := gin.Default()

	r.POST("/events", handler.PublishEvent)
	log.Println("Producer running on port 8080")
	r.Run(":8080")
}
