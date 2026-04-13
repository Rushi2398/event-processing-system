package main

import (
	"log"
	"os"

	"github.com/Rushi2398/event-processing-system/producer/handler"
	"github.com/Rushi2398/event-processing-system/producer/service"
	"github.com/gin-gonic/gin"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		log.Println("No .env file found")
	}
	brokers := []string{os.Getenv("KAFKA_BROKERS")}
	topic := os.Getenv("KAFKA_TOPIC")

	producer := service.NewProducer(brokers, topic)
	handler := handler.NewEventHandler(producer)

	r := gin.Default()

	r.POST("/events", handler.PublishEvent)
	log.Println("Producer running on port 8080")
	r.Run(":8080")
}
