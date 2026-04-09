package service

import (
	"context"
	"encoding/json"
	"log"

	"github.com/Rushi2398/event-processing-system/producer/model"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.LeastBytes{},
		},
	}
}

func (p *Producer) Publish(event model.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = p.writer.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte(event.ID),
			Value: data,
		},
	)

	if err != nil {
		log.Println("failed to write message:", err)
		return err
	}

	return nil
}
