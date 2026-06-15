package service

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/Rushi2398/event-processing-system/producer/model"
	"github.com/segmentio/kafka-go"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:                   kafka.TCP(brokers...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: true,
		},
	}
}

func (p *Producer) Publish(ctx context.Context, event model.Event) error {
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	key := event.Key
	if key == "" {
		key = event.ID
	}

	err = p.writer.WriteMessages(ctx,
		kafka.Message{
			Key:   []byte(key),
			Value: data,
		},
	)

	if err != nil {
		slog.Error("failed to publish event", "event_id", event.ID, "key", key, "error", err)
		return err
	}

	return nil
}

// Close flushes pending messages and closes the Kafka writer.
// Must be called on shutdown to avoid dropping buffered messages.
func (p *Producer) Close() error {
	return p.writer.Close()
}
