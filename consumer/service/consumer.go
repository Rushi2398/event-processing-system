package service

import (
	"context"

	"github.com/segmentio/kafka-go"
)

type Consumer struct {
	reader *kafka.Reader
}

func NewConsumer(brokers []string, topic string, groupID string) *Consumer {
	return &Consumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers: brokers,
			Topic:   topic,
			GroupID: groupID,
		}),
	}
}

func (c *Consumer) FetchMessage(ctx context.Context) (kafka.Message, error) {
	return c.reader.FetchMessage(ctx)
}

func (c *Consumer) CommitMessage(ctx context.Context, msg kafka.Message) error {
	return c.reader.CommitMessages(ctx, msg)
}

func (c *Consumer) Close() {
	c.reader.Close()
}
