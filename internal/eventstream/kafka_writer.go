package eventstream

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaWriter struct {
	writer *kafka.Writer
}

func NewKafkaWriter(brokers []string, topic string) (*KafkaWriter, error) {
	cleanBrokers := make([]string, 0, len(brokers))
	for _, broker := range brokers {
		if broker = strings.TrimSpace(broker); broker != "" {
			cleanBrokers = append(cleanBrokers, broker)
		}
	}
	if len(cleanBrokers) == 0 || strings.TrimSpace(topic) == "" {
		return nil, errors.New("Kafka brokers and topic are required")
	}
	return &KafkaWriter{writer: &kafka.Writer{
		Addr:         kafka.TCP(cleanBrokers...),
		Topic:        strings.TrimSpace(topic),
		Balancer:     &kafka.Hash{},
		RequiredAcks: kafka.RequireAll,
		Async:        false,
		BatchSize:    100,
		BatchTimeout: 50 * time.Millisecond,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 10 * time.Second,
		MaxAttempts:  3,
	}}, nil
}

func (w *KafkaWriter) WriteEvent(ctx context.Context, event Event) error {
	value, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("encode event %d: %w", event.ID, err)
	}
	if err := w.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(event.AggregateID),
		Value: value,
		Time:  event.OccurredAt,
	}); err != nil {
		return fmt.Errorf("write event %d: %w", event.ID, err)
	}
	return nil
}

func (w *KafkaWriter) Close() error { return w.writer.Close() }
