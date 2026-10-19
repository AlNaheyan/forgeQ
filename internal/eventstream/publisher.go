package eventstream

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

type PublisherConfig struct {
	Owner         string
	BatchSize     int
	LeaseDuration time.Duration
	PollInterval  time.Duration
}

type Publisher struct {
	outbox Outbox
	writer Writer
	config PublisherConfig
	logger *slog.Logger
}

func NewPublisher(outbox Outbox, writer Writer, config PublisherConfig, logger *slog.Logger) (*Publisher, error) {
	if outbox == nil || writer == nil {
		return nil, errors.New("event outbox and writer are required")
	}
	if config.Owner == "" || config.BatchSize < 1 || config.LeaseDuration <= 0 || config.PollInterval <= 0 {
		return nil, errors.New("publisher owner, positive batch size, lease, and poll interval are required")
	}
	if logger == nil {
		logger = slog.Default()
	}
	return &Publisher{outbox: outbox, writer: writer, config: config, logger: logger}, nil
}

func (p *Publisher) Run(ctx context.Context) {
	if err := p.PublishPending(ctx); err != nil && ctx.Err() == nil {
		p.logger.Warn("publish event batch failed", "error", err)
	}
	ticker := time.NewTicker(p.config.PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := p.PublishPending(ctx); err != nil && ctx.Err() == nil {
				p.logger.Warn("publish event batch failed", "error", err)
			}
		}
	}
}

func (p *Publisher) PublishPending(ctx context.Context) error {
	events, err := p.outbox.ClaimEvents(ctx, p.config.Owner, p.config.BatchSize, p.config.LeaseDuration)
	if err != nil {
		return fmt.Errorf("claim event outbox: %w", err)
	}
	for index, event := range events {
		if err := p.writer.WriteEvent(ctx, event); err != nil {
			joined := err
			for _, pending := range events[index:] {
				if retryErr := p.outbox.RetryEvent(ctx, p.config.Owner, pending.ID, retryDelay(pending.Attempts), err); retryErr != nil {
					joined = errors.Join(joined, fmt.Errorf("release event %d for retry: %w", pending.ID, retryErr))
				}
			}
			return fmt.Errorf("write event %d to Kafka: %w", event.ID, joined)
		}
		if err := p.outbox.MarkEventPublished(ctx, p.config.Owner, event.ID); err != nil {
			return fmt.Errorf("mark event %d published: %w", event.ID, err)
		}
	}
	return nil
}

func retryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := time.Second
	for range attempt - 1 {
		if delay >= time.Minute/2 {
			return time.Minute
		}
		delay *= 2
	}
	if delay > time.Minute {
		return time.Minute
	}
	return delay
}
