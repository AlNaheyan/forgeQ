package eventstream

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"
	"time"
)

type testOutbox struct {
	events   []Event
	marked   []int64
	retried  []int64
	delays   []time.Duration
	retryErr error
}

func (o *testOutbox) ClaimEvents(context.Context, string, int, time.Duration) ([]Event, error) {
	return o.events, nil
}
func (o *testOutbox) MarkEventPublished(_ context.Context, _ string, id int64) error {
	o.marked = append(o.marked, id)
	return nil
}
func (o *testOutbox) RetryEvent(_ context.Context, _ string, id int64, delay time.Duration, _ error) error {
	o.retried = append(o.retried, id)
	o.delays = append(o.delays, delay)
	return o.retryErr
}

type testWriter struct {
	writeIDs []int64
	failID   int64
}

func (w *testWriter) WriteEvent(_ context.Context, event Event) error {
	w.writeIDs = append(w.writeIDs, event.ID)
	if event.ID == w.failID {
		return errors.New("broker unavailable")
	}
	return nil
}
func (*testWriter) Close() error { return nil }

func TestPublisherPublishesAndMarksEvents(t *testing.T) {
	outbox := &testOutbox{events: []Event{{ID: 1}, {ID: 2}}}
	writer := &testWriter{}
	publisher := newTestPublisher(t, outbox, writer)
	if err := publisher.PublishPending(context.Background()); err != nil {
		t.Fatalf("publish pending: %v", err)
	}
	if !reflect.DeepEqual(writer.writeIDs, []int64{1, 2}) || !reflect.DeepEqual(outbox.marked, []int64{1, 2}) {
		t.Fatalf("writes=%v marked=%v", writer.writeIDs, outbox.marked)
	}
}

func TestPublisherSchedulesFailedAndRemainingEvents(t *testing.T) {
	outbox := &testOutbox{events: []Event{{ID: 1, Attempts: 1}, {ID: 2, Attempts: 2}, {ID: 3, Attempts: 3}}}
	writer := &testWriter{failID: 2}
	publisher := newTestPublisher(t, outbox, writer)
	if err := publisher.PublishPending(context.Background()); err == nil {
		t.Fatal("expected write failure")
	}
	if !reflect.DeepEqual(outbox.marked, []int64{1}) || !reflect.DeepEqual(outbox.retried, []int64{2, 3}) {
		t.Fatalf("marked=%v retried=%v", outbox.marked, outbox.retried)
	}
	if !reflect.DeepEqual(outbox.delays, []time.Duration{2 * time.Second, 4 * time.Second}) {
		t.Fatalf("retry delays=%v", outbox.delays)
	}
}

func TestRetryDelayCapsAtOneMinute(t *testing.T) {
	if got := retryDelay(100); got != time.Minute {
		t.Fatalf("retry delay=%s, want 1m", got)
	}
}

func TestKafkaWriterRequiresBrokersAndTopic(t *testing.T) {
	if _, err := NewKafkaWriter(nil, "topic"); err == nil {
		t.Fatal("expected missing brokers to fail")
	}
	if _, err := NewKafkaWriter([]string{"broker:9092"}, " "); err == nil {
		t.Fatal("expected missing topic to fail")
	}
}

func newTestPublisher(t *testing.T, outbox *testOutbox, writer *testWriter) *Publisher {
	t.Helper()
	publisher, err := NewPublisher(outbox, writer, PublisherConfig{
		Owner: "test", BatchSize: 10, LeaseDuration: time.Minute, PollInterval: time.Second,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatalf("new publisher: %v", err)
	}
	return publisher
}
