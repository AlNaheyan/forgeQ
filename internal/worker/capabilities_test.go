package worker

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestCapabilitiesAreCopiedAndNilHandlersRejected(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	config := Config{ID: "worker", Concurrency: 1, PollInterval: time.Second, JobTypes: []string{"sleep"}}
	w, err := NewWithBroker(config, &recordingBroker{}, BuiltInHandlers(), logger)
	if err != nil {
		t.Fatal(err)
	}
	config.JobTypes[0] = "mutated"
	capabilities := w.SupportedJobTypes()
	capabilities[0] = "changed"
	if got := w.SupportedJobTypes()[0]; got != "sleep" {
		t.Fatalf("capability mutated: %s", got)
	}
	var handler *SleepHandler
	config.JobTypes = []string{"sleep"}
	if _, err := NewWithBroker(config, &recordingBroker{}, map[string]Handler{"sleep": handler}, logger); err == nil {
		t.Fatal("typed nil handler accepted")
	}
}
