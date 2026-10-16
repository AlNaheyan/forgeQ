package main

import (
	"context"
	"errors"
	"strings"
	"testing"
)

type fakeRunner struct {
	outputs map[string][]byte
	calls   [][]string
	err     error
}

func (r *fakeRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.calls = append(r.calls, append([]string{name}, args...))
	if r.err != nil {
		return nil, r.err
	}
	return r.outputs[strings.Join(append([]string{name}, args...), " ")], nil
}

func TestDecodeWorkersAcceptsComposeJSONLines(t *testing.T) {
	workers, err := decodeWorkers([]byte("{\"ID\":\"abc\",\"Name\":\"forgequeue-worker-1\",\"State\":\"running\"}\n{\"ID\":\"def\",\"Name\":\"forgequeue-worker-2\",\"State\":\"exited\"}\n"))
	if err != nil {
		t.Fatalf("decode workers: %v", err)
	}
	if len(workers) != 2 || workers[0].ID != "abc" || workers[1].State != "exited" {
		t.Fatalf("decoded workers = %+v", workers)
	}
}

func TestKillWorkersSelectsDistinctRunningContainers(t *testing.T) {
	listing := []byte("{\"ID\":\"one\",\"Name\":\"worker-1\",\"State\":\"running\"}\n" +
		"{\"ID\":\"two\",\"Name\":\"worker-2\",\"State\":\"running\"}\n" +
		"{\"ID\":\"stopped\",\"Name\":\"worker-3\",\"State\":\"exited\"}\n")
	runner := &fakeRunner{outputs: map[string][]byte{
		"docker compose -f deployments/docker-compose.yml ps --format json worker": listing,
	}}
	var output strings.Builder
	if err := killWorkers(context.Background(), runner, &output, 2, false); err != nil {
		t.Fatalf("kill workers: %v", err)
	}
	if len(runner.calls) != 3 {
		t.Fatalf("command count = %d, want list + two kills: %v", len(runner.calls), runner.calls)
	}
	killed := map[string]bool{}
	for _, call := range runner.calls[1:] {
		if len(call) != 5 || call[0] != "docker" || call[1] != "kill" || call[2] != "--signal" || call[3] != "SIGKILL" {
			t.Fatalf("unexpected kill command: %v", call)
		}
		killed[call[4]] = true
	}
	if len(killed) != 2 || killed["stopped"] {
		t.Fatalf("killed containers = %v", killed)
	}
	if strings.Count(output.String(), "\"action\":\"kill_worker\"") != 2 {
		t.Fatalf("event output = %s", output.String())
	}
}

func TestKillWorkersRefusesToKillAllWhenCountUnavailable(t *testing.T) {
	runner := &fakeRunner{outputs: map[string][]byte{
		"docker compose -f deployments/docker-compose.yml ps --format json worker": []byte("{\"ID\":\"one\",\"Name\":\"worker-1\",\"State\":\"running\"}\n"),
	}}
	var output strings.Builder
	err := killWorkers(context.Background(), runner, &output, 2, false)
	if err == nil || !strings.Contains(err.Error(), "only 1 running workers") {
		t.Fatalf("error = %v, want insufficient workers", err)
	}
	if len(runner.calls) != 1 {
		t.Fatalf("unexpected destructive command after insufficient workers: %v", runner.calls)
	}
}

func TestRestartServiceRestrictsTargetAndRecordsEvent(t *testing.T) {
	runner := &fakeRunner{}
	var output strings.Builder
	if err := run(context.Background(), []string{"restart", "coordinator"}, &output, runner); err != nil {
		t.Fatalf("restart: %v", err)
	}
	if got := strings.Join(runner.calls[0], " "); got != "docker compose -f deployments/docker-compose.yml restart coordinator" {
		t.Fatalf("restart command = %q", got)
	}
	if !strings.Contains(output.String(), "\"target\":\"coordinator\"") {
		t.Fatalf("restart event = %s", output.String())
	}
	if err := run(context.Background(), []string{"restart", "postgres"}, &output, runner); err == nil {
		t.Fatal("restart postgres unexpectedly accepted")
	}
}

func TestKillWorkersReturnsListingError(t *testing.T) {
	runner := &fakeRunner{err: errors.New("docker unavailable")}
	err := killWorkers(context.Background(), runner, &strings.Builder{}, 1, true)
	if err == nil || !strings.Contains(err.Error(), "list Compose workers") {
		t.Fatalf("error = %v", err)
	}
}
