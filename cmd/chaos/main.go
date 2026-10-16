// Command chaos provides small, explicit failure-injection operations for the
// local Compose deployment. It never removes containers or volumes.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"
)

type composeWorker struct {
	ID    string `json:"ID"`
	Name  string `json:"Name"`
	State string `json:"State"`
}

type event struct {
	Action    string    `json:"action"`
	Target    string    `json:"target"`
	Container string    `json:"container,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at"`
	Duration  string    `json:"duration"`
}

type runner interface {
	Run(context.Context, string, ...string) ([]byte, error)
}

type commandRunner struct{}

func (commandRunner) Run(ctx context.Context, name string, args ...string) ([]byte, error) {
	command := exec.CommandContext(ctx, name, args...)
	return command.CombinedOutput()
}

func composeCommandArgs(args ...string) []string {
	return append([]string{"compose", "-f", "deployments/docker-compose.yml"}, args...)
}

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, commandRunner{}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, output io.Writer, commands runner) error {
	if len(args) == 0 {
		return errors.New("usage: chaos worker-kill [--count N] [--dry-run] | chaos restart redis|coordinator")
	}

	switch args[0] {
	case "worker-kill":
		flags := flag.NewFlagSet("worker-kill", flag.ContinueOnError)
		flags.SetOutput(io.Discard)
		count := flags.Int("count", 1, "number of distinct running workers to kill")
		dryRun := flags.Bool("dry-run", false, "select workers and print events without killing them")
		if err := flags.Parse(args[1:]); err != nil {
			return err
		}
		if flags.NArg() != 0 || *count < 1 {
			return errors.New("worker-kill requires --count >= 1 and no positional arguments")
		}
		return killWorkers(ctx, commands, output, *count, *dryRun)
	case "restart":
		if len(args) != 2 || (args[1] != "redis" && args[1] != "coordinator") {
			return errors.New("usage: chaos restart redis|coordinator")
		}
		return restartService(ctx, commands, output, args[1])
	default:
		return fmt.Errorf("unknown chaos action %q", args[0])
	}
}

func killWorkers(ctx context.Context, commands runner, output io.Writer, count int, dryRun bool) error {
	listing, err := commands.Run(ctx, "docker", composeCommandArgs("ps", "--format", "json", "worker")...)
	if err != nil {
		return fmt.Errorf("list Compose workers: %w: %s", err, strings.TrimSpace(string(listing)))
	}
	workers, err := decodeWorkers(listing)
	if err != nil {
		return err
	}
	running := workers[:0]
	for _, worker := range workers {
		if worker.State == "running" && worker.ID != "" {
			running = append(running, worker)
		}
	}
	if len(running) < count {
		return fmt.Errorf("requested %d worker kills but only %d running workers were found", count, len(running))
	}

	if err := shuffleWorkers(running); err != nil {
		return err
	}
	for _, worker := range running[:count] {
		started := time.Now().UTC()
		if !dryRun {
			if result, err := commands.Run(ctx, "docker", "kill", "--signal", "SIGKILL", worker.ID); err != nil {
				return fmt.Errorf("kill worker %s: %w: %s", worker.Name, err, strings.TrimSpace(string(result)))
			}
		}
		ended := time.Now().UTC()
		record := event{
			Action: "kill_worker", Target: worker.ID, Container: worker.Name,
			StartedAt: started, EndedAt: ended, Duration: ended.Sub(started).String(),
		}
		if err := json.NewEncoder(output).Encode(record); err != nil {
			return fmt.Errorf("write chaos event: %w", err)
		}
	}
	return nil
}

func restartService(ctx context.Context, commands runner, output io.Writer, service string) error {
	started := time.Now().UTC()
	result, err := commands.Run(ctx, "docker", composeCommandArgs("restart", service)...)
	if err != nil {
		return fmt.Errorf("restart Compose service %s: %w: %s", service, err, strings.TrimSpace(string(result)))
	}
	ended := time.Now().UTC()
	return json.NewEncoder(output).Encode(event{
		Action: "restart_service", Target: service,
		StartedAt: started, EndedAt: ended, Duration: ended.Sub(started).String(),
	})
}

func decodeWorkers(raw []byte) ([]composeWorker, error) {
	var workers []composeWorker
	decoder := json.NewDecoder(strings.NewReader(strings.TrimSpace(string(raw))))
	if err := decoder.Decode(&workers); err == nil {
		return workers, nil
	}

	workers = nil
	scanner := bufio.NewScanner(strings.NewReader(string(raw)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		var worker composeWorker
		if err := json.Unmarshal([]byte(line), &worker); err != nil {
			return nil, fmt.Errorf("decode Compose worker JSON: %w", err)
		}
		workers = append(workers, worker)
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("read Compose worker list: %w", err)
	}
	if len(workers) == 0 {
		return nil, errors.New("Compose returned no worker containers")
	}
	return workers, nil
}

func shuffleWorkers(workers []composeWorker) error {
	for i := len(workers) - 1; i > 0; i-- {
		var encoded [8]byte
		if _, err := rand.Read(encoded[:]); err != nil {
			return fmt.Errorf("choose random worker: %w", err)
		}
		j := int(binary.LittleEndian.Uint64(encoded[:]) % uint64(i+1))
		workers[i], workers[j] = workers[j], workers[i]
	}
	return nil
}
