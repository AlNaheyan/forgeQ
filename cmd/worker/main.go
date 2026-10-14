package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"forgequeue/internal/config"
	"forgequeue/internal/telemetry"
	workerpkg "forgequeue/internal/worker"
	"forgequeue/proto/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("worker")
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger, err := telemetry.NewLogger(os.Stdout, "worker", cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	connection, err := grpc.NewClient(cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create coordinator client: %w", err)
	}
	defer connection.Close()
	client := workerpb.NewWorkerCoordinatorClient(connection)
	broker, err := workerpkg.NewGRPCBroker(client)
	if err != nil {
		return fmt.Errorf("initialize coordinator broker: %w", err)
	}

	process, err := workerpkg.NewWithBroker(workerpkg.Config{
		ID:                 cfg.WorkerID,
		Concurrency:        cfg.WorkerConcurrency,
		PollInterval:       cfg.WorkerPollInterval,
		LeaseRenewInterval: cfg.LeaseRenewInterval,
		JobTypes:           cfg.WorkerJobTypes,
	}, broker, workerpkg.BuiltInHandlers(), logger)
	if err != nil {
		return fmt.Errorf("initialize worker: %w", err)
	}
	remote, err := workerpkg.NewRemoteCoordinator(workerpkg.RemoteConfig{
		WorkerID:          process.ID(),
		MaxConcurrency:    cfg.WorkerConcurrency,
		SupportedJobTypes: cfg.WorkerJobTypes,
	}, workerpkg.NewGRPCCoordinatorRPC(client), process.ActiveJobs, logger)
	if err != nil {
		return fmt.Errorf("initialize worker registration: %w", err)
	}
	settings, err := remote.Register(ctx)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- remote.RunHeartbeat(runCtx, settings.HeartbeatInterval) }()
	go func() { errorsChannel <- process.Run(runCtx) }()
	firstErr := <-errorsChannel
	cancel()
	secondErr := <-errorsChannel
	if firstErr != nil {
		return firstErr
	}
	return secondErr
}
