package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgequeue/internal/config"
	"forgequeue/internal/jobtypes"
	"forgequeue/internal/telemetry"
	"forgequeue/internal/tenant"
	workerpkg "forgequeue/internal/worker"
	"forgequeue/proto/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
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
	for _, name := range cfg.WorkerJobTypes {
		if _, ok := jobtypes.Fixtures().Lookup(name); !ok {
			return fmt.Errorf("no shared descriptor for configured worker job type %q", name)
		}
	}
	logger, err := telemetry.NewLogger(os.Stdout, "worker", cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dialOptions := []grpc.DialOption{}
	if cfg.ServiceTLS != nil {
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(credentials.NewTLS(cfg.ServiceTLS)))
	} else {
		rpcAuth, err := tenant.LoadRPCAuth()
		if err != nil {
			return err
		}
		dialOptions = append(dialOptions, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(rpcAuth))
	}
	connection, err := grpc.NewClient(cfg.CoordinatorAddr, dialOptions...)
	if err != nil {
		return fmt.Errorf("create coordinator client: %w", err)
	}
	defer connection.Close()
	client := workerpb.NewWorkerCoordinatorClient(connection)
	broker, err := workerpkg.NewGRPCBroker(client)
	if err != nil {
		return fmt.Errorf("initialize coordinator broker: %w", err)
	}
	broker.WithTimeout(cfg.GRPCTimeout)

	process, err := workerpkg.NewWithBroker(workerpkg.Config{
		ID:                 cfg.WorkerID,
		Concurrency:        cfg.WorkerConcurrency,
		PollInterval:       cfg.WorkerPollInterval,
		LeaseRenewInterval: cfg.LeaseRenewInterval,
		JobTimeout:         cfg.JobTimeout,
		JobTypes:           cfg.WorkerJobTypes,
		RequireTenant:      cfg.Env == config.Production,
		MaxResultBytes:     cfg.WorkerMaxResultBytes,
	}, broker, workerpkg.BuiltInHandlers(), logger)
	if err != nil {
		return fmt.Errorf("initialize worker: %w", err)
	}
	remoteRPC := workerpkg.NewGRPCCoordinatorRPC(client).WithTimeout(cfg.GRPCTimeout)
	remote, err := workerpkg.NewRemoteCoordinator(workerpkg.RemoteConfig{
		WorkerID:          process.ID(),
		MaxConcurrency:    cfg.WorkerConcurrency,
		SupportedJobTypes: cfg.WorkerJobTypes,
	}, remoteRPC, process.ActiveJobs, logger)
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
	select {
	case firstErr := <-errorsChannel:
		cancel()
		timer := time.NewTimer(cfg.ShutdownTimeout)
		defer timer.Stop()
		var secondErr error
		select {
		case secondErr = <-errorsChannel:
		case <-timer.C:
			return fmt.Errorf("worker graceful shutdown exceeded %s", cfg.ShutdownTimeout)
		}
		if firstErr != nil {
			return firstErr
		}
		return secondErr
	case <-ctx.Done():
		cancel()
	}
	timer := time.NewTimer(cfg.ShutdownTimeout)
	defer timer.Stop()
	for range 2 {
		select {
		case err := <-errorsChannel:
			if err != nil && err != context.Canceled {
				return err
			}
		case <-timer.C:
			return fmt.Errorf("worker graceful shutdown exceeded %s", cfg.ShutdownTimeout)
		}
	}
	return nil
}
