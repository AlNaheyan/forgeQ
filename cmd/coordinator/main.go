package main

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"forgequeue/internal/config"
	"forgequeue/internal/coordinator"
	"forgequeue/internal/platform"
	"forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/internal/telemetry"
	"forgequeue/migrations"
	"forgequeue/proto/workerpb"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load("coordinator")
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger, err := telemetry.NewLogger(os.Stdout, "coordinator", cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := platform.OpenPostgres(ctx, cfg.DatabaseURL)
	if err != nil {
		return err
	}
	defer db.Close()
	redisClient, err := platform.OpenRedis(ctx, cfg.RedisAddr)
	if err != nil {
		return err
	}
	defer redisClient.Close()
	if err := migrations.Up(ctx, db); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{
		SuspectAfter:   (cfg.HeartbeatInterval + cfg.WorkerTimeout) / 2,
		UnhealthyAfter: cfg.WorkerTimeout,
	}, nil)
	if err != nil {
		return fmt.Errorf("initialize worker registry: %w", err)
	}
	repository := postgresrepository.NewJobRepository(db)
	jobQueue := queue.NewRedisQueue(redisClient, "forgequeue")
	coordinatorServer, err := coordinator.NewGRPCServer(coordinator.ServerConfig{
		HeartbeatInterval: cfg.HeartbeatInterval,
		LeaseDuration:     cfg.LeaseDuration,
	}, registry, repository, jobQueue)
	if err != nil {
		return fmt.Errorf("initialize coordinator service: %w", err)
	}

	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.GRPCPort))
	if err != nil {
		return fmt.Errorf("listen for gRPC: %w", err)
	}
	grpcServer := grpc.NewServer()
	workerpb.RegisterWorkerCoordinatorServer(grpcServer, coordinatorServer)
	healthServer := health.NewServer()
	healthpb.RegisterHealthServer(grpcServer, healthServer)
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	serveErrors := make(chan error, 1)
	go func() {
		logger.Info("coordinator listening", "port", cfg.GRPCPort)
		serveErrors <- grpcServer.Serve(listener)
	}()

	select {
	case <-ctx.Done():
		healthServer.Shutdown()
		stopped := make(chan struct{})
		go func() {
			grpcServer.GracefulStop()
			close(stopped)
		}()
		select {
		case <-stopped:
		case <-time.After(10 * time.Second):
			grpcServer.Stop()
		}
		logger.Info("coordinator stopped")
		return nil
	case err := <-serveErrors:
		return fmt.Errorf("serve coordinator gRPC: %w", err)
	}
}
