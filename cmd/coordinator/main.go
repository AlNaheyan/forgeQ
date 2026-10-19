package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
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
	metrics := telemetry.NewMetrics()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	db, err := platform.OpenPostgresWithSettings(ctx, cfg.DatabaseURL, platform.PostgresSettings{
		MaxOpenConns: cfg.PostgresMaxOpenConns, MaxIdleConns: cfg.PostgresMaxIdleConns,
		ConnMaxIdleTime: cfg.PostgresConnMaxIdle, PingTimeout: cfg.DatabaseTimeout,
	})
	if err != nil {
		return err
	}
	defer db.Close()
	redisClient, err := platform.OpenRedisWithSettings(ctx, cfg.RedisAddr, platform.RedisSettings{
		PoolSize: cfg.RedisPoolSize, DialTimeout: cfg.RedisTimeout, IOTimeout: cfg.RedisTimeout, PingTimeout: cfg.RedisTimeout,
	})
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
	scheduler, err := newScheduler(cfg.SchedulerStrategy)
	if err != nil {
		return fmt.Errorf("initialize scheduler: %w", err)
	}
	coordinatorServer, err := coordinator.NewGRPCServer(coordinator.ServerConfig{
		HeartbeatInterval: cfg.HeartbeatInterval,
		LeaseDuration:     cfg.LeaseDuration,
		RetryPolicy: coordinator.NewRetryPolicy(
			cfg.RetryBaseDelay,
			cfg.RetryMaxDelay,
			coordinator.DefaultRetryJitterFraction,
			nil,
		),
		Scheduler: scheduler,
		Logger:    logger,
		Metrics:   metrics,
	}, registry, repository, jobQueue)
	if err != nil {
		return fmt.Errorf("initialize coordinator service: %w", err)
	}
	recoveryScanner := coordinator.NewRecoveryScanner(repository, jobQueue, logger, 100).WithMetrics(metrics)
	go recoveryScanner.Run(ctx, cfg.LeaseScanInterval)
	retryPromoter := coordinator.NewRetryPromoter(repository, jobQueue, logger, 100)
	go retryPromoter.Run(ctx, cfg.RetryPromotionInterval)
	go observeCoordinatorState(ctx, jobQueue, coordinatorServer, metrics, logger)
	metricsErrors := make(chan error, 1)
	go func() {
		metricsErrors <- telemetry.ServeMetrics(ctx, fmt.Sprintf(":%d", cfg.MetricsPort), metrics, logger)
	}()

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
		case <-time.After(cfg.ShutdownTimeout):
			grpcServer.Stop()
		}
		logger.Info("coordinator stopped")
		return nil
	case err := <-serveErrors:
		return fmt.Errorf("serve coordinator gRPC: %w", err)
	case err := <-metricsErrors:
		if err == nil {
			return nil
		}
		return fmt.Errorf("serve coordinator metrics: %w", err)
	}
}

type queueDepthReader interface {
	Depth(context.Context) (int64, error)
}

type metricsRefresher interface {
	RefreshMetrics()
}

func observeCoordinatorState(ctx context.Context, queue queueDepthReader, refresher metricsRefresher, metrics *telemetry.Metrics, logger *slog.Logger) {
	observe := func() {
		refresher.RefreshMetrics()
		depth, err := queue.Depth(ctx)
		if err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Warn("queue depth observation failed", "error", err)
			}
			return
		}
		metrics.QueueDepth.Set(float64(depth))
	}
	observe()
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			observe()
		}
	}
}

func newScheduler(strategy string) (coordinator.Scheduler, error) {
	switch strategy {
	case config.SchedulerRoundRobin:
		return coordinator.NewRoundRobin(), nil
	case config.SchedulerLeastLoaded:
		return coordinator.NewLeastLoaded(), nil
	case config.SchedulerWeightedCapacity:
		return coordinator.NewWeightedCapacity(nil), nil
	default:
		return nil, fmt.Errorf("unsupported strategy %q", strategy)
	}
}
