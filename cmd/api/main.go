package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"forgequeue/internal/api"
	"forgequeue/internal/config"
	"forgequeue/internal/jobs"
	"forgequeue/internal/platform"
	"forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/internal/telemetry"
	"forgequeue/migrations"
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
	cfg, err := config.Load("api")
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}
	logger, err := telemetry.NewLogger(os.Stdout, "api", cfg.LogLevel)
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
	coordinatorConnection, err := grpc.NewClient(cfg.CoordinatorAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("create coordinator client: %w", err)
	}
	defer coordinatorConnection.Close()
	if err := migrations.Up(ctx, db); err != nil {
		return fmt.Errorf("apply migrations: %w", err)
	}

	repository := postgresrepository.NewJobRepository(db)
	jobQueue := queue.NewRedisQueue(redisClient, "forgequeue")
	service := jobs.NewService(repository, jobQueue, cfg.DefaultMaxAttempts).
		WithBackpressure(jobQueue, cfg.MaxQueueDepth, metrics)
	reconciler := queue.NewReconciler(repository, jobQueue, logger)
	go reconciler.Run(ctx, cfg.QueueReconcileInterval)
	handler := api.NewServer(service, func(r *http.Request) error {
		readyCtx, cancel := context.WithTimeout(r.Context(), cfg.DatabaseTimeout)
		defer cancel()
		if err := db.PingContext(readyCtx); err != nil {
			return err
		}
		redisCtx, cancelRedis := context.WithTimeout(r.Context(), cfg.RedisTimeout)
		defer cancelRedis()
		return redisClient.Ping(redisCtx).Err()
	}, logger).
		WithWorkerProvider(api.NewGRPCWorkerProvider(workerpb.NewWorkerCoordinatorClient(coordinatorConnection)).WithTimeout(cfg.GRPCTimeout)).
		WithMetrics(metrics).
		WithRequestTimeout(cfg.HTTPRequestTimeout).
		Routes()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           handler,
		ReadHeaderTimeout: cfg.HTTPReadHeaderTimeout,
		ReadTimeout:       cfg.HTTPReadTimeout,
		WriteTimeout:      cfg.HTTPWriteTimeout,
		IdleTimeout:       cfg.HTTPIdleTimeout,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("API listening", "port", cfg.HTTPPort)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shut down API: %w", err)
		}
		logger.Info("API stopped")
		return nil
	case err := <-serverErrors:
		if err != nil && err != http.ErrServerClosed {
			return fmt.Errorf("serve API: %w", err)
		}
		return nil
	}
}
