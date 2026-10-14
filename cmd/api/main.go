package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

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
	service := jobs.NewService(repository, jobQueue, cfg.DefaultMaxAttempts)
	reconciler := queue.NewReconciler(repository, jobQueue, logger)
	go reconciler.Run(ctx, cfg.QueueReconcileInterval)
	handler := api.NewServer(service, func(r *http.Request) error {
		readyCtx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
		defer cancel()
		if err := db.PingContext(readyCtx); err != nil {
			return err
		}
		return redisClient.Ping(readyCtx).Err()
	}, logger).
		WithWorkerProvider(api.NewGRPCWorkerProvider(workerpb.NewWorkerCoordinatorClient(coordinatorConnection))).
		Routes()

	server := &http.Server{
		Addr:              fmt.Sprintf(":%d", cfg.HTTPPort),
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}

	serverErrors := make(chan error, 1)
	go func() {
		logger.Info("API listening", "port", cfg.HTTPPort)
		serverErrors <- server.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
