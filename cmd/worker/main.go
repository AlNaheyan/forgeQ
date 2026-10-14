package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"forgequeue/internal/config"
	"forgequeue/internal/platform"
	"forgequeue/internal/queue"
	postgresrepository "forgequeue/internal/repository/postgres"
	"forgequeue/internal/telemetry"
	workerpkg "forgequeue/internal/worker"
	"forgequeue/migrations"
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

	repository := postgresrepository.NewJobRepository(db)
	jobQueue := queue.NewRedisQueue(redisClient, "forgequeue")
	process, err := workerpkg.New(workerpkg.Config{
		ID:           cfg.WorkerID,
		Concurrency:  cfg.WorkerConcurrency,
		PollInterval: cfg.WorkerPollInterval,
		JobTypes:     cfg.WorkerJobTypes,
	}, repository, jobQueue, workerpkg.BuiltInHandlers(), logger)
	if err != nil {
		return fmt.Errorf("initialize worker: %w", err)
	}
	return process.Run(ctx)
}
