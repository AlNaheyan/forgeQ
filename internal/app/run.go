// Package app provides the common lifecycle used by ForgeQueue commands while
// their service-specific behavior is introduced in later waves.
package app

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/AlNaheyan/forgeQ/internal/config"
	"github.com/AlNaheyan/forgeQ/internal/telemetry"
)

// Run loads configuration, initializes logging, and waits for shutdown.
func Run(service string, output io.Writer) error {
	cfg, err := config.Load(service)
	if err != nil {
		return fmt.Errorf("load configuration: %w", err)
	}

	logger, err := telemetry.NewLogger(output, service, cfg.LogLevel)
	if err != nil {
		return fmt.Errorf("initialize logger: %w", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("service started", "environment", cfg.Env)
	<-ctx.Done()
	logger.Info("service stopped")
	return nil
}
