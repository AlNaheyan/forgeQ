// migrate is the explicitly privileged schema/configuration bootstrap command.
package main

import (
	"context"
	"fmt"
	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"github.com/AlNaheyan/forgeQ/migrations"
	"os"
	"strconv"
	"time"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	dsn, err := security.Secret("DATABASE_URL", os.Getenv("DATABASE_URL"), os.Getenv("DATABASE_URL_FILE"))
	if err != nil {
		return err
	}
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	if os.Getenv("APP_ENV") == "production" {
		if err := security.ValidateDatabaseURL(dsn); err != nil {
			return err
		}
	}
	events := false
	if raw := os.Getenv("EVENTS_ENABLED"); raw != "" {
		events, err = strconv.ParseBool(raw)
		if err != nil {
			return fmt.Errorf("EVENTS_ENABLED must be a boolean")
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	db, err := platform.OpenPostgres(ctx, dsn)
	if err != nil {
		return fmt.Errorf("migration database connection failed")
	}
	defer db.Close()
	if err := migrations.Up(ctx, db); err != nil {
		return fmt.Errorf("migration failed; inspect privileged database diagnostics")
	}
	if err := migrations.ConfigureEventStream(ctx, db, events); err != nil {
		return fmt.Errorf("event bootstrap failed")
	}
	return migrations.Verify(ctx, db, events)
}
