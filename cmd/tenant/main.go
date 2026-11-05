// tenant is an offline administrator tool. Database access is its authority.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/platform"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"github.com/AlNaheyan/forgeQ/internal/tenant"
	"github.com/AlNaheyan/forgeQ/migrations"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return fmt.Errorf("usage: tenant create|key|rotate|revoke|limits [flags]")
	}
	command := os.Args[1]
	f := flag.NewFlagSet(command, flag.ContinueOnError)
	name := f.String("name", "local", "tenant name")
	id := f.String("tenant-id", "", "tenant UUID")
	keyID := f.String("key-id", "", "key UUID (not secret)")
	operator := f.Bool("operator", false, "allow fleet and metrics views")
	l := tenant.DefaultLimits()
	f.IntVar(&l.MaxPending, "max-pending", l.MaxPending, "outstanding jobs")
	f.IntVar(&l.MaxConcurrent, "max-concurrent", l.MaxConcurrent, "active leases")
	f.IntVar(&l.PerMinute, "per-minute", l.PerMinute, "admissions per fixed minute")
	f.IntVar(&l.MaxPayloadBytes, "max-payload-bytes", l.MaxPayloadBytes, "payload size")
	if err := f.Parse(os.Args[2:]); err != nil {
		return err
	}
	switch command {
	case "create", "key", "rotate", "revoke", "limits":
	default:
		return fmt.Errorf("unknown command")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
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
	db, err := platform.OpenPostgres(ctx, dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	if err := migrations.Up(ctx, db); err != nil {
		return err
	}
	s := tenant.NewStore(db)
	var output any
	switch command {
	case "create":
		*id, err = s.Create(ctx, *name, l)
		if err != nil {
			return err
		}
		fallthrough
	case "key":
		output, err = s.Issue(ctx, *id, *operator)
	case "rotate":
		output, err = s.Rotate(ctx, *keyID)
	case "revoke":
		err = s.Revoke(ctx, *keyID)
		output = map[string]bool{"revoked": err == nil}
	case "limits":
		err = s.SetLimits(ctx, *id, l)
		output = map[string]bool{"updated": err == nil}
	}
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(output)
}
