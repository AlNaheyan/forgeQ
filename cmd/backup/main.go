// backup operates only local test storage until an off-host adapter is approved.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"filippo.io/age"
	"fmt"
	"forgequeue/internal/backup"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	if e := run(ctx); e != nil {
		fmt.Fprintln(os.Stderr, "backup operation failed:", e)
		os.Exit(1)
	}
}
func run(ctx context.Context) error {
	if len(os.Args) != 2 {
		return errors.New("usage: backup once|schedule|retention|restore")
	}
	if os.Getenv("BACKUP_STORAGE_KIND") != "local" {
		return errors.New("only explicit local test storage supported")
	}
	prefix := os.Getenv("BACKUP_OBJECT_PREFIX")
	root := os.Getenv("BACKUP_LOCAL_ROOT")
	if root == "" {
		return errors.New("BACKUP_LOCAL_ROOT required")
	}
	s, e := backup.NewLocal(root, prefix)
	if e != nil {
		return e
	}
	command := os.Args[1]
	if command == "restore" {
		m, e := backup.ReadManifest(ctx, s, os.Getenv("BACKUP_MANIFEST_KEY"))
		if e != nil {
			return e
		}
		secret, e := backup.SecretFile(os.Getenv("BACKUP_DECRYPTION_IDENTITY_FILE"))
		if e != nil {
			return e
		}
		ids, e := age.ParseIdentities(strings.NewReader(secret))
		if e != nil {
			return errors.New("invalid restore identity")
		}
		dsn, e := backup.SecretFile(os.Getenv("BACKUP_RESTORE_DATABASE_URL_FILE"))
		if e != nil {
			return e
		}
		return backup.RestorePostgres(ctx, s, m, dsn, "", ids...)
	}
	timeout := 30 * time.Minute
	if value := os.Getenv("BACKUP_TIMEOUT"); value != "" {
		timeout, e = time.ParseDuration(value)
		if e != nil || timeout < time.Second || timeout > 24*time.Hour {
			return errors.New("BACKUP_TIMEOUT must be between 1s and 24h")
		}
	}
	operation := func(ctx context.Context) error {
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		if command == "retention" {
			daily, e := strconv.Atoi(os.Getenv("BACKUP_RETENTION_DAILY"))
			if e != nil {
				return errors.New("daily retention required")
			}
			weekly, e := strconv.Atoi(os.Getenv("BACKUP_RETENTION_WEEKLY"))
			if e != nil {
				return errors.New("weekly retention required")
			}
			plan, e := backup.Retain(ctx, s, prefix, backup.Retention{Daily: daily, Weekly: weekly, MaxDelete: 100}, os.Getenv("BACKUP_RETENTION_APPLY") == "true")
			if e == nil {
				e = json.NewEncoder(os.Stdout).Encode(plan)
			}
			return e
		}
		secret, e := backup.SecretFile(os.Getenv("BACKUP_ENCRYPTION_RECIPIENTS_FILE"))
		if e != nil {
			return e
		}
		recipients, e := age.ParseRecipients(strings.NewReader(secret))
		if e != nil {
			return errors.New("invalid encryption recipients")
		}
		dsn, e := backup.SecretFile(os.Getenv("BACKUP_DATABASE_URL_FILE"))
		if e != nil {
			return e
		}
		r := backup.Runner{Store: s, Prefix: prefix, Recipients: recipients, KeyID: os.Getenv("BACKUP_KEY_ID"), Revision: os.Getenv("BACKUP_REVISION"), Schema: os.Getenv("BACKUP_SCHEMA"), DatabaseMajor: os.Getenv("BACKUP_DATABASE_MAJOR")}
		m, e := r.Backup(ctx, backup.DumpPostgres(dsn))
		if e == nil {
			e = json.NewEncoder(os.Stdout).Encode(m)
		}
		return e
	}
	if command != "once" && command != "schedule" && command != "retention" {
		return errors.New("unknown backup operation")
	}
	interval := time.Hour
	if command == "schedule" {
		interval, e = time.ParseDuration(os.Getenv("BACKUP_INTERVAL"))
		if e != nil {
			return errors.New("BACKUP_INTERVAL required")
		}
		return backup.Schedule(ctx, root, interval, operation)
	}
	onceCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	e = backup.Schedule(onceCtx, root, interval, func(ctx context.Context) error { e := operation(ctx); cancel(); return e })
	if errors.Is(e, context.Canceled) && ctx.Err() == nil {
		return nil
	}
	return e
}
