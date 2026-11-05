// Invoice-app demonstrates an external application using only ForgeQ's public SDKs.
package main

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/AlNaheyan/forgeQ/client"
	"github.com/AlNaheyan/forgeQ/worker"
	_ "github.com/jackc/pgx/v5/stdlib"
)

const taskName = "invoice.create"

var invoiceIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

type invoice struct {
	InvoiceID   string `json:"invoice_id"`
	AmountCents int64  `json:"amount_cents"`
}

func decodeInvoice(payload json.RawMessage) (invoice, error) {
	var v invoice
	d := json.NewDecoder(bytes.NewReader(payload))
	d.DisallowUnknownFields()
	if err := d.Decode(&v); err != nil {
		return v, errors.New("invalid invoice payload")
	}
	if err := d.Decode(new(any)); err != io.EOF {
		return v, errors.New("invalid invoice payload")
	}
	if !invoiceIDPattern.MatchString(v.InvoiceID) || v.AmountCents < 1 || v.AmountCents > 100000000 {
		return v, errors.New("invoice ID or amount is invalid")
	}
	return v, nil
}

func invoiceTask(db *sql.DB) worker.Task {
	return worker.Task{Name: taskName, MaxPayloadBytes: 4096, Validate: func(payload json.RawMessage) error { _, err := decodeInvoice(payload); return err }, Handle: func(ctx context.Context, payload json.RawMessage) (any, error) {
		input, err := decodeInvoice(payload)
		if err != nil {
			return nil, worker.Permanent(err)
		}
		metadata, ok := worker.MetadataFromContext(ctx)
		if !ok {
			return nil, worker.Permanent(errors.New("job identity missing"))
		}
		key, err := metadata.OperationKey("create_invoice")
		if err != nil {
			return nil, worker.Permanent(err)
		}
		var stored invoice
		err = db.QueryRowContext(ctx, `INSERT INTO invoice_app.invoices (tenant_id,operation_key,invoice_id,amount_cents) VALUES ($1,$2,$3,$4) ON CONFLICT (tenant_id,operation_key) DO NOTHING RETURNING invoice_id,amount_cents`, metadata.TenantID, key, input.InvoiceID, input.AmountCents).Scan(&stored.InvoiceID, &stored.AmountCents)
		if errors.Is(err, sql.ErrNoRows) {
			err = db.QueryRowContext(ctx, `SELECT invoice_id,amount_cents FROM invoice_app.invoices WHERE tenant_id=$1 AND operation_key=$2`, metadata.TenantID, key).Scan(&stored.InvoiceID, &stored.AmountCents)
		}
		if err != nil {
			return nil, err
		}
		return stored, nil
	}}
}

func producer() (*client.Client, error) {
	base := os.Getenv("FORGEQ_API_URL")
	if base == "" {
		base = "http://localhost:8080"
	}
	return client.New(client.Options{BaseURL: base, APIKey: os.Getenv("FORGEQ_API_KEY"), AllowInsecureHTTP: os.Getenv("FORGEQ_ALLOW_INSECURE") == "true"})
}

func workerTLS() (*tls.Config, error) {
	ca, err := os.ReadFile(os.Getenv("FORGEQ_TLS_CA_FILE"))
	if err != nil {
		return nil, errors.New("worker CA unavailable")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("invalid worker CA")
	}
	cert, err := tls.LoadX509KeyPair(os.Getenv("FORGEQ_TLS_CERT_FILE"), os.Getenv("FORGEQ_TLS_KEY_FILE"))
	if err != nil {
		return nil, errors.New("worker certificate unavailable")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, Certificates: []tls.Certificate{cert}, ServerName: os.Getenv("FORGEQ_TLS_SERVER_NAME")}, nil
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() error {
	if len(os.Args) < 2 {
		return errors.New("usage: invoice-app worker | submit <invoice-id> <amount-cents> | get <job-id>")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch os.Args[1] {
	case "worker":
		dsn := os.Getenv("INVOICE_DATABASE_URL")
		if dsn == "" {
			return errors.New("INVOICE_DATABASE_URL is required")
		}
		db, err := sql.Open("pgx", dsn)
		if err != nil {
			return errors.New("could not open application database")
		}
		defer db.Close()
		pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		if db.PingContext(pingCtx) != nil {
			return errors.New("application database unavailable")
		}
		addr := os.Getenv("FORGEQ_COORDINATOR_ADDR")
		if addr == "" {
			addr = "localhost:9090"
		}
		id := os.Getenv("FORGEQ_WORKER_ID")
		if id == "" {
			id = "invoice-worker"
		}
		cfg := worker.Config{CoordinatorAddr: addr, WorkerID: id, Concurrency: 2, AllowInsecure: os.Getenv("FORGEQ_ALLOW_INSECURE") == "true", DevelopmentToken: os.Getenv("FORGEQ_DEV_TOKEN")}
		if !cfg.AllowInsecure {
			cfg.TLSConfig, err = workerTLS()
			if err != nil {
				return err
			}
		}
		return worker.Run(ctx, cfg, invoiceTask(db))
	case "submit":
		if len(os.Args) != 4 {
			return errors.New("usage: invoice-app submit <invoice-id> <amount-cents>")
		}
		amount, err := strconv.ParseInt(os.Args[3], 10, 64)
		if err != nil {
			return errors.New("invalid amount")
		}
		value := invoice{InvoiceID: os.Args[2], AmountCents: amount}
		encoded, err := json.Marshal(value)
		if err != nil {
			return err
		}
		if _, err = decodeInvoice(encoded); err != nil {
			return err
		}
		key := strings.TrimSpace(os.Getenv("FORGEQ_IDEMPOTENCY_KEY"))
		if key == "" {
			return errors.New("FORGEQ_IDEMPOTENCY_KEY is required")
		}
		c, err := producer()
		if err != nil {
			return err
		}
		job, err := c.Submit(ctx, client.SubmitRequest{Type: taskName, Payload: value, IdempotencyKey: key})
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(job)
	case "get":
		if len(os.Args) != 3 {
			return errors.New("usage: invoice-app get <job-id>")
		}
		c, err := producer()
		if err != nil {
			return err
		}
		job, err := c.Get(ctx, os.Args[2])
		if err != nil {
			return err
		}
		return json.NewEncoder(os.Stdout).Encode(job)
	default:
		return errors.New("unknown command")
	}
}
