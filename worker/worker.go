// Package worker runs application-owned Go handlers on ForgeQ workers.
package worker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/jobtypes"
	"github.com/AlNaheyan/forgeQ/internal/security"
	internalworker "github.com/AlNaheyan/forgeQ/internal/worker"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

// Handler executes trusted application code. Payloads and results are JSON.
type Handler func(context.Context, json.RawMessage) (any, error)

// Task describes one named handler and its defensive payload validation.
type Task struct {
	Name            string
	MaxPayloadBytes int
	Validate        func(json.RawMessage) error
	Handle          Handler
}

// Config selects coordinator access and worker execution bounds.
// Production uses verified mTLS; AllowInsecure is for local development only.
type Config struct {
	CoordinatorAddr    string
	WorkerID           string
	Concurrency        int
	PollInterval       time.Duration
	LeaseRenewInterval time.Duration
	JobTimeout         time.Duration
	RPCTimeout         time.Duration
	ShutdownTimeout    time.Duration
	MaxResultBytes     int
	TLSConfig          *tls.Config
	AllowInsecure      bool
	DevelopmentToken   string
	Logger             *slog.Logger
}

// Metadata describes the current durable job and fenced execution attempt.
type Metadata struct {
	TenantID, JobID, JobType, SubmissionKey string
	Attempt                                 int
	LeaseGeneration                         int64
}

func MetadataFromContext(ctx context.Context) (Metadata, bool) {
	m, ok := internalworker.MetadataFromContext(ctx)
	return Metadata{TenantID: m.TenantID, JobID: m.JobID, JobType: m.JobType, SubmissionKey: m.SubmissionKey, Attempt: m.Attempt, LeaseGeneration: m.LeaseGeneration}, ok
}

// OperationKey remains stable across retries for the same named effect.
func (m Metadata) OperationKey(effect string) (string, error) {
	return (internalworker.ExecutionMetadata{TenantID: m.TenantID, JobID: m.JobID}).OperationKey(effect)
}

// Permanent marks an error that must not be retried. The cause is not persisted.
func Permanent(err error) error {
	return &internalworker.HandlerError{Code: internalworker.FailurePermanent, Cause: err}
}

// ObjectPayload validates that the payload is a JSON object.
func ObjectPayload(payload json.RawMessage) error {
	var object map[string]json.RawMessage
	if err := json.Unmarshal(payload, &object); err != nil || object == nil {
		return errors.New("payload must be a JSON object")
	}
	return nil
}

type handlerAdapter struct{ fn Handler }

func (h handlerAdapter) Execute(ctx context.Context, payload json.RawMessage) (any, error) {
	return h.fn(ctx, payload)
}

type tokenCredentials struct{ token string }

func (t tokenCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + t.token}, nil
}
func (tokenCredentials) RequireTransportSecurity() bool { return false }

// Run registers the supplied tasks and blocks until cancellation or failure.
// It waits for in-flight jobs to drain up to ShutdownTimeout.
func Run(ctx context.Context, cfg Config, tasks ...Task) error {
	if ctx == nil {
		return errors.New("context is required")
	}
	if strings.TrimSpace(cfg.CoordinatorAddr) == "" || strings.TrimSpace(cfg.WorkerID) == "" {
		return errors.New("coordinator address and worker ID are required")
	}
	if cfg.Concurrency == 0 {
		cfg.Concurrency = 1
	}
	if cfg.PollInterval == 0 {
		cfg.PollInterval = 250 * time.Millisecond
	}
	autoRenew := cfg.LeaseRenewInterval == 0
	if autoRenew {
		cfg.LeaseRenewInterval = 10 * time.Second
	}
	if cfg.JobTimeout == 0 {
		cfg.JobTimeout = 5 * time.Minute
	}
	if cfg.RPCTimeout == 0 {
		cfg.RPCTimeout = 5 * time.Second
	}
	if cfg.ShutdownTimeout == 0 {
		cfg.ShutdownTimeout = 10 * time.Second
	}
	if cfg.RPCTimeout <= 0 || cfg.ShutdownTimeout <= 0 {
		return errors.New("RPC and shutdown timeouts must be positive")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	if len(tasks) == 0 {
		return errors.New("at least one task is required")
	}
	descriptors := make([]jobtypes.Descriptor, 0, len(tasks))
	handlers := make(map[string]internalworker.Handler, len(tasks))
	names := make([]string, 0, len(tasks))
	for _, task := range tasks {
		if task.Handle == nil {
			return errors.New("task handler is required")
		}
		if task.MaxPayloadBytes == 0 {
			task.MaxPayloadBytes = jobtypes.MaxPayloadBytes
		}
		if task.Validate == nil {
			task.Validate = ObjectPayload
		}
		descriptors = append(descriptors, jobtypes.Descriptor{Name: task.Name, MaxPayloadBytes: task.MaxPayloadBytes, Validate: task.Validate})
		handlers[task.Name] = handlerAdapter{fn: task.Handle}
		names = append(names, task.Name)
	}
	registry, err := jobtypes.NewRegistry(descriptors)
	if err != nil {
		return fmt.Errorf("register tasks: %w", err)
	}
	var opts []grpc.DialOption
	switch {
	case cfg.TLSConfig != nil && !cfg.AllowInsecure && cfg.DevelopmentToken == "":
		if cfg.TLSConfig.InsecureSkipVerify || cfg.TLSConfig.ServerName == "" || cfg.TLSConfig.RootCAs == nil || len(cfg.TLSConfig.Certificates) == 0 {
			return errors.New("verified mTLS with CA, server name and client certificate is required")
		}
		tlsConfig := cfg.TLSConfig.Clone()
		certificate := tlsConfig.Certificates[0]
		if len(certificate.Certificate) == 0 {
			return errors.New("worker client certificate is required")
		}
		leaf, err := x509.ParseCertificate(certificate.Certificate[0])
		if err != nil {
			return errors.New("invalid worker client certificate")
		}
		identity, err := security.CertificateIdentity(leaf)
		if err != nil || identity.Role != "worker" || identity.ID != cfg.WorkerID {
			return errors.New("worker ID must match its client certificate identity")
		}
		if tlsConfig.MinVersion < tls.VersionTLS12 {
			tlsConfig.MinVersion = tls.VersionTLS12
		}
		opts = append(opts, grpc.WithTransportCredentials(credentials.NewTLS(tlsConfig)))
	case cfg.TLSConfig == nil && cfg.AllowInsecure && len(cfg.DevelopmentToken) >= 32:
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithPerRPCCredentials(tokenCredentials{token: cfg.DevelopmentToken}))
	default:
		return errors.New("configure verified mTLS or explicitly enable local insecure mode with a service token")
	}
	connection, err := grpc.NewClient(cfg.CoordinatorAddr, opts...)
	if err != nil {
		return fmt.Errorf("create coordinator client: %w", err)
	}
	defer connection.Close()
	client := workerpb.NewWorkerCoordinatorClient(connection)
	broker, err := internalworker.NewGRPCBroker(client)
	if err != nil {
		return err
	}
	broker.WithTimeout(cfg.RPCTimeout)
	process, err := internalworker.NewWithBroker(internalworker.Config{ID: cfg.WorkerID, Concurrency: cfg.Concurrency, PollInterval: cfg.PollInterval, LeaseRenewInterval: cfg.LeaseRenewInterval, JobTimeout: cfg.JobTimeout, MaxResultBytes: cfg.MaxResultBytes, JobTypes: names, Registry: registry, RequireTenant: cfg.TLSConfig != nil}, broker, handlers, cfg.Logger)
	if err != nil {
		return err
	}
	interval := cfg.LeaseRenewInterval
	configured := false
	validateSettings := func(settings internalworker.RemoteSettings) error {
		if !configured {
			if autoRenew && settings.LeaseDuration/3 < interval {
				interval = settings.LeaseDuration / 3
			}
			if interval <= 0 || interval >= settings.LeaseDuration {
				return errors.New("lease renewal interval must be positive and shorter than the coordinator lease")
			}
			if err := process.ConfigureLeaseRenewal(interval); err != nil {
				return err
			}
			configured = true
		}
		if interval >= settings.LeaseDuration {
			return errors.New("coordinator lease became shorter than the renewal interval; restart worker")
		}
		return nil
	}
	remote, err := internalworker.NewRemoteCoordinator(internalworker.RemoteConfig{WorkerID: process.ID(), MaxConcurrency: cfg.Concurrency, SupportedJobTypes: process.SupportedJobTypes(), ValidateSettings: validateSettings}, internalworker.NewGRPCCoordinatorRPC(client).WithTimeout(cfg.RPCTimeout), process.ActiveJobs, cfg.Logger)
	if err != nil {
		return err
	}
	settings, err := remote.Register(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return nil
		}
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	done := make(chan error, 2)
	go func() { done <- process.Run(runCtx) }()
	go func() { done <- remote.RunHeartbeat(runCtx, settings.HeartbeatInterval) }()
	var first error
	finished := 0
	select {
	case first = <-done:
		finished = 1
		cancel()
	case <-ctx.Done():
		cancel()
	}
	timer := time.NewTimer(cfg.ShutdownTimeout)
	defer timer.Stop()
	for finished < 2 {
		select {
		case next := <-done:
			finished++
			if first == nil || errors.Is(first, context.Canceled) {
				first = next
			}
		case <-timer.C:
			return errors.New("worker shutdown exceeded drain timeout")
		}
	}
	if first != nil && !errors.Is(first, context.Canceled) {
		return first
	}
	return nil
}
