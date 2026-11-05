package security_test

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"math/big"
	"net"
	"net/url"
	"testing"
	"time"

	"github.com/AlNaheyan/forgeQ/internal/api"
	"github.com/AlNaheyan/forgeQ/internal/coordinator"
	"github.com/AlNaheyan/forgeQ/internal/queue"
	pg "github.com/AlNaheyan/forgeQ/internal/repository/postgres"
	"github.com/AlNaheyan/forgeQ/internal/security"
	"github.com/AlNaheyan/forgeQ/internal/worker"
	"github.com/AlNaheyan/forgeQ/proto/workerpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/test/bufconn"
)

type fixtureQueue struct{ id string }

func (q *fixtureQueue) Dequeue(context.Context) (string, error) {
	if q.id == "" {
		return "", queue.ErrEmpty
	}
	id := q.id
	q.id = ""
	return id, nil
}
func (q *fixtureQueue) Enqueue(_ context.Context, id string, _ int, _ time.Time) error {
	q.id = id
	return nil
}
func (q *fixtureQueue) ScheduleRetry(context.Context, string, time.Time) error { return nil }
func (q *fixtureQueue) Depth(context.Context) (int64, error) {
	if q.id == "" {
		return 0, nil
	}
	return 1, nil
}

// Exercise real API fleet adapter and worker broker across authenticated gRPC,
// backed by the production repository using the coordinator runtime role.
func verifyPipeline(t *testing.T, db *sql.DB, jobID string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	ca := &x509.Certificate{SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "ephemeral"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign}
	der, _ := x509.CreateCertificate(rand.Reader, ca, ca, &key.PublicKey, key)
	ca, _ = x509.ParseCertificate(der)
	roots := x509.NewCertPool()
	roots.AddCert(ca)
	issue := func(identity string) tls.Certificate {
		k, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
		serial, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 100))
		c := &x509.Certificate{SerialNumber: serial, DNSNames: []string{"coordinator"}, NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour), KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth}}
		if identity != "" {
			u, _ := url.Parse(identity)
			c.URIs = []*url.URL{u}
		}
		b, _ := x509.CreateCertificate(rand.Reader, c, ca, &k.PublicKey, key)
		kb, _ := x509.MarshalPKCS8PrivateKey(k)
		pair, err := tls.X509KeyPair(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: b}), pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: kb}))
		if err != nil {
			t.Fatal(err)
		}
		return pair
	}
	registry, err := coordinator.NewRegistry(coordinator.HealthThresholds{SuspectAfter: 10 * time.Second, UnhealthyAfter: 20 * time.Second}, nil)
	if err != nil {
		t.Fatal(err)
	}
	service, err := coordinator.NewGRPCServer(coordinator.ServerConfig{HeartbeatInterval: time.Second, LeaseDuration: time.Minute}, registry, pg.NewJobRepository(db), &fixtureQueue{id: jobID})
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{issue("")}, ClientCAs: roots, ClientAuth: tls.RequireAndVerifyClientCert})), grpc.UnaryInterceptor(security.UnaryAuthorization), grpc.StreamInterceptor(security.StreamAuthorization))
	workerpb.RegisterWorkerCoordinatorServer(server, service)
	listener := bufconn.Listen(1024 * 1024)
	go server.Serve(listener)
	defer server.Stop()
	client := func(identity string) workerpb.WorkerCoordinatorClient {
		conn, err := grpc.NewClient("passthrough:///fixture", grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{issue(identity)}, RootCAs: roots, ServerName: "coordinator"})), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { conn.Close() })
		return workerpb.NewWorkerCoordinatorClient(conn)
	}
	workerClient := client("urn:forgeq:worker:w")
	if _, err := workerClient.RegisterWorker(ctx, &workerpb.RegisterWorkerRequest{WorkerId: "w", MaxConcurrency: 2, SupportedJobTypes: []string{"sleep"}}); err != nil {
		t.Fatal(err)
	}
	provider := api.NewGRPCWorkerProvider(client("urn:forgeq:api:a"))
	if _, err := provider.ListWorkers(ctx); err != nil {
		t.Fatal(err)
	}
	broker, err := worker.NewGRPCBroker(workerClient)
	if err != nil {
		t.Fatal(err)
	}
	job, err := broker.ClaimJob(ctx, "w", []string{"sleep"})
	if err != nil {
		t.Fatal(err)
	}
	if job.ID != jobID {
		t.Fatal("wrong job")
	}
	if err := broker.CompleteJob(ctx, "w", job.ID, []byte(`{"ok":true}`), job.LeaseGeneration); err != nil {
		t.Fatal(err)
	}
	if err := broker.CompleteJob(ctx, "other", job.ID, []byte(`{}`), job.LeaseGeneration); err == nil {
		t.Fatal("spoofed completion accepted")
	}
}
