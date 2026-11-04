package security

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func handshake(server, client *tls.Config) error {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	a.SetDeadline(time.Now().Add(time.Second))
	b.SetDeadline(time.Now().Add(time.Second))
	done := make(chan error, 1)
	go func() { done <- tls.Server(a, server).Handshake() }()
	clientErr := tls.Client(b, client).Handshake()
	serverErr := <-done
	if clientErr != nil {
		return clientErr
	}
	return serverErr
}
func TestCredentialRotationAndHostileCertificates(t *testing.T) {
	oldCA, newCA := makeCA(t), makeCA(t)
	oldSC, oldSK := oldCA.issue(t, "", time.Now().Add(time.Hour))
	oldCC, oldCK := oldCA.issue(t, "urn:forgeq:worker:w", time.Now().Add(time.Hour))
	newSC, newSK := newCA.issue(t, "", time.Now().Add(time.Hour))
	newCC, newCK := newCA.issue(t, "urn:forgeq:worker:w", time.Now().Add(time.Hour))
	oldBytes, _ := os.ReadFile(oldCA.path)
	newBytes, _ := os.ReadFile(newCA.path)
	bundle := filepath.Join(t.TempDir(), "overlap.pem")
	os.WriteFile(bundle, append(oldBytes, newBytes...), 0600)
	oldServer, _ := ServiceTLS(oldCA.path, oldSC, oldSK, "", true)
	oldClient, _ := ServiceTLS(oldCA.path, oldCC, oldCK, "localhost", false)
	if err := handshake(oldServer, oldClient); err != nil {
		t.Fatal(err)
	}
	overlapServer, _ := ServiceTLS(bundle, newSC, newSK, "", true)
	overlapClient, _ := ServiceTLS(bundle, oldCC, oldCK, "localhost", false)
	if err := handshake(overlapServer, overlapClient); err != nil {
		t.Fatal("overlap rotation failed", err)
	}
	newServer, _ := ServiceTLS(newCA.path, newSC, newSK, "", true)
	newClient, _ := ServiceTLS(newCA.path, newCC, newCK, "localhost", false)
	if err := handshake(newServer, newClient); err != nil {
		t.Fatal(err)
	}
	if err := handshake(newServer, overlapClient); err == nil {
		t.Fatal("old credential accepted after trust retirement")
	}
	absent := newClient.Clone()
	absent.Certificates = nil
	if err := handshake(newServer, absent); err == nil {
		t.Fatal("missing client credential accepted")
	}
	expiredC, expiredK := newCA.issue(t, "urn:forgeq:worker:w", time.Now().Add(-time.Minute))
	if _, err := ServiceTLS(newCA.path, expiredC, expiredK, "localhost", false); err == nil {
		t.Fatal("expired startup accepted")
	}
	expired, _ := tls.LoadX509KeyPair(expiredC, expiredK)
	hostile := newClient.Clone()
	hostile.Certificates = []tls.Certificate{expired}
	if err := handshake(newServer, hostile); err == nil {
		t.Fatal("expired client handshake accepted")
	}
	hostileServer := newServer.Clone()
	hostileServer.Certificates = []tls.Certificate{expired}
	if err := handshake(hostileServer, newClient); err == nil {
		t.Fatal("expired server handshake accepted")
	}
	// Rotations use immutable snapshots: a replacement file never mutates a live configuration.
	os.WriteFile(newCC, []byte("invalid replacement"), 0600)
	if _, err := ServiceTLS(newCA.path, newCC, newCK, "localhost", false); err == nil {
		t.Fatal("invalid replacement loaded")
	}
	if err := handshake(newServer, newClient); err != nil {
		t.Fatal("validated snapshot mutated", err)
	}
}

func TestTLSMaterialIsBoundedAndPrivate(t *testing.T) {
	ca := makeCA(t)
	c, k := ca.issue(t, "urn:forgeq:worker:w", time.Now().Add(time.Hour))
	if err := os.Chmod(k, 0644); err != nil {
		t.Fatal(err)
	}
	if _, err := Certificate(c, k); err == nil {
		t.Fatal("world-readable private key accepted")
	}
	os.Chmod(k, 0600)
	os.WriteFile(c, make([]byte, 65537), 0600)
	if _, err := Certificate(c, k); err == nil {
		t.Fatal("unbounded certificate accepted")
	}
	if _, err := ServiceTLS(t.TempDir(), c, k, "localhost", false); err == nil {
		t.Fatal("nonregular CA accepted")
	}
}
