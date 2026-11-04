package security

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"regexp"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Identity struct{ Role, ID string }

var identityPattern = regexp.MustCompile(`^urn:forgeq:(api|worker|monitor):([A-Za-z0-9][A-Za-z0-9._-]{0,63})$`)

func CertificateIdentity(cert *x509.Certificate) (Identity, error) {
	var result Identity
	count := 0
	for _, u := range cert.URIs {
		if !strings.HasPrefix(u.String(), "urn:forgeq:") {
			continue
		}
		m := identityPattern.FindStringSubmatch(u.String())
		if len(m) != 3 {
			return Identity{}, errors.New("invalid service identity")
		}
		count++
		result = Identity{m[1], m[2]}
	}
	if count != 1 {
		return Identity{}, errors.New("exactly one service identity is required")
	}
	return result, nil
}

func ServiceTLS(caFile, certFile, keyFile, serverName string, server bool) (*tls.Config, error) {
	if caFile == "" || certFile == "" || keyFile == "" || (!server && serverName == "") {
		return nil, errors.New("service TLS requires CA, certificate, key and client server name")
	}
	ca, err := readCredentialFile("TLS_CA_FILE", caFile, false)
	if err != nil {
		return nil, errors.New("service CA could not be loaded")
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(ca) {
		return nil, errors.New("service CA is invalid")
	}
	cert, err := Certificate(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}, RootCAs: roots, ServerName: serverName}
	if server {
		cfg.ClientCAs = roots
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	} else {
		if _, err = CertificateIdentity(cert.Leaf); err != nil {
			return nil, err
		}
	}
	return cfg, nil
}

func peerIdentity(ctx context.Context) (Identity, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return Identity{}, status.Error(codes.Unauthenticated, "verified service identity required")
	}
	auth, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || len(auth.State.VerifiedChains) == 0 || len(auth.State.PeerCertificates) == 0 {
		return Identity{}, status.Error(codes.Unauthenticated, "verified service identity required")
	}
	id, err := CertificateIdentity(auth.State.PeerCertificates[0])
	if err != nil {
		return Identity{}, status.Error(codes.Unauthenticated, "verified service identity required")
	}
	return id, nil
}
func authorizeMethod(ctx context.Context, method string, req any) error {
	id, err := peerIdentity(ctx)
	if err != nil {
		return err
	}
	if method == "/grpc.health.v1.Health/Check" || method == "/grpc.health.v1.Health/Watch" {
		return nil
	}
	const prefix = "/forgequeue.worker.v1.WorkerCoordinator/"
	if method == prefix+"ListWorkers" && id.Role == "api" {
		return nil
	}
	if strings.HasPrefix(method, prefix) && id.Role == "worker" {
		switch strings.TrimPrefix(method, prefix) {
		case "RegisterWorker", "Heartbeat", "ClaimJob", "StartJob", "CompleteJob", "FailJob", "RenewLease":
			bound, ok := req.(interface{ GetWorkerId() string })
			if ok && bound.GetWorkerId() == id.ID {
				return nil
			}
		}
	}
	return status.Error(codes.PermissionDenied, "service identity is not authorized")
}
func UnaryAuthorization(ctx context.Context, req any, info *grpc.UnaryServerInfo, next grpc.UnaryHandler) (any, error) {
	if err := authorizeMethod(ctx, info.FullMethod, req); err != nil {
		return nil, err
	}
	return next(ctx, req)
}
func StreamAuthorization(srv any, stream grpc.ServerStream, info *grpc.StreamServerInfo, next grpc.StreamHandler) error {
	if err := authorizeMethod(stream.Context(), info.FullMethod, nil); err != nil {
		return err
	}
	return next(srv, stream)
}
