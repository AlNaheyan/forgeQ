// Package security builds fail-closed transport and credential snapshots.
package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"time"
)

// Certificate loads a matched, currently valid certificate without exposing PEM or paths.
func Certificate(certFile, keyFile string) (tls.Certificate, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return tls.Certificate{}, errors.New("TLS certificate/key could not be loaded")
	}
	for _, der := range cert.Certificate {
		parsed, err := x509.ParseCertificate(der)
		if err != nil || time.Now().Before(parsed.NotBefore) || !time.Now().Before(parsed.NotAfter) {
			return tls.Certificate{}, errors.New("TLS certificate is invalid or outside its validity period")
		}
	}
	cert.Leaf, _ = x509.ParseCertificate(cert.Certificate[0])
	return cert, nil
}

func HTTPConfig(mode, certFile, keyFile string, production bool) (*tls.Config, error) {
	if mode != "disabled" && mode != "tls" {
		return nil, errors.New("HTTP_TLS_MODE must be disabled or tls")
	}
	if mode == "disabled" {
		if production || certFile != "" || keyFile != "" {
			return nil, errors.New("HTTP TLS is required or configuration conflicts with disabled mode")
		}
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("HTTP TLS requires certificate and key files")
	}
	cert, err := Certificate(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{cert}}, nil
}

// ServeHTTP never opens a plaintext redirect listener. Forwarded headers cannot
// downgrade TLS or determine redirect destinations; ingress is direct TLS.
func ServeHTTP(server *http.Server) error {
	if server.TLSConfig != nil {
		return server.ListenAndServeTLS("", "")
	}
	return server.ListenAndServe()
}
