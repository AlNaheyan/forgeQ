package security

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
)

func RedisTLS(enabled bool, caFile, serverName string, production bool) (*tls.Config, error) {
	if !enabled {
		if production || caFile != "" || serverName != "" {
			return nil, errors.New("Redis requires verified TLS or has conflicting TLS settings")
		}
		return nil, nil
	}
	if caFile == "" || serverName == "" {
		return nil, errors.New("Redis TLS requires CA file and server name")
	}
	raw, err := readCredentialFile("TLS_CA_FILE", caFile, false)
	if err != nil {
		return nil, errors.New("Redis CA is unreadable")
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(raw) {
		return nil, errors.New("Redis CA is invalid")
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: serverName}, nil
}
