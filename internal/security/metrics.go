package security

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"net/http"
	"time"
)

// MetricsHandler permits coarse health probes but requires the independent
// read-only metrics credential. Operator tenant keys protect API metrics instead.
func MetricsHandler(metrics http.Handler, token string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, r *http.Request) {
		if token != "" {
			values := r.Header.Values("Authorization")
			if len(values) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			got, want := sha256.Sum256([]byte(values[0])), sha256.Sum256([]byte("Bearer "+token))
			if subtle.ConstantTimeCompare(got[:], want[:]) != 1 {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		metrics.ServeHTTP(w, r)
	})
	return mux
}
func ServeMetrics(ctx context.Context, address string, config *tls.Config, token string, handler http.Handler) error {
	server := &http.Server{Addr: address, TLSConfig: config, Handler: MetricsHandler(handler, token), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	done := make(chan error, 1)
	go func() { done <- ServeHTTP(server) }()
	select {
	case err := <-done:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		return server.Shutdown(shutdown)
	}
}
