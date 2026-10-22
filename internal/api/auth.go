package api

import (
	"context"
	"errors"
	"forgequeue/internal/tenant"
	"net/http"
	"strings"
)

type Authenticator interface {
	Authenticate(context.Context, string) (tenant.Principal, error)
}

func (s *Server) WithAuthenticator(a Authenticator) *Server { s.auth = a; return s }

func (s *Server) protect(next http.HandlerFunc, operator bool) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		parts := strings.Fields(r.Header.Get("Authorization"))
		if s.auth == nil || len(r.Header.Values("Authorization")) != 1 || len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
			w.Header().Set("WWW-Authenticate", `Bearer realm="forgequeue"`)
			writeError(w, 401, "unauthorized", "valid API key required")
			return
		}
		p, err := s.auth.Authenticate(r.Context(), parts[1])
		if err != nil || p.ID == "" {
			if err != nil && !errors.Is(err, tenant.ErrUnauthorized) {
				writeError(w, 503, "auth_unavailable", "authentication unavailable")
				return
			}
			w.Header().Set("WWW-Authenticate", `Bearer realm="forgequeue"`)
			writeError(w, 401, "unauthorized", "valid API key required")
			return
		}
		if operator && !p.Operator {
			writeError(w, 403, "forbidden", "operator permission required")
			return
		}
		next(w, r.WithContext(tenant.WithPrincipal(r.Context(), p)))
	}
}
