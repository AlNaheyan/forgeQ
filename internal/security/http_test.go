package security

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestHTTPFailsClosed(t *testing.T) {
	for _, tc := range []struct {
		mode, cert, key string
		prod            bool
	}{
		{"disabled", "", "", true}, {"", "", "", true}, {"tls", "", "", false},
		{"tls", "missing", "missing", false}, {"disabled", "x", "", false},
	} {
		if _, err := HTTPConfig(tc.mode, tc.cert, tc.key, tc.prod); err == nil {
			t.Fatalf("accepted invalid configuration: %+v", tc)
		}
	}
	if c, err := HTTPConfig("disabled", "", "", false); err != nil || c != nil {
		t.Fatal("explicit local HTTP rejected")
	}
}

func TestHTTPDirectTLSIgnoresForwardedHeaders(t *testing.T) {
	ca := makeCA(t)
	c, k := ca.issue(t, "", time.Now().Add(time.Hour))
	cfg, err := HTTPConfig("tls", c, k, true)
	if err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.TLS == nil {
			t.Error("plaintext request")
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.TLS = cfg
	srv.StartTLS()
	defer srv.Close()
	req, _ := http.NewRequest("GET", srv.URL, nil)
	req.Header.Set("Forwarded", "proto=http;host=evil.example")
	req.Header.Set("X-Forwarded-Proto", "http")
	client := srv.Client()
	client.Transport.(*http.Transport).TLSClientConfig.ServerName = "localhost"
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != 204 || resp.Header.Get("Location") != "" {
		t.Fatal("forwarded header affected direct ingress")
	}
	c, k = ca.issue(t, "", time.Now().Add(-time.Minute))
	if _, err := HTTPConfig("tls", c, k, true); err == nil {
		t.Fatal("expired certificate accepted")
	}
}
