package security

import "testing"

func TestRedisFailsClosed(t *testing.T) {
	if _, err := RedisTLS(false, "", "", true); err == nil {
		t.Fatal("production plaintext Redis accepted")
	}
	if _, err := RedisTLS(true, "missing", "localhost", true); err == nil {
		t.Fatal("missing CA accepted")
	}
	if _, err := RedisTLS(false, "unexpected", "", false); err == nil {
		t.Fatal("partial TLS downgraded")
	}
	ca := makeCA(t)
	cfg, err := RedisTLS(true, ca.path, "localhost", true)
	if err != nil || cfg.InsecureSkipVerify || cfg.ServerName != "localhost" {
		t.Fatal("verified config rejected", err)
	}
}
