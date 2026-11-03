package security

import (
	"strings"
	"testing"
)

func TestProductionDatabaseTLS(t *testing.T) {
	for _, raw := range []string{"postgres://user:secret@localhost/db?sslmode=disable", "postgres://user:secret@localhost/db?sslmode=require", "postgres://user@localhost/db?sslmode=verify-full&sslrootcert=x", "postgres://user:secret@localhost/db?sslmode=verify-full&sslmode=disable&sslrootcert=x", "not a URL"} {
		err := ValidateDatabaseURL(raw)
		if err == nil {
			t.Fatal("unsafe DSN accepted")
		}
		if strings.Contains(err.Error(), "secret") || strings.Contains(err.Error(), raw) {
			t.Fatal("DSN disclosed")
		}
	}
	ca := makeCA(t)
	if err := ValidateDatabaseURL("postgres://scoped:password@localhost/db?sslmode=verify-full&sslrootcert=" + ca.path); err != nil {
		t.Fatal(err)
	}
}
