package security

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestSecretFiles(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "secret")
	token := " secret whitespace preserved "
	if err := os.WriteFile(path, []byte(token+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "mounted")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if got, err := Secret("TOKEN", "", link); err != nil || got != token {
		t.Fatal("mounted secret rejected or altered", err)
	}
	if _, err := Secret("TOKEN", token, path); err == nil || strings.Contains(err.Error(), token) {
		t.Fatal("conflict accepted or secret leaked")
	}
	for _, body := range []string{"", strings.Repeat("x", 65537)} {
		os.WriteFile(path, []byte(body), 0600)
		if _, err := Secret("TOKEN", "", path); err == nil {
			t.Fatal("invalid file accepted")
		}
	}
	os.WriteFile(path, []byte(token), 0600)
	os.Chmod(path, 0644)
	if _, err := Secret("TOKEN", "", path); err == nil {
		t.Fatal("public file accepted")
	}
	if _, err := Secret("TOKEN", "", dir); err == nil {
		t.Fatal("directory accepted")
	}
}
