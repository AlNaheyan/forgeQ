package security

import (
	"errors"
	"io"
	"os"
	"strings"
	"syscall"
)

// Secret resolves one source, following mounted-secret symlinks while checking
// the opened target. Errors disclose only the setting name and failure category.
func Secret(name, direct, path string) (string, error) {
	if direct != "" && path != "" {
		return "", errors.New(name + ": conflicting secret sources")
	}
	if path == "" {
		return direct, nil
	}
	raw, err := readCredentialFile(name, path, true)
	if err != nil {
		return "", err
	}
	value := string(raw)
	if strings.HasSuffix(value, "\n") {
		value = strings.TrimSuffix(strings.TrimSuffix(value, "\n"), "\r")
	}
	if value == "" {
		return "", errors.New(name + ": secret file is empty")
	}
	return value, nil
}

func readCredentialFile(name, path string, private bool) ([]byte, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, errors.New(name + ": secret file is unreadable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 || (private && info.Mode().Perm()&0007 != 0) {
		return nil, errors.New(name + ": secret file must be regular, bounded and inaccessible to others")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(raw) > 65536 {
		return nil, errors.New(name + ": secret file read failed or exceeded size limit")
	}

	return raw, nil
}
