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
	f, err := os.OpenFile(path, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return "", errors.New(name + ": secret file is unreadable")
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 65536 || info.Mode().Perm()&0007 != 0 {
		return "", errors.New(name + ": secret file must be regular, bounded and inaccessible to others")
	}
	raw, err := io.ReadAll(io.LimitReader(f, 65537))
	if err != nil || len(raw) > 65536 {
		return "", errors.New(name + ": secret file read failed or exceeded size limit")
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
