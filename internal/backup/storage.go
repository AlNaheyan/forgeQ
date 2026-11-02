// Package backup implements installation-scoped backup lifecycle operations.
package backup

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Storage implementations must commit Put atomically, never overwrite a key,
// and confine all operations to their configured installation namespace.
type Storage interface {
	Put(context.Context, string, io.Reader) error
	Get(context.Context, string) (io.ReadCloser, error)
	List(context.Context) ([]string, error)
	Delete(context.Context, string) error
}
type Local struct{ root, prefix string }

func NewLocal(root, prefix string) (*Local, error) {
	if prefix == "" || strings.ContainsAny(prefix, "/\\.") {
		return nil, errors.New("invalid owned prefix")
	}
	root, err := filepath.Abs(root)
	if err != nil {
		return nil, err
	}
	if err = os.MkdirAll(filepath.Join(root, prefix), 0700); err != nil {
		return nil, err
	}
	return &Local{root, prefix}, nil
}
func (s *Local) path(key string) (string, error) {
	if !strings.HasPrefix(key, s.prefix+"/") {
		return "", errors.New("outside owned prefix")
	}
	name := strings.TrimPrefix(key, s.prefix+"/")
	if name == "" || name == "." || name == ".." || strings.ContainsAny(name, "/\\") {
		return "", errors.New("invalid object key")
	}
	p := filepath.Join(s.root, s.prefix, name)
	for _, part := range []string{filepath.Join(s.root, s.prefix), p} {
		if fi, e := os.Lstat(part); e == nil && fi.Mode()&os.ModeSymlink != 0 {
			return "", errors.New("symlink object refused")
		}
	}
	return p, nil
}
func (s *Local) Put(ctx context.Context, key string, r io.Reader) error {
	p, e := s.path(key)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(p), ".upload-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = io.Copy(f, &contextReader{ctx, r}); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Link(f.Name(), p); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(p))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (s *Local) Get(ctx context.Context, key string) (io.ReadCloser, error) {
	if e := ctx.Err(); e != nil {
		return nil, e
	}
	p, e := s.path(key)
	if e != nil {
		return nil, e
	}
	return os.Open(p)
}
func (s *Local) List(ctx context.Context) ([]string, error) {
	entries, e := os.ReadDir(filepath.Join(s.root, s.prefix))
	if e != nil {
		return nil, e
	}
	var keys []string
	for _, v := range entries {
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		if v.Type().IsRegular() && !strings.HasPrefix(v.Name(), ".") {
			keys = append(keys, s.prefix+"/"+v.Name())
		}
	}
	return keys, nil
}
func (s *Local) Delete(ctx context.Context, key string) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	p, e := s.path(key)
	if e != nil {
		return e
	}
	return os.Remove(p)
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r *contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}

// Schedule holds an exclusive process-independent lock. A crash leaves a stale
// lock: an operator must verify no scheduler remains before removing it.
func Schedule(ctx context.Context, lockDir string, interval time.Duration, run func(context.Context) error) error {
	if interval <= 0 {
		return errors.New("positive interval required")
	}
	lock := filepath.Join(lockDir, "backup.lock")
	f, e := os.OpenFile(lock, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return errors.New("backup scheduler lock unavailable")
	}
	f.Close()
	defer os.Remove(lock)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-timer.C:
			if e = run(ctx); e != nil {
				return e
			}
			timer.Reset(interval)
		}
	}
}
