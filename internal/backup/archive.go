package backup

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"filippo.io/age"
)

type Manifest struct {
	Version       int       `json:"version"`
	Object        string    `json:"object"`
	SHA256        string    `json:"sha256"`
	Size          int64     `json:"size"`
	Snapshot      time.Time `json:"snapshot"`
	Uploaded      time.Time `json:"uploaded"`
	Revision      string    `json:"revision"`
	Schema        string    `json:"schema"`
	DatabaseMajor string    `json:"database_major"`
	Encryption    string    `json:"encryption"`
	KeyID         string    `json:"key_id"`
	Complete      bool      `json:"complete"`
}

// Observe is called for every completed attempt. Successful manifests alone
// advance freshness gauges; failure must preserve previous successful values.
type Observer func(Manifest, time.Duration, error)
type Runner struct {
	Store                                  Storage
	Prefix                                 string
	TempDir                                string
	Recipients                             []age.Recipient
	KeyID, Revision, Schema, DatabaseMajor string
	Observe                                Observer
}

func (r *Runner) Backup(ctx context.Context, dump func(context.Context, io.Writer) error) (m Manifest, err error) {
	start := time.Now()
	defer func() {
		if r.Observe != nil {
			r.Observe(m, time.Since(start), err)
		}
	}()
	if len(r.Recipients) == 0 || r.KeyID == "" || r.Revision == "" || r.Schema == "" || r.DatabaseMajor == "" {
		return m, errors.New("backup metadata or recipients missing")
	}
	f, err := os.CreateTemp(r.TempDir, "forgeq-ciphertext-")
	if err != nil {
		return m, err
	}
	defer os.Remove(f.Name())
	defer f.Close()
	w, err := age.Encrypt(f, r.Recipients...)
	if err != nil {
		return m, errors.New("encryption initialization failed")
	}
	m.Snapshot = start.UTC()
	if err = dump(ctx, w); err != nil {
		return m, errors.New("database snapshot failed")
	}
	if err = w.Close(); err != nil {
		return m, errors.New("encryption finalization failed")
	}
	if _, err = f.Seek(0, 0); err != nil {
		return m, err
	}
	hash := sha256.New()
	m.Size, err = io.Copy(hash, f)
	if err != nil {
		return m, err
	}
	m.SHA256 = hex.EncodeToString(hash.Sum(nil))
	id := make([]byte, 16)
	if _, err = rand.Read(id); err != nil {
		return m, err
	}
	m.Object = r.Prefix + "/" + hex.EncodeToString(id) + ".age"
	if _, err = f.Seek(0, 0); err != nil {
		return m, err
	}
	if err = r.Store.Put(ctx, m.Object, f); err != nil {
		return m, errors.New("archive upload failed")
	}
	if err = Verify(ctx, r.Store, m); err != nil {
		return m, err
	}
	m.Version = 1
	m.Encryption = "age-v1"
	m.KeyID = r.KeyID
	m.Revision = r.Revision
	m.Schema = r.Schema
	m.DatabaseMajor = r.DatabaseMajor
	m.Uploaded = time.Now().UTC()
	m.Complete = true
	b, _ := json.Marshal(m)
	if err = r.Store.Put(ctx, m.Object+".json", bytesReader(b)); err != nil {
		return m, errors.New("manifest upload failed")
	}
	return m, nil
}

type byteReader struct{ b []byte }

func bytesReader(b []byte) io.Reader { return &byteReader{b} }
func (b *byteReader) Read(p []byte) (int, error) {
	if len(b.b) == 0 {
		return 0, io.EOF
	}
	n := copy(p, b.b)
	b.b = b.b[n:]
	return n, nil
}
func Verify(ctx context.Context, s Storage, m Manifest) error {
	r, e := s.Get(ctx, m.Object)
	if e != nil {
		return errors.New("archive unavailable")
	}
	defer r.Close()
	h := sha256.New()
	n, e := io.Copy(h, &contextReader{ctx, r})
	if e != nil || n != m.Size || hex.EncodeToString(h.Sum(nil)) != m.SHA256 {
		return errors.New("archive checksum or size mismatch")
	}
	return nil
}
func ReadManifest(ctx context.Context, s Storage, key string) (Manifest, error) {
	var m Manifest
	r, e := s.Get(ctx, key)
	if e != nil {
		return m, errors.New("manifest unavailable")
	}
	defer r.Close()
	d := json.NewDecoder(io.LimitReader(r, 65537))
	d.DisallowUnknownFields()
	if e = d.Decode(&m); e != nil {
		return m, errors.New("invalid manifest")
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return m, errors.New("invalid manifest trailing data")
	}
	if m.Version != 1 || !m.Complete || m.Encryption != "age-v1" || m.Object+".json" != key || m.Size <= 0 || len(m.SHA256) != 64 || m.Snapshot.IsZero() || m.Uploaded.Before(m.Snapshot) {
		return m, errors.New("incomplete or unsupported manifest")
	}
	return m, nil
}

// Decrypt verifies ciphertext first and spools authenticated plaintext privately;
// callers never restore unauthenticated partial output.
func Decrypt(ctx context.Context, s Storage, m Manifest, dir string, ids ...age.Identity) (*os.File, error) {
	if e := Verify(ctx, s, m); e != nil {
		return nil, e
	}
	r, e := s.Get(ctx, m.Object)
	if e != nil {
		return nil, e
	}
	defer r.Close()
	plain, e := age.Decrypt(r, ids...)
	if e != nil {
		return nil, errors.New("decryption failed")
	}
	f, e := os.CreateTemp(dir, "forgeq-restore-")
	if e != nil {
		return nil, e
	}
	if _, e = io.Copy(f, &contextReader{ctx, plain}); e != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, errors.New("decryption authentication failed")
	}
	if _, e = f.Seek(0, 0); e != nil {
		f.Close()
		os.Remove(f.Name())
		return nil, fmt.Errorf("restore seek failed")
	}
	return f, nil
}
