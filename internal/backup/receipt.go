package backup

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"time"
)

// A receipt attests that this ciphertext was decrypted and pg_restore completed.
// Storage writers remain trusted: this is a durable operational record, not a
// signature or a substitute for application readiness validation.
type restoreReceipt struct {
	Version  int       `json:"version"`
	Object   string    `json:"object"`
	SHA256   string    `json:"sha256"`
	Restored time.Time `json:"restored"`
}

func restoredAt(ctx context.Context, s Storage, m Manifest) time.Time {
	r, e := s.Get(ctx, m.Object+".restored.json")
	if e != nil {
		return time.Time{}
	}
	defer r.Close()
	var receipt restoreReceipt
	d := json.NewDecoder(io.LimitReader(r, 4097))
	d.DisallowUnknownFields()
	if d.Decode(&receipt) != nil {
		return time.Time{}
	}
	var extra any
	if d.Decode(&extra) != io.EOF || receipt.Version != 1 || receipt.Object != m.Object || receipt.SHA256 != m.SHA256 || receipt.Restored.Before(m.Uploaded) {
		return time.Time{}
	}
	return receipt.Restored
}
func recordRestore(ctx context.Context, s Storage, m Manifest) error {
	if !restoredAt(ctx, s, m).IsZero() {
		return nil
	}
	b, _ := json.Marshal(restoreReceipt{Version: 1, Object: m.Object, SHA256: m.SHA256, Restored: time.Now().UTC()})
	if s.Put(ctx, m.Object+".restored.json", bytesReader(b)) != nil {
		return errors.New("restore succeeded but receipt publication failed")
	}
	return nil
}
