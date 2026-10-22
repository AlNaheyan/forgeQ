package tenant

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

type Limits struct{ MaxPending, MaxConcurrent, PerMinute, MaxPayloadBytes int }

func DefaultLimits() Limits { return Limits{1000, 4, 60, 65536} }
func (l Limits) Validate() error {
	if l.MaxPending < 1 || l.MaxConcurrent < 1 || l.PerMinute < 1 || l.MaxPayloadBytes < 1 || l.MaxPayloadBytes > 1048576 {
		return errors.New("limits must be positive; payload limit must be at most 1048576 bytes")
	}
	return nil
}

type Store struct{ db *sql.DB }

func NewStore(db *sql.DB) *Store { return &Store{db: db} }
func NewID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	b[6] = (b[6] & 15) | 64
	b[8] = (b[8] & 63) | 128
	s := hex.EncodeToString(b)
	return s[:8] + "-" + s[8:12] + "-" + s[12:16] + "-" + s[16:20] + "-" + s[20:]
}
func (s *Store) Create(ctx context.Context, name string, l Limits) (string, error) {
	if err := l.Validate(); err != nil {
		return "", err
	}
	name = strings.TrimSpace(name)
	if len(name) < 1 || len(name) > 120 {
		return "", errors.New("name must be 1 to 120 bytes")
	}
	id := NewID()
	_, err := s.db.ExecContext(ctx, `INSERT INTO tenants(id,name,max_pending,max_concurrent,per_minute,max_payload_bytes) VALUES($1,$2,$3,$4,$5,$6)`, id, name, l.MaxPending, l.MaxConcurrent, l.PerMinute, l.MaxPayloadBytes)
	return id, err
}
func (s *Store) SetLimits(ctx context.Context, id string, l Limits) error {
	if err := l.Validate(); err != nil {
		return err
	}
	result, err := s.db.ExecContext(ctx, `UPDATE tenants SET max_pending=$2,max_concurrent=$3,per_minute=$4,max_payload_bytes=$5 WHERE id=$1`, id, l.MaxPending, l.MaxConcurrent, l.PerMinute, l.MaxPayloadBytes)
	if err != nil {
		return err
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return errors.New("tenant not found")
	}
	return err
}

type Credential struct {
	ID       string `json:"key_id"`
	TenantID string `json:"tenant_id"`
	Token    string `json:"api_key"`
	Operator bool   `json:"operator"`
}

func (s *Store) Issue(ctx context.Context, id string, operator bool) (Credential, error) {
	return s.issue(ctx, id, operator, "")
}

// Rotate creates a replacement and revokes the old key in the same transaction.
func (s *Store) Rotate(ctx context.Context, keyID string) (Credential, error) {
	return s.issue(ctx, "", false, keyID)
}
func (s *Store) issue(ctx context.Context, id string, operator bool, oldKey string) (Credential, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return Credential{}, err
	}
	defer tx.Rollback()
	if oldKey != "" {
		if err := tx.QueryRowContext(ctx, `UPDATE api_keys SET revoked_at=clock_timestamp() WHERE id=$1 AND revoked_at IS NULL RETURNING tenant_id,operator`, oldKey).Scan(&id, &operator); err != nil {
			return Credential{}, err
		}
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return Credential{}, err
	}
	c := Credential{ID: NewID(), TenantID: id, Token: "fq_" + hex.EncodeToString(b), Operator: operator}
	sum := sha256.Sum256([]byte(c.Token))
	if _, err := tx.ExecContext(ctx, `INSERT INTO api_keys(id,tenant_id,verifier,operator) VALUES($1,$2,$3,$4)`, c.ID, id, sum[:], operator); err != nil {
		return Credential{}, err
	}
	if err := tx.Commit(); err != nil {
		return Credential{}, err
	}
	return c, nil
}
func (s *Store) Revoke(ctx context.Context, id string) error {
	r, err := s.db.ExecContext(ctx, "UPDATE api_keys SET revoked_at=COALESCE(revoked_at,clock_timestamp()) WHERE id=$1", id)
	if err != nil {
		return err
	}
	n, _ := r.RowsAffected()
	if n == 0 {
		return errors.New("key not found")
	}
	return nil
}
func (s *Store) Authenticate(ctx context.Context, token string) (Principal, error) {
	if len(token) != 67 || !strings.HasPrefix(token, "fq_") {
		return Principal{}, ErrUnauthorized
	}
	sum := sha256.Sum256([]byte(token))
	var p Principal
	err := s.db.QueryRowContext(ctx, `SELECT t.id,k.operator,t.max_payload_bytes FROM api_keys k JOIN tenants t ON t.id=k.tenant_id WHERE k.verifier=$1 AND k.revoked_at IS NULL`, sum[:]).Scan(&p.ID, &p.Operator, &p.MaxPayloadBytes)
	if errors.Is(err, sql.ErrNoRows) {
		return Principal{}, ErrUnauthorized
	}
	if err != nil {
		return Principal{}, fmt.Errorf("authenticate credential: %w", err)
	}
	return p, nil
}
