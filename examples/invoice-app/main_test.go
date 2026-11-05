package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestInvoiceValidation(t *testing.T) {
	for _, tc := range []struct {
		payload string
		valid   bool
	}{
		{`{"invoice_id":"inv-1","amount_cents":4500}`, true},
		{`{"invoice_id":"../bad","amount_cents":1}`, false},
		{`{"invoice_id":"inv-1","amount_cents":0}`, false},
		{`{"invoice_id":"inv-1","amount_cents":1,"url":"http://internal"}`, false},
	} {
		_, err := decodeInvoice(json.RawMessage(tc.payload))
		if (err == nil) != tc.valid {
			t.Errorf("payload %s: %v", tc.payload, err)
		}
	}
	if _, err := decodeInvoice(json.RawMessage(`{"invoice_id":"` + strings.Repeat("x", 65) + `","amount_cents":1}`)); err == nil {
		t.Fatal("oversized identifier accepted")
	}
}
