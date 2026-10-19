package postgres

import "testing"

func TestTruncateUTF8(t *testing.T) {
	got := truncateUTF8("ab🙂cd", 5)
	if got != "ab" {
		t.Fatalf("truncateUTF8() = %q, want %q", got, "ab")
	}
	if got := truncateUTF8("short", 20); got != "short" {
		t.Fatalf("truncateUTF8() changed short input: %q", got)
	}
}
