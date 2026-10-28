package operations

import "testing"

func TestRetentionConfiguration(t *testing.T) {
	for _, name := range []string{"RETENTION_ENABLED", "RETENTION_JOB_AGE", "RETENTION_EVENT_AGE", "RETENTION_BATCH"} {
		t.Setenv(name, "")
	}
	c, err := LoadConfig()
	if err != nil || c.Enabled || c.Batch != 100 {
		t.Fatalf("unsafe defaults: %+v %v", c, err)
	}
	for _, tc := range []struct{ name, value string }{{"RETENTION_ENABLED", "maybe"}, {"RETENTION_JOB_AGE", "1h"}, {"RETENTION_EVENT_AGE", "-1h"}, {"RETENTION_BATCH", "0"}, {"RETENTION_BATCH", "1001"}} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			t.Setenv(tc.name, tc.value)
			if _, err := LoadConfig(); err == nil {
				t.Fatal("invalid retention config accepted")
			}
		})
	}
}
