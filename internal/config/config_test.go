package config

import (
	"strings"
	"testing"
	"time"
)

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load with no environment set: %v", err)
	}

	if cfg.Retry.DefaultMaxAttempts != 5 {
		t.Errorf("DefaultMaxAttempts = %d, want 5", cfg.Retry.DefaultMaxAttempts)
	}
	if got, want := cfg.Worker.Queues, []string{"default"}; len(got) != 1 || got[0] != want[0] {
		t.Errorf("Worker.Queues = %v, want %v", got, want)
	}
	if cfg.Worker.LeaseTTL != 30*time.Second {
		t.Errorf("LeaseTTL = %s, want 30s", cfg.Worker.LeaseTTL)
	}
	if !cfg.Sweeper.Enabled || !cfg.Reaper.Enabled || !cfg.Dispatcher.Enabled {
		t.Error("background loops should default to enabled")
	}
}

func TestLoadParsesEnvironment(t *testing.T) {
	t.Setenv("JQ_WORKER_QUEUES", " emails , webhooks ,, ")
	t.Setenv("JQ_WORKER_CONCURRENCY", "32")
	t.Setenv("JQ_SWEEPER_INTERVAL", "2s")
	t.Setenv("JQ_SWEEPER_LOOKAHEAD", "5s")
	t.Setenv("JQ_DISPATCHER_ENABLED", "false")

	cfg, err := Load()
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	want := []string{"emails", "webhooks"}
	if len(cfg.Worker.Queues) != len(want) {
		t.Fatalf("Worker.Queues = %v, want %v", cfg.Worker.Queues, want)
	}
	for i, q := range want {
		if cfg.Worker.Queues[i] != q {
			t.Errorf("Worker.Queues[%d] = %q, want %q", i, cfg.Worker.Queues[i], q)
		}
	}
	if cfg.Worker.Concurrency != 32 {
		t.Errorf("Concurrency = %d, want 32", cfg.Worker.Concurrency)
	}
	if cfg.Sweeper.Interval != 2*time.Second {
		t.Errorf("Sweeper.Interval = %s, want 2s", cfg.Sweeper.Interval)
	}
	if cfg.Dispatcher.Enabled {
		t.Error("Dispatcher.Enabled = true, want false")
	}
}

func TestLoadRejectsMalformedValues(t *testing.T) {
	tests := []struct {
		name    string
		key     string
		value   string
		wantErr string
	}{
		{"not an integer", "JQ_WORKER_CONCURRENCY", "many", "is not an integer"},
		{"not a duration", "JQ_WORKER_LEASE_TTL", "30 seconds", "is not a duration"},
		{"not a boolean", "JQ_SWEEPER_ENABLED", "yes please", "is not a boolean"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("Load succeeded, want error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

// Load reports every problem at once so a misconfigured deployment takes one
// restart to fix rather than one per mistake.
func TestLoadReportsAllProblemsTogether(t *testing.T) {
	t.Setenv("JQ_WORKER_CONCURRENCY", "0")
	t.Setenv("JQ_DEFAULT_MAX_ATTEMPTS", "0")

	_, err := Load()
	if err == nil {
		t.Fatal("Load succeeded, want error")
	}
	for _, want := range []string{"JQ_WORKER_CONCURRENCY", "JQ_DEFAULT_MAX_ATTEMPTS"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

func TestValidateCatchesInconsistentTimings(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
	}{
		{
			// A reaper slower than the lease lets a dead worker's jobs stall
			// well past the point the lease said they were recoverable.
			name:    "reaper slower than lease",
			env:     map[string]string{"JQ_REAPER_INTERVAL": "60s", "JQ_WORKER_LEASE_TTL": "30s"},
			wantErr: "JQ_REAPER_INTERVAL",
		},
		{
			// With no lookahead the sweeper only ever indexes jobs that are
			// already late.
			name:    "lookahead shorter than sweep interval",
			env:     map[string]string{"JQ_SWEEPER_INTERVAL": "30s", "JQ_SWEEPER_LOOKAHEAD": "1s"},
			wantErr: "JQ_SWEEPER_LOOKAHEAD",
		},
		{
			name:    "backoff cap below base",
			env:     map[string]string{"JQ_BACKOFF_BASE": "10s", "JQ_BACKOFF_CAP": "1s"},
			wantErr: "JQ_BACKOFF_CAP",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			for k, v := range tc.env {
				t.Setenv(k, v)
			}
			if _, err := Load(); err == nil {
				t.Fatal("Load succeeded, want error")
			} else if !strings.Contains(err.Error(), tc.wantErr) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantErr)
			}
		})
	}
}

func TestHeartbeatIntervalFitsInsideLease(t *testing.T) {
	w := Worker{LeaseTTL: 30 * time.Second}
	if got, want := w.HeartbeatInterval(), 10*time.Second; got != want {
		t.Errorf("HeartbeatInterval() = %s, want %s", got, want)
	}
}
