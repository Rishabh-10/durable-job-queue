// Package scheduler holds the control plane's background loops: the dispatcher
// that moves due jobs into the ready list, the sweeper that backfills the Redis
// index from PostgreSQL, and the reaper that recovers jobs from dead workers.
//
// None of them needs leader election. Every action they take is a conditional
// UPDATE or an idempotent ZADD, so running N replicas is safe by construction
// and duplicate work is wasted rather than wrong. They live in one package
// because they share the same run loop and the same failure posture.
package scheduler

import (
	"context"
	"errors"
	"log/slog"
	"time"
)

// runLoop calls work on a fixed interval until ctx is cancelled.
//
// Errors are logged and the loop continues. A background reconciler that exits
// on the first transient database blip would take the system's whole recovery
// story with it, which is exactly backwards: these loops matter most when the
// infrastructure is unhealthy.
func runLoop(ctx context.Context, name string, interval time.Duration, log *slog.Logger, work func(context.Context) error) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	log.InfoContext(ctx, "background loop started", "loop", name, "interval", interval)

	for {
		select {
		case <-ctx.Done():
			log.InfoContext(ctx, "background loop stopped", "loop", name)
			return
		case <-ticker.C:
			if err := work(ctx); err != nil {
				// A cancelled context during shutdown is not a failure.
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					continue
				}
				log.ErrorContext(ctx, "background loop iteration failed",
					"loop", name, "error", err)
			}
		}
	}
}
