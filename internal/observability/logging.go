// Package observability wires logging and metrics for every binary.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds the process logger. Development uses human-readable text;
// anything else emits JSON for log aggregation.
func NewLogger(env, level string, out io.Writer) (*slog.Logger, error) {
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	if out == nil {
		out = os.Stderr
	}

	opts := &slog.HandlerOptions{Level: lvl}
	var h slog.Handler
	if strings.EqualFold(env, "dev") {
		h = slog.NewTextHandler(out, opts)
	} else {
		h = slog.NewJSONHandler(out, opts)
	}
	return slog.New(h), nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "", "info":
		return slog.LevelInfo, nil
	case "debug":
		return slog.LevelDebug, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q (want debug, info, warn or error)", level)
	}
}
