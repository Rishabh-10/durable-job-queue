package observability

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LoggingInterceptor logs one line per RPC.
//
// Failures log at error level with the code and the cause; successes log at
// debug, so a healthy server is quiet and a sick one is not.
func LoggingInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		started := time.Now()
		resp, err := handler(ctx, req)
		elapsed := time.Since(started)

		code := status.Code(err)
		attrs := []any{
			"method", info.FullMethod,
			"code", code.String(),
			"duration", elapsed,
		}

		if err != nil {
			log.ErrorContext(ctx, "rpc failed", append(attrs, "error", err)...)
		} else {
			log.DebugContext(ctx, "rpc served", attrs...)
		}
		return resp, err
	}
}

// RecoveryInterceptor turns a panic in a handler into an Internal error.
//
// Without it a single malformed request could take down a server that is
// serving every other caller perfectly well. The stack goes to the log, never
// to the client.
func RecoveryInterceptor(log *slog.Logger) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp any, err error) {
		defer func() {
			if r := recover(); r != nil {
				log.ErrorContext(ctx, "panic while serving rpc",
					"method", info.FullMethod,
					"panic", fmt.Sprint(r),
					"stack", string(debug.Stack()),
				)
				err = status.Error(codes.Internal, "internal error")
			}
		}()
		return handler(ctx, req)
	}
}
