//go:build integration

package integration

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/observability"
)

// grpcClient serves the API over a real gRPC connection, so the tests exercise
// the transport, the interceptors and the generated stubs rather than calling
// the service struct directly.
func grpcClient(t *testing.T, h *harness) jobqueuev1.JobQueueClient {
	t.Helper()
	return jobqueuev1.NewJobQueueClient(grpcConn(t, h))
}

// grpcConn stands the service up on an in-memory listener and dials it.
func grpcConn(t *testing.T, h *harness) *grpc.ClientConn {
	t.Helper()

	listener := bufconn.Listen(1 << 20)
	server := grpc.NewServer(
		grpc.ChainUnaryInterceptor(
			observability.RecoveryInterceptor(h.log),
			observability.LoggingInterceptor(h.log),
		),
	)
	jobqueuev1.RegisterJobQueueServer(server, h.API)

	healthSrv := health.NewServer()
	healthSrv.SetServingStatus("jobqueue.v1.JobQueue", healthpb.HealthCheckResponse_SERVING)
	healthpb.RegisterHealthServer(server, healthSrv)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("grpc server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient("passthrough://bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return listener.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
		_ = listener.Close()
	})
	return conn
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestGRPCEnqueueAndGet(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	resp, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
		Queue:    "default",
		Kind:     "echo",
		Payload:  []byte("hello"),
		Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(time.Hour)},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if resp.GetAlreadyExisted() {
		t.Error("already_existed = true on a first enqueue")
	}

	got, err := client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: resp.GetJob().GetId()})
	if err != nil {
		t.Fatalf("GetJob: %v", err)
	}
	if string(got.GetJob().GetPayload()) != "hello" {
		t.Errorf("payload = %q, want %q", got.GetJob().GetPayload(), "hello")
	}
	if got.GetJob().GetState() != jobqueuev1.JobState_JOB_STATE_PENDING {
		t.Errorf("state = %s, want pending", got.GetJob().GetState())
	}
	if len(got.GetAttempts()) != 0 {
		t.Errorf("attempts = %v, want none before the job runs", got.GetAttempts())
	}
}

// An absolute run_at and a relative delay must be interchangeable.
func TestGRPCEnqueueAcceptsAbsoluteTime(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	runAt := time.Now().Add(2 * time.Hour).UTC().Truncate(time.Second)
	resp, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
		Queue:    "default",
		Kind:     "echo",
		Payload:  []byte("{}"),
		Schedule: &jobqueuev1.EnqueueRequest_RunAt{RunAt: timestamppb.New(runAt)},
	})
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	if got := resp.GetJob().GetRunAt().AsTime().UTC().Truncate(time.Second); !got.Equal(runAt) {
		t.Errorf("run_at = %s, want %s", got, runAt)
	}
}

// A client that never saw its response must be able to retry safely.
func TestGRPCEnqueueIsIdempotent(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	req := &jobqueuev1.EnqueueRequest{
		Queue:          "default",
		Kind:           "charge_card",
		Payload:        []byte("{}"),
		IdempotencyKey: "order-42",
	}

	first, err := client.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("first Enqueue: %v", err)
	}
	second, err := client.Enqueue(ctx, req)
	if err != nil {
		t.Fatalf("second Enqueue: %v", err)
	}

	if !second.GetAlreadyExisted() {
		t.Error("already_existed = false on a replayed enqueue")
	}
	if second.GetJob().GetId() != first.GetJob().GetId() {
		t.Errorf("replay created job %s, want the original %s", second.GetJob().GetId(), first.GetJob().GetId())
	}
}

func TestGRPCValidationErrors(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	tests := []struct {
		name string
		req  *jobqueuev1.EnqueueRequest
	}{
		{"no queue", &jobqueuev1.EnqueueRequest{Kind: "echo"}},
		{"no kind", &jobqueuev1.EnqueueRequest{Queue: "default"}},
		{
			"negative delay",
			&jobqueuev1.EnqueueRequest{
				Queue:    "default",
				Kind:     "echo",
				Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(-time.Hour)},
			},
		},
		{
			"negative max_attempts",
			&jobqueuev1.EnqueueRequest{Queue: "default", Kind: "echo", MaxAttempts: -1},
		},
		{
			"oversized payload",
			&jobqueuev1.EnqueueRequest{Queue: "default", Kind: "echo", Payload: make([]byte, 2<<20)},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := client.Enqueue(ctx, tc.req)
			if got := status.Code(err); got != codes.InvalidArgument {
				t.Errorf("code = %s, want InvalidArgument (err: %v)", got, err)
			}
		})
	}
}

// A client has to be able to tell "no such job" from "that job is in a state
// where this makes no sense".
func TestGRPCStatusCodesDistinguishFailures(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	t.Run("missing job is NotFound", func(t *testing.T) {
		_, err := client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: "3f2504e0-4f89-11d3-9a0c-0305e82c3301"})
		if got := status.Code(err); got != codes.NotFound {
			t.Errorf("code = %s, want NotFound", got)
		}
	})

	t.Run("malformed id is InvalidArgument", func(t *testing.T) {
		_, err := client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: "not-a-uuid"})
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("code = %s, want InvalidArgument", got)
		}
	})

	t.Run("second cancel is FailedPrecondition", func(t *testing.T) {
		job, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
			Queue:    "default",
			Kind:     "echo",
			Payload:  []byte("{}"),
			Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(time.Hour)},
		})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		if _, err := client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: job.GetJob().GetId()}); err != nil {
			t.Fatalf("first CancelJob: %v", err)
		}
		_, err = client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: job.GetJob().GetId()})
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Errorf("code = %s, want FailedPrecondition", got)
		}
	})

	t.Run("requeueing a live job is FailedPrecondition", func(t *testing.T) {
		job, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
			Queue:    "default",
			Kind:     "echo",
			Payload:  []byte("{}"),
			Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(time.Hour)},
		})
		if err != nil {
			t.Fatalf("Enqueue: %v", err)
		}

		_, err = client.RequeueDeadLetter(ctx, &jobqueuev1.RequeueDeadLetterRequest{Id: job.GetJob().GetId()})
		if got := status.Code(err); got != codes.FailedPrecondition {
			t.Errorf("code = %s, want FailedPrecondition", got)
		}
	})
}

func TestGRPCListJobsPagesAndFilters(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	const total = 12
	for range total {
		if _, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
			Queue:    "default",
			Kind:     "echo",
			Payload:  []byte("{}"),
			Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(time.Hour)},
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	seen := map[string]bool{}
	token := ""
	for pages := 0; ; pages++ {
		if pages > total {
			t.Fatal("pagination did not terminate")
		}
		resp, err := client.ListJobs(ctx, &jobqueuev1.ListJobsRequest{
			Queue:     "default",
			PageSize:  5,
			PageToken: token,
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		for _, j := range resp.GetJobs() {
			if seen[j.GetId()] {
				t.Errorf("job %s appeared on two pages", j.GetId())
			}
			seen[j.GetId()] = true
		}
		token = resp.GetNextPageToken()
		if token == "" {
			break
		}
	}

	if len(seen) != total {
		t.Errorf("paged through %d jobs, want %d", len(seen), total)
	}

	// Filtering on a state nothing is in returns nothing, not everything.
	resp, err := client.ListJobs(ctx, &jobqueuev1.ListJobsRequest{
		Queue:  "default",
		States: []jobqueuev1.JobState{jobqueuev1.JobState_JOB_STATE_DEAD},
	})
	if err != nil {
		t.Fatalf("ListJobs filtered: %v", err)
	}
	if len(resp.GetJobs()) != 0 {
		t.Errorf("dead-state filter returned %d jobs, want 0", len(resp.GetJobs()))
	}
}

func TestGRPCRejectsMalformedPageToken(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	_, err := client.ListJobs(ctx, &jobqueuev1.ListJobsRequest{
		Queue:     "default",
		PageToken: "!!!not-base64!!!",
	})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Errorf("code = %s, want InvalidArgument", got)
	}
}

func TestGRPCQueueStats(t *testing.T) {
	h := newHarness(t)
	client := grpcClient(t, h)
	ctx := testCtx(t)

	const jobs = 5
	for range jobs {
		if _, err := client.Enqueue(ctx, &jobqueuev1.EnqueueRequest{
			Queue:    "default",
			Kind:     "echo",
			Payload:  []byte("{}"),
			Schedule: &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(time.Hour)},
		}); err != nil {
			t.Fatalf("Enqueue: %v", err)
		}
	}

	stats, err := client.QueueStats(ctx, &jobqueuev1.QueueStatsRequest{Queue: "default"})
	if err != nil {
		t.Fatalf("QueueStats: %v", err)
	}
	if got := stats.GetCountsByState()["pending"]; got != jobs {
		t.Errorf("pending = %d, want %d", got, jobs)
	}
	if got := stats.GetScheduledIndexSize(); got != jobs {
		t.Errorf("scheduled_index_size = %d, want %d", got, jobs)
	}
}

// Orchestrators and grpcurl both rely on the standard health service.
func TestGRPCHealthCheck(t *testing.T) {
	h := newHarness(t)
	ctx := testCtx(t)

	healthClient := healthpb.NewHealthClient(grpcConn(t, h))
	resp, err := healthClient.Check(ctx, &healthpb.HealthCheckRequest{Service: "jobqueue.v1.JobQueue"})
	if err != nil {
		t.Fatalf("health Check: %v", err)
	}
	if resp.GetStatus() != healthpb.HealthCheckResponse_SERVING {
		t.Errorf("status = %s, want SERVING", resp.GetStatus())
	}
}
