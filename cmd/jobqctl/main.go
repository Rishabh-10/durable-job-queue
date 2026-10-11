// Command jobqctl is a command-line client for the job queue.
//
//	jobqctl enqueue -kind echo -payload '{"hi":true}' -delay 30s
//	jobqctl get <job-id>
//	jobqctl watch <job-id>
//	jobqctl ls -state pending
//	jobqctl cancel <job-id>
//	jobqctl dlq ls
//	jobqctl dlq requeue <job-id>
//	jobqctl stats
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintf(os.Stderr, "jobqctl: %v\n", err)
		os.Exit(1)
	}
}

const usage = `jobqctl is a command-line client for the durable job queue.

Usage:
  jobqctl [-addr host:port] <command> [flags]

Commands:
  enqueue      Schedule a job
  get          Show one job and its attempt history
  watch        Follow a job until it reaches a terminal state
  ls           List jobs on a queue
  cancel       Cancel a job that has not started
  dlq ls       List dead-lettered jobs
  dlq requeue  Return a dead-lettered job to the queue
  stats        Summarise a queue

Run "jobqctl <command> -h" for the flags a command takes.
The server address defaults to localhost:8080, or $JQ_ADDR.
`

func run(args []string) error {
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	// A global -addr may appear before the subcommand.
	addr := envOr("JQ_ADDR", "localhost:8080")
	for len(args) > 0 && strings.HasPrefix(args[0], "-") {
		switch {
		case args[0] == "-h" || args[0] == "-help" || args[0] == "--help":
			fmt.Print(usage)
			return nil
		case args[0] == "-addr" && len(args) > 1:
			addr, args = args[1], args[2:]
		case strings.HasPrefix(args[0], "-addr="):
			addr, args = strings.TrimPrefix(args[0], "-addr="), args[1:]
		default:
			return fmt.Errorf("unknown global flag %q", args[0])
		}
	}
	if len(args) == 0 {
		fmt.Print(usage)
		return nil
	}

	cmd, rest := args[0], args[1:]

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("connect to %s: %w", addr, err)
	}
	defer func() { _ = conn.Close() }()

	client := jobqueuev1.NewJobQueueClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	switch cmd {
	case "enqueue":
		return cmdEnqueue(ctx, client, rest)
	case "get":
		return cmdGet(ctx, client, rest)
	case "watch":
		return cmdWatch(ctx, client, rest)
	case "ls":
		return cmdList(ctx, client, rest)
	case "cancel":
		return cmdCancel(ctx, client, rest)
	case "dlq":
		return cmdDLQ(ctx, client, rest)
	case "stats":
		return cmdStats(ctx, client, rest)
	case "help":
		fmt.Print(usage)
		return nil
	default:
		return fmt.Errorf("unknown command %q; run \"jobqctl help\"", cmd)
	}
}

func cmdEnqueue(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	fs := flag.NewFlagSet("enqueue", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue to schedule on")
	kind := fs.String("kind", "", "handler name (required)")
	payload := fs.String("payload", "{}", "payload, as a string")
	delay := fs.Duration("delay", 0, "run this long from now")
	runAt := fs.String("run-at", "", "run at this RFC3339 time (overrides -delay)")
	maxAttempts := fs.Int("max-attempts", 0, "attempts before dead-lettering (0 uses the server default)")
	idemKey := fs.String("idempotency-key", "", "make this enqueue idempotent within the queue")
	count := fs.Int("count", 1, "schedule this many copies, for load testing")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if *kind == "" {
		return fmt.Errorf("-kind is required")
	}
	if *count > 1 && *idemKey != "" {
		// Every copy would collapse onto the first, which is never what the
		// caller wants from -count.
		return fmt.Errorf("-count and -idempotency-key cannot be combined")
	}

	req := &jobqueuev1.EnqueueRequest{
		Queue:          *queue,
		Kind:           *kind,
		Payload:        []byte(*payload),
		MaxAttempts:    int32(*maxAttempts),
		IdempotencyKey: *idemKey,
	}
	switch {
	case *runAt != "":
		at, err := time.Parse(time.RFC3339, *runAt)
		if err != nil {
			return fmt.Errorf("-run-at: %w", err)
		}
		req.Schedule = &jobqueuev1.EnqueueRequest_RunAt{RunAt: timestamppb.New(at)}
	case *delay > 0:
		req.Schedule = &jobqueuev1.EnqueueRequest_Delay{Delay: durationpb.New(*delay)}
	}

	started := time.Now()
	for n := range *count {
		resp, err := client.Enqueue(ctx, req)
		if err != nil {
			return fmt.Errorf("enqueue: %w", err)
		}
		if *count == 1 {
			job := resp.GetJob()
			if resp.GetAlreadyExisted() {
				fmt.Printf("%s (already existed; idempotency key matched)\n", job.GetId())
			} else {
				fmt.Printf("%s scheduled for %s\n", job.GetId(), job.GetRunAt().AsTime().Local().Format(time.RFC3339))
			}
			return nil
		}
		if n%100 == 99 {
			fmt.Printf("\rscheduled %d/%d", n+1, *count)
		}
	}
	fmt.Printf("\rscheduled %d jobs in %s\n", *count, time.Since(started).Round(time.Millisecond))
	return nil
}

// parseWithID parses a command that takes exactly one job id, allowing flags on
// either side of it.
//
// flag.FlagSet stops at the first non-flag argument, so "watch <id> -interval 1s"
// would otherwise leave the flags unparsed and sitting in Args(). Parsing twice
// -- once up to the id, once after it -- accepts the order people actually type.
func parseWithID(fs *flag.FlagSet, args []string, usage string) (string, error) {
	if err := fs.Parse(args); err != nil {
		return "", err
	}
	rest := fs.Args()
	if len(rest) == 0 {
		return "", fmt.Errorf("usage: %s", usage)
	}

	id := rest[0]
	if err := fs.Parse(rest[1:]); err != nil {
		return "", err
	}
	if fs.NArg() != 0 {
		return "", fmt.Errorf("unexpected arguments %v; usage: %s", fs.Args(), usage)
	}
	return id, nil
}

func cmdGet(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	fs := flag.NewFlagSet("get", flag.ContinueOnError)
	asJSON := fs.Bool("json", false, "print raw JSON")

	id, err := parseWithID(fs, args, "jobqctl get <job-id>")
	if err != nil {
		return err
	}

	resp, err := client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: id})
	if err != nil {
		return err
	}
	if *asJSON {
		return printJSON(resp)
	}
	printJob(resp.GetJob())
	printAttempts(resp.GetAttempts())
	return nil
}

func cmdWatch(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	fs := flag.NewFlagSet("watch", flag.ContinueOnError)
	interval := fs.Duration("interval", time.Second, "how often to poll")

	id, err := parseWithID(fs, args, "jobqctl watch <job-id>")
	if err != nil {
		return err
	}

	var lastState jobqueuev1.JobState
	for {
		resp, err := client.GetJob(ctx, &jobqueuev1.GetJobRequest{Id: id})
		if err != nil {
			return err
		}
		job := resp.GetJob()

		if job.GetState() != lastState {
			fmt.Printf("%s  %-10s attempt %d/%d",
				time.Now().Format("15:04:05"), stateName(job.GetState()),
				job.GetAttempts(), job.GetMaxAttempts())
			if job.GetLastError() != "" {
				fmt.Printf("  last error: %s", job.GetLastError())
			}
			fmt.Println()
			lastState = job.GetState()
		}

		if isTerminal(job.GetState()) {
			printAttempts(resp.GetAttempts())
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*interval):
		}
	}
}

func cmdList(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue to list")
	stateFilter := fs.String("state", "", "comma-separated states to show (pending, running, succeeded, failed, dead, cancelled)")
	limit := fs.Int("limit", 20, "maximum jobs to show")
	if err := fs.Parse(args); err != nil {
		return err
	}

	states, err := parseStates(*stateFilter)
	if err != nil {
		return err
	}

	resp, err := client.ListJobs(ctx, &jobqueuev1.ListJobsRequest{
		Queue:    *queue,
		States:   states,
		PageSize: int32(*limit),
	})
	if err != nil {
		return err
	}
	printJobTable(resp.GetJobs())
	if resp.GetNextPageToken() != "" {
		fmt.Printf("\n(more jobs available; raise -limit to see them)\n")
	}
	return nil
}

func cmdCancel(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	if len(args) != 1 {
		return fmt.Errorf("usage: jobqctl cancel <job-id>")
	}
	resp, err := client.CancelJob(ctx, &jobqueuev1.CancelJobRequest{Id: args[0]})
	if err != nil {
		return err
	}
	fmt.Printf("%s cancelled\n", resp.GetJob().GetId())
	return nil
}

func cmdDLQ(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	if len(args) == 0 {
		return fmt.Errorf("usage: jobqctl dlq ls|requeue")
	}

	switch args[0] {
	case "ls":
		fs := flag.NewFlagSet("dlq ls", flag.ContinueOnError)
		queue := fs.String("queue", "default", "queue to list")
		limit := fs.Int("limit", 20, "maximum jobs to show")
		if err := fs.Parse(args[1:]); err != nil {
			return err
		}

		resp, err := client.ListDeadLetters(ctx, &jobqueuev1.ListDeadLettersRequest{
			Queue:    *queue,
			PageSize: int32(*limit),
		})
		if err != nil {
			return err
		}
		if len(resp.GetJobs()) == 0 {
			fmt.Printf("no dead-lettered jobs on %s\n", *queue)
			return nil
		}
		printJobTable(resp.GetJobs())
		return nil

	case "requeue":
		fs := flag.NewFlagSet("dlq requeue", flag.ContinueOnError)
		additional := fs.Int("additional-attempts", 0, "extra attempts to grant (0 uses the server default)")
		delay := fs.Duration("delay", 0, "wait this long before the job becomes due")

		id, err := parseWithID(fs, args[1:], "jobqctl dlq requeue <job-id>")
		if err != nil {
			return err
		}

		req := &jobqueuev1.RequeueDeadLetterRequest{
			Id:                 id,
			AdditionalAttempts: int32(*additional),
		}
		if *delay > 0 {
			req.Delay = durationpb.New(*delay)
		}

		resp, err := client.RequeueDeadLetter(ctx, req)
		if err != nil {
			return err
		}
		fmt.Printf("%s requeued for %s\n",
			resp.GetJob().GetId(), resp.GetJob().GetRunAt().AsTime().Local().Format(time.RFC3339))
		return nil

	default:
		return fmt.Errorf("unknown dlq subcommand %q; want ls or requeue", args[0])
	}
}

func cmdStats(ctx context.Context, client jobqueuev1.JobQueueClient, args []string) error {
	fs := flag.NewFlagSet("stats", flag.ContinueOnError)
	queue := fs.String("queue", "default", "queue to summarise")
	watch := fs.Duration("watch", 0, "repeat at this interval")
	if err := fs.Parse(args); err != nil {
		return err
	}

	for {
		resp, err := client.QueueStats(ctx, &jobqueuev1.QueueStatsRequest{Queue: *queue})
		if err != nil {
			return err
		}
		printStats(resp)

		if *watch <= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(*watch):
			fmt.Println()
		}
	}
}

func printJob(job *jobqueuev1.Job) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "id\t%s\n", job.GetId())
	fmt.Fprintf(w, "queue\t%s\n", job.GetQueue())
	fmt.Fprintf(w, "kind\t%s\n", job.GetKind())
	fmt.Fprintf(w, "state\t%s\n", stateName(job.GetState()))
	fmt.Fprintf(w, "run at\t%s\n", job.GetRunAt().AsTime().Local().Format(time.RFC3339))
	fmt.Fprintf(w, "attempts\t%d of %d\n", job.GetAttempts(), job.GetMaxAttempts())
	if job.GetIdempotencyKey() != "" {
		fmt.Fprintf(w, "idempotency key\t%s\n", job.GetIdempotencyKey())
	}
	if job.GetLeaseOwner() != "" {
		fmt.Fprintf(w, "lease\t%s until %s\n",
			job.GetLeaseOwner(), job.GetLeaseExpiresAt().AsTime().Local().Format(time.RFC3339))
	}
	if job.GetLastError() != "" {
		fmt.Fprintf(w, "last error\t%s\n", job.GetLastError())
	}
	fmt.Fprintf(w, "payload\t%s\n", job.GetPayload())
	_ = w.Flush()
}

func printAttempts(attempts []*jobqueuev1.Attempt) {
	if len(attempts) == 0 {
		return
	}
	fmt.Println("\nattempts:")

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tWORKER\tSTARTED\tDURATION\tERROR")
	for _, a := range attempts {
		duration := "running"
		if a.GetFinishedAt() != nil {
			duration = a.GetFinishedAt().AsTime().Sub(a.GetStartedAt().AsTime()).Round(time.Millisecond).String()
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\t%s\n",
			a.GetAttemptNo(), a.GetWorkerId(),
			a.GetStartedAt().AsTime().Local().Format("15:04:05"),
			duration, truncate(a.GetError(), 60))
	}
	_ = w.Flush()
}

func printJobTable(jobs []*jobqueuev1.Job) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ID\tKIND\tSTATE\tATTEMPTS\tRUN AT\tLAST ERROR")
	for _, j := range jobs {
		fmt.Fprintf(w, "%s\t%s\t%s\t%d/%d\t%s\t%s\n",
			j.GetId(), j.GetKind(), stateName(j.GetState()),
			j.GetAttempts(), j.GetMaxAttempts(),
			j.GetRunAt().AsTime().Local().Format("15:04:05"),
			truncate(j.GetLastError(), 40))
	}
	_ = w.Flush()
}

func printStats(s *jobqueuev1.QueueStatsResponse) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintf(w, "queue\t%s\n", s.GetQueue())

	states := make([]string, 0, len(s.GetCountsByState()))
	for state := range s.GetCountsByState() {
		states = append(states, state)
	}
	sort.Strings(states)
	for _, state := range states {
		fmt.Fprintf(w, "%s\t%d\n", state, s.GetCountsByState()[state])
	}

	fmt.Fprintf(w, "scheduled in index\t%d\n", s.GetScheduledIndexSize())
	fmt.Fprintf(w, "ready for workers\t%d\n", s.GetReadyDepth())
	fmt.Fprintf(w, "oldest due lag\t%s\n", s.GetOldestDueLag().AsDuration().Round(time.Millisecond))
	_ = w.Flush()
}

func printJSON(v any) error {
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(v)
}

func parseStates(raw string) ([]jobqueuev1.JobState, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}

	names := map[string]jobqueuev1.JobState{
		"pending":   jobqueuev1.JobState_JOB_STATE_PENDING,
		"running":   jobqueuev1.JobState_JOB_STATE_RUNNING,
		"succeeded": jobqueuev1.JobState_JOB_STATE_SUCCEEDED,
		"failed":    jobqueuev1.JobState_JOB_STATE_FAILED,
		"dead":      jobqueuev1.JobState_JOB_STATE_DEAD,
		"cancelled": jobqueuev1.JobState_JOB_STATE_CANCELLED,
	}

	var out []jobqueuev1.JobState
	for _, part := range strings.Split(raw, ",") {
		part = strings.ToLower(strings.TrimSpace(part))
		if part == "" {
			continue
		}
		state, ok := names[part]
		if !ok {
			return nil, fmt.Errorf("unknown state %q", part)
		}
		out = append(out, state)
	}
	return out, nil
}

func stateName(s jobqueuev1.JobState) string {
	return strings.ToLower(strings.TrimPrefix(s.String(), "JOB_STATE_"))
}

func isTerminal(s jobqueuev1.JobState) bool {
	switch s {
	case jobqueuev1.JobState_JOB_STATE_SUCCEEDED,
		jobqueuev1.JobState_JOB_STATE_DEAD,
		jobqueuev1.JobState_JOB_STATE_CANCELLED:
		return true
	default:
		return false
	}
}

func truncate(s string, n int) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
