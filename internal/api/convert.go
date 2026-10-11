package api

import (
	"encoding/base64"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	jobqueuev1 "github.com/Rishabh-10/durable-job-queue/gen/jobqueue/v1"
	"github.com/Rishabh-10/durable-job-queue/internal/store"
)

func toProtoJob(j store.Job) *jobqueuev1.Job {
	return &jobqueuev1.Job{
		Id:             j.ID.String(),
		Queue:          j.Queue,
		Kind:           j.Kind,
		Payload:        j.Payload,
		State:          toProtoState(j.State),
		RunAt:          timestamppb.New(j.RunAt),
		Attempts:       j.Attempts,
		MaxAttempts:    j.MaxAttempts,
		IdempotencyKey: j.IdempotencyKey,
		LastError:      j.LastError,
		LeaseExpiresAt: optionalTimestamp(j.LeaseExpiresAt),
		LeaseOwner:     j.LeaseOwner,
		CreatedAt:      timestamppb.New(j.CreatedAt),
		UpdatedAt:      timestamppb.New(j.UpdatedAt),
	}
}

func toProtoJobs(jobs []store.Job) []*jobqueuev1.Job {
	out := make([]*jobqueuev1.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, toProtoJob(j))
	}
	return out
}

func toProtoAttempts(attempts []store.Attempt) []*jobqueuev1.Attempt {
	out := make([]*jobqueuev1.Attempt, 0, len(attempts))
	for _, a := range attempts {
		out = append(out, &jobqueuev1.Attempt{
			AttemptNo:  a.AttemptNo,
			WorkerId:   a.WorkerID,
			StartedAt:  timestamppb.New(a.StartedAt),
			FinishedAt: optionalTimestamp(a.FinishedAt),
			Error:      a.Error,
		})
	}
	return out
}

// optionalTimestamp leaves a not-yet-happened time unset rather than sending
// the zero instant, which would read as the year 1.
func optionalTimestamp(t time.Time) *timestamppb.Timestamp {
	if t.IsZero() {
		return nil
	}
	return timestamppb.New(t)
}

var stateToProto = map[store.State]jobqueuev1.JobState{
	store.StatePending:   jobqueuev1.JobState_JOB_STATE_PENDING,
	store.StateRunning:   jobqueuev1.JobState_JOB_STATE_RUNNING,
	store.StateSucceeded: jobqueuev1.JobState_JOB_STATE_SUCCEEDED,
	store.StateFailed:    jobqueuev1.JobState_JOB_STATE_FAILED,
	store.StateDead:      jobqueuev1.JobState_JOB_STATE_DEAD,
	store.StateCancelled: jobqueuev1.JobState_JOB_STATE_CANCELLED,
}

var protoToState = func() map[jobqueuev1.JobState]store.State {
	m := make(map[jobqueuev1.JobState]store.State, len(stateToProto))
	for k, v := range stateToProto {
		m[v] = k
	}
	return m
}()

func toProtoState(s store.State) jobqueuev1.JobState {
	if p, ok := stateToProto[s]; ok {
		return p
	}
	return jobqueuev1.JobState_JOB_STATE_UNSPECIFIED
}

func fromProtoStates(in []jobqueuev1.JobState) ([]store.State, error) {
	out := make([]store.State, 0, len(in))
	for _, p := range in {
		if p == jobqueuev1.JobState_JOB_STATE_UNSPECIFIED {
			// Filtering on "unspecified" would match nothing, which is almost
			// certainly not what the caller meant.
			return nil, fmt.Errorf("state filter contains JOB_STATE_UNSPECIFIED")
		}
		s, ok := protoToState[p]
		if !ok {
			return nil, fmt.Errorf("unknown state %s", p)
		}
		out = append(out, s)
	}
	return out, nil
}

// Page tokens are opaque to clients but must survive a round trip exactly, so
// the cursor is encoded as nanoseconds plus the job id rather than a formatted
// timestamp that could lose precision.
func encodePageToken(c store.Cursor) string {
	raw := fmt.Sprintf("%d.%s", c.CreatedAt.UnixNano(), c.ID)
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func decodePageToken(token string) (*store.Cursor, error) {
	if token == "" {
		return nil, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("page token is not valid base64: %w", err)
	}

	nanos, idPart, ok := strings.Cut(string(raw), ".")
	if !ok {
		return nil, fmt.Errorf("page token is malformed")
	}
	n, err := strconv.ParseInt(nanos, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("page token has an invalid position: %w", err)
	}
	id, err := uuid.Parse(idPart)
	if err != nil {
		return nil, fmt.Errorf("page token has an invalid job id: %w", err)
	}
	return &store.Cursor{CreatedAt: time.Unix(0, n), ID: id}, nil
}

// nextPageToken returns a token only when a further page may exist. A page
// shorter than the requested size is the last one, so emitting a token there
// would send the client on a pointless extra round trip.
func nextPageToken(jobs []store.Job, pageSize int32) string {
	if len(jobs) < int(pageSize) {
		return ""
	}
	last := jobs[len(jobs)-1]
	return encodePageToken(store.Cursor{CreatedAt: last.CreatedAt, ID: last.ID})
}

func durationOrZero(d *durationpb.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return d.AsDuration()
}
