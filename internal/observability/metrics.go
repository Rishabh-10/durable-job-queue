package observability

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds the job queue's instruments.
//
// The set is chosen to answer the questions an operator actually has: is work
// getting through, is it getting through on time, and are the safety nets
// having to do anything.
type Metrics struct {
	registry *prometheus.Registry

	// Enqueued counts accepted jobs, split by whether an idempotency key
	// collapsed the request onto an existing job.
	Enqueued *prometheus.CounterVec

	// IndexWriteFailures counts enqueues that committed to PostgreSQL but could
	// not reach Redis. These are recovered by the sweeper rather than lost, but
	// a non-zero rate means Redis is unhealthy.
	IndexWriteFailures *prometheus.CounterVec

	// Dispatched counts job ids moved from the sorted set to the ready list.
	Dispatched *prometheus.CounterVec

	// Executed counts finished executions by outcome.
	Executed *prometheus.CounterVec

	// Duration measures handler runtime.
	Duration *prometheus.HistogramVec

	// DispatchLag measures how late a job started relative to its run_at. This
	// is the number that says whether the queue is keeping its promise.
	DispatchLag *prometheus.HistogramVec

	// SweeperBackfilled counts index entries the sweeper had to write.
	//
	// Worth watching closely: in a healthy system enqueue writes the index
	// directly and this stays near zero. A sustained non-zero rate means
	// enqueues are not reaching Redis and the durability net is load-bearing.
	SweeperBackfilled *prometheus.CounterVec

	// ReaperReclaimed counts jobs recovered from workers that stopped
	// heartbeating, split by whether they were retried or dead-lettered.
	ReaperReclaimed *prometheus.CounterVec
}

// NewMetrics registers the instruments on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)

	m := &Metrics{
		registry: reg,
		Enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_enqueued_total",
			Help: "Jobs accepted by the API, by queue and whether an idempotency key matched an existing job.",
		}, []string{"queue", "deduplicated"}),
		IndexWriteFailures: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_index_write_failures_total",
			Help: "Jobs committed to PostgreSQL that could not be written to the Redis index; the sweeper recovers these.",
		}, []string{"queue"}),
		Dispatched: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_dispatched_total",
			Help: "Job ids moved from the scheduling index to the ready list.",
		}, []string{"queue"}),
		Executed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_executed_total",
			Help: "Finished job executions, by outcome.",
		}, []string{"queue", "kind", "outcome"}),
		Duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "jobqueue_job_duration_seconds",
			Help:    "Handler runtime.",
			Buckets: prometheus.ExponentialBuckets(0.005, 3, 12),
		}, []string{"queue", "kind"}),
		DispatchLag: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name: "jobqueue_dispatch_lag_seconds",
			Help: "How late a job started relative to the time it was scheduled for.",
			// Spans sub-second scheduling through a backlog hours deep.
			Buckets: prometheus.ExponentialBuckets(0.05, 3, 14),
		}, []string{"queue"}),
		SweeperBackfilled: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_sweeper_backfilled_total",
			Help: "Index entries written by the sweeper because an enqueue did not reach Redis.",
		}, []string{"queue"}),
		ReaperReclaimed: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobqueue_reaper_reclaimed_total",
			Help: "Jobs recovered from workers that stopped heartbeating, by what happened to them.",
		}, []string{"queue", "outcome"}),
	}

	reg.MustRegister(
		m.Enqueued,
		m.IndexWriteFailures,
		m.Dispatched,
		m.Executed,
		m.Duration,
		m.DispatchLag,
		m.SweeperBackfilled,
		m.ReaperReclaimed,
	)
	return m
}

// Handler serves the metrics endpoint.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

// Registry exposes the registry so a binary can add its own collectors.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// ObserveDispatchLag records a lag measurement, clamping negatives to zero.
// A job claimed a shade before its run_at -- clock skew between the API and a
// worker -- would otherwise register as a negative lag and distort the
// histogram.
func (m *Metrics) ObserveDispatchLag(queue string, lag time.Duration) {
	if lag < 0 {
		lag = 0
	}
	m.DispatchLag.WithLabelValues(queue).Observe(lag.Seconds())
}
