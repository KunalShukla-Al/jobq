// Package metrics is what jobq tells Prometheus: jobs in and out, how long
// they take, and how far behind the queue is.
//
// Counters and histograms are updated as things happen. Queue depth and the
// oldest due job are read from Postgres at scrape time, so they're never
// stale and cost nothing between scrapes.
//
// Every method is safe on a nil *Metrics, so code and tests that don't care
// about metrics can leave them out.
package metrics

import (
	"context"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// Results of a finished attempt, the "result" label of jobq_finished_total.
const (
	ResultDone  = "done"
	ResultRetry = "retry"
	ResultDead  = "dead"
)

// Metrics holds jobq's collectors, on a registry of its own.
type Metrics struct {
	Registry *prometheus.Registry

	enqueued   *prometheus.CounterVec
	finished   *prometheus.CounterVec
	duration   *prometheus.HistogramVec
	latency    *prometheus.HistogramVec
	claimBatch prometheus.Histogram
	claimTime  prometheus.Histogram
	leaseLost  prometheus.Counter
}

// New registers jobq's metrics, plus Go runtime and process metrics. The
// store is read at scrape time for queue depth and lag.
func New(store *queue.Store) *Metrics {
	m := &Metrics{
		Registry: prometheus.NewRegistry(),
		enqueued: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobq_enqueued_total",
			Help: "Jobs created. A repeat of an existing idempotency key doesn't count.",
		}, []string{"kind"}),
		finished: prometheus.NewCounterVec(prometheus.CounterOpts{
			Name: "jobq_finished_total",
			Help: "Attempts whose outcome was recorded, by result: done, retry (will run again) or dead.",
		}, []string{"kind", "result"}),
		duration: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "jobq_job_duration_seconds",
			Help:    "Time a handler ran for one attempt.",
			Buckets: prometheus.ExponentialBuckets(0.005, 2, 14), // 5ms to ~41s
		}, []string{"kind"}),
		latency: prometheus.NewHistogramVec(prometheus.HistogramOpts{
			Name:    "jobq_job_latency_seconds",
			Help:    "Time from enqueue to done, retries included.",
			Buckets: prometheus.ExponentialBuckets(0.01, 2, 18), // 10ms to ~22min
		}, []string{"kind"}),
		claimBatch: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "jobq_claim_batch_size",
			Help:    "Jobs taken by one claim that found work.",
			Buckets: []float64{1, 2, 4, 8, 16, 32, 64},
		}),
		claimTime: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "jobq_claim_duration_seconds",
			Help:    "Time a claim took, empty claims included: waiting for a pool connection, then the query.",
			Buckets: prometheus.ExponentialBuckets(0.0005, 2, 14), // 0.5ms to ~4s
		}),
		leaseLost: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "jobq_lease_lost_total",
			Help: "Attempts whose outcome couldn't be recorded because the lease ran out and the job was claimed again.",
		}),
	}
	m.Registry.MustRegister(m.enqueued, m.finished, m.duration, m.latency, m.claimBatch, m.claimTime, m.leaseLost,
		&queueCollector{store: store},
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	return m
}

// Handler serves the registry in Prometheus' text format.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.Registry, promhttp.HandlerOpts{Registry: m.Registry})
}

// Enqueued counts a new job.
func (m *Metrics) Enqueued(kind string) {
	if m != nil {
		m.enqueued.WithLabelValues(kind).Inc()
	}
}

// Claimed records how long a claim took, and its size if it found work.
func (m *Metrics) Claimed(n int, took time.Duration) {
	if m == nil {
		return
	}
	m.claimTime.Observe(took.Seconds())
	if n > 0 {
		m.claimBatch.Observe(float64(n))
	}
}

// Ran records how long a handler took for one attempt.
func (m *Metrics) Ran(kind string, d time.Duration) {
	if m != nil {
		m.duration.WithLabelValues(kind).Observe(d.Seconds())
	}
}

// Finished records the outcome of an attempt. For a done job, latency is
// the time since it was enqueued.
func (m *Metrics) Finished(j queue.Job, result string) {
	if m == nil {
		return
	}
	m.finished.WithLabelValues(j.Kind, result).Inc()
	if result == ResultDone {
		m.latency.WithLabelValues(j.Kind).Observe(time.Since(j.CreatedAt).Seconds())
	}
}

// LeaseLost counts an outcome that arrived after another claim took the job.
func (m *Metrics) LeaseLost() {
	if m != nil {
		m.leaseLost.Inc()
	}
}

// queueCollector reads the queue's state from Postgres on each scrape.
type queueCollector struct {
	store *queue.Store
}

var (
	depthDesc = prometheus.NewDesc("jobq_queue_depth",
		"Jobs in the table, by status.", []string{"status"}, nil)
	oldestDesc = prometheus.NewDesc("jobq_oldest_ready_age_seconds",
		"How long the oldest due job has been waiting to be claimed (0 when none is due): the falling-behind number.", nil, nil)
)

func (c *queueCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- depthDesc
	ch <- oldestDesc
}

func (c *queueCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Every status is reported, at 0 when empty, so graphs don't have gaps.
	// Counting done jobs scans them all; fine at load-test sizes, and the
	// thing to change (a cleanup job, or an estimate) if the table grows large.
	depth := map[string]float64{queue.Ready: 0, queue.Running: 0, queue.Done: 0, queue.Dead: 0}
	rows, err := c.store.Pool.Query(ctx, `select status, count(*) from jobq.jobs group by status`)
	if err == nil {
		for rows.Next() {
			var status string
			var n int64
			if err = rows.Scan(&status, &n); err != nil {
				break
			}
			depth[status] = float64(n)
		}
		rows.Close()
		if err == nil {
			err = rows.Err()
		}
	}
	if err != nil {
		ch <- prometheus.NewInvalidMetric(depthDesc, err)
	} else {
		for status, n := range depth {
			ch <- prometheus.MustNewConstMetric(depthDesc, prometheus.GaugeValue, n, status)
		}
	}

	// Due = ready and run_at has passed. A job waiting out a retry delay
	// isn't late, so it doesn't count.
	var age float64
	err = c.store.Pool.QueryRow(ctx, `
		select coalesce(extract(epoch from now() - min(run_at)), 0)::float8
		from jobq.jobs where status = 'ready' and run_at <= now()`).Scan(&age)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(oldestDesc, err)
		return
	}
	ch <- prometheus.MustNewConstMetric(oldestDesc, prometheus.GaugeValue, age)
}
