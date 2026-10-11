package metrics_test

import (
	"context"
	"testing"
	"time"

	dto "github.com/prometheus/client_model/go"

	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
)

// value finds one sample in a scrape: the metric's value (or a histogram's
// count) for the series whose labels include all of want.
func value(t *testing.T, m *metrics.Metrics, name string, want map[string]string) (float64, bool) {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatalf("scrape: %v", err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
		for _, s := range f.GetMetric() {
			if has(s, want) {
				switch {
				case s.Gauge != nil:
					return s.Gauge.GetValue(), true
				case s.Counter != nil:
					return s.Counter.GetValue(), true
				case s.Histogram != nil:
					return float64(s.Histogram.GetSampleCount()), true
				}
			}
		}
	}
	return 0, false
}

func has(s *dto.Metric, want map[string]string) bool {
	got := map[string]string{}
	for _, l := range s.GetLabel() {
		got[l.GetName()] = l.GetValue()
	}
	for k, v := range want {
		if got[k] != v {
			return false
		}
	}
	return true
}

func TestQueueStateIsReadAtScrape(t *testing.T) {
	t.Parallel()
	store := &queue.Store{Pool: testdb.New(t)}
	m := metrics.New(store)

	// Empty: every status at 0, and nothing is late.
	for _, s := range []string{queue.Ready, queue.Running, queue.Done, queue.Dead} {
		if v, ok := value(t, m, "jobq_queue_depth", map[string]string{"status": s}); !ok || v != 0 {
			t.Fatalf("empty queue: depth{%s} = %v (present %v), want 0", s, v, ok)
		}
	}
	if v, _ := value(t, m, "jobq_oldest_ready_age_seconds", nil); v != 0 {
		t.Fatalf("empty queue: oldest ready age %v, want 0", v)
	}

	_, err := store.Pool.Exec(context.Background(), `
		insert into jobq.jobs (kind, idempotency_key, status, run_at) values
		  ('noop', 'due 30s ago',     'ready',   now() - interval '30 seconds'),
		  ('noop', 'retry in a min',  'ready',   now() + interval '1 minute'),
		  ('noop', 'running',         'running', now()),
		  ('noop', 'done 1',          'done',    now()),
		  ('noop', 'done 2',          'done',    now()),
		  ('noop', 'dead',            'dead',    now())`)
	if err != nil {
		t.Fatal(err)
	}

	for status, want := range map[string]float64{queue.Ready: 2, queue.Running: 1, queue.Done: 2, queue.Dead: 1} {
		if v, _ := value(t, m, "jobq_queue_depth", map[string]string{"status": status}); v != want {
			t.Errorf("depth{%s} = %v, want %v", status, v, want)
		}
	}
	// Only the due job counts: one waiting out a retry delay isn't late.
	if v, _ := value(t, m, "jobq_oldest_ready_age_seconds", nil); v < 29 || v > 60 {
		t.Errorf("oldest ready age %v, want about 30", v)
	}
}

func TestNilMetricsAreANoOp(t *testing.T) {
	var m *metrics.Metrics
	m.Enqueued("noop")
	m.Claimed(3, time.Millisecond)
	m.Ran("noop", time.Second)
	m.Finished(queue.Job{Kind: "noop"}, metrics.ResultDone)
	m.LeaseLost()
}
