package worker_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/handlers"
	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
	"github.com/KunalShukla-Al/jobq/internal/worker"
)

var quiet = slog.New(slog.NewTextHandler(io.Discard, nil))

// fast is a pool config for tests: short polls, millisecond backoff.
func fast(concurrency int) worker.Config {
	return worker.Config{
		Concurrency: concurrency,
		Lease:       5 * time.Second,
		PollMin:     5 * time.Millisecond,
		PollMax:     20 * time.Millisecond,
		Backoff:     queue.Backoff{Base: 5 * time.Millisecond, Max: 20 * time.Millisecond},
		Logger:      quiet,
	}
}

func setup(t *testing.T) *queue.Store {
	t.Parallel()
	return &queue.Store{Pool: testdb.New(t)}
}

// start runs the pool until the test ends (or stop is called), and returns stop.
func start(t *testing.T, p *worker.Pool) (stop func()) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { p.Run(ctx); close(done) }()
	var once sync.Once
	stop = func() { once.Do(func() { cancel(); <-done }) }
	t.Cleanup(stop)
	return stop
}

func noop(t *testing.T, s *queue.Store, key string, p handlers.NoopPayload, maxAttempts int) int64 {
	t.Helper()
	body, _ := json.Marshal(p)
	id, _, err := s.Enqueue(context.Background(), queue.NewJob{Kind: "noop", Payload: body, IdempotencyKey: key, MaxAttempts: maxAttempts})
	if err != nil {
		t.Fatal(err)
	}
	return id
}

// waitFor polls a job until it reaches a final state.
func waitFor(t *testing.T, s *queue.Store, id int64) queue.Job {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		j, err := s.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if j.Status == queue.Done || j.Status == queue.Dead {
			return j
		}
		time.Sleep(10 * time.Millisecond)
	}
	j, _ := s.Get(context.Background(), id)
	t.Fatalf("job %d never finished: %+v", id, j)
	return j
}

func TestOutcomes(t *testing.T) {
	s := setup(t)
	start(t, worker.New(s, handlers.All(), fast(4)))

	for _, c := range []struct {
		name         string
		payload      handlers.NoopPayload
		kind         string
		maxAttempts  int
		wantStatus   string
		wantAttempts int
	}{
		{"succeeds first time", handlers.NoopPayload{}, "noop", 0, queue.Done, 1},
		{"fails twice, then succeeds", handlers.NoopPayload{FailTimes: 2}, "noop", 0, queue.Done, 3},
		{"permanent failure goes straight to dead", handlers.NoopPayload{FailTimes: 1, Permanent: true}, "noop", 0, queue.Dead, 1},
		{"keeps failing: dead after max attempts", handlers.NoopPayload{FailTimes: 99}, "noop", 3, queue.Dead, 3},
		{"a panic is retried like an error", handlers.NoopPayload{Panic: true}, "noop", 2, queue.Dead, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			body, _ := json.Marshal(c.payload)
			id, _, err := s.Enqueue(context.Background(), queue.NewJob{Kind: c.kind, Payload: body, IdempotencyKey: c.name, MaxAttempts: c.maxAttempts})
			if err != nil {
				t.Fatal(err)
			}
			j := waitFor(t, s, id)
			if j.Status != c.wantStatus || j.Attempts != c.wantAttempts {
				t.Fatalf("status=%s attempts=%d last_error=%v, want %s after %d", j.Status, j.Attempts, deref(j.LastError), c.wantStatus, c.wantAttempts)
			}
		})
	}
}

// A kind the pool has no handler for isn't claimed: it waits for a process
// that can run it (say, one with push keys) instead of going to dead.
func TestAKindWithoutAHandlerWaits(t *testing.T) {
	s := setup(t)
	start(t, worker.New(s, handlers.All(), fast(2)))
	other, _, err := s.Enqueue(context.Background(), queue.NewJob{Kind: "webpush", IdempotencyKey: "push"})
	if err != nil {
		t.Fatal(err)
	}
	waitFor(t, s, noop(t, s, "after", handlers.NoopPayload{}, 0))
	time.Sleep(50 * time.Millisecond) // a few more polls
	if j, _ := s.Get(context.Background(), other); j.Status != queue.Ready || j.Attempts != 0 {
		t.Fatalf("job without a handler was touched: status=%s attempts=%d", j.Status, j.Attempts)
	}
}

// 500 jobs, 8 workers: each job runs once and only once when nothing crashes.
func TestEveryJobRunsExactlyOnceUnderConcurrency(t *testing.T) {
	s := setup(t)
	var mu sync.Mutex
	runs := map[int64]int{}
	var inFlight, peak atomic.Int64
	h := worker.HandlerFunc(func(ctx context.Context, j queue.Job) error {
		n := inFlight.Add(1)
		for p := peak.Load(); n > p && !peak.CompareAndSwap(p, n); p = peak.Load() {
		}
		time.Sleep(2 * time.Millisecond)
		inFlight.Add(-1)
		mu.Lock()
		runs[j.ID]++
		mu.Unlock()
		return nil
	})
	const total = 500
	for i := range total {
		if _, _, err := s.Enqueue(context.Background(), queue.NewJob{Kind: "count", IdempotencyKey: fmt.Sprint(i)}); err != nil {
			t.Fatal(err)
		}
	}
	start(t, worker.New(s, map[string]worker.Handler{"count": h}, fast(8)))

	deadline := time.Now().Add(20 * time.Second)
	for {
		var done int
		if err := s.Pool.QueryRow(context.Background(), `select count(*) from jobq.jobs where status = 'done'`).Scan(&done); err != nil {
			t.Fatal(err)
		}
		if done == total {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("only %d of %d done", done, total)
		}
		time.Sleep(20 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(runs) != total {
		t.Fatalf("%d jobs ran, want %d", len(runs), total)
	}
	for id, n := range runs {
		if n != 1 {
			t.Fatalf("job %d ran %d times", id, n)
		}
	}
	if p := peak.Load(); p > 8 || p < 2 {
		t.Fatalf("peak concurrency %d, want 2..8", p)
	}
}

// A worker claims a job and dies. Once its lease runs out, another finishes it.
func TestAJobFromACrashedWorkerIsFinishedByAnother(t *testing.T) {
	s := setup(t)
	id := noop(t, s, "orphan", handlers.NoopPayload{}, 0)
	if jobs, err := s.Claim(context.Background(), 1, 100*time.Millisecond, []string{"noop"}); err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	} // ...and never reports back.

	start(t, worker.New(s, handlers.All(), fast(2)))
	j := waitFor(t, s, id)
	if j.Status != queue.Done || j.Attempts != 2 {
		t.Fatalf("status=%s attempts=%d, want done after 2 (the crashed one counts)", j.Status, j.Attempts)
	}
}

// Stopping the pool lets a job that's already running finish.
func TestShutdownFinishesRunningJobs(t *testing.T) {
	s := setup(t)
	id := noop(t, s, "slow", handlers.NoopPayload{SleepMS: 300}, 0)
	stop := start(t, worker.New(s, handlers.All(), fast(1)))

	deadline := time.Now().Add(5 * time.Second)
	for {
		j, _ := s.Get(context.Background(), id)
		if j.Status == queue.Running {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("job never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	stop() // returns only when the pool has stopped
	if j, _ := s.Get(context.Background(), id); j.Status != queue.Done {
		t.Fatalf("after shutdown: %s, want done", j.Status)
	}
}

// After shutdown, nothing new is claimed.
func TestNoClaimsAfterShutdown(t *testing.T) {
	s := setup(t)
	stop := start(t, worker.New(s, handlers.All(), fast(2)))
	stop()
	id := noop(t, s, "late", handlers.NoopPayload{}, 0)
	time.Sleep(100 * time.Millisecond)
	if j, _ := s.Get(context.Background(), id); j.Status != queue.Ready || j.Attempts != 0 {
		t.Fatalf("job touched after shutdown: %+v", j)
	}
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func TestMetricsCountEveryOutcome(t *testing.T) {
	s := setup(t)
	m := metrics.New(s)
	cfg := fast(4)
	cfg.Metrics = m
	start(t, worker.New(s, handlers.All(), cfg))

	ids := []int64{
		noop(t, s, "ok 1", handlers.NoopPayload{}, 0),
		noop(t, s, "ok 2", handlers.NoopPayload{}, 0),
		noop(t, s, "fails once", handlers.NoopPayload{FailTimes: 1}, 0),
		noop(t, s, "permanent", handlers.NoopPayload{FailTimes: 1, Permanent: true}, 0),
	}
	for _, id := range ids {
		waitFor(t, s, id)
	}

	// 3 done, 1 retry (before "fails once" succeeded), 1 dead: 5 attempts in all.
	for _, c := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"jobq_finished_total", map[string]string{"kind": "noop", "result": "done"}, 3},
		{"jobq_finished_total", map[string]string{"kind": "noop", "result": "retry"}, 1},
		{"jobq_finished_total", map[string]string{"kind": "noop", "result": "dead"}, 1},
		{"jobq_job_duration_seconds", map[string]string{"kind": "noop"}, 5},
		{"jobq_job_latency_seconds", map[string]string{"kind": "noop"}, 3},
	} {
		if got := sample(t, m, c.name, c.labels); got != c.want {
			t.Errorf("%s%v = %v, want %v", c.name, c.labels, got, c.want)
		}
	}
	if got := sample(t, m, "jobq_claim_batch_size", nil); got < 1 {
		t.Errorf("no claims recorded")
	}
	if got := sample(t, m, "jobq_lease_lost_total", nil); got != 0 {
		t.Errorf("lease lost %v, want 0", got)
	}
}

// sample reads one series from a scrape: a counter's value, or a histogram's count.
func sample(t *testing.T, m *metrics.Metrics, name string, labels map[string]string) float64 {
	t.Helper()
	families, err := m.Registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, f := range families {
		if f.GetName() != name {
			continue
		}
	series:
		for _, s := range f.GetMetric() {
			for _, l := range s.GetLabel() {
				if want, ok := labels[l.GetName()]; ok && want != l.GetValue() {
					continue series
				}
			}
			if s.Histogram != nil {
				return float64(s.Histogram.GetSampleCount())
			}
			return s.Counter.GetValue()
		}
	}
	return 0
}

func TestNotifyWakesAnIdleWorker(t *testing.T) {
	s := setup(t)
	s.Notify = true
	lctx, cancel := context.WithCancel(context.Background())
	wake := make(chan struct{}, 1)
	listened := make(chan struct{})
	go func() { s.Listen(lctx, wake, nil); close(listened) }()
	t.Cleanup(func() { cancel(); <-listened })
	select { // Listen signals once when it's on LISTEN: take that, so the next wake can only be the NOTIFY
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("Listen never started")
	}

	cfg := fast(2)
	cfg.PollMin, cfg.PollMax = 10*time.Second, 10*time.Second // polling alone would take 10s
	cfg.Wake = wake
	start(t, worker.New(s, handlers.All(), cfg))
	time.Sleep(200 * time.Millisecond) // let the pool go idle

	began := time.Now()
	waitFor(t, s, noop(t, s, "wake me", handlers.NoopPayload{}, 0))
	if took := time.Since(began); took > 2*time.Second {
		t.Fatalf("job took %v: the worker waited for its poll instead of waking", took)
	}
}

// drain runs a pool with ExitWhenIdle and returns how long it took to return.
func drain(t *testing.T, s *queue.Store, cfg worker.Config, max time.Duration) time.Duration {
	t.Helper()
	cfg.ExitWhenIdle = true
	ctx, cancel := context.WithTimeout(context.Background(), max)
	defer cancel()
	began := time.Now()
	done := make(chan struct{})
	go func() { worker.New(s, handlers.All(), cfg).Run(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(max + 10*time.Second):
		t.Fatal("drain never returned")
	}
	return time.Since(began)
}

func countByStatus(t *testing.T, s *queue.Store) map[string]int {
	t.Helper()
	rows, err := s.Pool.Query(context.Background(), `select status, count(*) from jobq.jobs group by status`)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int{}
	for rows.Next() {
		var status string
		var n int
		if err := rows.Scan(&status, &n); err != nil {
			t.Fatal(err)
		}
		out[status] = n
	}
	return out
}

func TestDrainRunsEverythingDueThenReturns(t *testing.T) {
	s := setup(t)
	for i := range 40 {
		noop(t, s, fmt.Sprint(i), handlers.NoopPayload{SleepMS: 5}, 0)
	}
	if took := drain(t, s, fast(4), 30*time.Second); took > 10*time.Second {
		t.Fatalf("drain took %v for 40 short jobs", took)
	}
	if got := countByStatus(t, s); got[queue.Done] != 40 || len(got) != 1 {
		t.Fatalf("after drain: %v, want all 40 done", got)
	}
}

// A retry scheduled for later belongs to the next run: drain doesn't wait for it.
func TestDrainLeavesALaterRetryForTheNextRun(t *testing.T) {
	s := setup(t)
	ok := noop(t, s, "ok", handlers.NoopPayload{}, 0)
	flaky := noop(t, s, "flaky", handlers.NoopPayload{FailTimes: 1}, 0)
	future, _, err := s.Enqueue(context.Background(), queue.NewJob{Kind: "noop", IdempotencyKey: "future", RunAt: time.Now().Add(time.Hour)})
	if err != nil {
		t.Fatal(err)
	}
	cfg := fast(2)
	cfg.Backoff = queue.Backoff{Base: time.Hour, Max: time.Hour}
	if took := drain(t, s, cfg, 30*time.Second); took > 10*time.Second {
		t.Fatalf("drain took %v: it waited for something not due", took)
	}
	for id, want := range map[int64]struct {
		status   string
		attempts int
	}{ok: {queue.Done, 1}, flaky: {queue.Ready, 1}, future: {queue.Ready, 0}} {
		if j, _ := s.Get(context.Background(), id); j.Status != want.status || j.Attempts != want.attempts {
			t.Errorf("job %d: status=%s attempts=%d, want %s after %d", id, j.Status, j.Attempts, want.status, want.attempts)
		}
	}
}

// More work than fits in the time allowed: drain stops claiming at the
// deadline, lets the running job finish, and leaves the rest ready.
func TestDrainStopsAtItsDeadline(t *testing.T) {
	s := setup(t)
	for i := range 30 {
		noop(t, s, fmt.Sprint(i), handlers.NoopPayload{SleepMS: 100}, 0)
	}
	took := drain(t, s, fast(1), 300*time.Millisecond)
	if took > 2*time.Second {
		t.Fatalf("drain took %v with a 300ms limit", took)
	}
	got := countByStatus(t, s)
	if got[queue.Running] != 0 || got[queue.Done] == 0 || got[queue.Done] > 10 || got[queue.Done]+got[queue.Ready] != 30 {
		t.Fatalf("after drain: %v, want a few done, the rest ready, none running", got)
	}
}

// A claim that fails isn't "nothing due": drain keeps trying until its
// deadline instead of leaving due jobs behind after a database blip.
func TestDrainDoesntTakeAFailedClaimForIdle(t *testing.T) {
	s := setup(t)
	noop(t, s, "due", handlers.NoopPayload{}, 0)
	s.Pool.Close() // every claim now fails
	if took := drain(t, s, fast(1), 300*time.Millisecond); took < 250*time.Millisecond {
		t.Fatalf("drain returned after %v: it took a failed claim for an empty queue", took)
	}
}
