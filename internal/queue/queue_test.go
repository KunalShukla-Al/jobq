package queue_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
)

var ctx = context.Background()

// noopOnly is the kinds to claim in tests that only enqueue noop jobs.
var noopOnly = []string{"noop"}

func newStore(t *testing.T) *queue.Store {
	t.Parallel()
	return &queue.Store{Pool: testdb.New(t)}
}

func enqueue(t *testing.T, s *queue.Store, n queue.NewJob) int64 {
	t.Helper()
	id, created, err := s.Enqueue(ctx, n)
	if err != nil || !created {
		t.Fatalf("enqueue %+v: id=%d created=%v err=%v", n, id, created, err)
	}
	return id
}

func TestEnqueueIsIdempotent(t *testing.T) {
	s := newStore(t)
	job := queue.NewJob{Kind: "noop", Payload: json.RawMessage(`{"a": 1, "b": 2}`), IdempotencyKey: "k1"}
	first := enqueue(t, s, job)

	// Same key, same job (keys in another order): the first one back, nothing new.
	job.Payload = json.RawMessage(`{"b":2,"a":1}`)
	id, created, err := s.Enqueue(ctx, job)
	if err != nil || created || id != first {
		t.Fatalf("repeat: id=%d created=%v err=%v, want %d false nil", id, created, err, first)
	}

	// Same key, different job: a producer bug, reported rather than hidden.
	job.Payload = json.RawMessage(`{"a": 2}`)
	if _, _, err := s.Enqueue(ctx, job); !errors.Is(err, queue.ErrKeyConflict) {
		t.Fatalf("different payload: err=%v, want ErrKeyConflict", err)
	}
	if _, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "other", IdempotencyKey: "k1"}); !errors.Is(err, queue.ErrKeyConflict) {
		t.Fatalf("different kind: err=%v, want ErrKeyConflict", err)
	}
}

func TestConcurrentEnqueuesOfOneKeyMakeOneJob(t *testing.T) {
	s := newStore(t)
	var wg sync.WaitGroup
	ids := make([]int64, 20)
	errs := make([]error, 20)
	created := make([]bool, 20)
	for i := range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids[i], created[i], errs[i] = s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "same"})
		}()
	}
	wg.Wait()
	news := 0
	for i := range 20 {
		if errs[i] != nil {
			t.Fatalf("enqueue %d: %v", i, errs[i])
		}
		if ids[i] != ids[0] {
			t.Fatalf("enqueue %d got id %d, want %d", i, ids[i], ids[0])
		}
		if created[i] {
			news++
		}
	}
	if news != 1 {
		t.Fatalf("%d enqueues reported created, want exactly 1", news)
	}
}

func TestClaimOnlyTakesDueJobsInOrder(t *testing.T) {
	s := newStore(t)
	later := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "later", RunAt: time.Now().Add(time.Hour)})
	a := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "a", RunAt: time.Now().Add(-2 * time.Second)})
	b := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "b", RunAt: time.Now().Add(-time.Second)})

	jobs, err := s.Claim(ctx, 10, time.Minute, noopOnly)
	if err != nil {
		t.Fatal(err)
	}
	if len(jobs) != 2 || jobs[0].ID != a || jobs[1].ID != b {
		t.Fatalf("claimed %v, want [%d %d] (not %d, which isn't due)", ids(jobs), a, b, later)
	}
	for _, j := range jobs {
		if j.Status != queue.Running || j.Attempts != 1 || j.LockToken == nil || j.LockedUntil == nil {
			t.Fatalf("claimed job not leased: %+v", j)
		}
	}
	if more, _ := s.Claim(ctx, 10, time.Minute, noopOnly); len(more) != 0 {
		t.Fatalf("leased jobs were claimed again: %v", ids(more))
	}
}

// A process claims only the kinds it can run; the rest wait for one that can.
func TestClaimTakesOnlyTheGivenKinds(t *testing.T) {
	s := newStore(t)
	push := enqueue(t, s, queue.NewJob{Kind: "webpush", IdempotencyKey: "push"})
	n := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "noop"})

	jobs, err := s.Claim(ctx, 10, time.Minute, noopOnly)
	if err != nil || len(jobs) != 1 || jobs[0].ID != n {
		t.Fatalf("claimed %v (err %v), want only the noop job %d", ids(jobs), err, n)
	}
	if none, err := s.Claim(ctx, 10, time.Minute, nil); err != nil || len(none) != 0 {
		t.Fatalf("no kinds claimed %v (err %v), want nothing", ids(none), err)
	}
	if j, _ := s.Get(ctx, push); j.Status != queue.Ready || j.Attempts != 0 {
		t.Fatalf("the webpush job was touched: %+v", j)
	}
	jobs, err = s.Claim(ctx, 10, time.Minute, []string{"noop", "webpush"})
	if err != nil || len(jobs) != 1 || jobs[0].ID != push {
		t.Fatalf("claimed %v (err %v), want the webpush job %d", ids(jobs), err, push)
	}
}

// The property the whole queue rests on: workers claiming at once never get the same job.
func TestConcurrentClaimsNeverShareAJob(t *testing.T) {
	s := newStore(t)
	const total = 300
	for i := range total {
		enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprint(i)})
	}
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				jobs, err := s.Claim(ctx, 7, time.Minute, noopOnly)
				if err != nil {
					t.Error(err)
					return
				}
				if len(jobs) == 0 {
					return
				}
				mu.Lock()
				for _, j := range jobs {
					seen[j.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if len(seen) != total {
		t.Fatalf("claimed %d distinct jobs, want %d", len(seen), total)
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("job %d claimed %d times", id, n)
		}
	}
}

func TestAnExpiredLeaseIsReclaimedAndTheOldClaimCantFinish(t *testing.T) {
	s := newStore(t)
	enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "k"})
	first, _ := s.Claim(ctx, 1, 50*time.Millisecond, noopOnly) // this worker "dies"
	time.Sleep(100 * time.Millisecond)

	second, err := s.Claim(ctx, 1, time.Minute, noopOnly)
	if err != nil || len(second) != 1 || second[0].ID != first[0].ID {
		t.Fatalf("expired lease not reclaimed: %v %v", ids(second), err)
	}
	if second[0].Attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (the dead worker's claim counts)", second[0].Attempts)
	}
	// The first worker comes back and tries to record an outcome: refused.
	if err := s.Complete(ctx, first[0]); !errors.Is(err, queue.ErrLostLease) {
		t.Fatalf("stale claim completed the job: err=%v", err)
	}
	if err := s.Complete(ctx, second[0]); err != nil {
		t.Fatalf("current claim: %v", err)
	}
	if j, _ := s.Get(ctx, first[0].ID); j.Status != queue.Done || j.FinishedAt == nil || j.LockToken != nil {
		t.Fatalf("after complete: %+v", j)
	}
}

func TestRetryWaitsThenRunsAgain(t *testing.T) {
	s := newStore(t)
	id := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "k"})
	jobs, _ := s.Claim(ctx, 1, time.Minute, noopOnly)
	if err := s.Retry(ctx, jobs[0], 200*time.Millisecond, errors.New("push service 503")); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Get(ctx, id)
	if j.Status != queue.Ready || j.LastError == nil || *j.LastError != "push service 503" || !j.RunAt.After(time.Now()) {
		t.Fatalf("after retry: %+v", j)
	}
	if early, _ := s.Claim(ctx, 1, time.Minute, noopOnly); len(early) != 0 {
		t.Fatal("claimed before its retry time")
	}
	time.Sleep(250 * time.Millisecond)
	again, _ := s.Claim(ctx, 1, time.Minute, noopOnly)
	if len(again) != 1 || again[0].Attempts != 2 {
		t.Fatalf("retry not claimable when due: %+v", again)
	}
}

func TestKillLeavesTheJobDeadWithItsError(t *testing.T) {
	s := newStore(t)
	id := enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "k"})
	jobs, _ := s.Claim(ctx, 1, time.Minute, noopOnly)
	if err := s.Kill(ctx, jobs[0], errors.New("subscription gone (410)")); err != nil {
		t.Fatal(err)
	}
	j, _ := s.Get(ctx, id)
	if j.Status != queue.Dead || *j.LastError != "subscription gone (410)" || j.FinishedAt == nil {
		t.Fatalf("after kill: %+v", j)
	}
	if more, _ := s.Claim(ctx, 1, time.Minute, noopOnly); len(more) != 0 {
		t.Fatal("a dead job was claimed")
	}
}

func TestDeleteDoneKeepsRecentAndDeadJobs(t *testing.T) {
	s := newStore(t)
	// 25 old done jobs (deleted in batches of 10), and one of each that stays.
	for i := range 25 {
		enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprint("old-", i)})
	}
	for _, key := range []string{"recent", "dead", "ready"} {
		enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: key})
	}
	if _, err := s.Pool.Exec(ctx, `
		update jobq.jobs set status = 'done', finished_at = now() - interval '8 days' where idempotency_key like 'old-%';
		update jobq.jobs set status = 'done', finished_at = now() - interval '1 day' where idempotency_key = 'recent';
		update jobq.jobs set status = 'dead', finished_at = now() - interval '30 days' where idempotency_key = 'dead';`); err != nil {
		t.Fatal(err)
	}

	n, err := s.DeleteDone(ctx, 7*24*time.Hour, 10)
	if err != nil || n != 25 {
		t.Fatalf("deleted %d (err %v), want 25", n, err)
	}
	var left []string
	rows, _ := s.Pool.Query(ctx, `select idempotency_key from jobq.jobs order by idempotency_key`)
	for rows.Next() {
		var k string
		_ = rows.Scan(&k)
		left = append(left, k)
	}
	if strings.Join(left, ",") != "dead,ready,recent" {
		t.Fatalf("left %v, want dead, ready, recent", left)
	}
	// The deleted jobs' keys are free again.
	if _, created, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "old-0"}); err != nil || !created {
		t.Fatalf("re-enqueue a deleted key: created=%v err=%v", created, err)
	}
}

// An error's text is stored whatever bytes it holds: Postgres rejects NUL and
// invalid UTF-8, and the 2000-byte cut can land inside a character.
func TestAnyErrorTextCanBeRecorded(t *testing.T) {
	s := newStore(t)
	for i, msg := range []string{"nul\x00inside", strings.Repeat("a", 1999) + "é and more", "bad \xff byte"} {
		enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprint(i)})
		jobs, err := s.Claim(ctx, 1, time.Minute, noopOnly)
		if err != nil || len(jobs) != 1 {
			t.Fatalf("claim: %v %v", jobs, err)
		}
		if err := s.Kill(ctx, jobs[0], errors.New(msg)); err != nil {
			t.Fatalf("recording %q: %v", msg[:min(len(msg), 20)], err)
		}
	}
}

func TestGetUnknownJob(t *testing.T) {
	s := newStore(t)
	if _, err := s.Get(ctx, 999); !errors.Is(err, queue.ErrNotFound) {
		t.Fatalf("err = %v, want ErrNotFound", err)
	}
}

func TestBackoffDoublesWithinJitterAndStopsAtMax(t *testing.T) {
	t.Parallel()
	b := queue.Backoff{Base: time.Second, Max: 30 * time.Second}
	for attempt, want := range map[int]time.Duration{1: time.Second, 2: 2 * time.Second, 3: 4 * time.Second, 5: 16 * time.Second, 6: 30 * time.Second, 40: 30 * time.Second, 100: 30 * time.Second} {
		for range 200 {
			d := b.Delay(attempt)
			if d < want/2 || d > want {
				t.Fatalf("attempt %d: %v outside [%v, %v]", attempt, d, want/2, want)
			}
		}
	}
}

func ids(jobs []queue.Job) []int64 {
	out := make([]int64, len(jobs))
	for i, j := range jobs {
		out[i] = j.ID
	}
	return out
}

// listening starts Listen and waits until it's on LISTEN (it signals once then).
func listening(t *testing.T, s *queue.Store) <-chan struct{} {
	t.Helper()
	lctx, cancel := context.WithCancel(ctx)
	wake := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() { s.Listen(lctx, wake, func(err error) { t.Errorf("listen: %v", err) }); close(done) }()
	t.Cleanup(func() { cancel(); <-done })
	select {
	case <-wake:
	case <-time.After(5 * time.Second):
		t.Fatal("Listen never started")
	}
	return wake
}

func woken(wake <-chan struct{}, within time.Duration) bool {
	select {
	case <-wake:
		return true
	case <-time.After(within):
		return false
	}
}

func TestEnqueueNotifiesOnlyForANewDueJob(t *testing.T) {
	s := newStore(t)
	s.Notify = true
	wake := listening(t, s)

	enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "a"})
	if !woken(wake, 5*time.Second) {
		t.Fatal("a new due job sent no notification")
	}
	if _, created, _ := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "a"}); created {
		t.Fatal("repeat created a job")
	}
	enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "later", RunAt: time.Now().Add(time.Hour)})
	if woken(wake, 300*time.Millisecond) {
		t.Fatal("a repeat or a job due later sent a notification")
	}
}

func TestNoNotificationsUnlessAskedFor(t *testing.T) {
	s := newStore(t)
	wake := listening(t, s)
	enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "a"})
	if woken(wake, 300*time.Millisecond) {
		t.Fatal("notified with Notify off")
	}
}

func TestCountsSeeEveryState(t *testing.T) {
	s := newStore(t)
	for _, k := range []string{"a", "b", "c", "d", "e"} {
		enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: k})
	}
	enqueue(t, s, queue.NewJob{Kind: "noop", IdempotencyKey: "later", RunAt: time.Now().Add(time.Hour)})
	jobs, _ := s.Claim(ctx, 3, time.Minute, noopOnly)
	if err := s.Complete(ctx, jobs[0]); err != nil {
		t.Fatal(err)
	}
	if err := s.Kill(ctx, jobs[1], errors.New("gone")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond) // so the two still-due jobs have a measurable wait
	c, err := s.Counts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	want := queue.Counts{Due: 2, Later: 1, Running: 1, Done: 1, Dead: 1}
	got := c
	got.OldestDue = 0
	if got != want {
		t.Fatalf("counts = %+v, want %+v", c, want)
	}
	if c.OldestDue <= 0 || c.OldestDue > time.Minute {
		t.Fatalf("oldest due = %v, want a small positive wait", c.OldestDue)
	}
	if err := s.Ping(ctx); err != nil {
		t.Fatal(err)
	}
}
