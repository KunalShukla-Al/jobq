package logstore

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

var ctx = context.Background()

var noop = []string{"noop"}

// clock is a time the test moves by hand.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Date(2026, 10, 11, 9, 0, 0, 0, time.UTC)} }

func (c *clock) now() time.Time { c.mu.Lock(); defer c.mu.Unlock(); return c.t }

func (c *clock) add(d time.Duration) { c.mu.Lock(); c.t = c.t.Add(d); c.mu.Unlock() }

func open(t *testing.T, dir string, opts Options) *Store {
	t.Helper()
	opts.Dir = dir
	opts.NoFullSync = true
	s, err := Open(opts)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func reopen(t *testing.T, s *Store) *Store {
	t.Helper()
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return open(t, s.opts.Dir, s.opts)
}

func mustEnqueue(t *testing.T, s *Store, key string) int64 {
	t.Helper()
	id, created, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: key,
		Payload: json.RawMessage(fmt.Sprintf(`{"key":%q}`, key))})
	if err != nil || !created {
		t.Fatalf("enqueue %s: created=%v err=%v", key, created, err)
	}
	return id
}

func claimOne(t *testing.T, s *Store) queue.Job {
	t.Helper()
	jobs, err := s.Claim(ctx, 1, time.Hour, noop)
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", ids(jobs), err)
	}
	return jobs[0]
}

func ids(jobs []queue.Job) []int64 {
	var out []int64
	for _, j := range jobs {
		out = append(out, j.ID)
	}
	return out
}

// snapshot is every job in the store, by id.
func snapshot(t *testing.T, s *Store) map[int64]queue.Job {
	t.Helper()
	v, err := s.do(ctx, func(w *writer) (any, error) {
		out := map[int64]queue.Job{}
		for id, j := range w.st.jobs {
			out[id] = *j
		}
		return out, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return v.(map[int64]queue.Job)
}

// churn runs a random mix of every kind of change, the same for a given seed.
func churn(t *testing.T, s *Store, c *clock, seed uint64, n int) {
	t.Helper()
	r := rand.New(rand.NewPCG(seed, seed))
	var held []queue.Job
	for i := range n {
		switch op := r.IntN(10); {
		case op < 4 || len(held) == 0 && op < 8:
			key := fmt.Sprintf("s%d-%d", seed, i)
			if _, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: key,
				Payload: json.RawMessage(fmt.Sprintf(`{"i":%d}`, i))}); err != nil {
				t.Fatal(err)
			}
		case op < 6:
			jobs, err := s.Claim(ctx, 1+r.IntN(3), time.Hour, noop)
			if err != nil {
				t.Fatal(err)
			}
			held = append(held, jobs...)
		case len(held) > 0:
			j := held[0]
			held = held[1:]
			var err error
			switch r.IntN(3) {
			case 0:
				err = s.Complete(ctx, j)
			case 1:
				err = s.Retry(ctx, j, time.Duration(r.IntN(3))*time.Minute, errors.New("try later"))
			case 2:
				err = s.Kill(ctx, j, errors.New("gave up"))
			}
			if err != nil {
				t.Fatal(err)
			}
		default:
			if _, err := s.DeleteDone(ctx, time.Minute, 7); err != nil {
				t.Fatal(err)
			}
		}
		if c != nil {
			c.add(time.Duration(r.IntN(20)) * time.Second)
		}
	}
}

func TestReopenRebuildsEveryJobExactly(t *testing.T) {
	t.Parallel()
	c := newClock()
	s := open(t, t.TempDir(), Options{Now: c.now})
	churn(t, s, c, 1, 400)
	before := snapshot(t, s)

	s = reopen(t, s)
	if after := snapshot(t, s); !reflect.DeepEqual(after, before) {
		t.Fatalf("state after reopen differs: %d jobs vs %d", len(after), len(before))
	}
	rec := s.Recovered()
	if rec.Records == 0 || rec.Jobs != len(before) || rec.Truncated != 0 {
		t.Fatalf("recovery = %+v", rec)
	}
	// And it carries on: new ids follow the old ones.
	id := mustEnqueue(t, s, "after-reopen")
	for old := range before {
		if old >= id {
			t.Fatalf("new id %d isn't after old id %d", id, old)
		}
	}
}

func TestDeleteDoneKeepsRecentAndDeadJobs(t *testing.T) {
	t.Parallel()
	c := newClock()
	s := open(t, t.TempDir(), Options{Now: c.now})
	for i := range 25 {
		mustEnqueue(t, s, fmt.Sprint("old-", i))
	}
	for range 25 {
		if err := s.Complete(ctx, claimOne(t, s)); err != nil {
			t.Fatal(err)
		}
	}
	dead := mustEnqueue(t, s, "dead")
	if err := s.Kill(ctx, claimOne(t, s), errors.New("gone")); err != nil {
		t.Fatal(err)
	}
	c.add(8 * 24 * time.Hour)
	recent := mustEnqueue(t, s, "recent")
	if err := s.Complete(ctx, claimOne(t, s)); err != nil {
		t.Fatal(err)
	}
	ready := mustEnqueue(t, s, "ready")
	c.add(24 * time.Hour)

	n, err := s.DeleteDone(ctx, 7*24*time.Hour, 10)
	if err != nil || n != 25 {
		t.Fatalf("deleted %d (err %v), want 25", n, err)
	}
	s = reopen(t, s) // the deletes are in the log, not just in memory
	left := snapshot(t, s)
	if len(left) != 3 || left[dead].Status != queue.Dead || left[recent].Status != queue.Done || left[ready].Status != queue.Ready {
		t.Fatalf("left %v", left)
	}
	if _, created, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "old-0",
		Payload: json.RawMessage(`{"key":"old-0"}`)}); err != nil || !created {
		t.Fatalf("re-enqueue a deleted key: created=%v err=%v", created, err)
	}
}

func TestListenWakesForDueWorkOnly(t *testing.T) {
	t.Parallel()
	s := open(t, t.TempDir(), Options{})
	lctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wake := make(chan struct{}, 1)
	go s.Listen(lctx, wake, nil)
	<-wake // the first signal: "look now"

	woken := func() bool {
		select {
		case <-wake:
			return true
		case <-time.After(200 * time.Millisecond):
			return false
		}
	}
	mustEnqueue(t, s, "now")
	if !woken() {
		t.Fatal("a due job woke nobody")
	}
	if _, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "later", RunAt: time.Now().Add(time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if woken() {
		t.Fatal("a job due in an hour woke a worker")
	}
}

func TestSegmentsRollAndCompactionKeepsEveryJob(t *testing.T) {
	t.Parallel()
	c := newClock()
	s := open(t, t.TempDir(), Options{Now: c.now, SegmentSize: 4 << 10, CompactAt: 1 << 30})
	churn(t, s, c, 2, 600)
	size, segs, _ := s.Size(ctx)
	if segs < 5 {
		t.Fatalf("%d segments of at most 4 KiB for %d bytes", segs, size)
	}
	// Finish almost everything, so most of the log is dead.
	c.add(2 * time.Hour) // retries come due and churn's leases run out
	for {
		jobs, err := s.Claim(ctx, 100, time.Hour, noop)
		if err != nil {
			t.Fatal(err)
		}
		if len(jobs) == 0 {
			break
		}
		for _, j := range jobs[1:] { // the first of each claim stays running
			if err := s.Complete(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
	}
	c.add(time.Hour)
	if _, err := s.DeleteDone(ctx, 0, 1000); err != nil {
		t.Fatal(err)
	}
	before := snapshot(t, s)
	var maxID int64
	for id := range before {
		maxID = max(maxID, id)
	}

	if err := s.Compact(ctx); err != nil {
		t.Fatal(err)
	}
	after, segsAfter, _ := s.Size(ctx)
	if segsAfter > 2 || after*4 >= size {
		t.Fatalf("after compaction: %d bytes in %d segments (was %d in %d)", after, segsAfter, size, segs)
	}
	s = reopen(t, s)
	if got := snapshot(t, s); !reflect.DeepEqual(got, before) {
		t.Fatal("compaction changed the jobs")
	}
	// Ids keep counting from where they were, even past deleted jobs.
	if id := mustEnqueue(t, s, "next"); id <= maxID {
		t.Fatalf("id %d reused (max was %d)", id, maxID)
	}
}

func TestAutomaticCompactionWhenMostOfTheLogIsDead(t *testing.T) {
	t.Parallel()
	c := newClock()
	s := open(t, t.TempDir(), Options{Now: c.now, SegmentSize: 8 << 10, CompactAt: 32 << 10})
	for round := range 40 {
		for i := range 20 {
			mustEnqueue(t, s, fmt.Sprintf("r%d-%d", round, i))
		}
		jobs, err := s.Claim(ctx, 20, time.Hour, noop)
		if err != nil || len(jobs) != 20 {
			t.Fatalf("claim: %d %v", len(jobs), err)
		}
		for _, j := range jobs {
			if err := s.Complete(ctx, j); err != nil {
				t.Fatal(err)
			}
		}
		c.add(time.Hour)
		if _, err := s.DeleteDone(ctx, time.Minute, 1000); err != nil {
			t.Fatal(err)
		}
	}
	size, _, _ := s.Size(ctx)
	if size > 64<<10 {
		t.Fatalf("log is %d bytes for an empty queue: compaction never ran", size)
	}
}

// opsLog writes a short history of every kind of change, one at a time, and
// records the log's size and the state after each, for the tests that cut
// or damage the file.
func opsLog(t *testing.T, dir string) (sizes []int64, states []map[int64]queue.Job) {
	c := newClock()
	s := open(t, dir, Options{Now: c.now})
	step := func(change func() error) {
		t.Helper()
		if err := change(); err != nil {
			t.Fatal(err)
		}
		size, _, err := s.Size(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sizes, states = append(sizes, size), append(states, snapshot(t, s))
		c.add(time.Second)
	}
	step(func() error { return nil })
	for i := range 8 {
		step(func() error { mustEnqueue(t, s, fmt.Sprint("k", i)); return nil })
	}
	var held []queue.Job
	step(func() error {
		var err error
		held, err = s.Claim(ctx, 4, time.Hour, noop)
		return err
	})
	step(func() error { return s.Complete(ctx, held[0]) })
	step(func() error { return s.Retry(ctx, held[1], 0, errors.New("again")) })
	step(func() error { return s.Kill(ctx, held[2], errors.New("no")) })
	step(func() error { return s.Complete(ctx, held[3]) })
	step(func() error { c.add(time.Hour); _, err := s.DeleteDone(ctx, time.Minute, 10); return err })
	for i := range 4 {
		step(func() error { mustEnqueue(t, s, fmt.Sprint("tail-", i)); return nil })
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	return sizes, states
}

func readRecordBytes(b []byte) (recType, []byte, int, error) {
	return readRecord(bufio.NewReader(bytes.NewReader(b)))
}

func copyDir(t *testing.T, from string) string {
	t.Helper()
	to := t.TempDir()
	entries, _ := os.ReadDir(from)
	for _, e := range entries {
		b, err := os.ReadFile(filepath.Join(from, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(to, e.Name()), b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return to
}

// A crash can stop a write after any byte. Wherever the file ends, recovery
// must cut it back to the last whole record and rebuild exactly the state
// that was acknowledged up to there.
func TestATornWriteIsCutAtEveryPossibleByte(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sizes, states := opsLog(t, dir)
	segs, _ := listSegments(dir)
	if len(segs) != 1 {
		t.Fatalf("want one segment, got %d", len(segs))
	}
	full := sizes[len(sizes)-1]
	for cut := sizes[0]; cut <= full; cut++ {
		// The last change whose records all fit before the cut.
		want := 0
		for i, size := range sizes {
			if size <= cut {
				want = i
			}
		}
		d := copyDir(t, dir)
		if err := os.Truncate(filepath.Join(d, segmentName(0)), cut); err != nil {
			t.Fatal(err)
		}
		s, err := Open(Options{Dir: d, NoFullSync: true})
		if err != nil {
			t.Fatalf("cut at %d: %v", cut, err)
		}
		got := snapshot(t, s)
		if cut == sizes[want] {
			// Exactly at the end of a change: exactly what was acknowledged.
			if !reflect.DeepEqual(got, states[want]) {
				t.Fatalf("cut at %d: %d jobs, want the %d acknowledged", cut, len(got), len(states[want]))
			}
		} else {
			// Inside a change that wrote several records (a claim of 4): some
			// of them may survive, but each job is either as acknowledged or
			// as the unanswered change left it, never anything else.
			for id := range mergeKeys(states[want], states[want+1], got) {
				g, inGot := got[id]
				a, inA := states[want][id]
				b, inB := states[want+1][id]
				if !(inGot == inA && (!inGot || reflect.DeepEqual(g, a)) || inGot == inB && (!inGot || reflect.DeepEqual(g, b))) {
					t.Fatalf("cut at %d: job %d is %+v, neither before (%+v) nor after (%+v) the change", cut, id, g, a, b)
				}
			}
		}
		if rec := s.Recovered(); rec.Truncated > cut-sizes[want] {
			t.Fatalf("cut at %d: truncated %d bytes, more than the %d of the unfinished change", cut, rec.Truncated, cut-sizes[want])
		}
		_ = s.Close()
	}
}

func mergeKeys(ms ...map[int64]queue.Job) map[int64]bool {
	out := map[int64]bool{}
	for _, m := range ms {
		for k := range m {
			out[k] = true
		}
	}
	return out
}

func TestDamageInTheNewestSegmentCutsFromThere(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	sizes, states := opsLog(t, dir)
	path := filepath.Join(dir, segmentName(0))
	b, _ := os.ReadFile(path)
	at := sizes[len(sizes)-3] + headerSize + 2 // inside the body of the third-to-last change's record
	b[at] ^= 0xff
	if err := os.WriteFile(path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	s := open(t, dir, Options{})
	if got := snapshot(t, s); !reflect.DeepEqual(got, states[len(states)-3]) {
		t.Fatalf("got %d jobs, want the %d before the damaged record", len(got), len(states[len(states)-3]))
	}
}

func TestDamageInAnOlderSegmentRefusesToOpen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newClock()
	s := open(t, dir, Options{Now: c.now, SegmentSize: 2 << 10, CompactAt: 1 << 30})
	churn(t, s, c, 4, 200)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	segs, _ := listSegments(dir)
	if len(segs) < 3 {
		t.Fatalf("want several segments, got %d", len(segs))
	}
	b, _ := os.ReadFile(segs[0].path)
	b[headerSize+3] ^= 0xff
	if err := os.WriteFile(segs[0].path, b, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(Options{Dir: dir, NoFullSync: true}); err == nil {
		t.Fatal("opened a log with a damaged older segment")
	}
}

// A crash in the middle of a compaction leaves a new segment holding part
// of a checkpoint. The old segments must stay in charge.
func TestACompactionCutShortIsIgnored(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	c := newClock()
	s := open(t, dir, Options{Now: c.now})
	churn(t, s, c, 5, 300)
	want := snapshot(t, s)
	v, err := s.do(ctx, func(w *writer) (any, error) { return w.st.checkpoint(nil) })
	if err != nil {
		t.Fatal(err)
	}
	end, _, _ := s.Size(ctx)
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	checkpoint := v.([]byte)
	half := filepath.Join(dir, segmentName(end))
	if err := os.WriteFile(half, checkpoint[:len(checkpoint)/2], 0o644); err != nil {
		t.Fatal(err)
	}

	s = open(t, dir, Options{Now: c.now})
	if got := snapshot(t, s); !reflect.DeepEqual(got, want) {
		t.Fatalf("got %d jobs, want %d", len(got), len(want))
	}
	if rec := s.Recovered(); rec.Truncated != int64(len(checkpoint)/2) {
		t.Fatalf("recovery %+v: the half checkpoint wasn't cut", rec)
	}
	mustEnqueue(t, s, "after") // and the log is usable
}

// If a write fails, memory may be ahead of the disk. The store must stop
// rather than carry on from a state the disk doesn't have.
func TestAFailedWriteStopsTheStore(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	s := open(t, dir, Options{})
	mustEnqueue(t, s, "kept")
	if _, err := s.do(ctx, func(*writer) (any, error) { return nil, s.files.active.Close() }); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: "lost"}); err == nil {
		t.Fatal("an enqueue that couldn't be written was acknowledged")
	}
	if err := s.Ping(ctx); err == nil {
		t.Fatal("the store still says it's healthy")
	}
	_ = s.Close()
	s = open(t, dir, Options{})
	got := snapshot(t, s)
	if len(got) != 1 || got[1].IdempotencyKey != "kept" {
		t.Fatalf("after reopening: %v", got)
	}
}

func TestRecordsRoundTripAndRejectGarbage(t *testing.T) {
	t.Parallel()
	buf := appendRecord(nil, recEnqueue, []byte(`{"id":1}`))
	if len(buf) != headerSize+8 {
		t.Fatalf("record is %d bytes", len(buf))
	}
	for i := range buf {
		bad := append([]byte(nil), buf...)
		bad[i] ^= 0x01
		if _, _, _, err := readRecordBytes(bad); err == nil {
			t.Fatalf("a flipped bit in byte %d went unnoticed", i)
		}
	}
	tp, body, size, err := readRecordBytes(buf)
	if err != nil || tp != recEnqueue || string(body) != `{"id":1}` || size != len(buf) {
		t.Fatalf("read back %v %q %d %v", tp, body, size, err)
	}
}
