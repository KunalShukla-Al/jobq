package logstore

import (
	"container/heap"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"sync"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// Sync is when the log is fsynced, and so when an answer is safe.
type Sync int

const (
	// SyncBatch (the default) is group commit: the writer takes every request
	// waiting for it, writes their records, fsyncs once, then answers them
	// all. An answer means the change is on disk.
	SyncBatch Sync = iota
	// SyncAlways fsyncs after each request on its own: the slow baseline that
	// shows what group commit buys.
	SyncAlways
	// SyncInterval answers as soon as records are written to the OS, and
	// fsyncs every Interval. Faster, but a power cut can lose the last
	// Interval of answered work (a process crash can't: the OS has the data).
	SyncInterval
)

func (s Sync) String() string {
	return [...]string{"batch", "always", "interval"}[s]
}

// ParseSync reads "batch", "always" or "interval".
func ParseSync(v string) (Sync, error) {
	for _, s := range []Sync{SyncBatch, SyncAlways, SyncInterval} {
		if v == s.String() {
			return s, nil
		}
	}
	return 0, fmt.Errorf("sync must be batch, always or interval, not %q", v)
}

// Options configure Open. Only Dir is required.
type Options struct {
	Dir      string
	Sync     Sync
	Interval time.Duration // SyncInterval's fsync period (10ms)
	// Window makes group commit wait this long for more requests before
	// writing a batch. Zero takes only what's already waiting, which batches
	// well under load without slowing a lone request.
	Window time.Duration
	// SegmentSize closes the newest segment once it's this big (64 MiB).
	SegmentSize int64
	// CompactAt starts a compaction when the log is this big (64 MiB) and
	// at least 4 times what its jobs would take written out fresh.
	CompactAt int64
	// NoFullSync uses a plain fsync instead of F_FULLFSYNC on macOS (see files.sync).
	NoFullSync bool
	// Now is the clock; tests move it.
	Now func() time.Time
}

// ErrClosed is the answer to a request after Close.
var ErrClosed = errors.New("logstore: closed")

// Store is a queue.Backend kept in an append-only log on local disk.
//
// One goroutine, the writer, owns the files and the in-memory state; every
// request, reads included, reaches it through a channel. That keeps locks
// out of the core and makes group commit natural. Reads are answered after
// the fsync of their batch, so they never show a change that a crash could
// still take back.
type Store struct {
	opts     Options
	files    files
	st       *state
	recovery Recovery

	reqs   chan *request
	closed chan struct{}
	done   chan struct{}
	once   sync.Once

	// failed is set by the writer when a write or fsync fails. The state in
	// memory may then be ahead of the disk, so every later request gets the
	// error: reopen the store to recover from what is on disk.
	failed error

	mu        sync.Mutex
	listeners map[chan<- struct{}]struct{}

	estimate int // bytes per job in the last checkpoint, for the compaction rule
}

var _ queue.Backend = (*Store)(nil)

type request struct {
	// run executes in the writer. It may emit records; it must not block.
	run func(w *writer) (any, error)
	// compact asks for a compaction after the batch's records are on disk
	// (compacting in the middle of a batch would put its records after the
	// checkpoint that already holds them).
	compact bool
	result  any
	err     error
	done    chan struct{}
}

// writer is what a request's run sees: the state and the batch's record buffer.
type writer struct {
	st      *state
	now     time.Time
	buf     []byte
	wakeAll bool // a job became due: wake listening workers after the fsync
}

// emit applies a record to the state and adds it to the batch.
func (w *writer) emit(t recType, body any) error {
	b, err := json.Marshal(body)
	if err != nil {
		return err
	}
	if err := w.st.apply(t, b); err != nil {
		return err // a bug: the writer made a change the log can't hold
	}
	w.buf = appendRecord(w.buf, t, b)
	return nil
}

// Open recovers the log in opts.Dir (creating it if needed) and starts the writer.
func Open(opts Options) (*Store, error) {
	if opts.Dir == "" {
		return nil, errors.New("logstore: no directory")
	}
	if opts.Interval <= 0 {
		opts.Interval = 10 * time.Millisecond
	}
	if opts.SegmentSize <= 0 {
		opts.SegmentSize = 64 << 20
	}
	if opts.CompactAt <= 0 {
		opts.CompactAt = 64 << 20
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(opts.Dir, 0o755); err != nil {
		return nil, err
	}
	st, segs, rec, err := recover(opts.Dir)
	if err != nil {
		return nil, err
	}
	s := &Store{
		opts:      opts,
		files:     files{dir: opts.Dir, segs: segs, fullSync: !opts.NoFullSync},
		st:        st,
		recovery:  rec,
		reqs:      make(chan *request),
		closed:    make(chan struct{}),
		done:      make(chan struct{}),
		listeners: map[chan<- struct{}]struct{}{},
		estimate:  256,
	}
	if err := s.files.openActive(); err != nil {
		return nil, err
	}
	go s.write()
	return s, nil
}

// Recovered reports what Open found and did.
func (s *Store) Recovered() Recovery { return s.recovery }

// Close stops the writer after the requests already taken, fsyncs, and closes the files.
func (s *Store) Close() error {
	s.once.Do(func() { close(s.closed) })
	<-s.done
	return s.failed
}

// do sends a request to the writer and waits for its answer.
func (s *Store) do(ctx context.Context, run func(w *writer) (any, error)) (any, error) {
	return s.send(ctx, &request{run: run, done: make(chan struct{})})
}

func (s *Store) send(ctx context.Context, r *request) (any, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	select {
	case s.reqs <- r:
	case <-s.closed:
		return nil, ErrClosed
	case <-ctx.Done():
		return nil, ctx.Err()
	}
	// Once taken, a request is always answered, and promptly: the writer
	// never waits on anything but the disk. Waiting here, even past ctx,
	// means a caller is never told "failed" about a change that happened.
	<-r.done
	return r.result, r.err
}

// maxBatch bounds one group commit, so one fsync never answers an unbounded crowd.
const maxBatch = 4096

func (s *Store) write() {
	defer close(s.done)
	var tick <-chan time.Time
	if s.opts.Sync == SyncInterval {
		t := time.NewTicker(s.opts.Interval)
		defer t.Stop()
		tick = t.C
	}
	dirty := false // written but not fsynced (SyncInterval)
	batch := make([]*request, 0, 64)
	w := &writer{st: s.st}

	for {
		batch = batch[:0]
		select {
		case r := <-s.reqs:
			batch = append(batch, r)
		case <-tick:
			if dirty {
				s.fail(s.files.sync())
				dirty = false
			}
			continue
		case <-s.closed:
			if s.failed == nil {
				s.fail(s.files.sync())
			}
			s.files.close()
			return
		}
		if s.opts.Sync != SyncAlways {
			batch = s.gather(batch)
		}

		w.now, w.buf, w.wakeAll = s.opts.Now(), w.buf[:0], false
		for _, r := range batch {
			if s.failed != nil {
				r.err = s.failed
				continue
			}
			r.result, r.err = r.run(w)
		}
		if len(w.buf) > 0 && s.failed == nil {
			s.fail(s.files.write(w.buf))
			if s.opts.Sync == SyncInterval {
				dirty = true
			} else {
				s.fail(s.files.sync())
			}
			if s.failed != nil {
				for _, r := range batch {
					r.result, r.err = nil, s.failed
				}
			}
		}
		for _, r := range batch {
			if r.compact && r.err == nil {
				if err := s.compact(); err != nil {
					s.fail(err)
					r.err = s.failed
				}
			}
		}
		for _, r := range batch {
			close(r.done)
		}
		if w.wakeAll {
			s.wake()
		}
		if s.failed == nil {
			s.fail(s.maintain())
		}
		if cap(w.buf) > 4<<20 {
			w.buf = nil // don't keep a huge buffer after one big batch
		}
	}
}

// gather adds every request already waiting (or arriving within Window) to the batch.
func (s *Store) gather(batch []*request) []*request {
	var deadline <-chan time.Time
	if s.opts.Window > 0 {
		t := time.NewTimer(s.opts.Window)
		defer t.Stop()
		deadline = t.C
	}
	for len(batch) < maxBatch {
		if deadline == nil {
			select {
			case r := <-s.reqs:
				batch = append(batch, r)
			default:
				return batch
			}
		} else {
			select {
			case r := <-s.reqs:
				batch = append(batch, r)
			case <-deadline:
				return batch
			}
		}
	}
	return batch
}

func (s *Store) fail(err error) {
	if err != nil && s.failed == nil {
		s.failed = fmt.Errorf("logstore: %w (the store stopped; reopen it to recover from disk)", err)
	}
}

// maintain runs between batches: start a new segment when the newest is
// full, and compact when most of the log is dead.
func (s *Store) maintain() error {
	if s.files.newest().size >= s.opts.SegmentSize {
		if err := s.files.startSegment(s.files.end()); err != nil {
			return err
		}
	}
	size := s.files.size()
	if size >= s.opts.CompactAt && size >= 4*int64(len(s.st.jobs)*s.estimate) {
		return s.compact()
	}
	return nil
}

// compact writes the whole state as a checkpoint at the start of a fresh
// segment, fsyncs it, then deletes every older segment. A crash before the
// checkpoint is complete leaves the old segments in charge; a crash after
// leaves both, and recovery deletes the old ones.
func (s *Store) compact() error {
	buf, err := s.st.checkpoint(nil)
	if err != nil {
		return err
	}
	if s.files.newest().size > 0 {
		if err := s.files.startSegment(s.files.end()); err != nil {
			return err
		}
	}
	if err := s.files.write(buf); err != nil {
		return err
	}
	if err := s.files.sync(); err != nil {
		return err
	}
	if err := s.files.dropBefore(); err != nil {
		return err
	}
	if n := len(s.st.jobs); n > 0 {
		s.estimate = len(buf) / n
	}
	return nil
}

// Compact compacts now, whatever the log's size (for tests and tools).
func (s *Store) Compact(ctx context.Context) error {
	_, err := s.send(ctx, &request{run: func(*writer) (any, error) { return nil, nil }, compact: true, done: make(chan struct{})})
	return err
}

// Size is the bytes on disk in all segments, and how many segments there are.
func (s *Store) Size(ctx context.Context) (bytes int64, segments int, err error) {
	v, err := s.do(ctx, func(*writer) (any, error) {
		return [2]int64{s.files.size(), int64(len(s.files.segs))}, nil
	})
	if err != nil {
		return 0, 0, err
	}
	a := v.([2]int64)
	return a[0], int(a[1]), nil
}

func newToken() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Enqueue adds a job, or returns the existing one with the same idempotency
// key (created = false); the same key with a different kind or payload is
// queue.ErrKeyConflict.
func (s *Store) Enqueue(ctx context.Context, n queue.NewJob) (int64, bool, error) {
	payload := n.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return 0, false, fmt.Errorf("payload is not valid JSON")
	}
	type out struct {
		id      int64
		created bool
	}
	v, err := s.do(ctx, func(w *writer) (any, error) {
		if id, ok := w.st.keys[n.IdempotencyKey]; ok {
			j := w.st.jobs[id]
			if j.Kind != n.Kind || !queue.SameJSON(j.Payload, payload) {
				return out{id, false}, queue.ErrKeyConflict
			}
			return out{id, false}, nil
		}
		b := enqueueBody{ID: w.st.nextID, Kind: n.Kind, Payload: payload, Key: n.IdempotencyKey,
			RunAt: n.RunAt, MaxAttempts: n.MaxAttempts, CreatedAt: w.now}
		if b.RunAt.IsZero() {
			b.RunAt = w.now
		}
		if b.MaxAttempts <= 0 {
			b.MaxAttempts = 8
		}
		if err := w.emit(recEnqueue, b); err != nil {
			return nil, err
		}
		if !b.RunAt.After(w.now) {
			w.wakeAll = true
		}
		return out{b.ID, true}, nil
	})
	o, _ := v.(out)
	return o.id, o.created, err
}

// Get returns one job.
func (s *Store) Get(ctx context.Context, id int64) (queue.Job, error) {
	v, err := s.do(ctx, func(w *writer) (any, error) {
		j := w.st.jobs[id]
		if j == nil {
			return nil, queue.ErrNotFound
		}
		return *j, nil
	})
	if err != nil {
		return queue.Job{}, err
	}
	return v.(queue.Job), nil
}

// Claim leases up to limit jobs of the given kinds that are due, or whose
// lease has run out, oldest run_at first, the same order as Postgres's.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration, kinds []string) ([]queue.Job, error) {
	if limit <= 0 || len(kinds) == 0 {
		return nil, ctx.Err()
	}
	v, err := s.do(ctx, func(w *writer) (any, error) {
		return claim(w, limit, lease, kinds)
	})
	jobs, _ := v.([]queue.Job)
	return jobs, err
}

func claim(w *writer, limit int, lease time.Duration, kinds []string) ([]queue.Job, error) {
	st := w.st
	wanted := map[string]bool{}
	var candidates []entry // (run_at, id) of every job that may be taken

	// Due jobs: up to limit from each kind's heap, which is ordered by (run_at, id).
	for _, k := range kinds {
		if wanted[k] {
			continue
		}
		wanted[k] = true
		h := st.ready[k]
		for taken := 0; h != nil && h.Len() > 0 && taken < limit; {
			top := (*h)[0]
			if !st.stillReady(top) {
				heap.Pop(h)
				continue
			}
			if top.at.After(w.now) {
				break
			}
			candidates = append(candidates, heap.Pop(h).(entry))
			taken++
		}
	}
	// Running jobs whose lease has run out (a worker died holding them).
	var otherKinds []entry
	for st.leases.Len() > 0 {
		top := st.leases[0]
		if !st.stillLeased(top) {
			heap.Pop(&st.leases)
			continue
		}
		if !top.at.Before(w.now) {
			break
		}
		e := heap.Pop(&st.leases).(entry)
		if j := st.jobs[e.id]; wanted[j.Kind] {
			candidates = append(candidates, entry{j.RunAt, j.ID})
		} else {
			otherKinds = append(otherKinds, e)
		}
	}
	for _, e := range otherKinds {
		heap.Push(&st.leases, e)
	}

	slices.SortFunc(candidates, func(a, b entry) int {
		if c := a.at.Compare(b.at); c != 0 {
			return c
		}
		return int(a.id - b.id)
	})
	var jobs []queue.Job
	for i, e := range candidates {
		j := st.jobs[e.id]
		if i >= limit {
			// Not taken: back into the heap it came from.
			if j.Status == queue.Ready {
				heap.Push(st.ready[j.Kind], e)
			} else {
				heap.Push(&st.leases, entry{*j.LockedUntil, j.ID})
			}
			continue
		}
		if err := w.emit(recClaim, claimBody{ID: j.ID, Token: newToken(), Attempts: j.Attempts + 1,
			LockedUntil: w.now.Add(lease)}); err != nil {
			return jobs, err
		}
		jobs = append(jobs, *j)
	}
	return jobs, nil
}

// finish records an outcome, if this claim still holds the job.
func (s *Store) finish(ctx context.Context, j queue.Job, t recType, fill func(now time.Time, b *finishBody)) error {
	if j.LockToken == nil {
		return queue.ErrLostLease
	}
	_, err := s.do(ctx, func(w *writer) (any, error) {
		cur := w.st.jobs[j.ID]
		if cur == nil || cur.Status != queue.Running || cur.LockToken == nil || *cur.LockToken != *j.LockToken {
			return nil, queue.ErrLostLease
		}
		b := finishBody{ID: j.ID, Token: *j.LockToken}
		fill(w.now, &b)
		if t == recRetry && !b.RunAt.After(w.now) {
			w.wakeAll = true
		}
		return nil, w.emit(t, b)
	})
	return err
}

// Complete marks a claimed job done.
func (s *Store) Complete(ctx context.Context, j queue.Job) error {
	return s.finish(ctx, j, recComplete, func(now time.Time, b *finishBody) { b.FinishedAt = &now })
}

// Retry puts a claimed job back to run again after delay.
func (s *Store) Retry(ctx context.Context, j queue.Job, delay time.Duration, cause error) error {
	msg := queue.ErrorText(cause)
	return s.finish(ctx, j, recRetry, func(now time.Time, b *finishBody) {
		at := now.Add(delay)
		b.RunAt, b.LastError = &at, &msg
	})
}

// Kill gives up on a claimed job; it stays as dead for a person to look at.
func (s *Store) Kill(ctx context.Context, j queue.Job, cause error) error {
	msg := queue.ErrorText(cause)
	return s.finish(ctx, j, recKill, func(now time.Time, b *finishBody) { b.FinishedAt, b.LastError = &now, &msg })
}

// DeleteDone deletes done jobs that finished more than keep ago, batch per
// record, and returns how many. Dead jobs stay. The disk space comes back at
// the next compaction.
func (s *Store) DeleteDone(ctx context.Context, keep time.Duration, batch int) (int64, error) {
	if batch <= 0 {
		batch = 1000
	}
	v, err := s.do(ctx, func(w *writer) (any, error) {
		cutoff := w.now.Add(-keep)
		var ids []int64
		for id, j := range w.st.jobs {
			if j.Status == queue.Done && j.FinishedAt != nil && j.FinishedAt.Before(cutoff) {
				ids = append(ids, id)
			}
		}
		slices.Sort(ids)
		var n int64
		for chunk := range slices.Chunk(ids, batch) {
			if err := w.emit(recDelete, deleteBody{IDs: chunk}); err != nil {
				return n, err
			}
			n += int64(len(chunk))
		}
		return n, nil
	})
	n, _ := v.(int64)
	return n, err
}

// Counts is the queue's state at a glance.
func (s *Store) Counts(ctx context.Context) (queue.Counts, error) {
	v, err := s.do(ctx, func(w *writer) (any, error) {
		var c queue.Counts
		var oldest time.Time
		for _, j := range w.st.jobs {
			switch j.Status {
			case queue.Ready:
				if j.RunAt.After(w.now) {
					c.Later++
				} else {
					c.Due++
					if oldest.IsZero() || j.RunAt.Before(oldest) {
						oldest = j.RunAt
					}
				}
			case queue.Running:
				c.Running++
			case queue.Done:
				c.Done++
			case queue.Dead:
				c.Dead++
			}
		}
		if !oldest.IsZero() {
			c.OldestDue = w.now.Sub(oldest)
		}
		return c, nil
	})
	c, _ := v.(queue.Counts)
	return c, err
}

// Ping reports whether the store is open and healthy.
func (s *Store) Ping(ctx context.Context) error {
	_, err := s.do(ctx, func(*writer) (any, error) { return nil, nil })
	return err
}

// Listen signals wake whenever a job becomes due here (a new one, or a retry
// with no delay), until ctx ends. The log lives in this process, so the
// writer wakes workers directly: no LISTEN/NOTIFY needed.
func (s *Store) Listen(ctx context.Context, wake chan<- struct{}, _ func(error)) {
	s.mu.Lock()
	s.listeners[wake] = struct{}{}
	s.mu.Unlock()
	signal(wake) // jobs may have arrived before we listened
	select {
	case <-ctx.Done():
	case <-s.closed:
	}
	s.mu.Lock()
	delete(s.listeners, wake)
	s.mu.Unlock()
}

func (s *Store) wake() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.listeners {
		signal(c)
	}
}

func signal(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default: // a wake-up is already pending
	}
}
