package logstore

import (
	"container/heap"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// Record bodies: what replay needs and nothing else (DESIGN-v2.md, "Records").

type enqueueBody struct {
	ID          int64           `json:"id"`
	Kind        string          `json:"kind"`
	Payload     json.RawMessage `json:"payload"`
	Key         string          `json:"key"`
	RunAt       time.Time       `json:"run_at"`
	MaxAttempts int             `json:"max_attempts"`
	CreatedAt   time.Time       `json:"created_at"`
}

type claimBody struct {
	ID          int64     `json:"id"`
	Token       string    `json:"token"`
	Attempts    int       `json:"attempts"`
	LockedUntil time.Time `json:"locked_until"`
}

// finishBody is complete, retry and kill: the lease holder's token and what changed.
type finishBody struct {
	ID         int64      `json:"id"`
	Token      string     `json:"token"`
	FinishedAt *time.Time `json:"finished_at,omitempty"` // complete, kill
	RunAt      *time.Time `json:"run_at,omitempty"`      // retry
	LastError  *string    `json:"last_error,omitempty"`  // retry, kill
}

type deleteBody struct {
	IDs []int64 `json:"ids"`
}

type checkpointBody struct {
	NextID int64 `json:"next_id"`
	Jobs   int   `json:"jobs,omitempty"` // in the end record: how many puts came between
}

// putBody is a whole job, lease included (queue.Job hides its lock token from JSON).
type putBody struct {
	queue.Job
	Token *string `json:"token,omitempty"`
}

// state is every job, plus the indexes that make claiming fast. It is only
// ever touched by one goroutine at a time: the writer, or recovery before
// the writer starts.
type state struct {
	jobs   map[int64]*queue.Job
	keys   map[string]int64      // idempotency key → id
	ready  map[string]*entryHeap // kind → ready jobs by (run_at, id)
	leases entryHeap             // running jobs by locked_until
	nextID int64
}

func newState() *state {
	return &state{jobs: map[int64]*queue.Job{}, keys: map[string]int64{}, ready: map[string]*entryHeap{}, nextID: 1}
}

// entry points at a job in a heap. Heaps aren't updated when a job changes;
// an entry is checked against the job when it comes off, and dropped if it
// no longer matches (lazy deletion).
type entry struct {
	at time.Time // run_at in a ready heap, locked_until in the lease heap
	id int64
}

type entryHeap []entry

func (h entryHeap) Len() int { return len(h) }
func (h entryHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].id < h[j].id
}
func (h entryHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *entryHeap) Push(x any)   { *h = append(*h, x.(entry)) }
func (h *entryHeap) Pop() any {
	old := *h
	e := old[len(old)-1]
	*h = old[:len(old)-1]
	return e
}

func (s *state) pushReady(j *queue.Job) {
	h := s.ready[j.Kind]
	if h == nil {
		h = &entryHeap{}
		s.ready[j.Kind] = h
	}
	heap.Push(h, entry{j.RunAt, j.ID})
}

// stillReady says whether a ready-heap entry still describes its job.
func (s *state) stillReady(e entry) bool {
	j := s.jobs[e.id]
	return j != nil && j.Status == queue.Ready && j.RunAt.Equal(e.at)
}

// stillLeased says whether a lease-heap entry still describes its job.
func (s *state) stillLeased(e entry) bool {
	j := s.jobs[e.id]
	return j != nil && j.Status == queue.Running && j.LockedUntil != nil && j.LockedUntil.Equal(e.at)
}

// corrupt is replay finding a record that can't follow the ones before it.
func corrupt(t recType, format string, args ...any) error {
	return fmt.Errorf("log is inconsistent at a %s record: %s", t, fmt.Sprintf(format, args...))
}

// apply changes the state by one record. The writer uses it for every change
// it makes and recovery uses it to replay, so the two can't drift apart.
// Replay is checked as well as applied: a record that can't follow the ones
// before it is an error, never a guess.
func (s *state) apply(t recType, body []byte) error {
	switch t {
	case recEnqueue:
		var b enqueueBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		if b.ID < s.nextID {
			return corrupt(t, "id %d after %d was already given out", b.ID, s.nextID-1)
		}
		if _, ok := s.keys[b.Key]; ok {
			return corrupt(t, "idempotency key %q is already used", b.Key)
		}
		j := &queue.Job{ID: b.ID, Kind: b.Kind, Payload: b.Payload, IdempotencyKey: b.Key,
			Status: queue.Ready, MaxAttempts: b.MaxAttempts, RunAt: b.RunAt, CreatedAt: b.CreatedAt}
		s.jobs[j.ID] = j
		s.keys[j.IdempotencyKey] = j.ID
		s.nextID = b.ID + 1
		s.pushReady(j)

	case recClaim:
		var b claimBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		j := s.jobs[b.ID]
		if j == nil || j.Status == queue.Done || j.Status == queue.Dead || b.Attempts != j.Attempts+1 {
			return corrupt(t, "job %d can't be claimed for attempt %d", b.ID, b.Attempts)
		}
		token, until := b.Token, b.LockedUntil
		j.Status, j.Attempts, j.LockToken, j.LockedUntil = queue.Running, b.Attempts, &token, &until
		heap.Push(&s.leases, entry{until, j.ID})

	case recComplete, recRetry, recKill:
		var b finishBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		j := s.jobs[b.ID]
		if j == nil || j.Status != queue.Running || j.LockToken == nil || *j.LockToken != b.Token {
			return corrupt(t, "job %d isn't held by lease %s", b.ID, b.Token)
		}
		j.LockToken, j.LockedUntil = nil, nil
		switch t {
		case recComplete:
			j.Status, j.FinishedAt = queue.Done, b.FinishedAt
		case recRetry:
			if b.RunAt == nil {
				return corrupt(t, "job %d has no run_at", b.ID)
			}
			j.Status, j.RunAt, j.LastError = queue.Ready, *b.RunAt, b.LastError
			s.pushReady(j)
		case recKill:
			j.Status, j.FinishedAt, j.LastError = queue.Dead, b.FinishedAt, b.LastError
		}

	case recDelete:
		var b deleteBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		for _, id := range b.IDs {
			j := s.jobs[id]
			if j == nil || j.Status != queue.Done {
				return corrupt(t, "job %d isn't a done job", id)
			}
			delete(s.jobs, id)
			delete(s.keys, j.IdempotencyKey)
		}

	case recCheckpointBegin:
		var b checkpointBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		*s = *newState()
		s.nextID = b.NextID

	case recPut:
		var b putBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		j := b.Job
		j.LockToken = b.Token
		if j.ID >= s.nextID || s.jobs[j.ID] != nil {
			return corrupt(t, "job %d doesn't fit the checkpoint", j.ID)
		}
		s.jobs[j.ID] = &j
		s.keys[j.IdempotencyKey] = j.ID
		switch j.Status {
		case queue.Ready:
			s.pushReady(&j)
		case queue.Running:
			if j.LockedUntil == nil || j.LockToken == nil {
				return corrupt(t, "running job %d has no lease", j.ID)
			}
			heap.Push(&s.leases, entry{*j.LockedUntil, j.ID})
		}

	case recCheckpointEnd:
		var b checkpointBody
		if err := json.Unmarshal(body, &b); err != nil {
			return corrupt(t, "%v", err)
		}
		if b.Jobs != len(s.jobs) || b.NextID != s.nextID {
			return corrupt(t, "%d jobs and next id %d, the checkpoint says %d and %d",
				len(s.jobs), s.nextID, b.Jobs, b.NextID)
		}

	default:
		return corrupt(t, "unknown type")
	}
	return nil
}

// checkpoint writes the whole state as records: what compaction puts in a new segment.
func (s *state) checkpoint(buf []byte) ([]byte, error) {
	begin, _ := json.Marshal(checkpointBody{NextID: s.nextID})
	buf = appendRecord(buf, recCheckpointBegin, begin)
	for _, j := range s.jobs {
		body, err := json.Marshal(putBody{Job: *j, Token: j.LockToken})
		if err != nil {
			return nil, err
		}
		buf = appendRecord(buf, recPut, body)
	}
	end, _ := json.Marshal(checkpointBody{NextID: s.nextID, Jobs: len(s.jobs)})
	return appendRecord(buf, recCheckpointEnd, end), nil
}
