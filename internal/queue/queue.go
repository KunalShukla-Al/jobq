// Package queue is the job table and the four things done to it: enqueue,
// claim, and finish (done, retry later, or dead).
//
// Execution is at-least-once. A job is claimed under a lease. If its worker
// dies, the lease runs out and another worker claims it again, so handlers
// must be idempotent. The idempotency key makes enqueueing idempotent too.
package queue

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Status of a job.
const (
	Ready   = "ready"
	Running = "running"
	Done    = "done"
	Dead    = "dead"
)

var (
	// ErrKeyConflict: the idempotency key is already used by a job with a different kind or payload.
	ErrKeyConflict = errors.New("idempotency key already used for a different job")
	// ErrLostLease: the job is no longer held by this claim (its lease ran out and someone else took it).
	ErrLostLease = errors.New("lease lost: the job was claimed again")
	// ErrNotFound: no job with that id.
	ErrNotFound = errors.New("job not found")
)

// Job is a row of jobq.jobs.
type Job struct {
	ID             int64           `json:"id"`
	Kind           string          `json:"kind"`
	Payload        json.RawMessage `json:"payload"`
	IdempotencyKey string          `json:"idempotency_key"`
	Status         string          `json:"status"`
	Attempts       int             `json:"attempts"`
	MaxAttempts    int             `json:"max_attempts"`
	RunAt          time.Time       `json:"run_at"`
	LockedUntil    *time.Time      `json:"locked_until,omitempty"`
	LockToken      *string         `json:"-"`
	LastError      *string         `json:"last_error,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	FinishedAt     *time.Time      `json:"finished_at,omitempty"`
}

const columns = `id, kind, payload, idempotency_key, status, attempts, max_attempts, run_at,
	locked_until, lock_token::text, last_error, created_at, finished_at`

// The same columns, qualified, for the claim's UPDATE … FROM (where "id" alone is ambiguous).
const claimedColumns = `j.id, j.kind, j.payload, j.idempotency_key, j.status, j.attempts, j.max_attempts,
	j.run_at, j.locked_until, j.lock_token::text, j.last_error, j.created_at, j.finished_at`

func scan(row pgx.Row) (Job, error) {
	var j Job
	err := row.Scan(&j.ID, &j.Kind, &j.Payload, &j.IdempotencyKey, &j.Status, &j.Attempts, &j.MaxAttempts,
		&j.RunAt, &j.LockedUntil, &j.LockToken, &j.LastError, &j.CreatedAt, &j.FinishedAt)
	return j, err
}

// Store reads and writes jobs.
type Store struct {
	Pool *pgxpool.Pool
	// Notify sends a NOTIFY on Channel with each new job that's due now, so
	// workers using Listen wake at once instead of at their next poll.
	Notify bool
}

// Channel is the LISTEN/NOTIFY channel for "a job is ready".
const Channel = "jobq_ready"

// NewJob is what a producer asks for.
type NewJob struct {
	Kind           string
	Payload        json.RawMessage // nil means {}
	IdempotencyKey string
	RunAt          time.Time // zero means now
	MaxAttempts    int       // zero means the table default (8)
}

// Enqueue adds a job, or returns the existing one with the same idempotency
// key (created = false). The same key with a different kind or payload is
// ErrKeyConflict: silently returning the old job would hide a producer bug.
func (s *Store) Enqueue(ctx context.Context, n NewJob) (id int64, created bool, err error) {
	payload := n.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	if !json.Valid(payload) {
		return 0, false, fmt.Errorf("payload is not valid JSON")
	}
	var runAt *time.Time
	if !n.RunAt.IsZero() {
		runAt = &n.RunAt
	}
	var maxAttempts *int
	if n.MaxAttempts > 0 {
		maxAttempts = &n.MaxAttempts
	}

	// The NOTIFY is in the same statement, so it's sent when the insert
	// commits and never for a duplicate. Notifications only say "look now";
	// workers still find the jobs by claiming, so a lost one costs a poll.
	err = s.Pool.QueryRow(ctx, `
		with ins as (
			insert into jobq.jobs (kind, payload, idempotency_key, run_at, max_attempts)
			values ($1, $2, $3, coalesce($4, now()), coalesce($5, 8))
			on conflict (idempotency_key) do nothing
			returning id, run_at
		)
		select ins.id from ins
		left join lateral (select pg_notify('`+Channel+`', '') where $6 and ins.run_at <= now()) n on true`,
		n.Kind, payload, n.IdempotencyKey, runAt, maxAttempts, s.Notify).Scan(&id)
	if err == nil {
		return id, true, nil
	}
	if !errors.Is(err, pgx.ErrNoRows) {
		return 0, false, err
	}

	// The key exists. (A conflicting insert still in flight in another
	// transaction is waited for by ON CONFLICT, so this read sees it.)
	var kind string
	var existing json.RawMessage
	if err := s.Pool.QueryRow(ctx,
		`select id, kind, payload from jobq.jobs where idempotency_key = $1`, n.IdempotencyKey,
	).Scan(&id, &kind, &existing); err != nil {
		return 0, false, err
	}
	if kind != n.Kind || !sameJSON(existing, payload) {
		return id, false, ErrKeyConflict
	}
	return id, false, nil
}

// sameJSON compares two JSON documents by meaning, not bytes (jsonb reorders keys).
func sameJSON(a, b json.RawMessage) bool {
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		return false
	}
	xa, _ := json.Marshal(x)
	ya, _ := json.Marshal(y)
	return bytes.Equal(xa, ya)
}

// Listen holds one connection (its own, not the pool's) on LISTEN and signals wake (without blocking)
// each time a job is enqueued, until ctx ends. A dropped connection is
// reopened after a pause; while it's down, workers fall back to polling.
func (s *Store) Listen(ctx context.Context, wake chan<- struct{}, onError func(error)) {
	for ctx.Err() == nil {
		err := s.listen(ctx, wake)
		if ctx.Err() != nil {
			return
		}
		if onError != nil {
			onError(err)
		}
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
		}
	}
}

func (s *Store) listen(ctx context.Context, wake chan<- struct{}) error {
	// A connection of its own, outside the pool: it sits in LISTEN for good,
	// and borrowing one would leave the workers a connection short.
	conn, err := pgx.ConnectConfig(ctx, s.Pool.Config().ConnConfig)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	if _, err := conn.Exec(ctx, "listen "+Channel); err != nil {
		return err
	}
	signal(wake) // jobs may have arrived while we weren't listening
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			return err
		}
		signal(wake)
	}
}

func signal(wake chan<- struct{}) {
	select {
	case wake <- struct{}{}:
	default: // a wake-up is already pending
	}
}

// Get returns one job.
func (s *Store) Get(ctx context.Context, id int64) (Job, error) {
	j, err := scan(s.Pool.QueryRow(ctx, `select `+columns+` from jobq.jobs where id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return Job{}, ErrNotFound
	}
	return j, err
}

// Claim takes up to limit jobs of the given kinds that are due, or whose
// lease has run out, and leases them for lease. SKIP LOCKED lets any number
// of workers claim at once without waiting on each other or taking the same
// job. Each claim counts as an attempt, so a job that keeps killing its
// worker still runs out of attempts.
//
// kinds should be what the caller has handlers for: a process without, say,
// push keys then leaves webpush jobs waiting for one that has them, instead
// of claiming them and failing. No kinds claims nothing.
func (s *Store) Claim(ctx context.Context, limit int, lease time.Duration, kinds []string) ([]Job, error) {
	rows, err := s.Pool.Query(ctx, `
		with picked as (
			select id from jobq.jobs
			where ((status = 'ready' and run_at <= now())
			    or (status = 'running' and locked_until < now()))
			  and kind = any($3::text[])
			order by run_at, id
			limit $1
			for update skip locked
		)
		update jobq.jobs j
		set status = 'running',
		    attempts = j.attempts + 1,
		    locked_until = now() + $2::interval,
		    lock_token = gen_random_uuid()
		from picked
		where j.id = picked.id
		returning `+claimedColumns, limit, lease, kinds)
	if err != nil {
		return nil, err
	}
	return pgx.CollectRows(rows, func(r pgx.CollectableRow) (Job, error) { return scan(r) })
}

// finish moves a job this claim still holds out of 'running'.
func (s *Store) finish(ctx context.Context, j Job, sql string, args ...any) error {
	if j.LockToken == nil {
		return ErrLostLease
	}
	tag, err := s.Pool.Exec(ctx, sql, append([]any{j.ID, *j.LockToken}, args...)...)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return ErrLostLease
	}
	return nil
}

// Complete marks a claimed job done.
func (s *Store) Complete(ctx context.Context, j Job) error {
	return s.finish(ctx, j, `
		update jobq.jobs set status = 'done', finished_at = now(), locked_until = null, lock_token = null
		where id = $1 and status = 'running' and lock_token = $2::uuid`)
}

// Retry puts a claimed job back to run again after delay.
func (s *Store) Retry(ctx context.Context, j Job, delay time.Duration, cause error) error {
	return s.finish(ctx, j, `
		update jobq.jobs set status = 'ready', run_at = now() + $3::interval, last_error = $4,
		       locked_until = null, lock_token = null
		where id = $1 and status = 'running' and lock_token = $2::uuid`, delay, errText(cause))
}

// Kill gives up on a claimed job: it stays in the table as 'dead' for a person to look at.
func (s *Store) Kill(ctx context.Context, j Job, cause error) error {
	return s.finish(ctx, j, `
		update jobq.jobs set status = 'dead', finished_at = now(), last_error = $3,
		       locked_until = null, lock_token = null
		where id = $1 and status = 'running' and lock_token = $2::uuid`, errText(cause))
}

// DeleteDone deletes done jobs that finished more than keep ago, batch rows
// per statement so no one statement holds many locks or runs long, and
// returns how many it deleted. Dead jobs are kept for a person to look at.
//
// A deleted job's idempotency key is free again: enqueueing the same key
// later makes a new job. For webpush that's safe, because AcadKit's planner
// also skips anything already in sent_notifications.
func (s *Store) DeleteDone(ctx context.Context, keep time.Duration, batch int) (int64, error) {
	var total int64
	for {
		tag, err := s.Pool.Exec(ctx, `
			delete from jobq.jobs where id in (
				select id from jobq.jobs
				where status = 'done' and finished_at < now() - $1::interval
				limit $2
			)`, keep, batch)
		if err != nil {
			return total, err
		}
		total += tag.RowsAffected()
		if tag.RowsAffected() < int64(batch) {
			return total, nil
		}
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	// Postgres text can't hold NUL or invalid UTF-8, and cutting at byte
	// 2000 can split a character: either would make the write fail every time.
	s := strings.ToValidUTF8(strings.ReplaceAll(err.Error(), "\x00", ""), "\uFFFD")
	if len(s) > 2000 {
		s = strings.ToValidUTF8(s[:2000], "")
	}
	return s
}

// Backoff is how long to wait before attempt n+1, after n attempts have
// failed: Base·2^(n-1), at most Max, with "equal jitter" (half fixed, half
// random) so retries from one outage don't all land at the same instant.
type Backoff struct {
	Base, Max time.Duration
}

// Delay for the given number of attempts so far (1 = the first try failed).
func (b Backoff) Delay(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	d := b.Max
	if shift := attempts - 1; shift < 62 && b.Base<<shift > 0 && b.Base<<shift < b.Max {
		d = b.Base << shift
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(half)+1))
}
