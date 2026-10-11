package queue

import (
	"context"
	"time"
)

// Backend is everything jobq needs from storage. The worker, the API and the
// metrics depend on this, not on Postgres. Store (Postgres) is the first
// implementation; jobq v2 adds an append-only log on local disk
// (DESIGN-v2.md), and later the same log replicated with Raft.
//
// Every implementation keeps the same guarantees: one job per idempotency key,
// a claim holds a lease that a crashed worker loses, an outcome is only
// recorded by the holder of the current lease (ErrLostLease otherwise), and
// a job is dead after its last attempt.
type Backend interface {
	Enqueue(ctx context.Context, n NewJob) (id int64, created bool, err error)
	Get(ctx context.Context, id int64) (Job, error)
	Claim(ctx context.Context, limit int, lease time.Duration, kinds []string) ([]Job, error)
	Complete(ctx context.Context, j Job) error
	Retry(ctx context.Context, j Job, delay time.Duration, cause error) error
	Kill(ctx context.Context, j Job, cause error) error
	DeleteDone(ctx context.Context, keep time.Duration, batch int) (int64, error)
	// Counts is the queue's state at a glance, for metrics and logs.
	Counts(ctx context.Context) (Counts, error)
	// Ping reports whether storage is reachable (the API's health check).
	Ping(ctx context.Context) error
	// Listen signals wake (without blocking) whenever a job may be ready,
	// until ctx ends. A backend with no way to notify may just return.
	Listen(ctx context.Context, wake chan<- struct{}, onError func(error))
}

// Counts are jobs by state. Due jobs are ready and past their run time; Later
// ones are ready but waiting out a delay (a retry's backoff, say).
type Counts struct {
	Due, Later, Running, Done, Dead int64
	// OldestDue is how long the oldest due job has waited: the
	// falling-behind number. Zero when nothing is due.
	OldestDue time.Duration
}

var _ Backend = (*Store)(nil)

// Counts reads every state in one query.
func (s *Store) Counts(ctx context.Context) (Counts, error) {
	var c Counts
	var oldest float64
	err := s.Pool.QueryRow(ctx, `
		select count(*) filter (where status = 'ready' and run_at <= now()),
		       count(*) filter (where status = 'ready' and run_at > now()),
		       count(*) filter (where status = 'running'),
		       count(*) filter (where status = 'done'),
		       count(*) filter (where status = 'dead'),
		       coalesce(extract(epoch from now() - min(run_at)
		           filter (where status = 'ready' and run_at <= now())), 0)::float8
		from jobq.jobs`).Scan(&c.Due, &c.Later, &c.Running, &c.Done, &c.Dead, &oldest)
	c.OldestDue = time.Duration(oldest * float64(time.Second))
	return c, err
}

// Ping checks the database answers.
func (s *Store) Ping(ctx context.Context) error { return s.Pool.Ping(ctx) }
