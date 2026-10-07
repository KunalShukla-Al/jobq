// Package worker runs claimed jobs: a pool of goroutines fed by one claim
// loop, with a handler per job kind.
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// Handler does one kind of job. It must be idempotent: a job can run more
// than once (a worker died mid-job, or its lease ran out). Return nil when
// done, Permanent(err) when retrying can't help, any other error to retry.
type Handler interface {
	Handle(ctx context.Context, job queue.Job) error
}

// HandlerFunc adapts a function to Handler.
type HandlerFunc func(ctx context.Context, job queue.Job) error

func (f HandlerFunc) Handle(ctx context.Context, job queue.Job) error { return f(ctx, job) }

type permanent struct{ err error }

func (p permanent) Error() string { return p.err.Error() }
func (p permanent) Unwrap() error { return p.err }

// Permanent marks an error as not worth retrying: the job goes straight to dead.
func Permanent(err error) error { return permanent{err} }

// IsPermanent reports whether err was marked with Permanent.
func IsPermanent(err error) bool {
	var p permanent
	return errors.As(err, &p)
}

// Config tunes a Pool. Zero values get sensible defaults.
type Config struct {
	Concurrency int           // jobs running at once (4)
	Batch       int           // most jobs taken per claim (Concurrency; at most Concurrency)
	Lease       time.Duration // how long a claim holds a job (1m); a handler gets Lease minus a margin
	PollMin     time.Duration // wait after an empty claim, doubling to PollMax (50ms)
	PollMax     time.Duration // (2s)
	Backoff     queue.Backoff // between retries (2s doubling to 10m)
	Logger      *slog.Logger
	Metrics     *metrics.Metrics // nil: no metrics
	// Wake, when set, cuts an idle wait short (see queue.Store.Listen), but
	// not the backoff after a failed claim. Polling continues regardless:
	// it's what finds retries coming due.
	Wake <-chan struct{}
	// ExitWhenIdle makes Run return once a claim finds nothing due and no job
	// is running in this pool: drain mode, for a scheduled run instead of a
	// service that stays up. A retry scheduled for later is left for the next
	// run. A claim that fails isn't "idle": it's retried until ctx ends.
	ExitWhenIdle bool
}

func (c *Config) defaults() {
	if c.Concurrency <= 0 {
		c.Concurrency = 4
	}
	if c.Batch <= 0 || c.Batch > c.Concurrency {
		c.Batch = c.Concurrency // a claim takes at most one job per free slot anyway
	}
	if c.Lease <= 0 {
		c.Lease = time.Minute
	}
	if c.PollMin <= 0 {
		c.PollMin = 50 * time.Millisecond
	}
	if c.PollMax <= 0 {
		c.PollMax = 2 * time.Second
	}
	if c.Backoff.Base <= 0 {
		c.Backoff.Base = 2 * time.Second
	}
	if c.Backoff.Max <= 0 {
		c.Backoff.Max = 10 * time.Minute
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Pool claims jobs and runs them.
type Pool struct {
	store    *queue.Store
	handlers map[string]Handler
	kinds    []string // the handlers' kinds: the only ones claimed
	cfg      Config
}

// New makes a pool for these handlers, by job kind. It claims only jobs of
// these kinds.
func New(store *queue.Store, handlers map[string]Handler, cfg Config) *Pool {
	cfg.defaults()
	kinds := make([]string, 0, len(handlers))
	for k := range handlers {
		kinds = append(kinds, k)
	}
	slices.Sort(kinds)
	return &Pool{store: store, handlers: handlers, kinds: kinds, cfg: cfg}
}

// Kinds the pool can run.
func (p *Pool) Kinds() map[string]Handler { return p.handlers }

// Run claims and runs jobs until ctx is cancelled (or, with ExitWhenIdle,
// until there's nothing left to do), then stops claiming and waits for the
// jobs already running (each is bounded by its lease). A job that doesn't
// finish in time is left to its lease and claimed again later.
func (p *Pool) Run(ctx context.Context) {
	slots := make(chan struct{}, p.cfg.Concurrency)
	var running sync.WaitGroup
	defer running.Wait()

	wait := p.cfg.PollMin
	for {
		// Claim only as many as there are free slots.
		free := 0
	fill:
		for free < p.cfg.Batch {
			select {
			case slots <- struct{}{}:
				free++
			default:
				break fill
			}
		}
		if free == 0 { // all busy: wait for one to finish
			select {
			case slots <- struct{}{}:
				free = 1
			case <-ctx.Done():
				return
			}
		}
		if ctx.Err() != nil {
			release(slots, free)
			return
		}

		began := time.Now()
		jobs, err := p.store.Claim(ctx, free, p.cfg.Lease, p.kinds)
		if err != nil && ctx.Err() == nil {
			p.cfg.Logger.Error("claim failed", "err", err)
		}
		release(slots, free-len(jobs))
		p.cfg.Metrics.Claimed(len(jobs), time.Since(began))
		for _, j := range jobs {
			running.Add(1)
			go func(j queue.Job) {
				defer running.Done()
				defer func() { <-slots }()
				p.run(j)
			}(j)
		}

		if len(jobs) > 0 {
			wait = p.cfg.PollMin
			continue
		}
		// Every slot not held by a running job was just released, so an
		// empty slots channel means nothing is running.
		if p.cfg.ExitWhenIdle && err == nil && len(slots) == 0 {
			return
		}
		wake := p.cfg.Wake
		if err != nil {
			wake = nil // a failed claim keeps its backoff: a NOTIFY can't help a database that's out of reach
		}
		select { // nothing due: back off up to PollMax, or until woken
		case <-time.After(wait):
			wait = min(wait*2, p.cfg.PollMax)
		case <-wake:
			wait = p.cfg.PollMin
		case <-ctx.Done():
			return
		}
	}
}

func release(slots chan struct{}, n int) {
	for i := 0; i < n; i++ {
		<-slots
	}
}

// run executes one claimed job and records the outcome. It doesn't use the
// pool's context: a job already running is allowed to finish during shutdown.
func (p *Pool) run(j queue.Job) {
	log := p.cfg.Logger.With("job", j.ID, "kind", j.Kind, "attempt", j.Attempts)
	ctx := context.Background()

	var err error
	h, ok := p.handlers[j.Kind]
	switch {
	case !ok:
		// Claim only takes the pool's own kinds, so this shouldn't happen;
		// kept so a job that somehow gets here is recorded, not stuck.
		err = Permanent(fmt.Errorf("no handler for kind %q", j.Kind))
	case j.Attempts > j.MaxAttempts:
		// Claimed again after its lease ran out once too often (the worker died each time).
		err = Permanent(fmt.Errorf("gave up after %d attempts", j.MaxAttempts))
	default:
		// Stop the handler well before the lease ends, so the job can't be
		// claimed by someone else while this worker still believes it holds it.
		hctx, cancel := context.WithTimeout(ctx, p.cfg.Lease*9/10)
		began := time.Now()
		err = safely(hctx, h, j)
		p.cfg.Metrics.Ran(j.Kind, time.Since(began))
		cancel()
	}

	var result string
	var record func() error
	switch {
	case err == nil:
		result = metrics.ResultDone
		record = func() error { return p.store.Complete(ctx, j) }
		log.Debug("done")
	case IsPermanent(err) || j.Attempts >= j.MaxAttempts:
		result = metrics.ResultDead
		log.Warn("dead", "err", err)
		cause := err
		record = func() error { return p.store.Kill(ctx, j, cause) }
	default:
		result = metrics.ResultRetry
		delay := p.cfg.Backoff.Delay(j.Attempts)
		log.Info("retrying", "err", err, "in", delay)
		cause := err
		record = func() error { return p.store.Retry(ctx, j, delay, cause) }
	}
	switch err, retried := persist(record, recordWaits); {
	case errors.Is(err, queue.ErrLostLease) && retried:
		// An earlier try's error may have come after its commit: then the
		// outcome is recorded and that's why the lease is gone. Can't tell.
		log.Info("outcome may already be recorded: an earlier try failed, and now the lease is gone")
	case errors.Is(err, queue.ErrLostLease):
		p.cfg.Metrics.LeaseLost()
		log.Warn("lease lost before the outcome was recorded; another claim owns the job")
	case err != nil:
		log.Error("recording the outcome failed; the lease will expire and the job will run again", "err", err)
	default:
		p.cfg.Metrics.Finished(j, result)
	}
}

// recordWaits are the pauses between tries at recording an outcome. A
// failure here is usually the database briefly out of reach (in the load
// test: out of connections); giving up at once would leave the job to its
// lease and run it a second time.
var recordWaits = []time.Duration{50 * time.Millisecond, 200 * time.Millisecond, time.Second}

// persist calls record until it succeeds, the lease is lost (retrying can't
// help), or the waits run out. retried says whether more than one try was made.
func persist(record func() error, waits []time.Duration) (err error, retried bool) {
	err = record()
	for _, w := range waits {
		if err == nil || errors.Is(err, queue.ErrLostLease) {
			return err, retried
		}
		time.Sleep(w)
		retried = true
		err = record()
	}
	return err, retried
}

// safely runs a handler, turning a panic into a retryable error.
func safely(ctx context.Context, h Handler, j queue.Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("handler panicked: %v", r)
		}
	}()
	return h.Handle(ctx, j)
}
