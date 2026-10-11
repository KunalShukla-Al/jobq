// Command jobq runs the job queue.
//
//	jobq [serve]   the HTTP API and a pool of workers, until stopped
//	jobq drain     workers only: run what's due, then exit (for a scheduled run)
//
// Settings, all from the environment:
//
//	JOBQ_STORE     where jobs live: "postgres" (default) or "log", the
//	               append-only log on local disk (DESIGN-v2.md)
//	DATABASE_URL   Postgres connection string (required with postgres; with
//	               log, only for webpush jobs, whose subscriptions live there)
//	JOBQ_LOG_DIR   the log's directory (required with log)
//	JOBQ_LOG_SYNC  log: "batch" (default, group commit), "always" or "interval"
//	JOBQ_SECRET    bearer token for the API (required to serve)
//	JOBQ_ADDR      listen address (":8080")
//	JOBQ_WORKERS   jobs running at once (4)
//	JOBQ_BATCH     most jobs taken by one claim, at most JOBQ_WORKERS (JOBQ_WORKERS)
//	JOBQ_WAKE      "poll" (default), or "notify": also LISTEN, so idle workers wake as soon as a job arrives
//	JOBQ_LEASE     how long a claim holds a job ("1m")
//	JOBQ_GRACE     how long shutdown waits for running jobs ("30s")
//	JOBQ_DRAIN_MAX drain: stop claiming after this long ("4m")
//	JOBQ_KEEP_DONE delete done jobs older than this ("168h"): hourly when
//	               serving, at the end of a drain
//	VAPID_PUBLIC   web push key pair (base64url); without both, webpush jobs
//	VAPID_PRIVATE  aren't claimed here and wait for a process that has them
//	VAPID_SUBJECT  "mailto:..." or "https://..." sent to push services
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KunalShukla-Al/jobq/internal/api"
	"github.com/KunalShukla-Al/jobq/internal/handlers"
	"github.com/KunalShukla-Al/jobq/internal/logstore"
	"github.com/KunalShukla-Al/jobq/internal/metrics"
	"github.com/KunalShukla-Al/jobq/internal/migrate"
	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/worker"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	var err error
	switch cmd {
	case "serve":
		err = serve(log)
	case "drain":
		err = drain(log)
	default:
		err = fmt.Errorf("unknown command %q: use serve (the default) or drain", cmd)
	}
	if err != nil {
		log.Error("jobq stopped", "err", err)
		os.Exit(1)
	}
}

// settings shared by serve and drain.
type settings struct {
	store        string // "postgres" or "log"
	dbURL        string
	logDir       string
	logSync      logstore.Sync
	workers      int
	batch        int
	lease, grace time.Duration
	keepDone     time.Duration
}

func load() (settings, error) {
	var s settings
	var err error
	s.dbURL = os.Getenv("DATABASE_URL")
	switch s.store = envString("JOBQ_STORE", "postgres"); s.store {
	case "postgres":
		if s.dbURL == "" {
			return s, errors.New("set DATABASE_URL")
		}
	case "log":
		if s.logDir = os.Getenv("JOBQ_LOG_DIR"); s.logDir == "" {
			return s, errors.New("set JOBQ_LOG_DIR")
		}
		if s.logSync, err = logstore.ParseSync(envString("JOBQ_LOG_SYNC", "batch")); err != nil {
			return s, err
		}
	default:
		return s, errors.New(`JOBQ_STORE must be "postgres" or "log"`)
	}
	if s.workers, err = envInt("JOBQ_WORKERS", 4); err != nil {
		return s, err
	}
	if s.lease, err = envDuration("JOBQ_LEASE", time.Minute); err != nil {
		return s, err
	}
	if s.grace, err = envDuration("JOBQ_GRACE", 30*time.Second); err != nil {
		return s, err
	}
	if s.batch, err = envInt("JOBQ_BATCH", s.workers); err != nil {
		return s, err
	}
	s.batch = min(s.batch, s.workers) // a claim takes at most one job per free worker
	if s.keepDone, err = envDuration("JOBQ_KEEP_DONE", 7*24*time.Hour); err != nil {
		return s, err
	}
	return s, nil
}

// opened is the job store, the handlers this process can run, and how to close both.
type opened struct {
	store queue.Backend
	pg    *queue.Store // the same store when it's Postgres, for LISTEN/NOTIFY
	kinds map[string]worker.Handler
	close func()
}

// open opens the store (connecting and migrating Postgres, or recovering the
// log) and the handlers.
func open(ctx context.Context, log *slog.Logger, s settings) (*opened, error) {
	o := &opened{close: func() {}}
	var pool *pgxpool.Pool
	if s.dbURL != "" {
		var err error
		if pool, err = pgxpool.New(ctx, s.dbURL); err != nil {
			return nil, err
		}
		o.close = pool.Close
	}
	fail := func(err error) (*opened, error) { o.close(); return nil, err }

	switch s.store {
	case "postgres":
		if err := migrate.Up(ctx, pool); err != nil {
			return fail(fmt.Errorf("migrate: %w", err))
		}
		o.pg = &queue.Store{Pool: pool}
		o.store = o.pg
	case "log":
		ls, err := logstore.Open(logstore.Options{Dir: s.logDir, Sync: s.logSync})
		if err != nil {
			return fail(fmt.Errorf("open the log: %w", err))
		}
		rec := ls.Recovered()
		log.Info("log recovered", "dir", s.logDir, "segments", rec.Segments, "records", rec.Records,
			"jobs", rec.Jobs, "truncated_bytes", rec.Truncated, "took", rec.Took.String())
		closePool := o.close
		o.close = func() {
			if err := ls.Close(); err != nil {
				log.Error("closing the log", "err", err)
			}
			closePool()
		}
		o.store = ls
	}

	o.kinds = handlers.All()
	switch pub, priv := os.Getenv("VAPID_PUBLIC"), os.Getenv("VAPID_PRIVATE"); {
	case pub != "" && priv != "":
		if pool == nil {
			return fail(errors.New("webpush jobs need DATABASE_URL: the push subscriptions live in Postgres"))
		}
		push, err := handlers.NewWebPush(pool, pub, priv, os.Getenv("VAPID_SUBJECT"))
		if err != nil {
			return fail(err)
		}
		push.Logger = log
		o.kinds["webpush"] = push
	case pub != "" || priv != "":
		return fail(errors.New("set both VAPID_PUBLIC and VAPID_PRIVATE, or neither"))
	default:
		log.Warn("no VAPID keys: webpush jobs are left for a process that has them")
	}
	return o, nil
}

func (s settings) pool(log *slog.Logger) worker.Config {
	return worker.Config{Concurrency: s.workers, Batch: s.batch, Lease: s.lease, Logger: log}
}

func serve(log *slog.Logger) error {
	s, err := load()
	if err != nil {
		return err
	}
	secret := os.Getenv("JOBQ_SECRET")
	if secret == "" {
		return errors.New("set JOBQ_SECRET")
	}
	wakeMode := envString("JOBQ_WAKE", "poll")
	if wakeMode != "poll" && wakeMode != "notify" {
		return errors.New(`JOBQ_WAKE must be "poll" or "notify"`)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	o, err := open(ctx, log, s)
	if err != nil {
		return err
	}
	defer o.close()
	store, kinds := o.store, o.kinds
	if o.pg != nil {
		o.pg.Notify = wakeMode == "notify"
	}
	allowed := map[string]bool{}
	for _, k := range handlers.Kinds {
		allowed[k] = true
	}

	m := metrics.New(store)
	srv := &http.Server{
		Addr:              envString("JOBQ_ADDR", ":8080"),
		Handler:           (&api.Server{Store: store, Secret: secret, Kinds: allowed, Metrics: m}).Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	cfg := s.pool(log)
	cfg.Metrics = m
	var wg sync.WaitGroup
	if wakeMode == "notify" {
		wake := make(chan struct{}, 1)
		cfg.Wake = wake
		wg.Add(1)
		go func() {
			defer wg.Done()
			store.Listen(ctx, wake, func(err error) { log.Warn("listen failed; polling until it's back", "err", err) })
		}()
	}
	p := worker.New(store, kinds, cfg)

	wg.Add(1)
	go func() {
		defer wg.Done()
		p.Run(ctx)
	}()

	wg.Add(1)
	go func() { // cleanup: now, then hourly
		defer wg.Done()
		for {
			cleanup(ctx, log, store, s.keepDone)
			select {
			case <-time.After(time.Hour):
			case <-ctx.Done():
				return
			}
		}
	}()

	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr, "store", s.store, "workers", s.workers, "batch", s.batch, "wake", wakeMode)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errc <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down: no new jobs, finishing the running ones", "grace", s.grace)
	case err := <-errc:
		stop()
		return err
	}

	shutdown, cancel := context.WithTimeout(context.Background(), s.grace)
	defer cancel()
	_ = srv.Shutdown(shutdown)

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		log.Info("stopped cleanly")
	case <-shutdown.Done():
		log.Warn("grace period over; unfinished jobs return to the queue when their leases expire")
	}
	return nil
}

// drain runs what's due and exits: for a scheduled job (GitHub Actions)
// instead of a service that stays up. No HTTP server, so producers enqueue
// straight into the table. It stops when a claim finds nothing due and
// nothing is running, or after JOBQ_DRAIN_MAX; jobs already running then get
// JOBQ_LEASE (JOBQ_GRACE if longer, or after a signal) to finish. A retry
// scheduled for later waits for the next run.
func drain(log *slog.Logger) error {
	s, err := load()
	if err != nil {
		return err
	}
	maxRun, err := envDuration("JOBQ_DRAIN_MAX", 4*time.Minute)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	o, err := open(ctx, log, s)
	if err != nil {
		return err
	}
	defer o.close()
	store, kinds := o.store, o.kinds

	cfg := s.pool(log)
	cfg.ExitWhenIdle = true
	runCtx, cancel := context.WithTimeout(ctx, maxRun)
	defer cancel()
	began := time.Now()
	done := make(chan struct{})
	go func() {
		worker.New(store, kinds, cfg).Run(runCtx)
		close(done)
	}()
	log.Info("draining", "workers", s.workers, "batch", s.batch, "max", maxRun.String())

	select {
	case <-done:
		log.Info("drained: nothing due is left", "took", time.Since(began).Round(time.Millisecond).String())
	case <-runCtx.Done():
		// At the deadline, nothing is waiting on this process, so give running
		// jobs their whole lease (each handler stops before it ends): a push cut
		// off mid-send would be sent again by the next claim. A signal gets JOBQ_GRACE.
		wait := s.grace
		if ctx.Err() == nil {
			wait = max(s.grace, s.lease)
		}
		log.Info("stopping: no new jobs, finishing the running ones", "why", context.Cause(runCtx), "wait", wait.String())
		select {
		case <-done:
		case <-time.After(wait):
			log.Warn("grace period over; unfinished jobs return to the queue when their leases expire")
			return nil
		}
	}
	if ctx.Err() != nil {
		return nil // stopped by a signal: leave the cleanup for next time
	}

	cctx, ccancel := context.WithTimeout(ctx, time.Minute)
	defer ccancel()
	cleanup(cctx, log, store, s.keepDone)
	left(cctx, log, store)
	return nil
}

// cleanup deletes done jobs older than keep; dead ones stay.
func cleanup(ctx context.Context, log *slog.Logger, store queue.Backend, keep time.Duration) {
	n, err := store.DeleteDone(ctx, keep, 1000)
	switch {
	case err != nil && ctx.Err() == nil:
		log.Warn("deleting old done jobs failed; trying again next time", "err", err, "deleted", n)
	case n > 0:
		log.Info("deleted old done jobs", "deleted", n, "older_than", keep.String())
	}
}

// left logs what a drain leaves for the next run.
func left(ctx context.Context, log *slog.Logger, store queue.Backend) {
	c, err := store.Counts(ctx)
	if err != nil {
		log.Warn("counting what's left failed", "err", err)
		return
	}
	log.Info("left in the queue", "due", c.Due, "later", c.Later, "running", c.Running, "dead", c.Dead)
}

func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive number", key)
	}
	return n, nil
}

func envDuration(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil || d <= 0 {
		return 0, fmt.Errorf("%s must be a duration like 30s", key)
	}
	return d, nil
}
