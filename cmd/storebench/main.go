// Command storebench measures jobq's two storage backends side by side on
// this machine: the same Go calls (queue.Backend), the same workloads, no
// HTTP in the way. It answers "what does owning the storage buy or cost?"
//
//	go run ./cmd/storebench -dsn postgres://localhost/postgres -out loadtest/results/store.json
//
// Workloads, each run for -for (5s) per setting:
//
//	enqueue   P producers enqueueing new jobs as fast as they can
//	work      W workers claiming 10 at a time and completing each, from a full queue
//	recover   (log only) how long opening a log of N jobs takes
//
// Settings: the log with group commit, with an fsync per request, and with
// an fsync every 10ms; Postgres as installed, and with synchronous_commit
// off. On macOS each fsync-based setting runs twice: a plain fsync, and one
// that also flushes the drive's cache (see "Durability" in the README).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/KunalShukla-Al/jobq/internal/logstore"
	"github.com/KunalShukla-Al/jobq/internal/migrate"
	"github.com/KunalShukla-Al/jobq/internal/queue"
)

var ctx = context.Background()

type result struct {
	Store     string  `json:"store"`
	Setting   string  `json:"setting"`
	Workload  string  `json:"workload"`
	Clients   int     `json:"clients"`
	Ops       int64   `json:"ops"`
	Seconds   float64 `json:"seconds"`
	PerSecond float64 `json:"per_second"`
	P50ms     float64 `json:"p50_ms"`
	P99ms     float64 `json:"p99_ms"`
	Jobs      int     `json:"jobs,omitempty"`  // recover
	Bytes     int64   `json:"bytes,omitempty"` // recover
}

// setting is one way to run one backend: open gives a fresh, empty store.
type setting struct {
	store, name string
	open        func() (queue.Backend, func())
}

func main() {
	dsn := flag.String("dsn", "", "Postgres server to create the benchmark database on (empty: log only)")
	dir := flag.String("dir", "", "where log stores go (default: a temp dir)")
	dur := flag.Duration("for", 5*time.Second, "how long each measurement runs")
	only := flag.String("only", "", "run only settings whose name contains this")
	recoverJobs := flag.String("recover", "100000,1000000", "log sizes (jobs) for the recovery test; empty skips it")
	out := flag.String("out", "", "write the results as JSON here")
	writethrough := flag.Bool("writethrough", false, "also run Postgres with wal_sync_method = fsync_writethrough "+
		"(ALTER SYSTEM on the server, reset afterwards; macOS)")
	flag.Parse()

	root := *dir
	if root == "" {
		var err error
		if root, err = os.MkdirTemp("", "storebench"); err != nil {
			log.Fatal(err)
		}
		defer os.RemoveAll(root)
	}

	var settings []setting
	n := 0
	logSetting := func(name string, opts logstore.Options) setting {
		return setting{"log", name, func() (queue.Backend, func()) {
			n++
			opts.Dir = fmt.Sprintf("%s/log-%d", root, n)
			s, err := logstore.Open(opts)
			if err != nil {
				log.Fatal(err)
			}
			return s, func() { _ = s.Close(); _ = os.RemoveAll(opts.Dir) }
		}}
	}
	settings = append(settings,
		logSetting("group commit, fsync", logstore.Options{NoFullSync: true}),
		logSetting("fsync per request, fsync", logstore.Options{Sync: logstore.SyncAlways, NoFullSync: true}),
		logSetting("fsync every 10ms", logstore.Options{Sync: logstore.SyncInterval}),
	)
	if runtime.GOOS == "darwin" {
		settings = append(settings,
			logSetting("group commit, full fsync", logstore.Options{}),
			logSetting("fsync per request, full fsync", logstore.Options{Sync: logstore.SyncAlways}),
		)
	}
	if *dsn != "" {
		db := benchDatabase(*dsn)
		pg := func(name string, params map[string]string) setting {
			return setting{"postgres", name, func() (queue.Backend, func()) {
				cfg, err := pgxpool.ParseConfig(db)
				if err != nil {
					log.Fatal(err)
				}
				cfg.MaxConns = 80
				for k, v := range params {
					cfg.ConnConfig.RuntimeParams[k] = v
				}
				pool, err := pgxpool.NewWithConfig(ctx, cfg)
				if err != nil {
					log.Fatal(err)
				}
				if _, err := pool.Exec(ctx, "truncate jobq.jobs restart identity"); err != nil {
					log.Fatal(err)
				}
				return &queue.Store{Pool: pool}, pool.Close
			}}
		}
		method := pgSetting(db, "wal_sync_method")
		settings = append(settings,
			pg("as installed (wal_sync_method "+method+")", nil),
			pg("synchronous_commit off", map[string]string{"synchronous_commit": "off"}),
		)
		if *writethrough {
			// The server-wide fsync method, so it's set for this run and reset after.
			wt := pg("wal_sync_method fsync_writethrough", nil)
			open := wt.open
			wt.open = func() (queue.Backend, func()) {
				alterSystem(db, "alter system set wal_sync_method = 'fsync_writethrough'")
				s, done := open()
				return s, func() { done(); alterSystem(db, "alter system reset wal_sync_method") }
			}
			settings = append(settings, wt)
		}
	}

	var results []result
	add := func(r result) {
		results = append(results, r)
		if r.Workload == "recover" {
			fmt.Printf("log       recover %8d jobs (%5.1f MB): %7.1f ms\n", r.Jobs, float64(r.Bytes)/1e6, r.Seconds*1000)
			return
		}
		fmt.Printf("%-9s %-42s %-8s %3d  %9.0f/s  p50 %7.2f ms  p99 %7.2f ms\n",
			r.Store, r.Setting, r.Workload, r.Clients, r.PerSecond, r.P50ms, r.P99ms)
	}
	for _, st := range settings {
		if *only != "" && !strings.Contains(st.store+" "+st.name, *only) {
			continue
		}
		for _, p := range []int{1, 16, 64} {
			add(enqueue(st, p, *dur))
		}
		for _, w := range []int{1, 16} {
			add(work(st, w, *dur))
		}
	}
	if *recoverJobs != "" && (*only == "" || strings.Contains("log recover", *only)) {
		for _, f := range strings.Split(*recoverJobs, ",") {
			var jobs int
			fmt.Sscan(f, &jobs)
			add(recoverTime(root, jobs))
		}
	}
	if *out != "" {
		b, _ := json.MarshalIndent(map[string]any{
			"when": time.Now().Format(time.RFC3339), "goos": runtime.GOOS, "cpus": runtime.NumCPU(),
			"for_seconds": dur.Seconds(), "results": results,
		}, "", "  ")
		if err := os.WriteFile(*out, append(b, '\n'), 0o644); err != nil {
			log.Fatal(err)
		}
	}
}

// benchDatabase creates (or empties) the jobq_bench database and migrates it.
func benchDatabase(dsn string) string {
	admin, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	_, _ = admin.Exec(ctx, "drop database if exists jobq_bench with (force)")
	if _, err := admin.Exec(ctx, "create database jobq_bench"); err != nil {
		log.Fatal(err)
	}
	_ = admin.Close(ctx)
	db := strings.Replace(dsn, "/"+dbName(dsn), "/jobq_bench", 1)
	pool, err := pgxpool.New(ctx, db)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		log.Fatal(err)
	}
	return db
}

func dbName(dsn string) string {
	cfg, err := pgx.ParseConfig(dsn)
	if err != nil {
		log.Fatal(err)
	}
	return cfg.Database
}

func alterSystem(dsn, sql string) {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)
	if _, err := conn.Exec(ctx, sql); err != nil {
		log.Fatal(err)
	}
	if _, err := conn.Exec(ctx, "select pg_reload_conf()"); err != nil {
		log.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond) // the reload reaches every backend
}

func pgSetting(dsn, name string) string {
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close(ctx)
	var v string
	if err := conn.QueryRow(ctx, "show "+name).Scan(&v); err != nil {
		log.Fatal(err)
	}
	return v
}

// run starts n clients calling op until d has passed and collects every call's latency.
func run(n int, d time.Duration, op func(client int, i int) error) (int64, float64, []time.Duration) {
	var wg sync.WaitGroup
	var count atomic.Int64
	lat := make([][]time.Duration, n)
	began := time.Now()
	deadline := began.Add(d)
	for c := range n {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; time.Now().Before(deadline); i++ {
				t := time.Now()
				if err := op(c, i); err != nil {
					if err == errStop {
						return
					}
					log.Fatal(err)
				}
				lat[c] = append(lat[c], time.Since(t))
				count.Add(1)
			}
		}()
	}
	wg.Wait()
	return count.Load(), time.Since(began).Seconds(), slices.Concat(lat...)
}

var errStop = fmt.Errorf("nothing left")

func percentile(lat []time.Duration, p float64) float64 {
	if len(lat) == 0 {
		return 0
	}
	slices.Sort(lat)
	i := int(math.Ceil(p*float64(len(lat)))) - 1
	return float64(lat[max(i, 0)].Microseconds()) / 1000
}

func summarise(st setting, workload string, clients int, ops int64, secs float64, lat []time.Duration) result {
	return result{Store: st.store, Setting: st.name, Workload: workload, Clients: clients, Ops: ops,
		Seconds: secs, PerSecond: float64(ops) / secs, P50ms: percentile(lat, 0.50), P99ms: percentile(lat, 0.99)}
}

func enqueue(st setting, producers int, d time.Duration) result {
	s, done := st.open()
	defer done()
	ops, secs, lat := run(producers, d, func(c, i int) error {
		_, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprintf("%d-%d", c, i),
			Payload: json.RawMessage(`{"to":"someone","n":42}`)})
		return err
	})
	return summarise(st, "enqueue", producers, ops, secs, lat)
}

// work fills the queue, then measures claim + complete. One op is one job
// done; its latency is the claim's share plus its own complete.
func work(st setting, workers int, d time.Duration) result {
	s, done := st.open()
	defer done()
	// Fill: enough that the workers don't run dry, written with 64 producers.
	fill := 60_000
	run(64, time.Hour, func(c, i int) error {
		if i >= fill/64 {
			return errStop
		}
		_, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprintf("%d-%d", c, i)})
		return err
	})
	kinds := []string{"noop"}
	var mu sync.Mutex
	var lat []time.Duration
	var count atomic.Int64
	var wg sync.WaitGroup
	began := time.Now()
	deadline := began.Add(d)
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			for time.Now().Before(deadline) {
				t := time.Now()
				jobs, err := s.Claim(ctx, 10, time.Minute, kinds)
				if err != nil {
					log.Fatal(err)
				}
				if len(jobs) == 0 {
					break
				}
				share := time.Since(t) / time.Duration(len(jobs))
				for _, j := range jobs {
					t := time.Now()
					if err := s.Complete(ctx, j); err != nil {
						log.Fatal(err)
					}
					mine = append(mine, share+time.Since(t))
					count.Add(1)
				}
			}
			mu.Lock()
			lat = append(lat, mine...)
			mu.Unlock()
		}()
	}
	wg.Wait()
	return summarise(st, "work", workers, count.Load(), time.Since(began).Seconds(), lat)
}

// recoverTime writes a log of n enqueued jobs, closes it, and times opening it.
func recoverTime(root string, n int) result {
	dir := fmt.Sprintf("%s/recover-%d", root, n)
	_ = os.RemoveAll(dir)
	s, err := logstore.Open(logstore.Options{Dir: dir, Sync: logstore.SyncInterval, Interval: 100 * time.Millisecond})
	if err != nil {
		log.Fatal(err)
	}
	run(64, time.Hour, func(c, i int) error {
		if i >= n/64 {
			return errStop
		}
		_, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "noop", IdempotencyKey: fmt.Sprintf("%d-%d", c, i),
			Payload: json.RawMessage(`{"to":"someone","n":42}`)})
		return err
	})
	size, _, _ := s.Size(ctx)
	if err := s.Close(); err != nil {
		log.Fatal(err)
	}
	var best time.Duration
	var rec logstore.Recovery
	for range 3 { // best of 3: the file is in the OS cache after the first
		began := time.Now()
		s, err := logstore.Open(logstore.Options{Dir: dir})
		if err != nil {
			log.Fatal(err)
		}
		took := time.Since(began)
		rec = s.Recovered()
		_ = s.Close()
		if best == 0 || took < best {
			best = took
		}
	}
	_ = os.RemoveAll(dir)
	return result{Store: "log", Setting: "open after a clean close", Workload: "recover", Ops: int64(rec.Records),
		Seconds: best.Seconds(), PerSecond: float64(rec.Records) / best.Seconds(),
		P50ms: float64(best.Microseconds()) / 1000, Jobs: rec.Jobs, Bytes: size}
}
