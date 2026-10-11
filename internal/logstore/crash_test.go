package logstore

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

// The test that matters most (DESIGN-v2.md, "Recovery"): a process writing
// as fast as it can is killed with SIGKILL at a random moment, again and
// again, on the same log. After each kill the log must open, every
// acknowledged change must be there, nothing may be duplicated, and every
// job must be in a state its history allows.
//
// The writer is a child process: this test binary run again with
// JOBQ_CRASH_DIR set. Its goroutines each own one kind and one job at a
// time (enqueue → claim → complete, retry or kill → next job), so each job's
// history is a sequence, and the child prints a line before each change
// ("B") and after its answer ("A"). At most one change per goroutine is in
// flight when the kill lands; it may or may not have happened.
//
// SIGKILL tests a process crash: the OS keeps what was written, so it
// checks the replay, the truncation and the bookkeeping, not the fsyncs.
// A power cut would also drop what wasn't fsynced; testing that needs a
// disk that can be cut off (a VM or a fault-injecting block device).

const crashWorkers = 8

// Small segments and an eager compaction rule, so kills also land in the
// middle of segment rolls and compactions.
func crashOptions(dir string) Options {
	return Options{Dir: dir, NoFullSync: true, SegmentSize: 16 << 10, CompactAt: 64 << 10}
}

func TestSIGKILLRecovery(t *testing.T) {
	if dir := os.Getenv("JOBQ_CRASH_DIR"); dir != "" {
		crashChild(dir, os.Getenv("JOBQ_CRASH_ROUND"))
		return
	}
	rounds := 40
	if testing.Short() {
		rounds = 10
	}
	if v, err := strconv.Atoi(os.Getenv("JOBQ_CRASH_ROUNDS")); err == nil && v > 0 {
		rounds = v
	}
	dir := t.TempDir()
	settled := map[string]model{} // jobs of earlier rounds, as found after their kill
	var report crashReport
	r := rand.New(rand.NewPCG(uint64(time.Now().UnixNano()), 7))

	for round := range rounds {
		lines := runAndKill(t, dir, round, 1+r.IntN(800))
		s, err := Open(crashOptions(dir))
		if err != nil {
			t.Fatalf("round %d: the log didn't open after the kill: %v", round, err)
		}
		rec := s.Recovered()
		jobs := snapshot(t, s)
		_ = s.Close()

		byKey := map[string]queue.Job{}
		for _, j := range jobs {
			if _, dup := byKey[j.IdempotencyKey]; dup {
				t.Fatalf("round %d: two jobs with key %s", round, j.IdempotencyKey)
			}
			byKey[j.IdempotencyKey] = j
		}
		acked, inflight, applied := check(t, round, lines, byKey, settled)
		for key, m := range settled {
			j, ok := byKey[key]
			if !m.allows(j, ok) {
				t.Fatalf("round %d: job %s from an earlier round changed: %+v, was %v", round, key, j, m)
			}
		}
		report.Rounds++
		report.Acked += acked
		report.InFlight += inflight
		report.InFlightApplied += applied
		report.Jobs = len(jobs)
		report.Segments = rec.Segments
		if rec.Truncated > 0 {
			report.Torn++
		}
		report.Recovery = append(report.Recovery, rec.Took.Seconds()*1000)
	}
	t.Logf("%d kills: %d acknowledged changes all found; %d in flight at the kill, %d of them had happened; %d torn tails cut; %d jobs at the end",
		report.Rounds, report.Acked, report.InFlight, report.InFlightApplied, report.Torn, report.Jobs)
	if path := os.Getenv("JOBQ_CRASH_REPORT"); path != "" {
		b, _ := json.MarshalIndent(report, "", "  ")
		if err := os.WriteFile(path, b, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

type crashReport struct {
	Rounds          int       `json:"rounds"`
	Acked           int       `json:"acknowledged_changes"`
	InFlight        int       `json:"in_flight_at_kill"`
	InFlightApplied int       `json:"in_flight_that_happened"`
	Torn            int       `json:"torn_tails_cut"`
	Jobs            int       `json:"jobs_at_end"`
	Segments        int       `json:"segments_at_end"`
	Recovery        []float64 `json:"recovery_ms"`
}

// runAndKill starts the child, lets it acknowledge `after` changes (or run
// for two seconds), kills it with SIGKILL, and returns every line it printed.
func runAndKill(t *testing.T, dir string, round, after int) []string {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^TestSIGKILLRecovery$", "-test.count=1")
	cmd.Env = append(os.Environ(), "JOBQ_CRASH_DIR="+dir, fmt.Sprint("JOBQ_CRASH_ROUND=", round))
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var lines []string
	acks := 0
	enough := make(chan struct{})
	eof := make(chan struct{})
	go func() {
		defer close(eof)
		sc := bufio.NewScanner(out)
		for sc.Scan() {
			mu.Lock()
			lines = append(lines, sc.Text())
			if strings.HasPrefix(sc.Text(), "A ") {
				if acks++; acks == after {
					close(enough)
				}
			}
			mu.Unlock()
		}
	}()
	select {
	case <-enough:
	case <-eof:
	case <-time.After(2 * time.Second):
	}
	_ = cmd.Process.Kill() // SIGKILL: no deferred functions, no flushing, no goodbye
	<-eof
	err = cmd.Wait()
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != -1 {
		t.Fatalf("round %d: the child wasn't killed but ended on its own (%v); its output:\n%s", round, err, strings.Join(lines, "\n"))
	}
	return lines
}

// model is what a job may be: its status and attempts, or absent.
type model struct {
	status   string // "" = no job
	attempts int
}

func (m model) String() string { return fmt.Sprintf("%s/%d", m.status, m.attempts) }

// allows says whether the job found matches. A done job may be gone: the
// child's cleanup goroutine deletes done jobs.
func (m model) allows(j queue.Job, found bool) bool {
	if !found {
		return m.status == "" || m.status == queue.Done
	}
	return j.Status == m.status && j.Attempts == m.attempts
}

// next is the job after one change.
func (m model) next(op string) model {
	switch op {
	case "enqueue":
		return model{queue.Ready, 0}
	case "claim":
		return model{queue.Running, m.attempts + 1}
	case "complete":
		return model{queue.Done, m.attempts}
	case "retry":
		return model{queue.Ready, m.attempts}
	case "kill":
		return model{queue.Dead, m.attempts}
	}
	panic(op)
}

// check replays each goroutine's printed history and compares every job's
// model with what recovery found. It returns how many changes were
// acknowledged, how many were in flight at the kill, and how many of those
// turned out to have happened.
func check(t *testing.T, round int, lines []string, found map[string]queue.Job, settled map[string]model) (acked, inflight, applied int) {
	t.Helper()
	type op struct {
		name, key string
		done      bool
	}
	ops := make([][]op, crashWorkers)
	for _, l := range lines {
		f := strings.Fields(l)
		if len(f) < 3 {
			continue
		}
		g, _ := strconv.Atoi(f[1])
		switch f[0] {
		case "B":
			ops[g] = append(ops[g], op{name: f[2], key: f[3]})
		case "A":
			last := &ops[g][len(ops[g])-1]
			if last.name != f[2] {
				t.Fatalf("round %d: worker %d answered %s while %s was in flight", round, g, f[2], last.name)
			}
			last.done = true
		case "E":
			t.Fatalf("round %d: the child failed: %s", round, l)
		}
	}
	for g, history := range ops {
		models := map[string]model{}
		var order []string
		for i, o := range history {
			if _, seen := models[o.key]; !seen {
				order = append(order, o.key)
			}
			before := models[o.key]
			after := before.next(o.name)
			if o.done {
				acked++
				models[o.key] = after
				continue
			}
			if i != len(history)-1 {
				t.Fatalf("round %d: worker %d started %s before %s was answered", round, g, history[i+1].name, o.name)
			}
			// In flight: it may or may not have happened.
			inflight++
			j, ok := found[o.key]
			switch {
			case before.allows(j, ok):
				models[o.key] = before
			case after.allows(j, ok):
				applied++
				models[o.key] = after
			default:
				t.Fatalf("round %d: job %s is %+v (found %v), but its history allows only %v or %v",
					round, o.key, j, ok, before, after)
			}
		}
		for _, key := range order {
			j, ok := found[key]
			if !models[key].allows(j, ok) {
				t.Fatalf("round %d: job %s is %s/%d (found %v), but every change to it up to %v was acknowledged",
					round, key, j.Status, j.Attempts, ok, models[key])
			}
			settled[key] = models[key]
		}
	}
	return acked, inflight, applied
}

// crashChild is the process that gets killed: crashWorkers goroutines
// changing jobs as fast as they can, and one deleting done jobs.
func crashChild(dir, round string) {
	fail := func(err error) {
		fmt.Printf("E %v\n", err)
		os.Exit(3)
	}
	s, err := Open(crashOptions(dir))
	if err != nil {
		fail(err)
	}
	go func() {
		for {
			if _, err := s.DeleteDone(ctx, 0, 50); err != nil {
				fail(err)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}()
	for g := range crashWorkers {
		go func() {
			r := rand.New(rand.NewPCG(uint64(g), uint64(time.Now().UnixNano())))
			kind := fmt.Sprintf("r%s-g%d", round, g)
			// Every line is one write to stdout, which isn't buffered: a line
			// printed before the kill reaches the parent.
			say := func(what string, g int, op, key string) { fmt.Printf("%s %d %s %s\n", what, g, op, key) }
			for n := 0; ; n++ {
				key := fmt.Sprintf("%s-%d", kind, n)
				say("B", g, "enqueue", key)
				if _, _, err := s.Enqueue(ctx, queue.NewJob{Kind: kind, IdempotencyKey: key}); err != nil {
					fail(err)
				}
				say("A", g, "enqueue", key)
				for {
					say("B", g, "claim", key)
					jobs, err := s.Claim(ctx, 1, time.Hour, []string{kind})
					if err != nil || len(jobs) != 1 || jobs[0].IdempotencyKey != key {
						fail(fmt.Errorf("claim of %s: %v %v", key, ids(jobs), err))
					}
					say("A", g, "claim", key)
					op := []string{"complete", "complete", "retry", "kill"}[r.IntN(4)]
					say("B", g, op, key)
					switch op {
					case "complete":
						err = s.Complete(ctx, jobs[0])
					case "retry":
						err = s.Retry(ctx, jobs[0], 0, errors.New("again"))
					case "kill":
						err = s.Kill(ctx, jobs[0], errors.New("gave up"))
					}
					if err != nil {
						fail(err)
					}
					say("A", g, op, key)
					if op != "retry" {
						break
					}
				}
			}
		}()
	}
	select {} // until killed
}
