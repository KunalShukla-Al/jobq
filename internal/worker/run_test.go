package worker

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/testdb"
)

// Claim only takes the pool's kinds, so a job without a handler can't reach
// run that way. If one does anyway, it's recorded as dead, not left running.
func TestAJobWithNoHandlerIsKilled(t *testing.T) {
	t.Parallel()
	s := &queue.Store{Pool: testdb.New(t)}
	ctx := context.Background()
	id, _, err := s.Enqueue(ctx, queue.NewJob{Kind: "nope", IdempotencyKey: "k"})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := s.Claim(ctx, 1, time.Minute, []string{"nope"})
	if err != nil || len(jobs) != 1 {
		t.Fatalf("claim: %v %v", jobs, err)
	}
	p := New(s, map[string]Handler{"noop": HandlerFunc(func(context.Context, queue.Job) error { return nil })},
		Config{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	p.run(jobs[0])
	j, _ := s.Get(ctx, id)
	if j.Status != queue.Dead || j.LastError == nil || !strings.Contains(*j.LastError, "no handler") {
		t.Fatalf("status=%s last_error=%v, want dead with no handler", j.Status, j.LastError)
	}
}
