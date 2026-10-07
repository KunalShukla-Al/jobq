package worker

import (
	"errors"
	"testing"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
)

func TestPersistRetriesOnlyWhatRetryingCanFix(t *testing.T) {
	waits := []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond}
	down := errors.New("too many clients already")
	for _, c := range []struct {
		name      string
		errs      []error // what each try returns; the last repeats
		wantErr   error
		wantTries int
	}{
		{"works first time", []error{nil}, nil, 1},
		{"database back on the third try", []error{down, down, nil}, nil, 3},
		{"database never back", []error{down}, down, 4},
		{"lease lost: no point retrying", []error{queue.ErrLostLease}, queue.ErrLostLease, 1},
		{"lease lost after a blip", []error{down, queue.ErrLostLease}, queue.ErrLostLease, 2},
	} {
		t.Run(c.name, func(t *testing.T) {
			tries := 0
			err, retried := persist(func() error {
				e := c.errs[min(tries, len(c.errs)-1)]
				tries++
				return e
			}, waits)
			if !errors.Is(err, c.wantErr) || (c.wantErr == nil && err != nil) || tries != c.wantTries {
				t.Fatalf("err=%v after %d tries, want %v after %d", err, tries, c.wantErr, c.wantTries)
			}
			if retried != (tries > 1) {
				t.Fatalf("retried=%v after %d tries", retried, tries)
			}
		})
	}
}
