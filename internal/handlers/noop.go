// Package handlers holds the job kinds jobq can run.
package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/KunalShukla-Al/jobq/internal/queue"
	"github.com/KunalShukla-Al/jobq/internal/worker"
)

// NoopPayload tells the noop handler how to behave, for tests and load tests.
type NoopPayload struct {
	SleepMS   int     `json:"sleep_ms"`   // pretend to work this long
	FailTimes int     `json:"fail_times"` // fail (retryably) on the first N attempts
	FailRate  float64 `json:"fail_rate"`  // and fail this share of the rest at random
	Permanent bool    `json:"permanent"`  // fail permanently instead
	Panic     bool    `json:"panic"`      // panic instead of returning
}

// Noop does nothing, in whatever way its payload asks.
var Noop = worker.HandlerFunc(func(ctx context.Context, j queue.Job) error {
	var p NoopPayload
	if len(j.Payload) > 0 {
		if err := json.Unmarshal(j.Payload, &p); err != nil {
			return worker.Permanent(fmt.Errorf("bad noop payload: %w", err))
		}
	}
	if p.SleepMS > 0 {
		select {
		case <-time.After(time.Duration(p.SleepMS) * time.Millisecond):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if p.Panic {
		panic("noop asked to panic")
	}
	if j.Attempts <= p.FailTimes || (p.FailRate > 0 && rand.Float64() < p.FailRate) {
		err := fmt.Errorf("noop failing on attempt %d, as asked", j.Attempts)
		if p.Permanent {
			return worker.Permanent(err)
		}
		return err
	}
	return nil
})

// Kinds is every kind jobq knows, whether or not this process can run it:
// what the API accepts. A process claims only the kinds it has a handler
// for, so a webpush job waits for one that has the push keys.
var Kinds = []string{"noop", "webpush"}

// All is the kinds that need no settings. webpush needs keys: see NewWebPush.
func All() map[string]worker.Handler {
	return map[string]worker.Handler{"noop": Noop}
}
