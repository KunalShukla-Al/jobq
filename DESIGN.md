# jobq: a Postgres-backed job queue in Go

**Status:** weeks 1–3 built (queue, workers, API, tests, metrics, dashboard, load test); week 4 code done (web push, drain mode on GitHub Actions, cleanup). Not delivering yet: the AcadKit rollout is in shadow mode, not switched on, and there's no release yet · **Author:** Kunal Shukla · **Date:** 2 Oct 2026 · **Planned build:** 13 Oct – 9 Nov 2026 (the code was finished early, 2–5 Oct)

## Why

AcadKit's push reminders run as one Supabase Edge Function that `pg_cron` calls every 10 minutes
(`supabase/functions/send-reminders`).
For each reminder it first inserts a `sent_notifications` row as the dedupe key, then sends web pushes
one by one. That has three problems:

1. **It can lose reminders.** The reminder is marked sent *before* it's delivered. If the push service
   times out or returns a 5xx, the row already exists, so the reminder is never retried. Delivery is
   at-most-once.
2. **It can't grow.** Every push is sent serially inside one function run, which has a wall-clock limit.
   More subscribers means a longer run, until it gets cut off part-way.
3. **It's invisible.** Nothing records what was sent, what failed, or how long anything took.

jobq separates *deciding* what to send (which stays in the Edge Function, in TypeScript) from
*delivering* it (a Go service with retries, idempotency and metrics). Then it measures how far one
small instance on Postgres can be pushed.

## Goals and non-goals

**Goals:** at-least-once execution with idempotent handlers. Retries with exponential backoff and
jitter, and a dead-letter state. Idempotency keys at enqueue. Several workers sharing one queue
safely. Prometheus metrics. Load-tested at 1k and 10k jobs, with a write-up of what broke.

**Non-goals:** exactly-once (no such thing over a network; idempotency gets the same effect),
priorities beyond `run_at` ordering, multi-region, a UI, and replacing `pg_cron` (it stays the clock).

## Design

```
pg_cron ──> send-reminders (Edge Fn, TS)        jobq (Go)
            decides who gets what  ──POST /jobs──> HTTP API ──INSERT──> jobq.jobs (Postgres)
                                                  workers  <──SKIP LOCKED── claim
                                                  handler: web push ──> push service
                                                  /metrics ──> Prometheus ──> Grafana
```

(As built in week 4, the production design has no API in the path: AcadKit inserts jobs with a SQL function
and a scheduled `jobq drain` delivers them. See the rollout below.)

**Table** (its own schema, `jobq`, not exposed to the Supabase REST API, so no RLS surprises):

```sql
create table jobq.jobs (
  id              bigint generated always as identity primary key,
  kind            text not null,                    -- 'webpush', 'noop' (load tests)
  payload         jsonb not null,
  idempotency_key text not null unique,             -- e.g. 'webpush:<device>:<kind>:<ref>'
  status          text not null default 'ready',    -- ready | running | done | dead
  attempts        int  not null default 0,
  max_attempts    int  not null default 8,
  run_at          timestamptz not null default now(),
  locked_until    timestamptz,                      -- lease; an expired one is reclaimed
  lock_token      uuid,                             -- which claim holds it (see below)
  last_error      text,
  created_at      timestamptz not null default now(),
  finished_at     timestamptz
);
create index jobs_ready on jobq.jobs (run_at) where status in ('ready', 'running');
```

- **Enqueue:** `INSERT … ON CONFLICT (idempotency_key) DO NOTHING`, so a re-run of the planner can't
  double-send. The API answers `201` for a new job and `200` with the existing one's id otherwise. The
  same key with a *different* kind or payload is `409`, because returning the old job would hide a
  producer bug. (Ids can have gaps: a duplicate insert still takes an id before `ON CONFLICT` skips it.)
- **Claim:** in one statement, select up to N jobs `where status = 'ready' and run_at <= now()`, or
  `status = 'running'` with an expired lease (a crashed worker), `FOR UPDATE SKIP LOCKED`. Mark them
  `running` with `locked_until = now() + lease`, a fresh `lock_token`, and `attempts + 1`.
  Counting the attempt at claim time means a job that keeps crashing its worker still runs out of
  attempts. Finishing a job (done, retry or dead) requires the token, so a worker whose lease expired
  and whose job was claimed again can't overwrite the new claim's outcome.
- **Finish:**
  - Success → `done`.
  - Failure → `attempts + 1`, with `run_at = now() + min(base · 2^attempts, cap) ± jitter`.
  - Too many attempts, or a permanent error (a 404/410 push subscription, a bad payload) → `dead`.
    The handler returns *retryable* or *permanent*.
- **Waking workers:** polling with backoff. `LISTEN/NOTIFY` is optional (`JOBQ_WAKE=notify`): the load
  test found it cut latency by ~25ms at a steady 20 jobs/s and slowed enqueue by 37% under a burst, with
  no change in job throughput (README, "What broke at 10k jobs").
- **Handlers:** `webpush` sends one notification and marks `sent_notifications` *after* success,
  fixing problem 1. `noop` exists for load tests and can be told to sleep or fail at a given rate.
- **Shutdown:** on SIGTERM, stop claiming and finish in-flight jobs within the grace period. Anything
  unfinished goes back to the queue when its lease expires.

**Metrics:**
- `jobq_enqueued_total{kind}`
- `jobq_finished_total{kind,result}`, where result is done/retry/dead
- `jobq_job_duration_seconds{kind}`, a histogram
- `jobq_queue_depth{status}`
- `jobq_oldest_ready_age_seconds`, the "are we falling behind" number
- `jobq_claim_batch_size`
- `jobq_job_latency_seconds{kind}`, enqueue to done (added in week 2: the load test's main number)
- `jobq_lease_lost_total` (added in week 2: should stay 0)

**API:**
- `POST /jobs`, with a shared secret, the same idea as today's `x-cron-secret`
- `GET /jobs/{id}`
- `GET /healthz`
- `GET /metrics`

## Rollout in AcadKit (behind a flag)

**How it runs (decided in week 4, revised 5 Oct):** a GitHub Actions workflow runs `jobq drain` against
the Supabase Postgres: it runs what's due, then exits. No always-on host. AcadKit's Edge Function starts
it (`workflow_dispatch`) right after it queues work, because GitHub's schedule turned out not to be
something to rely on: the first 5-minute schedule never fired at all. The schedule stays as a backup. Because nothing is listening
between runs, AcadKit doesn't call `POST /jobs`. It enqueues in the database, through a function its own
migration creates:

```
pg_cron ──> send-reminders (Edge Fn) ──rpc──> public.jobq_enqueue(jobs) ──INSERT──> jobq.jobs
send-reminders ──workflow_dispatch──> GitHub Actions (backup: 5-min schedule)
GitHub Actions ──> jobq drain ──claim──> jobq.jobs
                                   ──webpush──> push service, then ──> public.sent_notifications
```

- `public.jobq_enqueue(jobs jsonb) returns integer`: an array of `{kind, payload, idempotency_key, run_at?}`,
  each inserted with `ON CONFLICT (idempotency_key) DO NOTHING`; returns how many were new. `plpgsql`, so it
  can be created before jobq has made its schema; `SECURITY DEFINER` with an empty `search_path`; callable by
  `service_role` only.
- `webpush`: key `webpush:<device_id>:<kind>:<ref>`, payload `{device_id, kind, ref, title, body, url}`.
- `noop` (shadow): key `shadow:<device_id>:<kind>:<ref>`, payload `{}`.
- `public.jobq_due() returns integer`: ready jobs whose `run_at` has come, plus running ones whose lease
  ran out, the same rows a claim would pick. Same rules as `jobq_enqueue`. With nothing new queued, the
  Edge Function starts a run only when this is above 0, so a retry scheduled for later gets one.
- Starting a run: `POST /repos/KunalShukla-Al/jobq/actions/workflows/deliver.yml/dispatches` with
  `{"ref":"main"}`, using a fine-grained token limited to this repo with Actions: Read and write. Without
  the token nothing is started; a failed start is logged and reported, never stops the enqueue.

Steps:

1. **Shadow:** with `REMINDERS_VIA_JOBQ=shadow`, the Edge Function still sends as today, *and* enqueues
   `noop` copies. Compare counts for a few days.
2. **Start runs from AcadKit:** set its `GITHUB_DISPATCH_TOKEN` and apply `jobq_due`. Each Edge Function
   run that enqueues starts Deliver; check that runs appear under `--event workflow_dispatch`, still in
   shadow, before switching.
3. **Switch:** with `REMINDERS_VIA_JOBQ=on`, it only enqueues `webpush` jobs, and jobq delivers.
4. **Rollback:** unset the flag. The old path is untouched until jobq has run cleanly for two weeks.

The delay this adds: a started run sets up a runner and the Go toolchain before it sends (20 s in the first
successful run, started by hand on 5 Oct), so a reminder arrives within a minute or so of the planner
deciding to send it. Retries scheduled for later wait for the next Edge Function run after they're due (up to 10 minutes, not the 2s of the first backoff
step). Without the token, everything waits for the schedule, which can be late by many minutes or not
come at all.

## Load test (the write-up)

k6 drives `POST /jobs` with 1k and then 10k jobs against a local fake push service (a Go `httptest`
server with set latency and error rate).

**Measures:** throughput, enqueue-to-done p50/p95/p99, retries, dead jobs, Postgres lock waits and CPU,
by number of workers and claim batch size.

**Questions:**
- When does `SKIP LOCKED` contention stop more workers from helping?
- Does polling or `LISTEN/NOTIFY` win?
- What does a 20% push-failure rate do to the queue?

The answers go in the README as **"What broke at 10k jobs"**.

## Running it for free

No Docker on this Mac, so everything comes from Homebrew:
- `go`
- `postgresql@17` (local database, and the integration tests)
- `k6`
- `prometheus`
- `grafana`

CI uses a GitHub Actions Postgres service container (free for public repos). The database in
production is the existing Supabase Postgres, reached through its connection pooler.

**Where it runs (was the open question):** a GitHub Actions run drains the queue, started by AcadKit when
there's work and by a 5-minute schedule as a backup, chosen over an always-on free host. It's free for a
public repo and needs no card. The cost is the runner's start-up, under a minute per run (26 s from start
to finish for that first successful run), and GitHub turns the workflow off after 60 days without repo
activity, which stops AcadKit's starts too (they fail with a `dispatch_error`) until it's turned back on
in the Actions tab. It reaches Supabase through the session-mode pooler: the direct host is IPv6-only, which
GitHub's runners can't reach, and a transaction pooler breaks pgx's prepared statements and `LISTEN`.

## Plan

The weeks are the planned dates; the dates in brackets are when each was actually finished.

| Planned week | Deliverable |
|---|---|
| 13–19 Oct ✅ (done 2 Oct) | Schema and migrations, enqueue API with idempotency, worker pool with claim/lease/backoff/dead-letter, `noop` handler, integration tests against real Postgres |
| 20–26 Oct ✅ (done 5 Oct) | Prometheus metrics, Grafana dashboard (JSON in the repo), graceful shutdown, CI |
| 27 Oct – 2 Nov ✅ (done 5 Oct) | k6 runs at 1k and 10k, tuning (workers, batch size, polling vs NOTIFY), the write-up |
| 3–9 Nov 🟡 (code done 5 Oct) | Done: `webpush` handler (marks sent only after delivery), claims limited to the kinds a process can run, drain mode and the Actions workflow, cleanup of old done jobs, README. Still to do: run in shadow mode for a few days and compare, the switch (`REMINDERS_VIA_JOBQ=on`), release v0.1.0 |

**Found in week 3, for week 4 or after:** a cleanup job for old done jobs (the table grows forever; done
in week 4: done jobs older than `JOBQ_KEEP_DONE`, 7 days, are deleted, dead ones kept), and keeping
processes × pool under the Supabase connection limit (in production: one drain at a time, with
`pool_max_conns=4`). Notify mode would need a session-mode connection, since `LISTEN` doesn't work through
a transaction pooler; drain mode doesn't use it.

**Done when:** in AcadKit, a reminder whose first push attempt fails is retried and delivered,
re-running the planner never sends twice, and the README answers the three load-test questions with
numbers.
