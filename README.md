# jobq

[![CI](https://github.com/KunalShukla-Al/jobq/actions/workflows/ci.yml/badge.svg)](https://github.com/KunalShukla-Al/jobq/actions/workflows/ci.yml)

A job queue in Go that I built to deliver [AcadKit](https://github.com/KunalShukla-Al/Acadkit)'s push
reminders, load-tested to see what breaks, and then gave a second storage engine that I wrote myself: an
append-only log on local disk, next to Postgres. Design: [DESIGN.md](DESIGN.md) (the queue) and
[DESIGN-v2.md](DESIGN-v2.md) (the log).

- **Idempotent enqueue:** one job per idempotency key. A repeat gets the first job back; a reused key
  with different content is rejected.
- **Safe parallel workers:** `FOR UPDATE SKIP LOCKED` claims, with a lease per claim, so a crashed
  worker's job is picked up again. A worker that lost its lease can't record an outcome.
- **Retries:** exponential backoff with jitter, then **dead** after `max_attempts`, or straight away for a
  permanent error. Panics in handlers are caught and retried.
- **Graceful shutdown:** stops claiming, and lets running jobs finish.
- **Web push:** the `webpush` kind is built to deliver AcadKit's reminders, and marks one sent only after it's
  delivered.
- **Runs as a service or as a drain:** `jobq drain` runs what's due and exits. That's how it runs in
  production: on GitHub Actions, started by AcadKit right after it queues work, with a schedule as a
  backup.
- **Metrics:** Prometheus at `/metrics`, with a Grafana dashboard in the repo.
- **Two storage backends behind one interface:** Postgres, or my own log (checksummed records, group
  commit, crash recovery, compaction). The same test suite runs against both, and the log is also killed
  with SIGKILL hundreds of times mid-write. [Owning the storage](#owning-the-storage-v2-part-1) has the
  numbers, including where it doesn't pay off.

Status (11 Oct 2026, v1.0.0): it has delivered AcadKit's reminders since 9 Oct 14:06 IST. Every reminder
since then went through jobq: 6 of 6 delivered on the first attempt, 25 s from queued to delivered at the
median and 28 s at most. Nearly all of that is AcadKit starting the Deliver workflow and a GitHub runner
starting up.

Before that it ran in shadow from 5 to 9 Oct, with AcadKit still sending every reminder itself and queueing a
matching no-op job: 25 reminders, 25 shadow jobs, each done on its first attempt. The one slow job (17 minutes,
the first) waited for AcadKit's token to start runs to work. Before switching on I removed the 11 push
subscriptions still on AcadKit's old VAPID key (below).

## Run it

Needs Go 1.25+ and Postgres. No Docker: `brew install go postgresql@17`.

```sh
createdb jobq
DATABASE_URL=postgres://localhost/jobq JOBQ_SECRET=dev go run ./cmd/jobq
```

Migrations run on start. Settings, all environment variables: `JOBQ_ADDR` (`:8080`), `JOBQ_WORKERS` (4),
`JOBQ_BATCH` (most jobs per claim, `JOBQ_WORKERS`), `JOBQ_WAKE` (`poll`, or `notify` to also wake idle workers
with `LISTEN/NOTIFY`), `JOBQ_LEASE` (`1m`), `JOBQ_GRACE` (`30s`), `JOBQ_KEEP_DONE` (`168h`: done jobs older
than this are deleted, hourly; dead ones are kept), and `VAPID_PUBLIC`, `VAPID_PRIVATE`, `VAPID_SUBJECT`
for web push (below).

**Drain mode:** `jobq drain` runs the workers without the HTTP server (so no `JOBQ_SECRET`), and exits
when a claim finds nothing due and nothing is running, or after `JOBQ_DRAIN_MAX` (`4m`). Jobs still
running then get `JOBQ_LEASE` to finish (each stops before its lease ends), or `JOBQ_GRACE` after a
signal. A retry scheduled for later waits for the next run. At the end it deletes old done jobs and logs
what's left. `jobq` with no command, or `jobq serve`, is the server.

```sh
DATABASE_URL=postgres://localhost/jobq go run ./cmd/jobq drain
```

**On the log instead of Postgres:** `JOBQ_STORE=log JOBQ_LOG_DIR=./data JOBQ_SECRET=dev go run ./cmd/jobq`.
`JOBQ_LOG_SYNC` is `batch` (the default: group commit), `always` or `interval` (fsync every 10ms). The log
lives in one process, so `JOBQ_WAKE=notify` wakes workers directly. `DATABASE_URL` is then only needed for
`webpush` jobs, whose subscriptions are in AcadKit's database. Production stays on Postgres: a GitHub
Actions runner starts with an empty disk every time, so it has nowhere to keep a log.

**Connections:** each process opens up to `pool_max_conns` (pgx's default is the number of CPUs, at least 4),
plus one more in `notify` mode. Set it in `DATABASE_URL` (`...?pool_max_conns=8`) and keep
processes × (pool + 1) + everything else under the database's limit. Going over is what broke first in the
load test below.

```sh
curl -H 'Authorization: Bearer dev' -d '{"kind":"noop","payload":{"sleep_ms":100},"idempotency_key":"try-1"}' localhost:8080/jobs
# {"id":1,"created":true}      201; the same request again gives 200 and "created": false
curl -H 'Authorization: Bearer dev' localhost:8080/jobs/1
# {"id":1,"kind":"noop","status":"done","attempts":1,...}
```

| Endpoint | |
|---|---|
| `POST /jobs` | `{kind, payload, idempotency_key, run_at?, max_attempts?}` → `201` new, `200` existing, `409` key reused, `422` invalid |
| `GET /jobs/{id}` | the job: status, attempts, last error |
| `GET /healthz` | database reachable (no secret needed) |
| `GET /metrics` | Prometheus metrics (no secret needed: counts and timings, never payloads) |

The `noop` kind exists for tests and load tests. Its payload can make it sleep (`sleep_ms`), fail the
first N attempts (`fail_times`), fail at random (`fail_rate`), fail permanently (`permanent`) or panic
(`panic`).

### The `webpush` kind

One AcadKit reminder for one device: payload `{device_id, kind, ref, title, body, url}` (`url` may be
null), idempotency key `webpush:<device_id>:<kind>:<ref>`. It runs against AcadKit's own tables, in the
same database: it reads the device's browsers from `public.push_subscriptions` when the job runs, and sends
each one `{"title","body","url","tag":<kind>}`, the same message the Edge Function sends today (VAPID,
`aes128gcm`, a 12-hour TTL, a 10-second timeout per push). Per browser:

| Push service answers | |
|---|---|
| `200`, `201`, `202` | delivered |
| `404`, `410` | the browser unsubscribed: its row is deleted, and it doesn't count as a failure |
| `429`, `5xx`, a network error | the job is retried |
| `400`, `401`, `403`, `413` | the job is dead, unless another browser got it |

Once at least one browser has it and none needs a retry, it's recorded in `public.sent_notifications`.
That's the fix for the Edge Function marking a reminder sent *before* sending it (DESIGN.md, problem 1). A
device with no subscriptions is done with nothing recorded. A reminder already in `sent_notifications`
(marked by an earlier run of the job, or sent by the Edge Function after a rollback) isn't sent again. A
retry after a partial failure sends to every browser again, including those that already got it: the
notification's tag is its kind, so the second replaces the first on the device instead of showing twice.

Only browsers subscribed with jobq's public key are pushed: `push_subscriptions.vapid_public` (AcadKit
migration 041) must equal `VAPID_PUBLIC`. The others (`null`: AcadKit's original key, whose private half
jobq doesn't have) are skipped: not pushed, not failures, not deleted, and counted in a log line. A device
whose browsers are all on another key is done with nothing recorded.

The handler is only registered when `VAPID_PUBLIC` and `VAPID_PRIVATE` are set (base64url, AcadKit's new
key pair, the one the Edge Function has as `VAPID_PUBLIC_V2` / `VAPID_PRIVATE_V2`; `VAPID_SUBJECT` defaults
to the Edge Function's `mailto:`). A process claims only the kinds it has handlers for, so without the
keys `webpush` jobs wait for a process that has them, instead of failing. A private key that isn't the
public key's other half stops the process at start.

**Cleanup and idempotency:** deleting a done job frees its idempotency key, so the same key enqueued after
`JOBQ_KEEP_DONE` makes a new job. For `webpush` that's harmless: AcadKit's planner also skips reminders
already in `sent_notifications`, and every ref it makes carries a date.

## Run it in production on GitHub Actions

There's no always-on host: [`.github/workflows/deliver.yml`](.github/workflows/deliver.yml) runs
`jobq drain` against AcadKit's Supabase Postgres. Until the secrets exist, each run stops at its first
step with a notice, so the workflow is safe to merge first.

**What starts a run:**
- **AcadKit, right after it queues work.** Its `send-reminders` Edge Function (which Supabase's `pg_cron`
  calls every 10 minutes, reliably) starts Deliver through GitHub's API, a `workflow_dispatch` like the
  "Run workflow" button, whenever it queued new jobs, or, with jobq on, whenever
  `public.jobq_due()` says jobs are already due (a retry whose time has come, or a job whose run died).
  It uses a fine-grained token that can only run this repo's Actions; setting it up is in AcadKit's
  `supabase/functions/send-reminders/DEPLOY.md`, "Starting Deliver right away". A start that fails is
  logged there and never stops a reminder from being queued.
- **The schedule, as a backup.** It asks for a run every 5 minutes (minutes 2, 7, 12 …), but GitHub ran it
  only 7 times between 6 Oct 02:00 and 7 Oct 10:20 UTC, 3 to 7 hours apart. It is a backup for a missed
  start, not a 5-minute heartbeat. `gh run list -R KunalShukla-Al/jobq -w Deliver --event schedule` shows the
  real runs.
- **By hand:** Actions → Deliver → Run workflow.

One drain runs at a time (`concurrency: deliver`), so a run started while another is going waits for it
instead of doubling up. Runs AcadKit started show up under
`gh run list -R KunalShukla-Al/jobq -w Deliver --event workflow_dispatch`.

**Repository secrets** (Settings → Secrets and variables → Actions):

| Secret | |
|---|---|
| `JOBQ_DATABASE_URL` | Supabase's **session-mode pooler** connection string, with `?sslmode=require&pool_max_conns=4` added |
| `VAPID_PUBLIC`, `VAPID_PRIVATE` | AcadKit's **new** key pair: the Edge Function's `VAPID_PUBLIC_V2` / `VAPID_PRIVATE_V2` (see below) |
| `VAPID_SUBJECT` | the same value as the Edge Function's `VAPID_SUBJECT` |

The connection string comes from the Supabase dashboard (Connect → Session pooler), and looks like
`postgresql://postgres.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:5432/postgres` (the
`aws-0` part differs between projects, so copy the host rather than typing it, and percent-encode any
special characters in the password). Why that one:

- The direct host (`db.<project-ref>.supabase.co`) is IPv6-only, and GitHub's runners can't reach IPv6.
  The pooler host has IPv4.
- **Session** mode (port 5432 on the pooler), not transaction mode (port 6543): pgx caches prepared
  statements per connection, which a transaction pooler breaks by handing each transaction a different
  server connection, and `LISTEN` doesn't work through it either.
- `pool_max_conns=4` matches the default 4 workers and keeps jobq's share of the pooler small.

The first run creates the `jobq` schema (the migrations run on every start, and do nothing once applied).
That schema isn't exposed to Supabase's REST API.

**Run it once by hand:** Actions → Deliver → Run workflow (or `gh workflow run deliver.yml`). The log
should end with `drained: nothing due is left` and `left in the queue`. From a laptop, the same thing is
`DATABASE_URL='<the secret>' go run ./cmd/jobq drain`.

**What to know about scheduled workflows:**
- A schedule is a request, not a promise. GitHub starts scheduled runs late when it's busy, and can drop them
  altogether: this repo's 5-minute schedule has run a few times a day. That's why AcadKit starts the runs
  it needs itself. Without its token, the schedule is all there is, and a reminder can take hours to
  arrive.
- **GitHub turns off a public repo's schedule after 60 days without activity in the repo** (a push, for
  example). That disables the whole workflow, so AcadKit's starts fail too (its answer shows a
  `dispatch_error`) until it's on again. Turn it back on in the Actions tab, or push something before then.
- Actions minutes are free for public repos. Runs started by AcadKit come to fewer than 10 a day, plus the
  schedule's few. If the schedule ever ran as asked, 288 runs a day at about a minute each would use up
  a private repo's free minutes within a week, so this repo stays public.
- AcadKit enqueues with a SQL function, `public.jobq_enqueue(jobs jsonb)`, not `POST /jobs`: between
  runs nothing is listening. See the rollout in [DESIGN.md](DESIGN.md#rollout-in-acadkit-behind-a-flag).

**Rollout** (all done):
1. Merge; the workflow runs and no-ops. (5 Oct)
2. Add the four secrets, run the workflow by hand, and check the log. (5 Oct)
3. In AcadKit, apply the migration that adds `public.jobq_enqueue`, and set `REMINDERS_VIA_JOBQ=shadow`. (5 Oct)
   For a few days, compare `shadow:` jobs done in `jobq.jobs` with what the Edge Function sent.
4. Give AcadKit a token to start Deliver (`GITHUB_DISPATCH_TOKEN`: fine-grained, this repo only,
   Actions: Read and write), and apply its migration that adds `public.jobq_due`. In shadow, each run
   that queues jobs now starts Deliver; check with `gh run list -R KunalShukla-Al/jobq -w Deliver --event
   workflow_dispatch`. (5 Oct; working since the token was replaced that evening)
5. Set `REMINDERS_VIA_JOBQ=on`: jobq delivers. Set it back to `shadow` to roll back. (9 Oct, after a test
   push reached both of my devices)

**Moving to the new VAPID key:** AcadKit's original private key survives only inside Supabase, so jobq has
a new pair, and a browser's subscription works only with the key it was made with. AcadKit re-subscribes
each browser with the new key the next time it's opened, and stores that key in `vapid_public`; the Edge
Function signs each subscription with the pair it was made with. The full order is in AcadKit's
`supabase/functions/send-reminders/DEPLOY.md` ("Moving to the new VAPID key"). For jobq:
- The handler selects `vapid_public`, so it goes to `main` (where the workflow runs it from) only once
  AcadKit's migration 041 is applied; without the column every `webpush` job fails.
- While `REMINDERS_VIA_JOBQ` isn't `on`, jobq sends nothing, so subscriptions still on the old key lose
  nothing. Watch the move with
  `select vapid_public is null as old_key, count(*) from public.push_subscriptions group by 1;`
- Switch to `on` only when (nearly) every active subscription is on the new key. From then on, a browser
  that was never reopened gets no reminders (the job logs `skipped subscriptions on another VAPID key`).

## Watch it

```sh
brew install prometheus grafana
prometheus --config.file=deploy/prometheus.yml        # scrapes localhost:8080 every 5s
brew services start grafana                           # localhost:3000, admin/admin
```

In Grafana, add Prometheus (`http://localhost:9090`) as a data source, then import
[`deploy/grafana/jobq.json`](deploy/grafana/jobq.json) (Dashboards → New → Import).

| Metric | |
|---|---|
| `jobq_enqueued_total{kind}` | new jobs (a repeated idempotency key doesn't count) |
| `jobq_finished_total{kind,result}` | attempts by result: `done`, `retry` or `dead` |
| `jobq_job_duration_seconds{kind}` | how long a handler ran, per attempt |
| `jobq_job_latency_seconds{kind}` | enqueue to done, retries included: what a user waits |
| `jobq_queue_depth{status}` | jobs by status, read from Postgres at scrape time |
| `jobq_oldest_ready_age_seconds` | how long the oldest due job has waited: rising means falling behind |
| `jobq_claim_batch_size` | jobs per claim that found work |
| `jobq_claim_duration_seconds` | how long a claim took: waiting for a pool connection, then the query |
| `jobq_lease_lost_total` | outcomes thrown away because the lease ran out first (should stay 0) |

Plus the standard `go_*` and `process_*` metrics.

## What broke at 10k jobs

The week 3 load test ([`loadtest/`](loadtest)): k6 posts 1k and 10k jobs to the API, or jobs are inserted
straight into the table and the workers drain them ("preload"). Each job is a `noop` that sleeps about 50ms
(±50%), standing in for a push service. Every run was done twice and the numbers below are the mean.
Throughput with 50ms jobs agreed within 3% between runs. The no-work runs varied by up to 11%, enqueue rates
by up to 13%, and the lock-wait averages from 2-second runs by several times, so treat those as rough.

**Short version:** on one laptop, 256 workers (64× the default 4) ran about 5,000 push-like jobs a second,
just under the 5,120 that 256 workers × 50ms allow. Jobs that do no work reached about 26,000 a second. Both
are far beyond anything AcadKit will send. For jobs like these, the worker count is the setting that matters.
What actually broke was running out of database connections, which left 57 finished jobs unrecorded and due
to run again. `LISTEN/NOTIFY` cut latency by ~25ms at a steady 20 jobs/s, and cut the enqueue rate by 37%
under a 10k burst without changing how fast jobs ran.

### The three questions from the design

**1. When does `SKIP LOCKED` contention stop more workers from helping?** Not with jobs that take ~50ms.
Throughput followed workers × 20/s all the way: 78 → 313 → 1,250 → 2,461 → 4,774 jobs/s for 4 → 256 workers,
and 5,071/s in steady state at 256, which is 99% of the ideal 5,120. Splitting the same 256 workers over
1, 2, 4 or 8 processes changed nothing (5,078, 5,064, 5,061, 5,067/s in steady state).

Contention only shows up when jobs take no time at all, so that the database is all that's left:

| No-work jobs, 256 workers in total | 1 process | 2 | 4 | 8 |
|---|---|---|---|---|
| Jobs per second (50k preloaded) | 17,122 | **26,336** | 25,903 | 24,104 |
| Jobs per claim / ms per claim | 127 / 7.3 | 59 / 4.2 | 29 / 3.9 | 13 / 3.4 |
| Postgres CPU (cores) | 3.2 | 4.4 | 4.7 | 4.8 |
| Sessions waiting on a lock (avg, 300k-job runs) | 0 | 0.8 | – | 17.2 |

One process is limited by its single claim loop: in the 300k-job run it spent 13.2 of 13.4 seconds
claiming. A second process helps (+19% to +54%, depending on run size). Beyond that throughput doesn't
improve, and lock waits climb. Sampling `pg_stat_activity` during the 300k-job runs showed what they are. At
8 processes the top wait event is `Lock:transactionid` (1,366 samples; next is `LWLock:BufferContent` at 533),
and in 1,329 of 1,335 samples the waiting statement was a worker recording a finished job. Where the blocker
could be identified (927 samples), it was a claim that hadn't committed yet. The likely mechanism, not
confirmed directly: a claim's snapshot still shows a row as ready, but another process's claim has since
taken it and committed. `FOR UPDATE` follows the update to the newest version, locks it, rechecks it, finds
it running and skips it, but keeps the lock until the claim commits. The worker finishing that job waits.
Conclusion: one or two processes, with workers set for the job time.

**2. Does polling or `LISTEN/NOTIFY` win?** Each wins one case.

| | poll | notify |
|---|---|---|
| Steady 20 jobs/s: latency p50 / p99 | 76 / 121 ms | **52 / 77 ms** |
| Burst of 10k: enqueue rate | **22,286/s** | 14,084/s (−37%) |
| Burst of 10k: sessions waiting on a lock (avg / peak) | 0 / 0 | 1.9 / 29 |
| Burst of 10k: jobs per second | 1,254 | 1,238 |

At a steady rate, notify removes the ~25ms an idle worker spends waiting for its next poll. Under a burst,
each enqueue's `NOTIFY` most likely takes Postgres's global notify lock (one lock for the whole server, held
through commit), so notifying enqueues commit one at a time. Meanwhile the workers are busy anyway and gain
nothing from being woken. The lock waits fit that, though which lock wasn't sampled in these runs.

`poll` stays the default. At AcadKit's volume neither side matters much: an idle poller wakes within 2s,
reminders scheduled for later aren't notified anyway, and the enqueue cost only showed above 14k enqueues/s.
Notify also needs one more connection per process, and a session-mode one, since `LISTEN` doesn't work
through a transaction pooler.

**3. What does a 20% push-failure rate do?** It costs time, and very few jobs. Out of 10k: 2,494 and 2,543
retries (expected 2,496) and 4 and 5 dead after 5 attempts (expected 3.2, which is 0.2⁵ × 10k; with the
default of 8 attempts it would be about 0.03). The first tries ran at 992 jobs/s, but the batch took 36s
instead of 8s, and p99 latency went from 7.4s to 12.6s, because the last retries wait out their backoff
(1–2, 2–4, 4–8 and 8–16s, so up to 30s for a job that fails four times).

### What broke, and what changed

1. **Out of database connections.** 8 processes × a 16-connection pool is 128, and Postgres allows 100. The
   pools held on to their connections, so even `psql` couldn't get in, and 57 finished jobs (in 2 of the 8
   processes) couldn't be recorded as done. They would have stayed `running` until their 1-minute lease ran
   out, then run again. Recording an outcome is now retried for about a second (after 50ms, 200ms and 1s),
   which should cover a blip like this one: all 57 failures fell within 0.3s. That's tested with a fake
   error, not against the real failure. Staying under the limit is what actually prevents it, so the
   connection budget is documented above and the load test refuses settings over it.
2. **The defaults are small.** 4 workers run 78 jobs/s, so a burst of 10k waits: p50 64s, p99 127s. Set
   `JOBQ_WORKERS` for the job time and the rate you need.
3. **`JOBQ_BATCH` rarely matters with jobs like these.** With 50ms jobs, claims took 1.0–1.1 jobs whether the
   batch was 1, 8 or 64: the pool claims again as soon as one worker is free, so the batch only applies when
   several workers are free at once (start-up, or the first claim after an idle spell). It matters a lot
   when jobs are much shorter than a claim: the no-work runs took 13–127 jobs per claim.
4. **A bigger pool doesn't help.** pgx's default of 8 connections kept up with 256 workers (4,774 jobs/s;
   4,776 with 32 and 4,705 with 64). With 64, enqueue may have been a little slower (18,948/s against 21,053/s
   at 32), but the default pool's own two runs differed by 13%, so that's within noise.
5. **Done jobs are never deleted**, so the table grows forever. That's fine at AcadKit's volume, but a
   cleanup job is needed before jobq runs for months. Each job also leaves two dead row versions (600k in a
   300k-job run). The runs were too short to tell whether autovacuum keeps up.

The review of this week's code also found and fixed: an error message cut at 2,000 bytes could split a
character (or hold a NUL) and then fail to save every time; in notify mode, wake-ups skipped the backoff
after a failed claim; and `JOBQ_BATCH` above `JOBQ_WORKERS` is now capped.

**Setup and limits:** an M2 MacBook Air (8 cores, 16GB) running k6, jobq and Postgres 17 together, with
Postgres's default settings (`shared_buffers` 128MB, `max_connections` 100). The jobs are a stand-in, not real
pushes. Throughput is jobs done ÷ (last finish − first enqueue, or worker start for a preload); latency is
enqueue to done, from the table's own timestamps. The runs are in
[`loadtest/results/results.jsonl`](loadtest/results/results.jsonl) and the 300k-job runs in
[`diagnostics.jsonl`](loadtest/results/diagnostics.jsonl). The wait-event samples, the blocker samples and
the logs of the run that ran out of connections are in [`loadtest/results/evidence/`](loadtest/results/evidence).
`loadtest/matrix.sh` reruns the main set (about 15 minutes).

## Owning the storage (v2, part 1)

`JOBQ_STORE=log` keeps the jobs in an append-only log on local disk that I wrote, instead of Postgres. Every
change is a checksummed record added to the end of a file; the jobs live in memory and are rebuilt by
replaying the file on start. Requests that arrive together share one fsync (group commit), and nothing is
acknowledged before its fsync. It sits behind the same `queue.Backend` interface as Postgres, so the worker,
API and metrics didn't change, and the same tests run on both. The format and rules are in
[DESIGN-v2.md](DESIGN-v2.md), including what I changed while building it.

**Short version:** when a write must survive a power cut, my log is no faster than Postgres: both wait about
3 ms for the drive, and both batch. Group commit is what matters there: 27× more throughput with 64 producers.
When the drive's cache isn't flushed, the log is up to 6× faster, because it skips the network hop, SQL and
row versions. I also found that Postgres's default on macOS doesn't flush the drive's cache either.

`go run ./cmd/storebench` on my MacBook Air (M2, 16 GB), 5 s per measurement. Postgres 17 runs on the same
machine over a local socket, while the log runs inside the benchmark's own process, which is part of its lead.
Jobs are tiny `noop`s, so this measures storage and nothing else. "Work" is claiming 10 at a time and
completing each, from a queue of 60,000; the fastest settings emptied it in under 5 s.

| | enqueue, 1 producer | enqueue, 64 producers | work, 16 workers |
|---|---|---|---|
| **Plain fsync (the drive may still lose it in a power cut)** | | | |
| my log, group commit | 26,669/s · p99 0.05 ms | 252,410/s · p99 0.52 ms | 119,349/s · p99 0.29 ms |
| my log, fsync per request | 26,782/s · p99 0.05 ms | 26,572/s · p99 2.83 ms | 23,739/s · p99 0.86 ms |
| Postgres as installed (`open_datasync`) | 12,491/s · p99 0.22 ms | 44,011/s · p99 5.48 ms | 26,410/s · p99 3.58 ms |
| **Full fsync (`F_FULLFSYNC`: survives a power cut)** | | | |
| my log, group commit | 335/s · p99 4.64 ms | 9,194/s · p99 9.31 ms | 2,332/s · p99 8.87 ms |
| my log, fsync per request | 336/s · p99 4.68 ms | 334/s · p99 203.83 ms | 224/s · p99 89.33 ms |
| Postgres, `fsync_writethrough` | 334/s · p99 4.00 ms | 9,667/s · p99 11.97 ms | 2,328/s · p99 13.18 ms |
| **No fsync before answering (a power cut loses the last moments)** | | | |
| my log, fsync every 10 ms | 93,340/s · p99 0.02 ms | 259,748/s · p99 3.00 ms | 177,344/s · p99 0.39 ms |
| Postgres, `synchronous_commit = off` | 17,021/s · p99 0.09 ms | 78,107/s · p99 4.48 ms | 36,852/s · p99 3.40 ms |

What the numbers say:
1. **A plain fsync on a Mac costs about 0.03 ms and a full one about 3 ms.** The plain one only hands the data
   to the drive, which can lose it from its cache in a power cut. Postgres's default here (`open_datasync`)
   is the plain kind, so out of the box it isn't power-safe on macOS. That's why every fsync setting runs
   both ways: my full fsync against its plain one would be unfair to me, and the reverse unfair to it.
2. **With a full fsync, the two are the same speed**: 335 vs 334/s for one producer (one flush each),
   9,194 vs 9,667/s for 64. The drive is the limit, and Postgres's WAL batches commits just like my group
   commit does. For jobq's real job, a few reminders a day, owning the storage buys nothing.
3. **Group commit is the setting that matters.** With an fsync per request, 64 producers get no more than
   one (334/s) and wait 200 ms each at p99. With group commit they get 9,194/s.
4. **Without the full flush, the log wins** by 2× for one producer and about 6× for 64. That's what skipping
   the network, the SQL and the MVCC row versions is worth when the disk isn't the bottleneck.
5. **Starting up replays the whole log:** 118 ms for 100k jobs (19 MB) and 1.35 s for 1M jobs (195 MB).
   Compaction keeps that bounded. Postgres doesn't replay its tables on start.

**Crash test:** a process writing as fast as it can (8 workers plus a cleanup goroutine) was killed with
SIGKILL **500 times** on the same log. After every kill, the log opened (68 ms at the median, 174 ms at
most). All **207,138 acknowledged changes** were there. Of the 3,979 changes in flight at a kill, 915 had
happened, and each job was always in a state its own history allows. There were no duplicates and no lost
jobs. No kill ever left half a record, because the OS finishes each write, so torn records are tested
separately: the file is cut at every possible byte. Results: [`crash-500.json`](loadtest/results/crash-500.json),
[`store-apple-m2.json`](loadtest/results/store-apple-m2.json).

**Where the log loses:**
- It's one process on one machine. Workers must run inside it, so it can't be the GitHub Actions drain that
  production uses.
- Every job is held in memory.
- There's no SQL to look at the jobs with.
- Compaction pauses writes while it runs.
- A power cut is untested: SIGKILL can't drop what the OS already has.
- There's no replication yet. That's part 2: Raft across three nodes, tested with injected faults.

## Test

Every Postgres test runs against real Postgres, each in its own throwaway database, so they run in
parallel. The log's tests need nothing:

```sh
JOBQ_TEST_DATABASE_URL=postgres://localhost/postgres go test -race ./...
JOBQ_CRASH_ROUNDS=500 go test -run SIGKILL -v ./internal/logstore   # the long crash run
```

Covered:
- the queue's contract (idempotency, claim order and kinds, concurrent claims, leases, retry, kill, error
  text, counts) against **both** backends
- the log: a reopen rebuilds every job exactly; a torn write cut at every possible byte; damage in the newest
  segment (cut there) and in an older one (refuses to open); a compaction cut short (ignored); a failed
  write (the store stops); segments rolling; compaction keeping every job and never reusing an id; a
  process killed with SIGKILL mid-write, again and again on one log
- idempotent enqueue, including 20 concurrent enqueues of one key
- 8 workers claiming at once never sharing a job
- an expired lease reclaimed, with the old claim locked out
- retry timing, the dead letter, every outcome through the worker pool
- a crashed worker's job finished by another
- 500 jobs on 8 workers each running exactly once
- shutdown, concurrent migrations
- the HTTP API
- metrics: every outcome counted, queue depth and lag read at scrape, `/metrics` open without the secret
- `LISTEN/NOTIFY`: only a new job that's due notifies, and it wakes a worker that would otherwise poll in 10s
- recording an outcome: retried through a database blip, not retried once the lease is lost
- claims take only the kinds a process has handlers for; others wait
- drain mode: runs everything due then returns, leaves a retry due later, stops at its deadline, and doesn't
  take a failed claim for an empty queue
- cleanup: old done jobs deleted in batches, recent and dead ones kept, the key free again
- `webpush` against AcadKit's two tables and a fake push service that decrypts each message and checks
  the VAPID signature: delivered and recorded, 404/410 deleted, 429/500/network errors retried, 400/403
  permanent, mixed results, no subscriptions, bad payloads, bad keys and endpoints, nothing sent again once
  it's in `sent_notifications`, no endpoint path in errors, subscriptions on another VAPID key skipped and
  left alone (a device with only those is done with nothing recorded)

CI runs the same suite against Postgres 17 and fails if any integration test skips.

## How I built this

I built this with heavy help from an AI coding assistant (Claude Code). I chose what to build and the trade-offs, ran the experiments, and checked the results; the assistant wrote much of the code and docs.

In my own words, the design choices I'd defend:

**Why `SKIP LOCKED` instead of locking the whole table?** Because I have multiple workers doing jobs at the
same time. If I lock the whole table, one worker could block all the other workers. `SKIP LOCKED` lets a worker
take a job that is free and skip jobs that another worker is already working on. So multiple workers can work
at the same time without fighting over the same job.

**Why a lease instead of just marking a job "taken"?** Because a worker can crash. If I simply mark a job as
taken and the worker crashes, that job could stay stuck forever. With a lease, the job is only taken for a
certain amount of time. If the worker crashes, the lease expires and another worker can take the job. The
lease makes sure a crashed worker doesn't permanently steal a job.

**Can a reminder be sent twice?** I chose at-least-once processing: I care more about making sure the reminder
eventually gets processed than guaranteeing it is processed exactly once. So yes, in a failure a reminder could
be processed twice. I handle this with idempotency: the same reminder has an idempotency key, and the system
checks whether it was already sent. In an interview I'd say: "Yes, it's possible during a failure. I don't
claim exactly-once delivery. I use idempotency so that retries don't normally result in the user seeing the
reminder twice."

**What I'd defend, and what I'd change.** I'd defend using Postgres for the queue, because the application
already uses Postgres and I didn't want to add another system like Redis just for the queue. One thing I'd
change later is running the worker on GitHub Actions. It was a good cheap choice for this project, but for a
real production system I'd use a proper always-running worker.
