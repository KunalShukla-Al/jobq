#!/usr/bin/env bash
# One load-test run: a fresh database, PROCS jobq processes, k6 enqueueing N
# jobs, then wait until the queue is empty and record what happened.
#
#   LABEL=baseline N=10000 WORKERS=16 loadtest/run.sh
#
# Settings (environment): N, VUS, RATE (jobs/s; empty = burst), SLEEP_MS,
# FAIL_RATE, PROCS, WORKERS, BATCH, WAKE (poll|notify), POOL (pgx
# pool_max_conns; empty = pgx's default), LABEL. Results are appended to
# loadtest/results/results.jsonl, one JSON object per run.
#
# PRELOAD=1 inserts the N jobs straight into the table before any worker
# starts, then measures how fast the workers drain them: throughput without
# the enqueue path (k6, HTTP, the API) in the way.
#
# The run refuses settings whose connection pools could exhaust Postgres
# (PROCS × (pool, plus 1 in notify mode) + 10 for psql > max_connections): that's how the first
# version of this matrix broke. ALLOW_OVER=1 runs it anyway.
set -euo pipefail
cd "$(dirname "$0")/.."

: "${N:=1000}" "${VUS:=50}" "${RATE:=}" "${SLEEP_MS:=50}" "${FAIL_RATE:=0}"
: "${PROCS:=1}" "${WORKERS:=4}" "${BATCH:=}" "${WAKE:=poll}" "${POOL:=}" "${LABEL:=run}"
: "${PRELOAD:=}" "${ALLOW_OVER:=}" "${DRAIN_TIMEOUT:=300}"
DB=jobq_load
PGURL="postgres://$(whoami)@localhost:5432"
URL="$PGURL/$DB${POOL:+?pool_max_conns=$POOL}"
OUT=loadtest/results/results.jsonl
WORK=$(mktemp -d)
BIN=$WORK/jobq
RUN="$LABEL-$(date +%s)"

# pgx's default pool is max(4, CPUs) connections per process.
per_proc=${POOL:-$(( $(sysctl -n hw.ncpu) > 4 ? $(sysctl -n hw.ncpu) : 4 ))}
if [ "$WAKE" = notify ]; then per_proc=$((per_proc + 1)); fi # the LISTEN connection, outside the pool
max_conns=$(psql -d postgres -Atc "show max_connections")
if [ $((PROCS * per_proc + 10)) -gt "$max_conns" ] && [ -z "$ALLOW_OVER" ]; then
  echo "refusing: $PROCS processes × $per_proc connections + 10 > max_connections ($max_conns)"; exit 2
fi

go build -o "$BIN" ./cmd/jobq
dropdb --if-exists --force "$DB" && createdb "$DB"

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
  rm -rf "$WORK"
}
trap cleanup EXIT

start_jobq() { # $1 = index; workers from $2 (default WORKERS)
  DATABASE_URL="$URL" JOBQ_SECRET=load JOBQ_ADDR=":$((18181 + $1))" JOBQ_WORKERS="${2:-$WORKERS}" \
    JOBQ_BATCH="$BATCH" JOBQ_WAKE="$WAKE" "$BIN" >"$WORK/jobq-$1.log" 2>&1 &
  pids+=($!)
  until curl -sf "localhost:$((18181 + $1))/healthz" >/dev/null; do sleep 0.1; done
}

if [ -n "$PRELOAD" ]; then
  # Start one process just to run the migrations, stop it, then fill the table.
  start_jobq 0 1 && kill "${pids[0]}" && wait "${pids[0]}" 2>/dev/null; pids=()
  psql -d "$DB" -qc "insert into jobq.jobs (kind, payload, idempotency_key, max_attempts)
    select 'noop', jsonb_build_object('sleep_ms', case when $SLEEP_MS > 0 then round($SLEEP_MS * (0.5 + random())) else 0 end,
           'fail_rate', $FAIL_RATE), '$RUN-' || i, 5
    from generate_series(1, $N) i"
  started=$(psql -d "$DB" -Atc "select now()")
fi
# Throughput is measured from the first enqueue, or for a preload from just
# before the workers start (so process start-up counts against it).
since=${started:+"'$started'::timestamptz"}
since=${since:-"min(created_at)"}

for i in $(seq 0 $((PROCS - 1))); do
  start_jobq "$i"
done
jobq_pids=$(IFS=,; echo "${pids[*]}")

# Every 250ms: sessions waiting on locks / lightweight locks / I/O. (CPU is
# measured as CPU time at the end, not sampled: macOS's %cpu is a decaying average.)
(
  set +e +o pipefail # one failed psql must not end the sampling
  while true; do
    w=$(psql -d "$DB" -Atc "select count(*) filter (where wait_event_type = 'Lock'),
        count(*) filter (where wait_event_type = 'LWLock'), count(*) filter (where wait_event_type = 'IO'),
        count(*) filter (where state = 'active') from pg_stat_activity where datname = '$DB' and pid <> pg_backend_pid()" 2>/dev/null | tr '|' ' ')
    echo "$w"
    sleep 0.25
  done
) >"$WORK/samples" &
sampler=$!
pids+=($sampler)

if [ -z "$PRELOAD" ]; then
  N=$N VUS=$VUS RATE=$RATE SLEEP_MS=$SLEEP_MS FAIL_RATE=$FAIL_RATE RUN=$RUN JOBQ_URL=http://localhost:18181 \
    k6 run --quiet --summary-export "$WORK/k6.json" loadtest/enqueue.js >"$WORK/k6.log" 2>&1 || {
    echo "k6 failed:"; tail -20 "$WORK/k6.log"; exit 1; }
else
  echo '{"metrics": {}}' >"$WORK/k6.json"
fi

deadline=$((SECONDS + DRAIN_TIMEOUT))
until [ "$(psql -d "$DB" -Atc "select count(*) from jobq.jobs where status in ('ready', 'running')" 2>/dev/null)" = 0 ]; do
  [ $SECONDS -lt $deadline ] || { echo "queue never drained (logs: $WORK)"; trap - EXIT; for p in "${pids[@]}"; do kill "$p" 2>/dev/null; done; exit 1; }
  sleep 0.2
done
{ kill "$sampler" && wait "$sampler"; } 2>/dev/null || true

# Claim size and time, and lost leases, summed over every process.
for i in $(seq 0 $((PROCS - 1))); do curl -s "localhost:$((18181 + i))/metrics"; done >"$WORK/metrics"
# CPU time used: the jobq processes, and the Postgres backends serving them.
pg_pids=$(psql -d postgres -Atc "select string_agg(pid::text, ',') from pg_stat_activity where datname = '$DB' and pid <> pg_backend_pid()")
cpu_secs() { [ -n "$1" ] && ps -o time= -p "$1" | awk '{n = split($1, t, ":"); s = 0; for (i = 1; i <= n; i++) s = s * 60 + t[i]; total += s} END {printf "%.2f", total + 0}' || echo 0; }
jobq_cpu=$(cpu_secs "$jobq_pids")
pg_cpu=$(cpu_secs "$pg_pids")
# Stop jobq: closing its connections makes the backends flush their table statistics.
for p in "${pids[@]}"; do [ "$p" = "$sampler" ] || kill "$p" 2>/dev/null; done
for p in "${pids[@]}"; do [ "$p" = "$sampler" ] || wait "$p" 2>/dev/null; done
pids=()
sleep 0.5

result=$(psql -d "$DB" -Atc "
  with d as (select extract(epoch from finished_at - created_at) * 1000 as ms from jobq.jobs where status = 'done')
  select json_build_object(
    'done', (select count(*) from jobq.jobs where status = 'done'),
    'dead', (select count(*) from jobq.jobs where status = 'dead'),
    'retries', (select sum(attempts) - count(*) from jobq.jobs),
    'enqueue_s', (select round(extract(epoch from max(created_at) - min(created_at))::numeric, 2) from jobq.jobs),
    'wall_s', (select round(extract(epoch from max(finished_at) - $since)::numeric, 3) from jobq.jobs),
    'throughput_per_s', (select round((count(*) filter (where status = 'done') /
        nullif(extract(epoch from max(finished_at) - $since), 0))::numeric, 1) from jobq.jobs),
    'first_try_per_s', (select round((count(*) / nullif(extract(epoch from max(finished_at) - $since), 0))::numeric, 1)
        from jobq.jobs where status = 'done' and attempts = 1),
    'steady_per_s', (select round((0.8 * count(*) / nullif(extract(epoch from
        percentile_cont(0.9) within group (order by finished_at - '2000-01-01'::timestamptz) -
        percentile_cont(0.1) within group (order by finished_at - '2000-01-01'::timestamptz)), 0))::numeric, 1)
        from jobq.jobs where status = 'done'),
    'latency_p50_ms', (select round(percentile_cont(0.5) within group (order by ms)::numeric) from d),
    'latency_p95_ms', (select round(percentile_cont(0.95) within group (order by ms)::numeric) from d),
    'latency_p99_ms', (select round(percentile_cont(0.99) within group (order by ms)::numeric) from d),
    'latency_max_ms', (select round(max(ms)::numeric) from d),
    'dead_tuples', (select n_dead_tup from pg_stat_user_tables where relname = 'jobs'),
    'autovacuums', (select autovacuum_count from pg_stat_user_tables where relname = 'jobs'))")

samples=$(awk 'NF == 4 {n++; l += $1; if ($1 > lm) lm = $1; lw += $2; if ($2 > lwm) lwm = $2; io += $3; a += $4; if ($4 > am) am = $4}
  END {d = n ? n : 1; printf "{\"lock_waits_avg\":%.2f,\"lock_waits_max\":%d,\"lwlock_waits_avg\":%.2f,\"lwlock_waits_max\":%d,\"io_waits_avg\":%.2f,\"active_avg\":%.1f,\"active_max\":%d,\"samples\":%d}",
  l/d, lm, lw/d, lwm, io/d, a/d, am, n}' "$WORK/samples")

python3 - "$WORK/k6.json" "$result" "$samples" "$WORK/metrics" <<PY >>"$OUT"
import json, sys
k6 = json.load(open(sys.argv[1]))["metrics"]
r, s = json.loads(sys.argv[2]), json.loads(sys.argv[3])
m = {}
for line in open(sys.argv[4]):
    if line.startswith("jobq_") and " " in line:
        k, v = line.rsplit(" ", 1)
        m[k] = m.get(k, 0) + float(v)
d = k6.get("http_req_duration", {})
out = {
    "label": "$LABEL", "run": "$RUN", "preload": bool("$PRELOAD"),
    "settings": {"n": $N, "vus": $VUS, "rate": "${RATE}" or None, "sleep_ms": $SLEEP_MS, "fail_rate": $FAIL_RATE,
                 "procs": $PROCS, "workers": $WORKERS, "batch": "${BATCH}" or None, "wake": "$WAKE", "pool": "${POOL}" or None},
    **r,
    "enqueue_http_p50_ms": round(d["p(50)"], 1) if d else None, "enqueue_http_p95_ms": round(d["p(95)"], 1) if d else None,
    "enqueue_http_p99_ms": round(d["p(99)"], 1) if d else None,
    "enqueue_rate_per_s": round(k6["http_reqs"]["rate"], 1) if "http_reqs" in k6 else None,
    "claims": m.get("jobq_claim_duration_seconds_count", 0),
    "claim_avg_batch": round(m.get("jobq_claim_batch_size_sum", 0) / max(1, m.get("jobq_claim_batch_size_count", 0)), 2),
    "claim_avg_ms": round(1000 * m.get("jobq_claim_duration_seconds_sum", 0) / max(1, m.get("jobq_claim_duration_seconds_count", 0)), 2),
    "lease_lost": m.get("jobq_lease_lost_total", 0),
    "jobq_cpu_s": $jobq_cpu, "postgres_cpu_s": $pg_cpu,
    "jobq_cores": round($jobq_cpu / r["wall_s"], 2) if r["wall_s"] else None,
    "postgres_cores": round($pg_cpu / r["wall_s"], 2) if r["wall_s"] else None,
    **s,
}
print(json.dumps(out))
PY
python3 loadtest/summary.py --last 1
