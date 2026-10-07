#!/usr/bin/env bash
# Every run behind the README's "What broke at 10k jobs", in order, twice.
# About 15 minutes on an M2 MacBook Air (8 cores, Postgres 17, max_connections
# 100). Results: loadtest/results/results.jsonl; table: python3 loadtest/summary.py
set -uo pipefail
cd "$(dirname "$0")/.."
run() {
  local out
  if out=$(env "$@" bash loadtest/run.sh 2>&1); then printf '%s\n' "$out" | tail -1
  else echo "FAILED: $*"; printf '%s\n' "$out" | tail -5; fi
}

for rep in 1 2; do
  # 1. 1k and 10k at the defaults (4 workers); jobs take ~50ms, like a push.
  run LABEL=scale-1k  N=1000  WORKERS=4
  run LABEL=scale-10k N=10000 WORKERS=4
  # 2. More workers in one process (pgx's default pool: 8 connections).
  for w in 16 64 128 256; do run LABEL=workers-$w N=10000 WORKERS=$w; done
  # 3. A bigger pool for 256 workers.
  for p in 32 64; do run LABEL=pool-$p N=10000 WORKERS=256 POOL=$p; done
  # 4. Claim batch size at 64 workers.
  for b in 1 8 64; do run LABEL=batch-$b N=10000 WORKERS=64 BATCH=$b POOL=32; done
  # 5. SKIP LOCKED contention: the same 256 workers and 64 connections split
  #    across 1-8 processes, draining a preloaded table (no enqueue in the way).
  for p in 1 2 4 8; do
    run LABEL=procs-$p PRELOAD=1 N=50000 PROCS=$p WORKERS=$((256 / p)) POOL=$((64 / p))
  done
  # 6. Jobs that take no time: the database is all that's left. Same split.
  for p in 1 2 4 8; do
    run LABEL=zero-procs-$p PRELOAD=1 N=50000 SLEEP_MS=0 PROCS=$p WORKERS=$((256 / p)) POOL=$((64 / p))
  done
  # 7. A 20% push-failure rate.
  run LABEL=fail-20 N=10000 WORKERS=64 POOL=32 FAIL_RATE=0.2
  # 8. Polling vs LISTEN/NOTIFY: steady arrivals at 20/s, then a burst.
  for wake in poll notify; do
    run LABEL=steady-$wake N=600 RATE=20 WORKERS=4 WAKE=$wake
    run LABEL=burst-$wake  N=10000 WORKERS=64 POOL=32 WAKE=$wake
  done
done
