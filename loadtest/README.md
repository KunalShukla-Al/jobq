# Load test

What's behind the README's "What broke at 10k jobs".

- `run.sh`: one run. A fresh `jobq_load` database, `PROCS` jobq processes, then k6 (`enqueue.js`) posts
  `N` jobs, or with `PRELOAD=1` they're inserted straight into the table. When the queue is empty it
  records throughput, latency, retries, claims, lock waits and CPU time in `results/results.jsonl`.
- `matrix.sh`: the main runs in the write-up, twice (about 15 minutes).
- `summary.py`: the results as a table (`python3 loadtest/summary.py [--last N] [label prefix…]`).
- `waits.sh`: counts Postgres wait events during a run. Used for the 300k-job runs in
  `results/diagnostics.jsonl`; its output and the other raw samples are in `results/evidence/`.

Needs `k6` and a local Postgres (`brew install k6 postgresql@17`). Settings are environment variables;
see the top of `run.sh`.
