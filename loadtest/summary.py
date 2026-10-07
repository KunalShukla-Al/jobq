"""Print load-test results as a table.  python3 loadtest/summary.py [--last N] [LABEL-PREFIX ...]"""
import json
import sys
from pathlib import Path

args = sys.argv[1:]
last = None
if args[:1] == ["--last"]:
    last, args = int(args[1]), args[2:]
rows = [json.loads(line) for line in (Path(__file__).parent / "results" / "results.jsonl").read_text().splitlines() if line.strip()]
if args:
    rows = [r for r in rows if any(r["label"].startswith(a) for a in args)]
if last:
    rows = rows[-last:]

cols = [
    ("label", lambda r: r["label"]),
    ("n", lambda r: r["settings"]["n"]),
    ("procs", lambda r: r["settings"]["procs"]),
    ("workers", lambda r: r["settings"]["workers"]),
    ("batch", lambda r: r["settings"]["batch"] or "=w"),
    ("pool", lambda r: r["settings"]["pool"] or "def"),
    ("wake", lambda r: r["settings"]["wake"]),
    ("sleep", lambda r: r["settings"]["sleep_ms"]),
    ("fail", lambda r: r["settings"]["fail_rate"]),
    ("rate", lambda r: "preload" if r.get("preload") else (r["settings"]["rate"] or "burst")),
    ("jobs/s", lambda r: r["throughput_per_s"]),
    ("steady/s", lambda r: r.get("steady_per_s")),
    ("1st-try/s", lambda r: r.get("first_try_per_s")),
    ("p50ms", lambda r: r["latency_p50_ms"]),
    ("p95ms", lambda r: r["latency_p95_ms"]),
    ("p99ms", lambda r: r["latency_p99_ms"]),
    ("dead", lambda r: r["dead"]),
    ("retries", lambda r: r["retries"]),
    ("claim", lambda r: f'{r["claim_avg_batch"]}x {r["claim_avg_ms"]}ms'),
    ("lock", lambda r: f'{r["lock_waits_avg"]}/{r["lock_waits_max"]}'),
    ("lwlock", lambda r: r["lwlock_waits_avg"]),
    ("cores q/pg", lambda r: f'{r.get("jobq_cores")}/{r.get("postgres_cores")}'),
    ("enq/s", lambda r: r["enqueue_rate_per_s"]),
    ("enq p95", lambda r: r["enqueue_http_p95_ms"]),
    ("deadtup", lambda r: r["dead_tuples"]),
]
table = [[h for h, _ in cols]] + [[str(f(r)) for _, f in cols] for r in rows]
widths = [max(len(row[i]) for row in table) for i in range(len(cols))]
for row in table:
    print("  ".join(c.ljust(w) for c, w in zip(row, widths)))
