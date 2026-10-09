# jobq v2: storage I own

v1 keeps every job in Postgres, so Postgres does the hard parts: durability, crash recovery and
concurrency. v2 asks what it takes to do them myself. Postgres stays the default backend; v2 adds a
second one behind the same `queue.Backend` interface, so the worker, API and metrics don't change.

- **Part 1 (now):** an append-only log on local disk, for a single node.
- **Part 2 (later):** the same log replicated across three nodes with Raft, tested by injecting
  failures (killed nodes, cut networks, paused processes) and checking the history.

This document is the log's format and rules. Part 1's code follows it.

## The idea

Every change to a job is a **record appended to the end of a file**: a job enqueued, claimed,
completed, retried, killed, deleted. Nothing is overwritten in place. The current state of every job
lives in memory, built by replaying the records from the start. To survive a crash, it's enough
that the records reach the disk in order and that a half-written record can be recognised.

This is how Kafka, etcd's WAL and most databases' write-ahead logs work, and it's exactly what Raft
replicates in part 2: the log is the input, the job table is the state machine.

## Records

Each record is:

```
┌──────────┬──────────┬────────┬──────────────────┐
│ length   │ CRC-32C  │ type   │ body             │
│ uint32   │ uint32   │ uint8  │ length − 1 bytes │
└──────────┴──────────┴────────┴──────────────────┘
  big-endian; the CRC covers type + body
```

- **length** lets a reader skip to the next record without parsing the body.
- **CRC-32C** (Castagnoli, the variant with hardware instructions on ARM and x86) catches a torn or
  corrupted record. Recovery stops at the first record whose CRC doesn't match.
- **type** is one of: `enqueue`, `claim`, `complete`, `retry`, `kill`, `delete`.
- **body** is JSON for now: easy to read while debugging, and fast enough at jobq's rates. It's a
  candidate for a binary encoding if the benchmark against Postgres says it matters.

Bodies carry what replay needs and nothing else:

| type | body |
|---|---|
| enqueue | id, kind, payload, idempotency key, run_at, max_attempts, created_at |
| claim | id, lock token, attempts, locked_until |
| complete | id, lock token, finished_at |
| retry | id, lock token, run_at, last_error |
| kill | id, lock token, last_error, finished_at |
| delete | ids (old done jobs) |

**Replay is checked as well as applied.** A `complete` whose lock token doesn't match the job's
current one can't be in a valid log, so replay stops with an error rather than guessing.

## Segments

The log is a directory of segment files named by the offset of their first record
(`00000000000000000000.log`, …). A segment is closed when it reaches 64 MiB and a new one starts.

- Small files make deleting old data cheap: drop whole segments.
- **Compaction:** done and deleted jobs make most of an old segment dead. When the live fraction of
  closed segments falls below a threshold, their live jobs are rewritten as fresh `enqueue` (plus
  current-state) records into a new segment, and the old ones are removed.
- Only the newest segment is ever written to.

## Durability: when is a job safe?

**An enqueue is acknowledged only after its record is fsynced.** The cost is an `fsync` per request,
so writes are **group-committed**: records from requests arriving within a short window (1–2 ms)
share one `fsync`, and each request waits for the `fsync` that covers it. Claims and outcomes go
through the same path.

The `fsync` policy is a knob (`always` / `group` / `every N ms`), and the benchmark reports the
trade-off: an `every N ms` policy can lose the last N ms of acknowledged work in a power cut, which
the README has to say plainly.

## Recovery

On start: read the segments in order, check each record's CRC, apply it. At the first bad or
incomplete record, **truncate the file there**: everything before it was acknowledged, nothing after
it was, because acknowledgement waits for the `fsync`. Then rebuild:

- the job table (id → job);
- the idempotency index (key → id);
- a ready queue ordered by (run_at, id);
- leases: a `running` job whose `locked_until` has passed is claimable again, exactly as in v1.

The test for this is the one that matters most: **kill the process with SIGKILL hundreds of times
in the middle of writes**, restart, and check that every acknowledged job is there, nothing is
duplicated, and the state matches a model of the same history.

## Concurrency

One writer goroutine owns the log file and the in-memory state; requests reach it through a
channel. That removes locking from the core and makes group commit natural (the writer drains the
channel, writes the batch, fsyncs once, then answers everyone in it). Reads of a single job (`Get`)
can use a read-locked copy of the state.

## What's the same as v1

`queue.Backend`'s guarantees don't change: one job per idempotency key (with `ErrKeyConflict` for
a reused key with different content), leases and `ErrLostLease`, attempts counted at claim,
dead after `max_attempts`. The existing test suite runs against both backends.

## How I'll know it's worth it

The v1 load tests run unchanged against the log backend: throughput, p50/p99 latency for enqueue and
claim, and recovery time after a crash, side by side with Postgres. The README reports both,
including where the log loses.

## Not in part 1

- Replication and leader election (part 2, Raft).
- `LISTEN/NOTIFY`-style wake-ups across processes. The log backend is single-process, so the writer
  wakes idle workers directly.
- Multiple queues or tenants.
