# Changelog

## v1.0.0 (11 Oct 2026)

jobq delivers AcadKit's push reminders (since 9 Oct), and has a second storage backend.

- **The log backend** (`JOBQ_STORE=log`, [DESIGN-v2.md](DESIGN-v2.md)): an append-only log on local disk
  with CRC-32C records, 64 MiB segments, group commit, three fsync policies, crash recovery that cuts a torn
  tail, and compaction by checkpoint. Single process; Postgres stays the default.
- `queue.Backend`: the worker, the API and the metrics no longer depend on Postgres.
- The queue's test suite runs against both backends. The log also has torn-write tests at every byte, damage
  tests, a compaction cut short, a failed write, and a test that kills a writing process with SIGKILL
  hundreds of times.
- `cmd/storebench`: both backends under the same workloads, with both kinds of fsync on macOS.
- Delivery switched on in AcadKit (`REMINDERS_VIA_JOBQ=on`) after the shadow run; the 11 subscriptions on the
  old VAPID key were removed.

## Before v1.0.0

Built 2–8 Oct 2026 without releases: the Postgres queue (`SKIP LOCKED` claims, leases, retries with backoff,
dead letters, idempotent enqueue), the HTTP API, web push, drain mode on GitHub Actions, metrics and a
Grafana dashboard, and the 10k-job load test in the README.
