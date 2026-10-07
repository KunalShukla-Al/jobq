#!/usr/bin/env bash
# What jobq's Postgres sessions wait on during a run: samples pg_stat_activity
# every 100ms and counts wait events. Run it alongside loadtest/run.sh:
#
#   loadtest/waits.sh > waits.txt & LABEL=x PRELOAD=1 N=300000 SLEEP_MS=0 PROCS=8 ... loadtest/run.sh; kill %1
set -u
trap 'sort "$tmp" | uniq -c | sort -rn; rm -f "$tmp"; exit' TERM INT
tmp=$(mktemp)
while true; do
  psql -d postgres -Atc "select coalesce(wait_event_type, 'CPU') || ':' || coalesce(wait_event, '-')
    from pg_stat_activity where datname = 'jobq_load' and state = 'active' and pid <> pg_backend_pid()" >>"$tmp" 2>/dev/null
  sleep 0.1
done
