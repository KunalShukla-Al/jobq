-- Jobs live in their own schema: not exposed to Supabase's REST API, so
-- row-level security never gets a say in how the queue works.
create schema if not exists jobq;

create table jobq.jobs (
  id              bigint generated always as identity primary key,
  kind            text        not null check (kind <> ''),
  payload         jsonb       not null default '{}',
  -- Enqueueing twice with the same key returns the first job instead of
  -- making a second one: a re-run of the planner can't send twice.
  idempotency_key text        not null unique check (idempotency_key <> ''),
  status          text        not null default 'ready'
                  check (status in ('ready', 'running', 'done', 'dead')),
  attempts        int         not null default 0,  -- claims so far, crashed ones included
  max_attempts    int         not null default 8 check (max_attempts > 0),
  run_at          timestamptz not null default now(),
  -- The lease: a running job whose locked_until has passed belonged to a
  -- worker that died, and is claimed again. lock_token says which claim
  -- holds it, so a worker that lost its lease can't finish the job.
  locked_until    timestamptz,
  lock_token      uuid,
  last_error      text,
  created_at      timestamptz not null default now(),
  finished_at     timestamptz
);

-- What workers scan: work that is due, or held by a lease that may have expired.
create index jobs_claimable on jobq.jobs (run_at, id) where status in ('ready', 'running');
