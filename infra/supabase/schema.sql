-- Supabase/PostgreSQL schema (single-table queue).
-- This migration keeps only one alpha table for producer -> remote submitter.
-- Apply in Supabase SQL editor.

create extension if not exists pgcrypto;

-- Drop legacy trigger/function first so recreate stays idempotent.
drop trigger if exists trg_alpha_jobs_touch on public.alpha_jobs;
drop function if exists public.touch_updated_at();

-- Drop legacy two-table layout if present.
drop table if exists public.alpha_jobs cascade;
drop table if exists public.alpha_batches cascade;

create table if not exists public.alpha_jobs (
  id bigserial primary key,
  expression text not null,
  settings jsonb not null,
  region text not null,
  universe text not null,
  delay integer not null,
  neutralization text not null,
  language text not null default 'FASTEXPR',

  status text not null default 'queued'
    check (status in ('queued', 'in_progress', 'success', 'failed')),
  attempts integer not null default 0,
  locked_by text,
  locked_at timestamptz,

  alpha_id text,
  link text,
  sharpe double precision,
  fitness double precision,
  turnover double precision,
  submitted boolean not null default false,
  error_message text,

  source text not null default 'local_producer',
  source_host text,
  meta jsonb not null default '{}'::jsonb,
  last_response jsonb,

  created_at timestamptz not null default timezone('utc', now()),
  updated_at timestamptz not null default timezone('utc', now())
);

create index if not exists idx_alpha_jobs_status_created
  on public.alpha_jobs (status, created_at asc, id asc);
create index if not exists idx_alpha_jobs_alpha_id
  on public.alpha_jobs (alpha_id);

create or replace function public.touch_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

create trigger trg_alpha_jobs_touch
before update on public.alpha_jobs
for each row execute function public.touch_updated_at();

-- Optional strict RLS model:
-- service_role can read/write all queue rows.
alter table public.alpha_jobs enable row level security;

drop policy if exists "service role all on alpha_jobs" on public.alpha_jobs;
create policy "service role all on alpha_jobs"
  on public.alpha_jobs
  for all
  using (auth.role() = 'service_role')
  with check (auth.role() = 'service_role');

