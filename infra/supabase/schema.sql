-- Supabase/PostgreSQL schema for decoupled producer/submission architecture.
-- Apply in Supabase SQL editor.

create extension if not exists pgcrypto;

create table if not exists public.alpha_batches (
  id uuid primary key default gen_random_uuid(),
  region text not null,
  universe text not null,
  delay integer not null,
  neutralization text not null,
  language text not null default 'FASTEXPR',
  template_count integer not null,
  producer_host text,
  metadata jsonb not null default '{}'::jsonb,
  created_at timestamptz not null default timezone('utc', now())
);

create table if not exists public.alpha_jobs (
  id bigserial primary key,
  batch_id uuid references public.alpha_batches(id) on delete set null,
  expression text not null,
  settings jsonb not null,
  region text not null,
  universe text not null,
  delay integer not null,
  neutralization text not null,
  language text not null default 'FASTEXPR',
  status text not null default 'queued'
    check (status in ('queued', 'in_progress', 'retry', 'simulated', 'submitted', 'failed')),
  priority integer not null default 0,
  attempts integer not null default 0,
  max_attempts integer not null default 6,
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
create index if not exists idx_alpha_jobs_batch_id
  on public.alpha_jobs (batch_id);
create index if not exists idx_alpha_jobs_alpha_id
  on public.alpha_jobs (alpha_id);

create or replace function public.touch_updated_at()
returns trigger language plpgsql as $$
begin
  new.updated_at = timezone('utc', now());
  return new;
end;
$$;

drop trigger if exists trg_alpha_jobs_touch on public.alpha_jobs;
create trigger trg_alpha_jobs_touch
before update on public.alpha_jobs
for each row execute function public.touch_updated_at();

-- Optional strict RLS model:
-- - service_role (or postgres direct connection) can read/write all rows.
alter table public.alpha_batches enable row level security;
alter table public.alpha_jobs enable row level security;

drop policy if exists "service role all on alpha_batches" on public.alpha_batches;
create policy "service role all on alpha_batches"
  on public.alpha_batches
  for all
  using (auth.role() = 'service_role')
  with check (auth.role() = 'service_role');

drop policy if exists "service role all on alpha_jobs" on public.alpha_jobs;
create policy "service role all on alpha_jobs"
  on public.alpha_jobs
  for all
  using (auth.role() = 'service_role')
  with check (auth.role() = 'service_role');
