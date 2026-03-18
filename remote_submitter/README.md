# Remote Submitter (Rust)

Purpose:
- run on remote server only
- consume `alpha_jobs` from Supabase PostgreSQL
- submit/simulate to WorldQuant Brain
- write status/metrics back to DB

## Build
```bash
cd remote_submitter
cargo build --release
```

## Configure
```bash
cp .env.example .env
```

Required env:
- `DATABASE_URL`
- `WQB_USERNAME`
- `WQB_PASSWORD`

Tuning env:
- `CONCURRENCY` (default `56`)
- `FETCH_SIZE` (default `100`)
- `POLL_INTERVAL_MS` (default `2000`)
- `SIMULATION_WAIT_SECS` (default `900`)
- `STATUS_POLL_SECS` (default `5`)
- `AUTO_SUBMIT` (default `false`)

## Run
```bash
cargo run --release
```

## Job lifecycle
- claim: `queued/retry` -> `in_progress` (`created_at` old -> new)
- success: `simulated` or `submitted` (if `AUTO_SUBMIT=true`)
- failure: `retry` until `attempts >= max_attempts`, then `failed`

Scheduler model:
- continuous refill loop keeps slots saturated up to `CONCURRENCY`
- no "wait whole batch done before next fetch" bottleneck
