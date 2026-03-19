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

Prebuilt binary in this repo:
- `bin/remote_submitter_arm64` (macOS arm64 / Apple Silicon)
- `bin/remote_submitter_linux_arm64` (Linux arm64, musl/static)

For Raspberry Pi (Linux arm64), build on target host:
```bash
cd remote_submitter
cargo build --release
./target/release/remote_submitter
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
- claim: `queued` -> `in_progress` (`created_at` old -> new)
- success: `success` (with `submitted=true/false`)
- failure: `failed`

Scheduler model:
- continuous refill loop keeps slots saturated up to `CONCURRENCY`
- no "wait whole batch done before next fetch" bottleneck
