use anyhow::{anyhow, Context, Result};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::{Client, Method, Response, StatusCode};
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::env;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Semaphore;
use tokio::time::{sleep, Instant};
use tracing::{error, info, warn};

#[derive(Clone, Debug)]
struct AppConfig {
    database_url: String,
    wqb_username: String,
    wqb_password: String,
    wqb_base_url: String,
    worker_id: String,
    fetch_size: i64,
    concurrency: usize,
    poll_interval_ms: u64,
    simulation_wait_secs: u64,
    status_poll_secs: u64,
    auto_submit: bool,
    db_pool_max: u32,
}

impl AppConfig {
    fn from_env() -> Result<Self> {
        let database_url = required_env("DATABASE_URL")?;
        let wqb_username = required_env("WQB_USERNAME")?;
        let wqb_password = required_env("WQB_PASSWORD")?;
        let wqb_base_url = env_or("WQB_BASE_URL", "https://api.worldquantbrain.com");
        let worker_id = env_or("WORKER_ID", format!("remote-submitter-{}", std::process::id()));
        let fetch_size = parse_env::<i64>("FETCH_SIZE", 100)?;
        let concurrency = parse_env::<usize>("CONCURRENCY", 56)?;
        let poll_interval_ms = parse_env::<u64>("POLL_INTERVAL_MS", 2000)?;
        let simulation_wait_secs = parse_env::<u64>("SIMULATION_WAIT_SECS", 900)?;
        let status_poll_secs = parse_env::<u64>("STATUS_POLL_SECS", 5)?;
        let auto_submit = parse_env_bool("AUTO_SUBMIT", false);
        let db_pool_max = parse_env::<u32>("DB_POOL_MAX", 20)?;
        Ok(Self {
            database_url,
            wqb_username,
            wqb_password,
            wqb_base_url,
            worker_id,
            fetch_size: fetch_size.max(1),
            concurrency: concurrency.max(1),
            poll_interval_ms: poll_interval_ms.max(500),
            simulation_wait_secs: simulation_wait_secs.max(60),
            status_poll_secs: status_poll_secs.max(1),
            auto_submit,
            db_pool_max: db_pool_max.max(2),
        })
    }
}

#[derive(sqlx::FromRow, Debug, Clone)]
struct JobRow {
    id: i64,
    expression: String,
    settings: Value,
    region: String,
    universe: String,
    delay: i32,
    neutralization: String,
    language: String,
    attempts: i32,
}

#[derive(Debug, Clone)]
struct JobOutcome {
    alpha_id: String,
    link: String,
    sharpe: Option<f64>,
    fitness: Option<f64>,
    turnover: Option<f64>,
    submitted: bool,
    raw: Value,
}

#[derive(Clone)]
struct WqbClient {
    client: Client,
    base_url: String,
    username: String,
    password: String,
}

impl WqbClient {
    async fn new(base_url: String, username: String, password: String) -> Result<Self> {
        let client = Client::builder()
            .cookie_store(true)
            .use_rustls_tls()
            .timeout(Duration::from_secs(60))
            .build()
            .context("build reqwest client")?;
        let me = Self {
            client,
            base_url: base_url.trim_end_matches('/').to_string(),
            username,
            password,
        };
        me.authenticate().await?;
        Ok(me)
    }

    fn abs_url(&self, path_or_url: &str) -> String {
        if path_or_url.starts_with("http://") || path_or_url.starts_with("https://") {
            path_or_url.to_string()
        } else {
            format!("{}{}", self.base_url, path_or_url)
        }
    }

    async fn authenticate(&self) -> Result<()> {
        let url = self.abs_url("/authentication");
        let resp = self
            .client
            .post(url)
            .basic_auth(&self.username, Some(&self.password))
            .header("Accept", "application/json")
            .send()
            .await
            .context("auth request failed")?;
        if resp.status().is_success() {
            return Ok(());
        }
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        Err(anyhow!("authentication failed: {} {}", status, body))
    }

    async fn request_json(&self, method: Method, path_or_url: &str, body: Option<&Value>) -> Result<Response> {
        let url = self.abs_url(path_or_url);
        let mut last_err: Option<anyhow::Error> = None;
        for _ in 0..2 {
            let mut req = self
                .client
                .request(method.clone(), &url)
                .header("Accept", "application/json")
                .header("Content-Type", "application/json");
            if let Some(payload) = body {
                req = req.json(payload);
            }
            let resp = req.send().await.context("wqb request failed")?;
            if resp.status() != StatusCode::UNAUTHORIZED {
                return Ok(resp);
            }
            if let Err(e) = self.authenticate().await {
                last_err = Some(e);
            }
        }
        Err(last_err.unwrap_or_else(|| anyhow!("wqb request unauthorized twice")))
    }

    async fn poll_location(&self, location: &str, max_wait_secs: u64, status_poll_secs: u64) -> Result<Response> {
        let target = self.abs_url(location);
        let deadline = Instant::now() + Duration::from_secs(max_wait_secs);
        loop {
            if Instant::now() > deadline {
                return Err(anyhow!("simulation progress timeout"));
            }
            let resp = self.request_json(Method::GET, &target, None).await?;
            let retry_after = resp
                .headers()
                .get("Retry-After")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            if let Some(wait_secs) = retry_after {
                sleep(Duration::from_secs(wait_secs.max(1))).await;
                continue;
            }
            if resp.status().is_success() {
                return Ok(resp);
            }
            sleep(Duration::from_secs(status_poll_secs.max(1))).await;
        }
    }

    async fn load_alpha_detail(&self, alpha_id: &str, max_wait_secs: u64, status_poll_secs: u64) -> Result<Value> {
        let deadline = Instant::now() + Duration::from_secs(max_wait_secs);
        let path = format!("/alphas/{}", alpha_id);
        loop {
            if Instant::now() > deadline {
                return Err(anyhow!("alpha detail timeout: {}", alpha_id));
            }
            let resp = self.request_json(Method::GET, &path, None).await?;
            if !resp.status().is_success() {
                sleep(Duration::from_secs(status_poll_secs.max(1))).await;
                continue;
            }
            let payload: Value = resp.json().await.unwrap_or(json!({}));
            if payload.get("is").is_some() {
                return Ok(payload);
            }
            sleep(Duration::from_secs(status_poll_secs.max(1))).await;
        }
    }

    async fn simulate_job(&self, job: &JobRow, cfg: &AppConfig) -> Result<JobOutcome> {
        let settings = if job.settings.is_object() {
            job.settings.clone()
        } else {
            json!({
                "region": job.region,
                "universe": job.universe,
                "instrumentType": "EQUITY",
                "delay": job.delay,
                "decay": 0,
                "neutralization": job.neutralization,
                "truncation": 0.08,
                "pasteurization": "ON",
                "unitHandling": "VERIFY",
                "nanHandling": "OFF",
                "maxTrade": "OFF",
                "language": job.language,
                "visualization": false,
                "testPeriod": "P5Y0M0D"
            })
        };
        let payload = json!({
            "type": "REGULAR",
            "regular": job.expression,
            "settings": settings,
        });

        let sim_resp = self
            .request_json(Method::POST, "/simulations", Some(&payload))
            .await
            .context("post /simulations failed")?;
        if !(sim_resp.status().is_success() || sim_resp.status() == StatusCode::CREATED) {
            let status = sim_resp.status();
            let text = sim_resp.text().await.unwrap_or_default();
            return Err(anyhow!("simulation request failed: {} {}", status, text));
        }

        let final_resp = if let Some(loc) = sim_resp.headers().get("Location").and_then(|v| v.to_str().ok()) {
            self.poll_location(loc, cfg.simulation_wait_secs, cfg.status_poll_secs).await?
        } else {
            sim_resp
        };
        let body: Value = final_resp.json().await.unwrap_or(json!({}));
        let alpha_id = extract_alpha_id(&body).ok_or_else(|| anyhow!("missing alpha id in simulation response"))?;

        let detail = self
            .load_alpha_detail(&alpha_id, cfg.simulation_wait_secs, cfg.status_poll_secs)
            .await
            .context("load alpha detail failed")?;
        let is_block = detail.get("is").cloned().unwrap_or(json!({}));
        let sharpe = to_f64(is_block.get("sharpe"));
        let fitness = to_f64(is_block.get("fitness"));
        let turnover = to_f64(is_block.get("turnover")).map(|x| x * 100.0);
        let link = format!("https://platform.worldquantbrain.com/alpha/{}", alpha_id);

        let mut submitted = false;
        if cfg.auto_submit {
            let submit_path = format!("/alphas/{}/submit", alpha_id);
            let submit_resp = self.request_json(Method::POST, &submit_path, None).await?;
            if submit_resp.status().is_success() {
                submitted = true;
            } else {
                let status = submit_resp.status();
                let text = submit_resp.text().await.unwrap_or_default();
                warn!("submit failed alpha_id={} status={} body={}", alpha_id, status, text);
            }
        }

        Ok(JobOutcome {
            alpha_id,
            link,
            sharpe,
            fitness,
            turnover,
            submitted,
            raw: detail,
        })
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cfg = AppConfig::from_env()?;
    info!(
        "start remote_submitter worker_id={} concurrency={} fetch_size={} auto_submit={}",
        cfg.worker_id, cfg.concurrency, cfg.fetch_size, cfg.auto_submit
    );

    let pool = PgPoolOptions::new()
        .max_connections(cfg.db_pool_max)
        .connect(&cfg.database_url)
        .await
        .context("connect postgres failed")?;

    let wqb = Arc::new(
        WqbClient::new(
            cfg.wqb_base_url.clone(),
            cfg.wqb_username.clone(),
            cfg.wqb_password.clone(),
        )
        .await
        .context("init wqb client failed")?,
    );
    let semaphore = Arc::new(Semaphore::new(cfg.concurrency));

    loop {
        let mut active: FuturesUnordered<tokio::task::JoinHandle<()>> = FuturesUnordered::new();
        loop {
            while active.len() < cfg.concurrency {
                let room = (cfg.concurrency - active.len()) as i64;
                let claim_limit = cfg.fetch_size.min(room).max(1);
                let jobs = claim_jobs(&pool, claim_limit, &cfg.worker_id).await?;
                if jobs.is_empty() {
                    break;
                }
                info!("claimed {} jobs (active={}/{})", jobs.len(), active.len(), cfg.concurrency);
                for job in jobs {
                    let permit = semaphore.clone().acquire_owned().await?;
                    let pool_cloned = pool.clone();
                    let wqb_cloned = wqb.clone();
                    let cfg_cloned = cfg.clone();
                    active.push(tokio::spawn(async move {
                        let _permit = permit;
                        let result = wqb_cloned.simulate_job(&job, &cfg_cloned).await;
                        match result {
                            Ok(outcome) => {
                                if let Err(e) = mark_success(&pool_cloned, job.id, &outcome).await {
                                    error!("mark_success failed id={} err={}", job.id, e);
                                } else {
                                    info!(
                                        "job={} alpha_id={} sharpe={:?} fitness={:?} turnover={:?}",
                                        job.id, outcome.alpha_id, outcome.sharpe, outcome.fitness, outcome.turnover
                                    );
                                }
                            }
                            Err(e) => {
                                let message = e.to_string();
                                if let Err(db_err) = mark_failure(&pool_cloned, &job, &message).await {
                                    error!("mark_failure failed id={} err={}", job.id, db_err);
                                } else {
                                    warn!("job={} failed err={}", job.id, message);
                                }
                            }
                        }
                    }));
                }
            }

            if active.is_empty() {
                sleep(Duration::from_millis(cfg.poll_interval_ms)).await;
                break;
            }

            if let Some(done) = active.next().await {
                if let Err(e) = done {
                    error!("worker task panic: {}", e);
                }
            }
        }
    }
}

async fn claim_jobs(pool: &PgPool, fetch_size: i64, worker_id: &str) -> Result<Vec<JobRow>> {
    let sql = r#"
with picked as (
  select id
  from public.alpha_jobs
  where status = 'queued'
  order by created_at asc, id asc
  limit $1
  for update skip locked
)
update public.alpha_jobs j
set status = 'in_progress',
    locked_by = $2,
    locked_at = timezone('utc', now()),
    attempts = j.attempts + 1,
    updated_at = timezone('utc', now())
from picked
where j.id = picked.id
returning
  j.id,
  j.expression,
  j.settings,
  j.region,
  j.universe,
  j.delay,
  j.neutralization,
  j.language,
  j.attempts
"#;
    let rows = sqlx::query_as::<_, JobRow>(sql)
        .bind(fetch_size)
        .bind(worker_id)
        .fetch_all(pool)
        .await
        .context("claim_jobs query failed")?;
    Ok(rows)
}

async fn mark_success(pool: &PgPool, job_id: i64, outcome: &JobOutcome) -> Result<()> {
    sqlx::query(
        r#"
update public.alpha_jobs
set status = 'success',
    alpha_id = $2,
    link = $3,
    sharpe = $4,
    fitness = $5,
    turnover = $6,
    submitted = $7,
    error_message = null,
    last_response = $8,
    locked_by = null,
    locked_at = null,
    updated_at = timezone('utc', now())
where id = $1
"#,
    )
    .bind(job_id)
    .bind(&outcome.alpha_id)
    .bind(&outcome.link)
    .bind(outcome.sharpe)
    .bind(outcome.fitness)
    .bind(outcome.turnover)
    .bind(outcome.submitted)
    .bind(&outcome.raw)
    .execute(pool)
    .await
    .context("mark_success update failed")?;
    Ok(())
}

async fn mark_failure(pool: &PgPool, job: &JobRow, message: &str) -> Result<()> {
    sqlx::query(
        r#"
update public.alpha_jobs
set status = 'failed',
    error_message = $2,
    last_response = jsonb_build_object(
      'error', $2,
      'attempts', $3
    ),
    locked_by = null,
    locked_at = null,
    updated_at = timezone('utc', now())
where id = $1
"#,
    )
    .bind(job.id)
    .bind(message)
    .bind(job.attempts)
    .execute(pool)
    .await
    .context("mark_failure update failed")?;
    Ok(())
}

fn extract_alpha_id(body: &Value) -> Option<String> {
    if let Some(alpha) = body.get("alpha").and_then(|v| v.as_str()) {
        let tail = alpha.trim_end_matches('/').rsplit('/').next()?.trim();
        if !tail.is_empty() {
            return Some(tail.to_string());
        }
    }
    if let Some(alpha_id) = body.get("alphaId").and_then(|v| v.as_str()) {
        let v = alpha_id.trim();
        if !v.is_empty() {
            return Some(v.to_string());
        }
    }
    None
}

fn to_f64(value: Option<&Value>) -> Option<f64> {
    match value {
        Some(Value::Number(n)) => n.as_f64(),
        Some(Value::String(s)) => s.parse::<f64>().ok(),
        _ => None,
    }
}

fn required_env(name: &str) -> Result<String> {
    let v = env::var(name).unwrap_or_default();
    let trimmed = v.trim().to_string();
    if trimmed.is_empty() {
        return Err(anyhow!("missing env: {}", name));
    }
    Ok(trimmed)
}

fn env_or(name: &str, default: impl Into<String>) -> String {
    let v = env::var(name).unwrap_or_default();
    let trimmed = v.trim();
    if trimmed.is_empty() {
        default.into()
    } else {
        trimmed.to_string()
    }
}

fn parse_env<T: std::str::FromStr>(name: &str, default: T) -> Result<T> {
    let raw = env::var(name).unwrap_or_default();
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return Ok(default);
    }
    trimmed
        .parse::<T>()
        .map_err(|_| anyhow!("invalid env value for {}", name))
}

fn parse_env_bool(name: &str, default: bool) -> bool {
    let raw = env::var(name).unwrap_or_default();
    let v = raw.trim().to_lowercase();
    if v.is_empty() {
        return default;
    }
    matches!(v.as_str(), "1" | "true" | "yes" | "y" | "on")
}
