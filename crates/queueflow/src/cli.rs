//! Command-line interface, including a `spec` subcommand for OpenAPI/SDK
//! generation.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "queueflow",
    version,
    about = "QueueFlow: a PostgreSQL-native job queue and workflow engine"
)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Subcommand, Debug)]
pub enum Command {
    /// Run the API server and/or workers.
    Serve(ServeArgs),
    /// Write the OpenAPI document (openapi.json + openapi.yaml) to a directory.
    Spec(SpecArgs),
    /// Apply database migrations and exit.
    Migrate(DbArgs),
    /// Create, inspect, cancel, or await jobs on a running server.
    #[command(subcommand)]
    Job(JobCommand),
    /// Create, inspect, or cancel workflows on a running server.
    #[command(subcommand)]
    Workflow(WorkflowCommand),
    /// Inspect and replay dead-lettered jobs on a running server.
    #[command(subcommand)]
    Dlq(DlqCommand),
    /// Manage recurring enqueues (cron schedules, UTC) on a running server.
    #[command(subcommand)]
    Cron(CronCommand),
    /// List the task handlers registered in the server.
    Tasks(ClientArgs),
    /// Show the server's engine counters.
    Stats(ClientArgs),
}

/// How to reach a running QueueFlow server.
#[derive(Args, Debug, Clone)]
pub struct ClientArgs {
    /// Base URL of the QueueFlow API.
    #[arg(
        long,
        env = "QUEUEFLOW_SERVER_URL",
        default_value = "http://localhost:8000",
        global = false
    )]
    pub server_url: String,

    /// Bearer token for the API.
    #[arg(long, env = "QUEUEFLOW_TOKEN", default_value = "dev")]
    pub token: String,
}

#[derive(Subcommand, Debug)]
pub enum JobCommand {
    /// Enqueue a job; prints its id (or the original id on an idempotent replay).
    Create {
        #[command(flatten)]
        client: ClientArgs,
        /// Task handler name.
        #[arg(long)]
        task: String,
        /// JSON object payload.
        #[arg(long, default_value = "{}")]
        payload: String,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        max_retries: Option<u32>,
        #[arg(long)]
        timeout_secs: Option<u64>,
        /// Idempotency key: re-running the same command returns the same job.
        #[arg(long)]
        idempotency_key: Option<String>,
        /// Don't run before this instant (RFC 3339, e.g. 2026-06-09T15:00:00Z).
        #[arg(long)]
        run_at: Option<chrono::DateTime<chrono::Utc>>,
        /// Block until the job reaches a terminal state, then print it.
        #[arg(long)]
        wait: bool,
    },
    /// Fetch one job as JSON.
    Get {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// List jobs as JSON.
    List {
        #[command(flatten)]
        client: ClientArgs,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        offset: Option<i64>,
        /// Also compute the exact total (extra count query server-side).
        #[arg(long)]
        include_total: bool,
    },
    /// Cancel a job (409 if it already finished).
    Cancel {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// Wait for a job to finish and print it.
    Watch {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
        /// Give up after this many seconds.
        #[arg(long, default_value_t = 600)]
        timeout_secs: u64,
    },
}

#[derive(Subcommand, Debug)]
pub enum WorkflowCommand {
    /// Create a workflow from a JSON definition; prints its id.
    Create {
        #[command(flatten)]
        client: ClientArgs,
        /// Path to a JSON file with the CreateWorkflowRequest body ('-' for stdin).
        #[arg(long)]
        file: String,
    },
    /// Fetch one workflow as JSON.
    Get {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// List workflows as JSON.
    List {
        #[command(flatten)]
        client: ClientArgs,
        #[arg(long)]
        status: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        offset: Option<i64>,
        #[arg(long)]
        include_total: bool,
    },
    /// Cancel a workflow and its unscheduled steps.
    Cancel {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// Print the workflow DAG as a Mermaid document.
    Diagram {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
}

#[derive(Subcommand, Debug)]
pub enum DlqCommand {
    /// List dead letters as JSON (newest first).
    List {
        #[command(flatten)]
        client: ClientArgs,
        #[arg(long)]
        queue: Option<String>,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        offset: Option<i64>,
        #[arg(long)]
        include_total: bool,
    },
    /// Fetch one dead letter as JSON.
    Get {
        #[command(flatten)]
        client: ClientArgs,
        id: i64,
    },
    /// Replay a dead letter as a fresh job; prints the new job id.
    Replay {
        #[command(flatten)]
        client: ClientArgs,
        id: i64,
    },
}

#[derive(Subcommand, Debug)]
pub enum CronCommand {
    /// Create a schedule; prints its id.
    Create {
        #[command(flatten)]
        client: ClientArgs,
        /// Unique schedule name (per tenant).
        #[arg(long)]
        name: String,
        /// 5-field crontab, UTC (e.g. "*/5 * * * *"); 6/7 fields with leading
        /// seconds also accepted.
        #[arg(long)]
        schedule: String,
        /// Task handler to enqueue.
        #[arg(long)]
        task: String,
        /// JSON object payload for each firing.
        #[arg(long, default_value = "{}")]
        payload: String,
        #[arg(long)]
        queue: Option<String>,
    },
    /// List schedules as JSON.
    List {
        #[command(flatten)]
        client: ClientArgs,
        #[arg(long)]
        limit: Option<i64>,
        #[arg(long)]
        offset: Option<i64>,
        #[arg(long)]
        include_total: bool,
    },
    /// Fetch one schedule as JSON.
    Get {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// Delete a schedule (already-enqueued jobs are unaffected).
    Delete {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// Stop firings until resumed.
    Pause {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
    /// Resume firings at the next future occurrence.
    Resume {
        #[command(flatten)]
        client: ClientArgs,
        id: String,
    },
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, ValueEnum)]
pub enum Mode {
    /// REST API only.
    Api,
    /// Workers only.
    Worker,
    /// Both API and workers (default).
    All,
}

#[derive(Args, Debug)]
pub struct ServeArgs {
    /// What to run.
    #[arg(long, env = "QUEUEFLOW_MODE", value_enum, default_value_t = Mode::All)]
    pub mode: Mode,

    /// PostgreSQL connection string (any plain PostgreSQL 13+).
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,

    /// API server port.
    #[arg(long, env = "QUEUEFLOW_API_PORT", default_value_t = 8000)]
    pub api_port: u16,

    /// Number of concurrent workers per queue.
    #[arg(long, env = "QUEUEFLOW_WORKERS", default_value_t = 10)]
    pub workers: usize,

    /// Prometheus metrics port.
    #[arg(long, env = "QUEUEFLOW_METRICS_PORT", default_value_t = 9090)]
    pub metrics_port: u16,

    /// Default queue name.
    #[arg(long, env = "QUEUEFLOW_DEFAULT_QUEUE", default_value = "default")]
    pub default_queue: String,

    /// Credential required by the worker-protocol endpoints (lease,
    /// heartbeat, complete, fail). Workers execute arbitrary tenants' jobs,
    /// so this must not be a tenant token. Required in `api` and `all` mode
    /// unless --dev is set.
    #[arg(long, env = "QUEUEFLOW_WORKER_TOKEN")]
    pub worker_token: Option<String>,

    /// HS256 secret for validating tenant JWTs on `/api/v1` (the token's
    /// `sub` claim is the tenant id; `exp` is enforced). May be combined
    /// with --api-keys. In `api` and `all` mode at least one of
    /// --jwt-secret / --api-keys is required unless --dev is set.
    #[arg(long, env = "QUEUEFLOW_JWT_SECRET")]
    pub jwt_secret: Option<String>,

    /// Development mode: run the API without configured credentials. Any
    /// non-empty bearer token authenticates as tenant `tenant1`, and without
    /// --worker-token the worker-protocol endpoints accept any authenticated
    /// caller. Never set this on a reachable deployment.
    #[arg(long, env = "QUEUEFLOW_DEV", default_value_t = false)]
    pub dev: bool,

    /// Static tenant API keys, comma-separated `token:tenant` pairs
    /// (e.g. "k1:acme,k2:globex"). May be combined with --jwt-secret.
    #[arg(long, env = "QUEUEFLOW_API_KEYS")]
    pub api_keys: Option<String>,

    /// Comma-separated list of origins allowed by CORS
    /// (e.g. "https://app.example.com,https://admin.example.com").
    /// Unset = permissive CORS (development mode).
    #[arg(long, env = "QUEUEFLOW_CORS_ORIGINS")]
    pub cors_origins: Option<String>,

    /// Maximum database connections in the pool.
    #[arg(long, env = "QUEUEFLOW_MAX_DB_CONNECTIONS", default_value_t = 50)]
    pub max_db_connections: u32,

    /// Apply migrations on startup (idempotent). Pass `--auto-migrate false`
    /// (or QUEUEFLOW_AUTO_MIGRATE=false) to skip them, e.g. when migrations
    /// are run as a separate `queueflow migrate` step.
    #[arg(
        long,
        env = "QUEUEFLOW_AUTO_MIGRATE",
        default_value_t = true,
        action = clap::ArgAction::Set,
        num_args = 0..=1,
        default_missing_value = "true"
    )]
    pub auto_migrate: bool,

    /// Delete terminal jobs/workflows/dead letters older than this many
    /// hours. Off by default (history is kept forever); the hot claim path is
    /// unaffected either way.
    #[arg(long, env = "QUEUEFLOW_RETENTION_HOURS")]
    pub retention_hours: Option<u64>,
}

#[derive(Args, Debug)]
pub struct SpecArgs {
    /// Directory to write `openapi.json` and `openapi.yaml` into.
    #[arg(long, default_value = "spec")]
    pub output_dir: PathBuf,
}

#[derive(Args, Debug)]
pub struct DbArgs {
    #[arg(long, env = "DATABASE_URL")]
    pub database_url: String,
}
