//! Command-line interface, including a `spec` subcommand for OpenAPI/SDK
//! generation.

use std::path::PathBuf;

use clap::{Args, Parser, Subcommand, ValueEnum};

#[derive(Parser, Debug)]
#[command(
    name = "queueflow",
    version,
    about = "QueueFlow: a PostgreSQL/PGMQ-native job queue and workflow engine"
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

    /// PostgreSQL connection string (must have the PGMQ extension available).
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

    /// Maximum database connections in the pool.
    #[arg(long, env = "QUEUEFLOW_MAX_DB_CONNECTIONS", default_value_t = 50)]
    pub max_db_connections: u32,

    /// Apply migrations on startup (idempotent).
    #[arg(long, env = "QUEUEFLOW_AUTO_MIGRATE", default_value_t = true)]
    pub auto_migrate: bool,
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
