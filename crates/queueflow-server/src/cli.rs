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
