//! QueueFlow server binary.

mod cli;
mod metrics;
mod serve;
mod spec;

use clap::Parser;

use cli::{Cli, Command};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    init_tracing();

    match Cli::parse().command {
        Command::Serve(args) => serve::run(args).await,
        Command::Spec(args) => spec::write_spec(&args.output_dir),
        Command::Migrate(args) => {
            let pool = queueflow_core::connect(&args.database_url, 5).await?;
            queueflow_core::migrate(&pool).await?;
            tracing::info!("migrations applied");
            Ok(())
        }
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    fmt().with_env_filter(filter).json().init();
}
