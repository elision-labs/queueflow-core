//! QueueFlow server binary.

mod cli;
mod client_cmds;
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
        Command::Job(cmd) => client_cmds::job(cmd).await,
        Command::Workflow(cmd) => client_cmds::workflow(cmd).await,
        Command::Tasks(args) => client_cmds::tasks(args).await,
        Command::Stats(args) => client_cmds::stats(args).await,
    }
}

fn init_tracing() {
    use tracing_subscriber::{fmt, EnvFilter};

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    // Logs on stderr so the client subcommands' JSON output owns stdout.
    fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .json()
        .init();
}
