use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;

mod cli;
mod commands;
mod error;
mod util;

use clap::Parser;

use cli::{Cli, Commands};

#[tokio::main]
async fn main() -> miette::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse_from(wild::args_os());

    match cli.command {
        Commands::Create(args) => commands::create(args).await?,
        Commands::Extract(args) => commands::extract(args).await?,
        Commands::List(args) => commands::list(args).await?,
        Commands::Info(args) => commands::info(args).await?,
        Commands::Validate(args) => commands::validate(args).await?,
    };

    Ok(())
}
