mod config;
mod error;
mod storage;
mod peer;
mod checkpoint;
mod crawler;
mod systemd;
mod package;
mod invite;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "thoth", version, about = "P2P book archiver")]
struct Cli {
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the crawler daemon
    Crawl,
    /// Show peer status and shard claims
    Status,
    /// Package finished corpus into distributable dataset
    Package {
        #[arg(short, long, default_value = "dist")]
        output: PathBuf,
        #[arg(long, default_value_t = 512)]
        chunk_mib: u64,
    },
    /// Generate a config snippet + scoped credentials for a new peer to join
    Invite {
        #[arg(long, default_value_t = 24)]
        expiry_hours: u32,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;

    match cli.command {
        Commands::Crawl => crawler::run(cfg).await?,
        Commands::Status => peer::print_status(cfg).await?,
        Commands::Package { output, chunk_mib } => {
            package::run(cfg, output, chunk_mib * 1024 * 1024).await?
        }
        Commands::Invite { expiry_hours } => {
            invite::run_invite(&cfg, expiry_hours).await?
        }
    }
    Ok(())
}
