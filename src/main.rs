mod catalog;
mod checkpoint;
mod config;
mod crawler;
mod init;
mod invite;
mod package;
mod peer;
mod storage;
mod systemd;

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
    /// Initialize this machine for Thoth (identity + directories)
    Init,
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
    /// Create a short invite code for peer onboarding
    Invite {
        #[arg(long, default_value_t = 24)]
        expiry_hours: u32,
    },
    /// Join a cluster using a shortcode created by `thoth invite`
    Join {
        shortcode: String,
        #[arg(short, long, default_value = "joined.toml")]
        output: PathBuf,
        #[arg(long)]
        force: bool,
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
        Commands::Init => init::run_init(&cfg).await?,
        Commands::Crawl => crawler::run(cfg).await?,
        Commands::Status => peer::print_status(&cfg).await?,
        Commands::Package { output, chunk_mib } => {
            package::run(cfg, output, chunk_mib * 1024 * 1024).await?
        }
        Commands::Invite { expiry_hours } => invite::run_invite(&cfg, expiry_hours).await?,
        Commands::Join {
            shortcode,
            output,
            force,
        } => invite::run_join(&cfg, &shortcode, &output, force).await?,
    }
    Ok(())
}
