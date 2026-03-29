use crate::config::Config;
use crate::peer::PeerIdentity;

pub async fn run_init(cfg: &Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    std::fs::create_dir_all(&cfg.crawler.checkpoint_dir)?;

    println!("=== Thoth Initialized ===");
    println!("Peer ID:        {}", identity.peer_id);
    println!("Identity path:  {}", cfg.peer.identity_path.display());
    println!("Checkpoint dir: {}", cfg.crawler.checkpoint_dir.display());
    println!();
    println!("Next:");
    println!("1. Create invite code on coordinator: thoth --config config.toml invite");
    println!("2. Join from peer: thoth --config config.toml join <SHORTCODE>");
    println!("3. Start crawler: thoth --config config.toml crawl");
    Ok(())
}
