pub mod directory;
pub mod registry;
pub mod shard;

use crate::config::Config;
use std::path::Path;
use uuid::Uuid;

pub struct PeerIdentity {
    pub peer_id: String,
}

impl PeerIdentity {
    pub fn load_or_create(path: &Path) -> anyhow::Result<Self> {
        if path.exists() {
            let id = std::fs::read_to_string(path)?.trim().to_string();
            Ok(Self { peer_id: id })
        } else {
            let id = Uuid::new_v4().to_string();
            if let Some(parent) = path.parent() {
                std::fs::create_dir_all(parent)?;
            }
            std::fs::write(path, &id)?;
            Ok(Self { peer_id: id })
        }
    }
}

pub async fn print_status(_cfg: &Config) -> anyhow::Result<()> {
    // Full implementation comes in Task 12 (peer directory).
    // For now just confirm the peer identity loads.
    let identity = PeerIdentity::load_or_create(&_cfg.peer.identity_path)?;
    println!("Peer ID: {}", identity.peer_id);
    println!("(Full status display available after Task 12)");
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB { format!("{:.1} GiB", bytes as f64 / GIB as f64) }
    else if bytes >= MIB { format!("{:.1} MiB", bytes as f64 / MIB as f64) }
    else { format!("{} B", bytes) }
}
