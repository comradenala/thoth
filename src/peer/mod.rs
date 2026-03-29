pub mod directory;
pub mod registry;
pub mod shard;

use crate::config::Config;
use crate::peer::directory::PeerDirectoryStore;
use crate::peer::registry::ShardRegistry;
use crate::storage;
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

pub async fn print_status(cfg: &Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    println!("Peer ID: {}", identity.peer_id);
    let store = storage::from_config(&cfg.storage).await?;
    let (registry_shard_size, registry_total_books) = if cfg.catalog.enabled {
        let manifest =
            crate::catalog::CatalogManifest::load(&store, &cfg.catalog.manifest_key).await?;
        (1, manifest.total_shards)
    } else {
        (cfg.storage.shard_size, cfg.storage.total_books)
    };

    let registry = ShardRegistry::new(
        &store,
        &identity.peer_id,
        registry_shard_size,
        registry_total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let claims = registry.list_claims().await?;

    let peers = PeerDirectoryStore::new(&store)
        .list_active(cfg.crawler.claim_ttl_secs.saturating_mul(2))
        .await?;

    println!();
    println!("Active peers: {}", peers.len());
    for peer in peers {
        let age = crate::invite::now_unix().saturating_sub(peer.last_seen_unix);
        println!(
            "  - {} | claims={} | seen={}s ago | v{}",
            peer.peer_id, peer.active_claims, age, peer.version
        );
    }

    println!();
    println!("Live shard claims: {}", claims.len());
    for c in claims {
        let age = crate::invite::now_unix().saturating_sub(c.last_heartbeat_unix);
        println!(
            "  - shard {:>6} -> {} (heartbeat {}s ago)",
            c.shard_id, c.peer_id, age
        );
    }

    let invites = crate::invite::list_invites(cfg).await?;
    let mine: Vec<_> = invites
        .into_iter()
        .filter(|i| i.created_by_peer_id == identity.peer_id)
        .collect();
    println!();
    println!("Invites created by this node: {}", mine.len());
    for inv in mine {
        let state = inv.state(crate::invite::now_unix());
        let redeemed = inv.redeemed_by_peer_id.as_deref().unwrap_or("-");
        println!(
            "  - {} | {} | expires={} | redeemed_by={}",
            inv.code, state, inv.expires_at_unix, redeemed
        );
    }

    Ok(())
}
