pub mod directory;
pub mod registry;
pub mod shard;

use crate::checkpoint::RemoteCheckpoint;
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
    let (registry_shard_size, registry_total_books, catalog_total_records) = if cfg.catalog.enabled
    {
        let manifest =
            crate::catalog::CatalogManifest::load(&store, &cfg.catalog.manifest_key).await?;
        (1, manifest.total_shards, manifest.total_records)
    } else {
        (
            cfg.storage.shard_size,
            cfg.storage.total_books,
            cfg.storage.total_books,
        )
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

    let remote_cp = RemoteCheckpoint::new(&store);
    let shard_manifests = remote_cp.list_shard_manifests().await?;
    let completed_books: u64 = shard_manifests
        .iter()
        .filter(|m| m.is_complete)
        .map(|m| m.completed_books.min(m.total_books))
        .sum();
    let completed_shards = shard_manifests.iter().filter(|m| m.is_complete).count();
    let pct = if catalog_total_records == 0 {
        100.0
    } else {
        (completed_books as f64 / catalog_total_records as f64) * 100.0
    };
    let remaining_books = catalog_total_records.saturating_sub(completed_books);
    let now = crate::invite::now_unix();
    let recent_window_secs = 3600_u64;
    let recent_completed: u64 = shard_manifests
        .iter()
        .filter(|m| m.is_complete && now.saturating_sub(m.last_updated_unix) <= recent_window_secs)
        .map(|m| m.completed_books.min(m.total_books))
        .sum();
    let rate_books_per_sec = if recent_completed > 0 {
        Some(recent_completed as f64 / recent_window_secs as f64)
    } else {
        historical_rate_books_per_sec(&shard_manifests)
    };

    println!();
    if cfg.catalog.enabled {
        println!("Catalog progress:");
    } else {
        println!("Corpus progress:");
    }
    println!(
        "  - books completed: {}/{} ({:.2}%)",
        completed_books, catalog_total_records, pct
    );
    println!("  - shards completed: {}", completed_shards);
    match rate_books_per_sec {
        Some(rate) if rate > 0.0 => {
            let books_per_hour = rate * 3600.0;
            println!(
                "  - cluster throughput (recent): {:.0} books/hour",
                books_per_hour
            );
            if remaining_books == 0 {
                println!("  - cluster ETA: complete");
            } else {
                let eta_secs = (remaining_books as f64 / rate).ceil() as u64;
                println!("  - cluster ETA: {}", format_duration(eta_secs));
            }
        }
        _ => {
            println!("  - cluster ETA: unavailable (need completed shard history)");
        }
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

fn historical_rate_books_per_sec(manifests: &[crate::checkpoint::ShardManifest]) -> Option<f64> {
    let mut min_ts = u64::MAX;
    let mut max_ts = 0_u64;
    let mut completed_books = 0_u64;
    let mut n = 0_u64;
    for m in manifests.iter().filter(|m| m.is_complete) {
        completed_books = completed_books.saturating_add(m.completed_books.min(m.total_books));
        min_ts = min_ts.min(m.last_updated_unix);
        max_ts = max_ts.max(m.last_updated_unix);
        n = n.saturating_add(1);
    }
    if n < 2 || completed_books == 0 || max_ts <= min_ts {
        return None;
    }
    let span = max_ts - min_ts;
    Some(completed_books as f64 / span as f64)
}

fn format_duration(total_secs: u64) -> String {
    let days = total_secs / 86_400;
    let hours = (total_secs % 86_400) / 3_600;
    let minutes = (total_secs % 3_600) / 60;
    let seconds = total_secs % 60;
    if days > 0 {
        format!("{days}d {hours:02}h {minutes:02}m")
    } else if hours > 0 {
        format!("{hours:02}h {minutes:02}m {seconds:02}s")
    } else {
        format!("{minutes:02}m {seconds:02}s")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(shard_id: u64, books: u64, ts: u64) -> crate::checkpoint::ShardManifest {
        crate::checkpoint::ShardManifest {
            shard_id,
            peer_id: "p".to_string(),
            total_books: books,
            completed_books: books,
            is_complete: true,
            last_updated_unix: ts,
        }
    }

    #[test]
    fn test_historical_rate_books_per_sec() {
        let manifests = vec![manifest(0, 100, 1000), manifest(1, 300, 1100)];
        let rate = historical_rate_books_per_sec(&manifests).unwrap();
        assert!((rate - 4.0).abs() < 1e-9);
    }

    #[test]
    fn test_format_duration() {
        assert_eq!(format_duration(59), "00m 59s");
        assert_eq!(format_duration(3661), "01h 01m 01s");
        assert_eq!(format_duration(90_061), "1d 01h 01m");
    }
}
