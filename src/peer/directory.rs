use crate::storage::S3Store;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

const DIRECTORY_PREFIX: &str = "peers/directory/";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub peer_id: String,
    pub last_seen_unix: u64,
    pub active_claims: u64,
    pub version: String,
}

impl PeerEntry {
    fn key(peer_id: &str) -> String {
        format!("{DIRECTORY_PREFIX}{peer_id}.json")
    }
}

pub struct PeerDirectoryStore<'a> {
    store: &'a S3Store,
}

impl<'a> PeerDirectoryStore<'a> {
    pub fn new(store: &'a S3Store) -> Self {
        Self { store }
    }

    pub async fn heartbeat(&self, peer_id: &str, active_claims: u64) -> anyhow::Result<()> {
        let entry = PeerEntry {
            peer_id: peer_id.to_string(),
            last_seen_unix: now_unix(),
            active_claims,
            version: env!("CARGO_PKG_VERSION").to_string(),
        };
        let key = PeerEntry::key(peer_id);
        let data = Bytes::from(serde_json::to_vec(&entry)?);
        self.store.put_object(&key, data, "application/json").await
    }

    pub async fn list_active(&self, ttl_secs: u64) -> anyhow::Result<Vec<PeerEntry>> {
        let keys = self.store.list_keys(DIRECTORY_PREFIX).await?;
        let now = now_unix();
        let mut out = Vec::new();
        for key in keys {
            let Some(raw) = self.store.get_object(&key).await? else {
                continue;
            };
            if let Ok(entry) = serde_json::from_slice::<PeerEntry>(&raw) {
                if now.saturating_sub(entry.last_seen_unix) <= ttl_secs {
                    out.push(entry);
                }
            }
        }
        out.sort_by(|a, b| b.last_seen_unix.cmp(&a.last_seen_unix));
        Ok(out)
    }
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
