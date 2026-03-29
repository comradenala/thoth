use crate::peer::shard::{shard_range, total_shards, ShardRange};
use crate::storage::S3Store;
use serde::{Deserialize, Serialize};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClaimRecord {
    pub peer_id: String,
    pub shard_id: u64,
    pub claimed_at_unix: u64,
    pub last_heartbeat_unix: u64,
}

impl ClaimRecord {
    pub fn claim_key(shard_id: u64) -> String {
        format!("peers/claims/shard-{shard_id:010}.json")
    }

    pub fn now_unix() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs()
    }
}

pub struct ShardRegistry<'a> {
    store: &'a S3Store,
    peer_id: &'a str,
    shard_size: u64,
    total_books: u64,
    claim_ttl_secs: u64,
}

impl<'a> ShardRegistry<'a> {
    pub fn new(
        store: &'a S3Store,
        peer_id: &'a str,
        shard_size: u64,
        total_books: u64,
        claim_ttl_secs: u64,
    ) -> Self {
        Self {
            store,
            peer_id,
            shard_size,
            total_books,
            claim_ttl_secs,
        }
    }

    /// Claim up to `n` unclaimed (or stale) shards. Returns claimed ShardRanges.
    pub async fn claim_shards(&self, n: u64) -> anyhow::Result<Vec<ShardRange>> {
        let total = total_shards(self.shard_size, self.total_books);
        let claimed_keys = self.store.list_keys("peers/claims/").await?;
        let now = ClaimRecord::now_unix();

        let mut live_shard_ids = std::collections::HashSet::new();
        for key in &claimed_keys {
            if let Some(data) = self.store.get_object(key).await? {
                if let Ok(record) = serde_json::from_slice::<ClaimRecord>(&data) {
                    if now.saturating_sub(record.last_heartbeat_unix) < self.claim_ttl_secs {
                        live_shard_ids.insert(record.shard_id);
                    }
                }
            }
        }

        let mut claimed = Vec::new();
        for shard_id in 0..total {
            if claimed.len() as u64 >= n {
                break;
            }
            if live_shard_ids.contains(&shard_id) {
                continue;
            }
            let record = ClaimRecord {
                peer_id: self.peer_id.to_string(),
                shard_id,
                claimed_at_unix: now,
                last_heartbeat_unix: now,
            };
            let key = ClaimRecord::claim_key(shard_id);
            let data = bytes::Bytes::from(serde_json::to_vec(&record)?);
            self.store.put_if_absent(&key, data).await?;

            // Re-read to confirm we own it (detects races)
            if let Some(raw) = self.store.get_object(&key).await? {
                if let Ok(r) = serde_json::from_slice::<ClaimRecord>(&raw) {
                    if r.peer_id == self.peer_id {
                        claimed.push(shard_range(shard_id, self.shard_size, self.total_books));
                    }
                }
            }
        }
        Ok(claimed)
    }

    /// Refresh heartbeat timestamp for a shard this peer owns.
    pub async fn heartbeat(&self, shard_id: u64) -> anyhow::Result<()> {
        let key = ClaimRecord::claim_key(shard_id);
        if let Some(raw) = self.store.get_object(&key).await? {
            let mut record: ClaimRecord = serde_json::from_slice(&raw)?;
            if record.peer_id == self.peer_id {
                record.last_heartbeat_unix = ClaimRecord::now_unix();
                let data = bytes::Bytes::from(serde_json::to_vec(&record)?);
                self.store
                    .put_object(&key, data, "application/json")
                    .await?;
            }
        }
        Ok(())
    }

    /// Release claim when shard is fully complete.
    pub async fn release_shard(&self, shard_id: u64) -> anyhow::Result<()> {
        let key = ClaimRecord::claim_key(shard_id);
        if let Some(raw) = self.store.get_object(&key).await? {
            if let Ok(record) = serde_json::from_slice::<ClaimRecord>(&raw) {
                if record.peer_id == self.peer_id {
                    self.store.delete_object(&key).await?;
                }
            }
        }
        Ok(())
    }

    /// List all live claims (for status display).
    pub async fn list_claims(&self) -> anyhow::Result<Vec<ClaimRecord>> {
        let keys = self.store.list_keys("peers/claims/").await?;
        let now = ClaimRecord::now_unix();
        let mut records = Vec::new();
        for key in keys {
            if let Some(raw) = self.store.get_object(&key).await? {
                if let Ok(r) = serde_json::from_slice::<ClaimRecord>(&raw) {
                    if now.saturating_sub(r.last_heartbeat_unix) < self.claim_ttl_secs {
                        records.push(r);
                    }
                }
            }
        }
        Ok(records)
    }
}
