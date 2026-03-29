use crate::storage::S3Store;
use serde::{Deserialize, Serialize};

const SHARD_MANIFEST_PREFIX: &str = "manifests/shards/";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardManifest {
    pub shard_id: u64,
    pub peer_id: String,
    pub total_books: u64,
    pub completed_books: u64,
    pub is_complete: bool,
    pub last_updated_unix: u64,
}

impl ShardManifest {
    fn s3_key(shard_id: u64) -> String {
        format!("{SHARD_MANIFEST_PREFIX}shard-{shard_id:010}.json")
    }
}

pub struct RemoteCheckpoint<'a> {
    store: &'a S3Store,
}

impl<'a> RemoteCheckpoint<'a> {
    pub fn new(store: &'a S3Store) -> Self {
        Self { store }
    }

    pub async fn load(&self, shard_id: u64) -> anyhow::Result<Option<ShardManifest>> {
        let key = ShardManifest::s3_key(shard_id);
        if let Some(data) = self.store.get_object(&key).await? {
            Ok(Some(serde_json::from_slice(&data)?))
        } else {
            Ok(None)
        }
    }

    pub async fn save(&self, manifest: &ShardManifest) -> anyhow::Result<()> {
        let key = ShardManifest::s3_key(manifest.shard_id);
        let data = bytes::Bytes::from(serde_json::to_vec(manifest)?);
        self.store.put_object(&key, data, "application/json").await
    }

    pub async fn list_shard_manifests(&self) -> anyhow::Result<Vec<ShardManifest>> {
        let keys = self.store.list_keys(SHARD_MANIFEST_PREFIX).await?;
        let mut out = Vec::new();
        for key in keys {
            let Some(data) = self.store.get_object(&key).await? else {
                continue;
            };
            if let Ok(m) = serde_json::from_slice::<ShardManifest>(&data) {
                out.push(m);
            }
        }
        Ok(out)
    }
}
