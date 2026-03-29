use serde::{Deserialize, Serialize};
use crate::storage::S3Store;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardManifest {
    pub shard_id: u64,
    pub peer_id: String,
    pub total_books: u64,
    pub completed_books: u64,
    pub is_complete: bool,
    pub last_updated_unix: u64,
}

pub struct RemoteCheckpoint<'a> { _store: &'a S3Store }
impl<'a> RemoteCheckpoint<'a> {
    pub fn new(store: &'a S3Store) -> Self { Self { _store: store } }
    pub async fn save(&self, _m: &ShardManifest) -> anyhow::Result<()> { unimplemented!() }
    pub async fn load(&self, _shard_id: u64) -> anyhow::Result<Option<ShardManifest>> { unimplemented!() }
}
