use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::collections::HashSet;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRecord {
    pub book_id: u64,
    pub shard_id: u64,
    pub title: String,
    pub author: String,
    pub format: String,
    pub s3_key: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub crawled_at_unix: u64,
}

pub struct LocalCheckpoint { path: PathBuf }
impl LocalCheckpoint {
    pub fn new(_dir: &Path, _shard_id: u64) -> Self { unimplemented!() }
    pub fn completed_ids(&self) -> anyhow::Result<HashSet<u64>> { unimplemented!() }
    pub fn append(&self, _record: &BookRecord) -> anyhow::Result<()> { unimplemented!() }
    pub fn iter_records(&self) -> anyhow::Result<Vec<BookRecord>> { unimplemented!() }
}
