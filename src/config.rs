use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub peer: PeerConfig,
    pub storage: StorageConfig,
    pub crawler: CrawlerConfig,
    #[serde(default)]
    pub package: PackageConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PeerConfig {
    pub identity_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub bucket: String,
    pub endpoint_url: Option<String>,
    #[serde(default = "default_shard_size")]
    pub shard_size: u64,
    #[serde(default = "default_total_books")]
    pub total_books: u64,
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold_bytes: u64,
    #[serde(default = "default_part_size")]
    pub multipart_part_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CrawlerConfig {
    #[serde(default = "default_daily_quota")]
    pub daily_quota: u64,
    pub book_url_template: String,
    #[serde(default = "default_shards_per_peer")]
    pub shards_per_peer: u64,
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    #[serde(default = "default_claim_ttl")]
    pub claim_ttl_secs: u64,
    pub checkpoint_dir: PathBuf,
    #[serde(default)]
    pub use_spider: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PackageConfig {
    #[serde(default)]
    pub generate_torrent: bool,
    pub tracker_url: Option<String>,
    #[serde(default)]
    pub pin_ipfs: bool,
    #[serde(default = "default_ipfs_api")]
    pub ipfs_api_url: String,
}

impl Default for PackageConfig {
    fn default() -> Self {
        Self {
            generate_torrent: false,
            tracker_url: None,
            pin_ipfs: false,
            ipfs_api_url: default_ipfs_api(),
        }
    }
}

fn default_shard_size() -> u64 { 10_000 }
fn default_total_books() -> u64 { 40_000_000 }
fn default_multipart_threshold() -> u64 { 100 * 1024 * 1024 }
fn default_part_size() -> u64 { 10 * 1024 * 1024 }
fn default_daily_quota() -> u64 { 100_000 }
fn default_shards_per_peer() -> u64 { 2 }
fn default_concurrency() -> usize { 4 }
fn default_claim_ttl() -> u64 { 3600 }
fn default_ipfs_api() -> String { "http://127.0.0.1:5001".to_string() }

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| anyhow::anyhow!("cannot read config {}: {e}", path.display()))?;
        let config: Config = toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("config parse error: {e}"))?;
        Ok(config)
    }
}
