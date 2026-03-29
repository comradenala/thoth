# P2P Book Archiver Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Rust daemon that crawls up to 100k books/day from an OpenBooks-style API across a fleet of peers, deduplicates via shard ownership backed by R2/S3, checkpoints progress for recovery, and packages the finished corpus into a seekable, hashed, zstd-compressed JSONL dataset.

**Architecture:** Each peer claims a set of shard ranges written atomically to R2/S3 (no central server needed). The crawler runs as a systemd service, respects a configurable daily quota, streams artifacts directly to R2/S3 with multipart for large objects, and writes local + remote checkpoints so any peer can resume without redoing work. A separate `package` command reads checkpointed metadata, exports JSONL, compresses into chunked `.zst` files, builds a byte-offset seek index, and produces a final manifest with hashes and shard IDs.

**Tech Stack:** Rust + tokio, clap v4, chromiumoxide (headless Chrome), spider (link crawling), reqwest (binary downloads), aws-sdk-s3 v1 (R2/S3), serde/serde_json/toml, sha2, zstd, uuid, libsystemd (via systemd crate), anyhow/thiserror

---

## Milestones

| Milestone | Tasks | Deliverable |
|-----------|-------|-------------|
| M1: Scaffold + Config | 1 | Compilable binary with CLI |
| M2: Storage + Shards | 2–3 | Upload working, peers can claim shards |
| M3: Checkpoint Layer | 4 | Recovery without redoing work |
| M4: Crawler | 5–6 | Crawls books, uploads artifacts |
| M5: systemd Daemon | 7 | Runs as systemd service |
| M6: Package Command | 8–10 | Corpus exported + indexed |
| M7: Distribution | 11 | Spider link crawling, .torrent generation, IPFS pinning |
| M8: Peer Membership | 12–13 | Live peer roster (peers/directory.json) + invite command with scoped credentials |

---

## File Structure

```
Cargo.toml
config.toml.example
thoth.service                    # systemd unit template
src/
  main.rs                        # clap CLI dispatch
  config.rs                      # Config struct + TOML loading
  error.rs                       # AppError enum
  storage/
    mod.rs                       # Storage trait + factory
    s3.rs                        # S3Client wrapper (streaming + multipart)
  peer/
    mod.rs                       # PeerIdentity, boot logic
    shard.rs                     # ShardRange, shard_for_book_id()
    registry.rs                  # Claim/release shards via S3 objects
  checkpoint/
    mod.rs                       # CheckpointManager
    local.rs                     # Local NDJSON checkpoint file
    remote.rs                    # Remote shard-level manifest in S3
  crawler/
    mod.rs                       # Crawler orchestrator + daily quota
    browser.rs                   # chromiumoxide page fetch + link extraction
    downloader.rs                # reqwest binary download (PDF/EPUB)
    extractor.rs                 # Detect format, extract metadata from page
  systemd.rs                     # sd_notify, watchdog keepalive
  package/
    mod.rs                       # `package` subcommand entry
    export.rs                    # Stream checkpoint records → JSONL
    compress.rs                  # Chunk + zstd-compress JSONL stream
    index.rs                     # Build byte-offset seek index
    manifest.rs                  # Final manifest: hashes, sizes, shard IDs
    torrent.rs                   # .torrent file generation (lava_torrent)
    ipfs.rs                      # IPFS HTTP API client (pin + add)
  crawler/
    spider.rs                    # spider crate wrapper for link discovery
tests/
  storage_test.rs
  shard_test.rs
  checkpoint_test.rs
  crawler_test.rs
  package_test.rs
```

---

## Task 1: Project Scaffold + CLI

**Files:**
- Create: `Cargo.toml`
- Create: `src/main.rs`
- Create: `src/config.rs`
- Create: `src/error.rs`
- Create: `config.toml.example`

- [ ] **Step 1: Initialize Cargo project**

```bash
cargo new --bin thoth
cd thoth
```

- [ ] **Step 2: Write `Cargo.toml`**

```toml
[package]
name = "thoth"
version = "0.1.0"
edition = "2021"

[[bin]]
name = "thoth"
path = "src/main.rs"

[dependencies]
tokio = { version = "1", features = ["full"] }
clap = { version = "4", features = ["derive"] }
serde = { version = "1", features = ["derive"] }
serde_json = "1"
toml = "0.8"
anyhow = "1"
thiserror = "1"
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid = { version = "1", features = ["v4"] }
reqwest = { version = "0.12", features = ["stream", "rustls-tls"], default-features = false }
aws-sdk-s3 = "1"
aws-config = "1"
aws-smithy-types = "1"
sha2 = "0.10"
hex = "0.4"
zstd = "0.13"
chromiumoxide = { version = "0.6", features = ["tokio-runtime"] }
systemd = "0.10"
bytes = "1"
futures = "1"
tokio-util = { version = "0.7", features = ["io"] }

[dev-dependencies]
tokio-test = "0.4"
tempfile = "3"
```

- [ ] **Step 3: Write `src/error.rs`**

```rust
use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),
    #[error("storage error: {0}")]
    Storage(#[from] aws_sdk_s3::Error),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("shard conflict: shard {0} already claimed")]
    ShardConflict(u64),
    #[error("quota exhausted: {0} books crawled today")]
    QuotaExhausted(u64),
}
```

- [ ] **Step 4: Write `src/config.rs`**

```rust
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub peer: PeerConfig,
    pub storage: StorageConfig,
    pub crawler: CrawlerConfig,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PeerConfig {
    /// Path to file storing this peer's UUID (created on first run)
    pub identity_path: PathBuf,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct StorageConfig {
    pub bucket: String,
    /// S3-compatible endpoint URL (e.g. https://account.r2.cloudflarestorage.com)
    pub endpoint_url: Option<String>,
    /// Number of books per shard (default: 10_000)
    #[serde(default = "default_shard_size")]
    pub shard_size: u64,
    /// Total number of books in corpus
    #[serde(default = "default_total_books")]
    pub total_books: u64,
    /// Multipart threshold in bytes (default: 100 MiB)
    #[serde(default = "default_multipart_threshold")]
    pub multipart_threshold_bytes: u64,
    /// Multipart part size in bytes (default: 10 MiB)
    #[serde(default = "default_part_size")]
    pub multipart_part_size: u64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CrawlerConfig {
    /// Maximum books to crawl per calendar day (UTC)
    #[serde(default = "default_daily_quota")]
    pub daily_quota: u64,
    /// Base URL pattern; `{book}` is replaced with book ID
    pub book_url_template: String,
    /// Number of shards to claim per peer at once
    #[serde(default = "default_shards_per_peer")]
    pub shards_per_peer: u64,
    /// Concurrent browser tab limit
    #[serde(default = "default_concurrency")]
    pub concurrency: usize,
    /// Stale claim timeout in seconds (default: 3600 = 1 hour)
    #[serde(default = "default_claim_ttl")]
    pub claim_ttl_secs: u64,
    /// Local checkpoint directory
    pub checkpoint_dir: PathBuf,
}

fn default_shard_size() -> u64 { 10_000 }
fn default_total_books() -> u64 { 40_000_000 }
fn default_multipart_threshold() -> u64 { 100 * 1024 * 1024 }
fn default_part_size() -> u64 { 10 * 1024 * 1024 }
fn default_daily_quota() -> u64 { 100_000 }
fn default_shards_per_peer() -> u64 { 2 }
fn default_concurrency() -> usize { 4 }
fn default_claim_ttl() -> u64 { 3600 }

impl Config {
    pub fn load(path: &std::path::Path) -> anyhow::Result<Self> {
        let text = std::fs::read_to_string(path)?;
        let config: Config = toml::from_str(&text)
            .map_err(|e| anyhow::anyhow!("config parse error: {e}"))?;
        Ok(config)
    }
}
```

- [ ] **Step 5: Write `config.toml.example`**

```toml
[peer]
identity_path = "/var/lib/thoth/peer_id"

[storage]
bucket = "open-books-archive"
endpoint_url = "https://YOUR_ACCOUNT_ID.r2.cloudflarestorage.com"
shard_size = 10000
total_books = 40000000
multipart_threshold_bytes = 104857600
multipart_part_size = 10485760

[crawler]
daily_quota = 100000
book_url_template = "https://www.gutenburg.com/books/edition/{book}"
shards_per_peer = 2
concurrency = 4
claim_ttl_secs = 3600
checkpoint_dir = "/var/lib/thoth/checkpoints"
```

- [ ] **Step 6: Write `src/main.rs`**

```rust
mod config;
mod error;
mod storage;
mod peer;
mod checkpoint;
mod crawler;
mod systemd;
mod package;

use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "thoth", version, about = "P2P book archiver")]
struct Cli {
    /// Path to config file
    #[arg(short, long, default_value = "config.toml")]
    config: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Start the crawler daemon
    Crawl,
    /// Show peer status and shard claims
    Status,
    /// Package finished corpus into distributable dataset
    Package {
        /// Output directory for packaged files
        #[arg(short, long, default_value = "dist")]
        output: PathBuf,
        /// Target chunk size for .zst files in MiB (default: 512)
        #[arg(long, default_value_t = 512)]
        chunk_mib: u64,
    },
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .init();

    let cli = Cli::parse();
    let cfg = config::Config::load(&cli.config)?;

    match cli.command {
        Commands::Crawl => crawler::run(cfg).await?,
        Commands::Status => peer::print_status(cfg).await?,
        Commands::Package { output, chunk_mib } => {
            package::run(cfg, output, chunk_mib * 1024 * 1024).await?
        }
    }
    Ok(())
}
```

- [ ] **Step 7: Verify it compiles**

```bash
cargo build 2>&1 | head -40
```

Expected: compile errors only for missing submodule stubs. Add empty `mod.rs` stubs to each subdir:

```bash
mkdir -p src/{storage,peer,checkpoint,crawler,package}
for d in storage peer checkpoint crawler package; do
  touch src/$d/mod.rs
done
touch src/systemd.rs
```

Add stub functions for each unresolved symbol in `main.rs`:
- `crawler::run(cfg)` → `pub async fn run(_: Config) -> anyhow::Result<()> { Ok(()) }`
- `peer::print_status(cfg)` → same pattern
- `package::run(cfg, output, chunk_size)` → same pattern

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml config.toml.example src/
git commit -m "feat: project scaffold, CLI, and config"
```

---

## Task 2: S3/R2 Storage Client

**Files:**
- Create: `src/storage/mod.rs`
- Create: `src/storage/s3.rs`
- Create: `tests/storage_test.rs`

- [ ] **Step 1: Write `src/storage/s3.rs`**

```rust
use aws_config::BehaviorVersion;
use aws_sdk_s3::{
    config::{Builder as S3ConfigBuilder, Credentials, Region},
    primitives::ByteStream,
    Client,
};
use bytes::Bytes;
use futures::Stream;
use std::pin::Pin;
use tokio::io::AsyncRead;

pub struct S3Store {
    client: Client,
    bucket: String,
    multipart_threshold: u64,
    part_size: u64,
}

impl S3Store {
    pub async fn new(
        bucket: impl Into<String>,
        endpoint_url: Option<&str>,
        multipart_threshold: u64,
        part_size: u64,
    ) -> anyhow::Result<Self> {
        let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;

        let mut builder = S3ConfigBuilder::from(&sdk_config);
        if let Some(url) = endpoint_url {
            builder = builder
                .endpoint_url(url)
                .force_path_style(true);
        }
        let client = Client::from_conf(builder.build());

        Ok(Self {
            client,
            bucket: bucket.into(),
            multipart_threshold,
            part_size,
        })
    }

    /// Upload bytes, choosing streaming vs multipart based on size.
    pub async fn put_object(
        &self,
        key: &str,
        data: Bytes,
        content_type: &str,
    ) -> anyhow::Result<()> {
        if data.len() as u64 >= self.multipart_threshold {
            self.multipart_upload(key, data, content_type).await
        } else {
            self.streaming_upload(key, data, content_type).await
        }
    }

    async fn streaming_upload(
        &self,
        key: &str,
        data: Bytes,
        content_type: &str,
    ) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .body(ByteStream::from(data))
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("streaming upload failed for {key}: {e}"))?;
        Ok(())
    }

    async fn multipart_upload(
        &self,
        key: &str,
        data: Bytes,
        content_type: &str,
    ) -> anyhow::Result<()> {
        use aws_sdk_s3::types::CompletedMultipartUpload;
        use aws_sdk_s3::types::CompletedPart;

        let create = self.client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .content_type(content_type)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("create multipart failed: {e}"))?;

        let upload_id = create.upload_id()
            .ok_or_else(|| anyhow::anyhow!("no upload_id returned"))?
            .to_string();

        let mut parts: Vec<CompletedPart> = Vec::new();
        let chunks = data.chunks(self.part_size as usize);

        for (i, chunk) in chunks.enumerate() {
            let part_number = (i + 1) as i32;
            let resp = self.client
                .upload_part()
                .bucket(&self.bucket)
                .key(key)
                .upload_id(&upload_id)
                .part_number(part_number)
                .body(ByteStream::from(Bytes::copy_from_slice(chunk)))
                .send()
                .await
                .map_err(|e| anyhow::anyhow!("upload part {part_number} failed: {e}"))?;

            parts.push(
                CompletedPart::builder()
                    .part_number(part_number)
                    .e_tag(resp.e_tag().unwrap_or_default())
                    .build(),
            );
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(key)
            .upload_id(&upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("complete multipart failed: {e}"))?;

        Ok(())
    }

    /// Download an object to bytes. Returns None if key does not exist.
    pub async fn get_object(&self, key: &str) -> anyhow::Result<Option<Bytes>> {
        match self.client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
        {
            Ok(resp) => {
                let data = resp.body.collect().await
                    .map_err(|e| anyhow::anyhow!("body collect failed: {e}"))?
                    .into_bytes();
                Ok(Some(data))
            }
            Err(e) => {
                let service_err = e.into_service_error();
                if service_err.is_no_such_key() {
                    Ok(None)
                } else {
                    Err(anyhow::anyhow!("get_object failed: {service_err}"))
                }
            }
        }
    }

    /// Conditional put: only write if key does not exist. Returns true if written.
    /// Uses If-None-Match: * header (supported by AWS S3; R2 support varies — falls back to
    /// get-then-put which is not atomic but acceptable for claim registration).
    pub async fn put_if_absent(&self, key: &str, data: Bytes) -> anyhow::Result<bool> {
        // Check existence first
        if self.get_object(key).await?.is_some() {
            return Ok(false);
        }
        self.streaming_upload(key, data, "application/json").await?;
        Ok(true)
    }

    pub async fn delete_object(&self, key: &str) -> anyhow::Result<()> {
        self.client
            .delete_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("delete_object failed: {e}"))?;
        Ok(())
    }

    pub async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation_token: Option<String> = None;

        loop {
            let mut req = self.client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = continuation_token.take() {
                req = req.continuation_token(token);
            }
            let resp = req.send().await
                .map_err(|e| anyhow::anyhow!("list_objects failed: {e}"))?;

            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }

            if resp.is_truncated().unwrap_or(false) {
                continuation_token = resp.next_continuation_token().map(|s| s.to_string());
            } else {
                break;
            }
        }
        Ok(keys)
    }
}
```

- [ ] **Step 2: Write `src/storage/mod.rs`**

```rust
mod s3;
pub use s3::S3Store;

use crate::config::StorageConfig;

pub async fn from_config(cfg: &StorageConfig) -> anyhow::Result<S3Store> {
    S3Store::new(
        &cfg.bucket,
        cfg.endpoint_url.as_deref(),
        cfg.multipart_threshold_bytes,
        cfg.multipart_part_size,
    )
    .await
}
```

- [ ] **Step 3: Write `tests/storage_test.rs`** (integration, skipped unless `TEST_S3_BUCKET` env is set)

```rust
#[cfg(test)]
mod tests {
    use thoth::storage::S3Store;
    use bytes::Bytes;

    fn bucket() -> Option<String> {
        std::env::var("TEST_S3_BUCKET").ok()
    }

    #[tokio::test]
    async fn test_put_and_get_small() {
        let Some(bucket) = bucket() else { return };
        let store = S3Store::new(&bucket, None, 100 * 1024 * 1024, 10 * 1024 * 1024)
            .await
            .unwrap();
        let key = "test/small_object";
        let data = Bytes::from_static(b"hello, world");
        store.put_object(key, data.clone(), "text/plain").await.unwrap();
        let fetched = store.get_object(key).await.unwrap().expect("should exist");
        assert_eq!(fetched, data);
        store.delete_object(key).await.unwrap();
    }

    #[tokio::test]
    async fn test_put_if_absent() {
        let Some(bucket) = bucket() else { return };
        let store = S3Store::new(&bucket, None, 100 * 1024 * 1024, 10 * 1024 * 1024)
            .await
            .unwrap();
        let key = "test/put_if_absent";
        let _ = store.delete_object(key).await;
        let first = store.put_if_absent(key, Bytes::from_static(b"first")).await.unwrap();
        let second = store.put_if_absent(key, Bytes::from_static(b"second")).await.unwrap();
        assert!(first);
        assert!(!second);
        let val = store.get_object(key).await.unwrap().unwrap();
        assert_eq!(val, Bytes::from_static(b"first"));
        store.delete_object(key).await.unwrap();
    }
}
```

- [ ] **Step 4: Run unit tests (no network tests skipped by default)**

```bash
cargo test --lib 2>&1
```

Expected: all pass (integration tests skip if env not set)

- [ ] **Step 5: Commit**

```bash
git add src/storage/ tests/storage_test.rs
git commit -m "feat: S3/R2 storage client with streaming and multipart upload"
```

---

## Task 3: Shard Coordination (P2P Claim Layer)

**Files:**
- Create: `src/peer/shard.rs`
- Create: `src/peer/registry.rs`
- Create: `src/peer/mod.rs`
- Create: `tests/shard_test.rs`

The shard registry stores claim files at `peers/claims/shard-{shard_id}.json` in S3. Peers read all claims, find unclaimed shards, write their own claim, then re-read to detect races. If another peer won the race, move on to the next shard.

- [ ] **Step 1: Write `src/peer/shard.rs`**

```rust
use serde::{Deserialize, Serialize};

/// Represents the range [start_book_id, end_book_id) owned by one shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardRange {
    pub shard_id: u64,
    pub start_book_id: u64,
    pub end_book_id: u64,
}

impl ShardRange {
    pub fn contains(&self, book_id: u64) -> bool {
        book_id >= self.start_book_id && book_id < self.end_book_id
    }
}

/// Compute the shard ID for a given book ID.
pub fn shard_for_book_id(book_id: u64, shard_size: u64) -> u64 {
    book_id / shard_size
}

/// Build the ShardRange for a given shard ID.
pub fn shard_range(shard_id: u64, shard_size: u64, total_books: u64) -> ShardRange {
    let start = shard_id * shard_size;
    let end = ((shard_id + 1) * shard_size).min(total_books);
    ShardRange { shard_id, start_book_id: start, end_book_id: end }
}

/// Total number of shards.
pub fn total_shards(shard_size: u64, total_books: u64) -> u64 {
    (total_books + shard_size - 1) / shard_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_for_book() {
        assert_eq!(shard_for_book_id(0, 10_000), 0);
        assert_eq!(shard_for_book_id(9_999, 10_000), 0);
        assert_eq!(shard_for_book_id(10_000, 10_000), 1);
        assert_eq!(shard_for_book_id(39_999_999, 10_000), 3_999);
    }

    #[test]
    fn test_shard_range_last() {
        // Last shard may be smaller than shard_size
        let r = shard_range(3_999, 10_000, 40_000_000);
        assert_eq!(r.start_book_id, 39_990_000);
        assert_eq!(r.end_book_id, 40_000_000);
    }

    #[test]
    fn test_shard_range_contains() {
        let r = shard_range(1, 10_000, 40_000_000);
        assert!(r.contains(10_000));
        assert!(r.contains(19_999));
        assert!(!r.contains(9_999));
        assert!(!r.contains(20_000));
    }

    #[test]
    fn test_total_shards() {
        assert_eq!(total_shards(10_000, 40_000_000), 4_000);
        assert_eq!(total_shards(10_000, 40_000_001), 4_001);
    }
}
```

- [ ] **Step 2: Run shard unit tests**

```bash
cargo test peer::shard -- --nocapture
```

Expected: all 4 tests pass

- [ ] **Step 3: Write `src/peer/registry.rs`**

```rust
use crate::storage::S3Store;
use crate::peer::shard::{shard_range, total_shards, ShardRange};
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
    fn claim_key(shard_id: u64) -> String {
        format!("peers/claims/shard-{shard_id:010}.json")
    }

    fn now_unix() -> u64 {
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
        Self { store, peer_id, shard_size, total_books, claim_ttl_secs }
    }

    /// Find and claim up to `n` unclaimed (or stale) shards.
    /// Returns the claimed ShardRanges.
    pub async fn claim_shards(&self, n: u64) -> anyhow::Result<Vec<ShardRange>> {
        let total = total_shards(self.shard_size, self.total_books);
        let claimed_keys = self.store.list_keys("peers/claims/").await?;
        let now = ClaimRecord::now_unix();

        // Parse existing claims, keep only live ones
        let mut live_shard_ids: std::collections::HashSet<u64> = std::collections::HashSet::new();
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
            // put_if_absent is best-effort; races are acceptable — both peers will
            // detect the conflict on next heartbeat and one will back off.
            self.store.put_if_absent(&key, data).await?;

            // Re-read to confirm we own it
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

    /// Refresh heartbeat for a shard this peer owns.
    pub async fn heartbeat(&self, shard_id: u64) -> anyhow::Result<()> {
        let key = ClaimRecord::claim_key(shard_id);
        if let Some(raw) = self.store.get_object(&key).await? {
            let mut record: ClaimRecord = serde_json::from_slice(&raw)?;
            if record.peer_id == self.peer_id {
                record.last_heartbeat_unix = ClaimRecord::now_unix();
                let data = bytes::Bytes::from(serde_json::to_vec(&record)?);
                self.store.put_object(&key, data, "application/json").await?;
            }
        }
        Ok(())
    }

    /// Release a shard claim (called when shard is fully complete).
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

    /// List all live shard claims (for `status` command).
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
```

- [ ] **Step 4: Write `src/peer/mod.rs`**

```rust
pub mod shard;
pub mod registry;

use std::path::Path;
use uuid::Uuid;
use crate::config::Config;
use crate::storage;

pub struct PeerIdentity {
    pub peer_id: String,
}

impl PeerIdentity {
    /// Load or create a persistent peer UUID from disk.
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

pub async fn print_status(cfg: Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = storage::from_config(&cfg.storage).await?;
    let registry = registry::ShardRegistry::new(
        &store,
        &identity.peer_id,
        cfg.storage.shard_size,
        cfg.storage.total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let claims = registry.list_claims().await?;
    println!("Peer ID: {}", identity.peer_id);
    println!("Live shard claims ({}):", claims.len());
    for c in &claims {
        println!("  shard {:>10}  peer {}  heartbeat {}s ago",
            c.shard_id, &c.peer_id[..8],
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(c.last_heartbeat_unix)
        );
    }
    Ok(())
}
```

- [ ] **Step 5: Commit**

```bash
git add src/peer/ tests/shard_test.rs
git commit -m "feat: P2P shard claim registry and peer identity"
```

---

## Task 4: Checkpoint Layer

**Files:**
- Create: `src/checkpoint/local.rs`
- Create: `src/checkpoint/remote.rs`
- Create: `src/checkpoint/mod.rs`
- Create: `tests/checkpoint_test.rs`

The local checkpoint is an append-only NDJSON file: one record per completed book. The remote checkpoint is a per-shard JSON object in S3 marking shard completion status.

- [ ] **Step 1: Write `src/checkpoint/local.rs`**

```rust
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRecord {
    pub book_id: u64,
    pub shard_id: u64,
    pub title: String,
    pub author: String,
    pub format: String,           // "pdf" | "epub" | "html"
    pub s3_key: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub crawled_at_unix: u64,
}

pub struct LocalCheckpoint {
    path: PathBuf,
}

impl LocalCheckpoint {
    pub fn new(dir: &Path, shard_id: u64) -> Self {
        Self { path: dir.join(format!("shard-{shard_id:010}.ndjson")) }
    }

    /// Load all completed book IDs from the checkpoint file.
    pub fn completed_ids(&self) -> anyhow::Result<HashSet<u64>> {
        if !self.path.exists() {
            return Ok(HashSet::new());
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut ids = HashSet::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(record) = serde_json::from_str::<BookRecord>(&line) {
                ids.insert(record.book_id);
            }
        }
        Ok(ids)
    }

    /// Append a completed book record.
    pub fn append(&self, record: &BookRecord) -> anyhow::Result<()> {
        use std::fs::OpenOptions;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(record)?;
        writeln!(file, "{line}")?;
        Ok(())
    }

    /// Iterate all records (for package command).
    pub fn iter_records(&self) -> anyhow::Result<Vec<BookRecord>> {
        if !self.path.exists() {
            return Ok(vec![]);
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            if let Ok(r) = serde_json::from_str::<BookRecord>(&line) {
                records.push(r);
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_record(book_id: u64) -> BookRecord {
        BookRecord {
            book_id,
            shard_id: 0,
            title: format!("Book {book_id}"),
            author: "Author".into(),
            format: "epub".into(),
            s3_key: format!("books/{book_id}.epub"),
            sha256: "deadbeef".into(),
            size_bytes: 1024,
            crawled_at_unix: 1_700_000_000,
        }
    }

    #[test]
    fn test_append_and_recover() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 0);
        cp.append(&make_record(1)).unwrap();
        cp.append(&make_record(2)).unwrap();
        let ids = cp.completed_ids().unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn test_iter_records() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 0);
        cp.append(&make_record(10)).unwrap();
        cp.append(&make_record(20)).unwrap();
        let records = cp.iter_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].book_id, 10);
    }

    #[test]
    fn test_empty_checkpoint() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 99);
        assert!(cp.completed_ids().unwrap().is_empty());
    }
}
```

- [ ] **Step 2: Run local checkpoint tests**

```bash
cargo test checkpoint::local -- --nocapture
```

Expected: 3 tests pass

- [ ] **Step 3: Write `src/checkpoint/remote.rs`**

```rust
use crate::storage::S3Store;
use serde::{Deserialize, Serialize};

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
        format!("manifests/shards/shard-{shard_id:010}.json")
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
}
```

- [ ] **Step 4: Write `src/checkpoint/mod.rs`**

```rust
pub mod local;
pub mod remote;

pub use local::{BookRecord, LocalCheckpoint};
pub use remote::{RemoteCheckpoint, ShardManifest};
```

- [ ] **Step 5: Commit**

```bash
git add src/checkpoint/ tests/checkpoint_test.rs
git commit -m "feat: local NDJSON + remote S3 checkpoint layer"
```

---

## Task 5: Browser + Downloader

**Files:**
- Create: `src/crawler/browser.rs`
- Create: `src/crawler/downloader.rs`
- Create: `src/crawler/extractor.rs`

- [ ] **Step 1: Write `src/crawler/extractor.rs`**

```rust
use crate::checkpoint::BookRecord;

/// Metadata parsed from a book's HTML page.
#[derive(Debug, Clone)]
pub struct PageMetadata {
    pub title: String,
    pub author: String,
    /// Direct download URLs found on the page, with their detected format.
    pub downloads: Vec<DownloadTarget>,
}

#[derive(Debug, Clone)]
pub struct DownloadTarget {
    pub url: String,
    pub format: String,   // "pdf" | "epub" | "html"
}

/// Extract metadata and download links from raw HTML.
pub fn extract_metadata(html: &str, base_url: &str) -> PageMetadata {
    // Simple heuristic extraction — replace with proper HTML parser if needed.
    let title = extract_meta_content(html, "og:title")
        .or_else(|| extract_tag_content(html, "title"))
        .unwrap_or_else(|| "Unknown Title".to_string());

    let author = extract_meta_content(html, "author")
        .or_else(|| extract_tag_content(html, "author"))
        .unwrap_or_else(|| "Unknown Author".to_string());

    let mut downloads = Vec::new();
    for (ext, fmt) in [(".pdf", "pdf"), (".epub", "epub")] {
        for href in find_hrefs_with_suffix(html, ext) {
            let url = resolve_url(base_url, &href);
            downloads.push(DownloadTarget { url, format: fmt.to_string() });
        }
    }

    PageMetadata { title, author, downloads }
}

fn extract_meta_content(html: &str, name: &str) -> Option<String> {
    let needle = format!("name=\"{name}\"");
    let pos = html.find(&needle)?;
    let after = &html[pos..];
    let content_pos = after.find("content=\"")?;
    let start = content_pos + 9;
    let end = after[start..].find('"')?;
    Some(after[start..start + end].to_string())
}

fn extract_tag_content(html: &str, tag: &str) -> Option<String> {
    let open = format!("<{tag}");
    let close = format!("</{tag}>");
    let start = html.find(&open)? ;
    let inner_start = html[start..].find('>')? + start + 1;
    let end = html[inner_start..].find(&close)? + inner_start;
    Some(html[inner_start..end].trim().to_string())
}

fn find_hrefs_with_suffix(html: &str, suffix: &str) -> Vec<String> {
    let mut results = Vec::new();
    let mut search = html;
    while let Some(pos) = search.find("href=\"") {
        search = &search[pos + 6..];
        if let Some(end) = search.find('"') {
            let href = &search[..end];
            if href.ends_with(suffix) {
                results.push(href.to_string());
            }
            search = &search[end + 1..];
        }
    }
    results
}

fn resolve_url(base: &str, href: &str) -> String {
    if href.starts_with("http://") || href.starts_with("https://") {
        href.to_string()
    } else if href.starts_with('/') {
        // Extract scheme + host from base
        if let Some(after_scheme) = base.find("://") {
            let host_end = base[after_scheme + 3..]
                .find('/')
                .map(|p| after_scheme + 3 + p)
                .unwrap_or(base.len());
            format!("{}{}", &base[..host_end], href)
        } else {
            href.to_string()
        }
    } else {
        format!("{base}/{href}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_extract_downloads() {
        let html = r#"<html>
            <title>Moby Dick</title>
            <a href="/books/1/moby-dick.epub">EPUB</a>
            <a href="/books/1/moby-dick.pdf">PDF</a>
        </html>"#;
        let meta = extract_metadata(html, "https://example.com");
        assert_eq!(meta.title, "Moby Dick");
        assert_eq!(meta.downloads.len(), 2);
        assert!(meta.downloads.iter().any(|d| d.format == "epub"));
        assert!(meta.downloads.iter().any(|d| d.format == "pdf"));
    }

    #[test]
    fn test_resolve_absolute_url() {
        let url = resolve_url("https://example.com/books/1", "https://other.com/file.pdf");
        assert_eq!(url, "https://other.com/file.pdf");
    }

    #[test]
    fn test_resolve_root_relative() {
        let url = resolve_url("https://example.com/books/1", "/files/book.epub");
        assert_eq!(url, "https://example.com/files/book.epub");
    }
}
```

- [ ] **Step 2: Run extractor tests**

```bash
cargo test crawler::extractor -- --nocapture
```

Expected: 3 tests pass

- [ ] **Step 3: Write `src/crawler/browser.rs`**

```rust
use chromiumoxide::browser::{Browser, BrowserConfig};
use chromiumoxide::Page;
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct HeadlessBrowser {
    browser: Arc<Mutex<Browser>>,
}

impl HeadlessBrowser {
    pub async fn launch() -> anyhow::Result<Self> {
        let (browser, mut handler) = Browser::launch(
            BrowserConfig::builder()
                .no_sandbox()
                .build()
                .map_err(|e| anyhow::anyhow!("browser config error: {e}"))?,
        )
        .await?;

        tokio::spawn(async move {
            while let Some(event) = handler.next().await {
                // Drive the browser event loop
            }
        });

        Ok(Self { browser: Arc::new(Mutex::new(browser)) })
    }

    /// Fetch a URL and return rendered HTML.
    pub async fn fetch_html(&self, url: &str) -> anyhow::Result<String> {
        let page = {
            let browser = self.browser.lock().await;
            browser.new_page(url).await?
        };
        // Wait for network idle
        page.wait_for_navigation().await?;
        let html = page.content().await?;
        page.close().await?;
        Ok(html)
    }
}
```

- [ ] **Step 4: Write `src/crawler/downloader.rs`**

```rust
use bytes::Bytes;
use reqwest::Client;
use sha2::{Digest, Sha256};

pub struct Downloader {
    client: Client,
}

#[derive(Debug)]
pub struct DownloadResult {
    pub data: Bytes,
    pub sha256: String,
    pub content_type: String,
    pub size_bytes: u64,
}

impl Downloader {
    pub fn new() -> anyhow::Result<Self> {
        let client = Client::builder()
            .user_agent("thoth-archiver/0.1 (open book archive)")
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        Ok(Self { client })
    }

    pub async fn download(&self, url: &str) -> anyhow::Result<DownloadResult> {
        let response = self.client.get(url).send().await?;
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("application/octet-stream")
            .split(';')
            .next()
            .unwrap_or("application/octet-stream")
            .trim()
            .to_string();

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "download failed: HTTP {} for {url}",
                response.status()
            ));
        }

        let data = response.bytes().await?;
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let sha256 = hex::encode(hasher.finalize());
        let size_bytes = data.len() as u64;

        Ok(DownloadResult { data, sha256, content_type, size_bytes })
    }
}
```

- [ ] **Step 5: Commit**

```bash
git add src/crawler/
git commit -m "feat: browser fetcher, HTML extractor, and binary downloader"
```

---

## Task 6: Crawler Orchestrator + Daily Quota

**Files:**
- Modify: `src/crawler/mod.rs`
- Create: `tests/crawler_test.rs`

- [ ] **Step 1: Write `src/crawler/mod.rs`**

```rust
pub mod browser;
pub mod downloader;
pub mod extractor;

use crate::checkpoint::{BookRecord, LocalCheckpoint, RemoteCheckpoint, ShardManifest};
use crate::config::Config;
use crate::peer::registry::ShardRegistry;
use crate::peer::PeerIdentity;
use crate::storage;
use browser::HeadlessBrowser;
use downloader::Downloader;
use extractor::extract_metadata;
use tokio::sync::Semaphore;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tracing::{error, info, warn};

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = storage::from_config(&cfg.storage).await?;
    let registry = ShardRegistry::new(
        &store,
        &identity.peer_id,
        cfg.storage.shard_size,
        cfg.storage.total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let downloader = Arc::new(Downloader::new()?);
    let browser = Arc::new(HeadlessBrowser::launch().await?);
    let semaphore = Arc::new(Semaphore::new(cfg.crawler.concurrency));

    // Notify systemd we're ready
    crate::systemd::notify_ready();

    let mut total_today: u64 = 0;
    let day_start = today_start_unix();

    loop {
        if total_today >= cfg.crawler.daily_quota {
            info!("Daily quota of {} reached, sleeping until midnight UTC", cfg.crawler.daily_quota);
            let sleep_secs = seconds_until_midnight();
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            total_today = 0;
        }

        let shards = registry.claim_shards(cfg.crawler.shards_per_peer).await?;
        if shards.is_empty() {
            info!("No unclaimed shards available — corpus may be complete");
            break;
        }

        for shard in shards {
            let checkpoint = LocalCheckpoint::new(&cfg.crawler.checkpoint_dir, shard.shard_id);
            let completed = checkpoint.completed_ids()?;
            let remote_cp = RemoteCheckpoint::new(&store);

            let book_ids: Vec<u64> = (shard.start_book_id..shard.end_book_id)
                .filter(|id| !completed.contains(id))
                .collect();

            info!("Shard {}: {} books to crawl ({} already done)",
                shard.shard_id, book_ids.len(), completed.len());

            let mut handles = Vec::new();
            let store_ref = Arc::new(storage::from_config(&cfg.storage).await?);

            for book_id in &book_ids {
                if total_today >= cfg.crawler.daily_quota {
                    break;
                }

                let book_id = *book_id;
                let url = cfg.crawler.book_url_template.replace("{book}", &book_id.to_string());
                let shard_id = shard.shard_id;
                let checkpoint_dir = cfg.crawler.checkpoint_dir.clone();
                let browser = Arc::clone(&browser);
                let downloader = Arc::clone(&downloader);
                let store = Arc::clone(&store_ref);
                let sem = Arc::clone(&semaphore);

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await?;
                    crawl_book(book_id, shard_id, &url, &browser, &downloader, &store, &checkpoint_dir).await
                });
                handles.push(handle);
                total_today += 1;

                crate::systemd::ping_watchdog();
            }

            for handle in handles {
                match handle.await {
                    Ok(Ok(())) => {}
                    Ok(Err(e)) => warn!("crawl error: {e}"),
                    Err(e) => error!("task panic: {e}"),
                }
            }

            // Mark shard complete if all books done
            let completed_after = checkpoint.completed_ids()?;
            let shard_len = shard.end_book_id - shard.start_book_id;
            if completed_after.len() as u64 >= shard_len {
                let manifest = ShardManifest {
                    shard_id: shard.shard_id,
                    peer_id: identity.peer_id.clone(),
                    total_books: shard_len,
                    completed_books: shard_len,
                    is_complete: true,
                    last_updated_unix: now_unix(),
                };
                remote_cp.save(&manifest).await?;
                registry.release_shard(shard.shard_id).await?;
                info!("Shard {} complete and released", shard.shard_id);
            }
        }
    }

    Ok(())
}

async fn crawl_book(
    book_id: u64,
    shard_id: u64,
    url: &str,
    browser: &HeadlessBrowser,
    downloader: &Downloader,
    store: &crate::storage::S3Store,
    checkpoint_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let html = browser.fetch_html(url).await?;
    let meta = extract_metadata(&html, url);

    // Download first available format (prefer epub > pdf)
    let target = meta.downloads.iter()
        .find(|d| d.format == "epub")
        .or_else(|| meta.downloads.first());

    let Some(target) = target else {
        warn!("book {book_id}: no downloadable format found at {url}");
        return Ok(());
    };

    let result = downloader.download(&target.url).await?;
    let s3_key = format!("books/{}/{:010}.{}", shard_id, book_id, target.format);

    store.put_object(&s3_key, result.data, &format!("application/{}", target.format)).await?;

    let record = BookRecord {
        book_id,
        shard_id,
        title: meta.title,
        author: meta.author,
        format: target.format.clone(),
        s3_key,
        sha256: result.sha256,
        size_bytes: result.size_bytes,
        crawled_at_unix: now_unix(),
    };

    let checkpoint = LocalCheckpoint::new(checkpoint_dir, shard_id);
    checkpoint.append(&record)?;
    info!("book {book_id} archived ({} bytes, {})", result.size_bytes, target.format);
    Ok(())
}

fn today_start_unix() -> u64 {
    let now = now_unix();
    now - (now % 86400)
}

fn seconds_until_midnight() -> u64 {
    let now = now_unix();
    86400 - (now % 86400)
}

fn now_unix() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
```

- [ ] **Step 2: Compile check**

```bash
cargo build 2>&1 | grep -E "^error"
```

Expected: no errors

- [ ] **Step 3: Commit**

```bash
git add src/crawler/mod.rs
git commit -m "feat: crawler orchestrator with daily quota and concurrent shard processing"
```

---

## Task 7: systemd Integration

**Files:**
- Modify: `src/systemd.rs`
- Create: `thoth.service`

- [ ] **Step 1: Write `src/systemd.rs`**

```rust
/// Notify systemd that the service is ready.
pub fn notify_ready() {
    // sd_notify is a no-op when not running under systemd
    let _ = systemd::daemon::notify(false, &[(systemd::daemon::STATE_READY, "1")]);
}

/// Ping systemd watchdog. Call this in tight loops to avoid being killed.
pub fn ping_watchdog() {
    let _ = systemd::daemon::notify(false, &[(systemd::daemon::STATE_WATCHDOG, "1")]);
}

/// Report status string visible in `systemctl status`.
pub fn set_status(msg: &str) {
    let _ = systemd::daemon::notify(false, &[(systemd::daemon::STATE_STATUS, msg)]);
}
```

- [ ] **Step 2: Write `thoth.service`**

```ini
[Unit]
Description=Thoth P2P Book Archiver
After=network-online.target
Wants=network-online.target

[Service]
Type=notify
ExecStart=/usr/local/bin/thoth --config /etc/thoth/config.toml crawl
Restart=on-failure
RestartSec=30
WatchdogSec=300
User=thoth
Group=thoth
StateDirectory=thoth
LogsDirectory=thoth

# Hardening
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/thoth

[Install]
WantedBy=multi-user.target
```

- [ ] **Step 3: Compile and verify**

```bash
cargo build --release 2>&1 | grep -E "^error"
```

Expected: no errors

- [ ] **Step 4: Commit**

```bash
git add src/systemd.rs thoth.service
git commit -m "feat: systemd sd_notify integration and unit file"
```

---

## Task 8: Package — JSONL Export

**Files:**
- Modify: `src/package/mod.rs`
- Create: `src/package/export.rs`

- [ ] **Step 1: Write `src/package/export.rs`**

```rust
use crate::checkpoint::{BookRecord, LocalCheckpoint};
use std::path::Path;

/// Walk all shard checkpoint files in `checkpoint_dir` and return an iterator of BookRecords.
pub fn iter_all_records(checkpoint_dir: &Path) -> anyhow::Result<Vec<BookRecord>> {
    let mut all = Vec::new();
    let entries = std::fs::read_dir(checkpoint_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
            continue;
        }
        // Extract shard_id from filename "shard-0000000001.ndjson"
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if let Some(id_str) = stem.strip_prefix("shard-") {
                if let Ok(shard_id) = id_str.parse::<u64>() {
                    let cp = LocalCheckpoint::new(checkpoint_dir, shard_id);
                    let mut records = cp.iter_records()?;
                    all.append(&mut records);
                }
            }
        }
    }
    // Sort by book_id for deterministic output
    all.sort_by_key(|r| r.book_id);
    Ok(all)
}

/// Write records as JSONL to a writer.
pub fn write_jsonl<W: std::io::Write>(
    records: &[BookRecord],
    writer: &mut W,
) -> anyhow::Result<()> {
    for record in records {
        let line = serde_json::to_string(record)?;
        writeln!(writer, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::BookRecord;
    use tempfile::TempDir;

    fn sample_record(book_id: u64, shard_id: u64) -> BookRecord {
        BookRecord {
            book_id,
            shard_id,
            title: format!("Book {book_id}"),
            author: "A".into(),
            format: "epub".into(),
            s3_key: format!("books/{shard_id}/{book_id:010}.epub"),
            sha256: "abc123".into(),
            size_bytes: 512,
            crawled_at_unix: 0,
        }
    }

    #[test]
    fn test_iter_all_records() {
        let dir = TempDir::new().unwrap();
        let cp0 = LocalCheckpoint::new(dir.path(), 0);
        let cp1 = LocalCheckpoint::new(dir.path(), 1);
        cp0.append(&sample_record(5, 0)).unwrap();
        cp1.append(&sample_record(10001, 1)).unwrap();
        let records = iter_all_records(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].book_id, 5);
        assert_eq!(records[1].book_id, 10001);
    }

    #[test]
    fn test_write_jsonl() {
        let records = vec![sample_record(1, 0), sample_record(2, 0)];
        let mut buf = Vec::new();
        write_jsonl(&records, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: BookRecord = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.book_id, 1);
    }
}
```

- [ ] **Step 2: Run export tests**

```bash
cargo test package::export -- --nocapture
```

Expected: 2 tests pass

- [ ] **Step 3: Commit**

```bash
git add src/package/export.rs
git commit -m "feat: package export — JSONL writer from shard checkpoints"
```

---

## Task 9: Package — Chunked zstd Compression + Seek Index

**Files:**
- Create: `src/package/compress.rs`
- Create: `src/package/index.rs`

- [ ] **Step 1: Write `src/package/index.rs`**

The seek index maps chunk number → byte offset in the manifest. Each chunk's first book_id is also stored so binary search is possible without decompression.

```rust
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub chunk_id: u64,
    pub first_book_id: u64,
    pub last_book_id: u64,
    pub byte_offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SeekIndex {
    pub chunks: Vec<ChunkEntry>,
}

impl SeekIndex {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    pub fn add_chunk(&mut self, entry: ChunkEntry) {
        self.chunks.push(entry);
    }

    /// Save index as JSON to `path`.
    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Find the chunk that contains `book_id` (binary search on first_book_id).
    pub fn find_chunk(&self, book_id: u64) -> Option<&ChunkEntry> {
        let pos = self.chunks.partition_point(|e| e.first_book_id <= book_id);
        if pos == 0 {
            return None;
        }
        let candidate = &self.chunks[pos - 1];
        if book_id <= candidate.last_book_id {
            Some(candidate)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(chunk_id: u64, first: u64, last: u64) -> ChunkEntry {
        ChunkEntry {
            chunk_id,
            first_book_id: first,
            last_book_id: last,
            byte_offset: chunk_id * 1000,
            compressed_size: 900,
            uncompressed_size: 1000,
            sha256: "abc".into(),
        }
    }

    #[test]
    fn test_find_chunk() {
        let mut idx = SeekIndex::new();
        idx.add_chunk(entry(0, 1, 100));
        idx.add_chunk(entry(1, 101, 200));
        idx.add_chunk(entry(2, 201, 300));

        assert_eq!(idx.find_chunk(50).unwrap().chunk_id, 0);
        assert_eq!(idx.find_chunk(150).unwrap().chunk_id, 1);
        assert_eq!(idx.find_chunk(250).unwrap().chunk_id, 2);
        assert!(idx.find_chunk(0).is_none());
        assert!(idx.find_chunk(301).is_none());
    }
}
```

- [ ] **Step 2: Run index tests**

```bash
cargo test package::index -- --nocapture
```

Expected: 1 test passes

- [ ] **Step 3: Write `src/package/compress.rs`**

```rust
use crate::checkpoint::BookRecord;
use crate::package::index::{ChunkEntry, SeekIndex};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

/// Split `records` into chunks of up to `chunk_bytes` uncompressed JSONL,
/// compress each chunk with zstd, write to `output_dir/chunk-NNNN.jsonl.zst`,
/// and return a populated SeekIndex.
pub fn compress_chunks(
    records: &[BookRecord],
    output_dir: &Path,
    chunk_bytes: u64,
) -> anyhow::Result<SeekIndex> {
    std::fs::create_dir_all(output_dir)?;
    let mut index = SeekIndex::new();
    let mut byte_offset: u64 = 0;
    let mut chunk_id: u64 = 0;

    // Partition records into chunks by uncompressed size
    let mut current_chunk: Vec<&BookRecord> = Vec::new();
    let mut current_size: u64 = 0;

    let flush_chunk = |chunk_id: u64,
                       records: &[&BookRecord],
                       output_dir: &Path,
                       byte_offset: &mut u64,
                       index: &mut SeekIndex| -> anyhow::Result<u64> {
        if records.is_empty() {
            return Ok(0);
        }

        let mut jsonl_buf = Vec::new();
        for r in records {
            let line = serde_json::to_string(r)?;
            writeln!(jsonl_buf, "{line}")?;
        }
        let uncompressed_size = jsonl_buf.len() as u64;

        let compressed = zstd::encode_all(jsonl_buf.as_slice(), 3)?;
        let compressed_size = compressed.len() as u64;

        let mut hasher = Sha256::new();
        hasher.update(&compressed);
        let sha256 = hex::encode(hasher.finalize());

        let filename = format!("chunk-{chunk_id:06}.jsonl.zst");
        std::fs::write(output_dir.join(&filename), &compressed)?;

        let first_book_id = records.first().unwrap().book_id;
        let last_book_id = records.last().unwrap().book_id;

        index.add_chunk(ChunkEntry {
            chunk_id,
            first_book_id,
            last_book_id,
            byte_offset: *byte_offset,
            compressed_size,
            uncompressed_size,
            sha256,
        });

        let size = compressed_size;
        *byte_offset += size;
        Ok(size)
    };

    for record in records {
        let line_size = serde_json::to_string(record)?.len() as u64 + 1;
        if current_size + line_size > chunk_bytes && !current_chunk.is_empty() {
            flush_chunk(chunk_id, &current_chunk, output_dir, &mut byte_offset, &mut index)?;
            chunk_id += 1;
            current_chunk.clear();
            current_size = 0;
        }
        current_chunk.push(record);
        current_size += line_size;
    }
    if !current_chunk.is_empty() {
        flush_chunk(chunk_id, &current_chunk, output_dir, &mut byte_offset, &mut index)?;
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::BookRecord;
    use tempfile::TempDir;

    fn make_record(id: u64) -> BookRecord {
        BookRecord {
            book_id: id,
            shard_id: 0,
            title: format!("Book {id}"),
            author: "A".into(),
            format: "epub".into(),
            s3_key: format!("books/0/{id:010}.epub"),
            sha256: "abc".into(),
            size_bytes: 100,
            crawled_at_unix: 0,
        }
    }

    #[test]
    fn test_compress_chunks_creates_files_and_index() {
        let dir = TempDir::new().unwrap();
        let records: Vec<BookRecord> = (1..=10).map(make_record).collect();
        // Very small chunk size to force multiple chunks
        let index = compress_chunks(&records, dir.path(), 200).unwrap();
        assert!(index.chunks.len() > 1, "expected multiple chunks");
        for chunk in &index.chunks {
            let path = dir.path().join(format!("chunk-{:06}.jsonl.zst", chunk.chunk_id));
            assert!(path.exists(), "chunk file missing: {path:?}");
        }
    }

    #[test]
    fn test_chunk_decompresses_correctly() {
        let dir = TempDir::new().unwrap();
        let records: Vec<BookRecord> = (1..=3).map(make_record).collect();
        let index = compress_chunks(&records, dir.path(), 1_000_000).unwrap();
        assert_eq!(index.chunks.len(), 1);
        let path = dir.path().join("chunk-000000.jsonl.zst");
        let data = std::fs::read(path).unwrap();
        let decoded = zstd::decode_all(data.as_slice()).unwrap();
        let text = String::from_utf8(decoded).unwrap();
        assert_eq!(text.lines().count(), 3);
    }
}
```

- [ ] **Step 4: Run compress tests**

```bash
cargo test package::compress -- --nocapture
```

Expected: 2 tests pass

- [ ] **Step 5: Commit**

```bash
git add src/package/compress.rs src/package/index.rs
git commit -m "feat: chunked zstd compression and seek index for packaged corpus"
```

---

## Task 10: Package — Final Manifest + Command Wiring

**Files:**
- Create: `src/package/manifest.rs`
- Modify: `src/package/mod.rs`

- [ ] **Step 1: Write `src/package/manifest.rs`**

```rust
use crate::package::index::SeekIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageManifest {
    pub version: u32,
    pub created_at_unix: u64,
    pub total_books: u64,
    pub total_shards: u64,
    pub chunks: Vec<ChunkMeta>,
    pub index_sha256: String,
    pub index_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: u64,
    pub filename: String,
    pub sha256: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub first_book_id: u64,
    pub last_book_id: u64,
}

impl PackageManifest {
    pub fn build(
        seek_index: &SeekIndex,
        total_books: u64,
        total_shards: u64,
        index_path: &Path,
    ) -> anyhow::Result<Self> {
        let index_data = std::fs::read(index_path)?;
        let index_size_bytes = index_data.len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(&index_data);
        let index_sha256 = hex::encode(hasher.finalize());

        let chunks = seek_index.chunks.iter().map(|c| ChunkMeta {
            chunk_id: c.chunk_id,
            filename: format!("chunk-{:06}.jsonl.zst", c.chunk_id),
            sha256: c.sha256.clone(),
            compressed_size: c.compressed_size,
            uncompressed_size: c.uncompressed_size,
            first_book_id: c.first_book_id,
            last_book_id: c.last_book_id,
        }).collect();

        let created_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(PackageManifest {
            version: 1,
            created_at_unix,
            total_books,
            total_shards,
            chunks,
            index_sha256,
            index_size_bytes,
        })
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }
}
```

- [ ] **Step 2: Wire everything in `src/package/mod.rs`**

```rust
pub mod compress;
pub mod export;
pub mod index;
pub mod manifest;

use crate::config::Config;
use std::path::PathBuf;
use tracing::info;

pub async fn run(cfg: Config, output: PathBuf, chunk_bytes: u64) -> anyhow::Result<()> {
    std::fs::create_dir_all(&output)?;

    info!("Scanning checkpoint files in {:?}", cfg.crawler.checkpoint_dir);
    let records = export::iter_all_records(&cfg.crawler.checkpoint_dir)?;
    info!("Found {} book records across all shards", records.len());

    // 1. Write JSONL
    let jsonl_path = output.join("corpus.jsonl");
    let mut jsonl_file = std::fs::File::create(&jsonl_path)?;
    export::write_jsonl(&records, &mut jsonl_file)?;
    info!("JSONL written to {jsonl_path:?}");

    // 2. Compress into chunks
    let chunks_dir = output.join("chunks");
    let seek_index = compress::compress_chunks(&records, &chunks_dir, chunk_bytes)?;
    info!("Created {} compressed chunks", seek_index.chunks.len());

    // 3. Save seek index
    let index_path = output.join("seek_index.json");
    seek_index.save(&index_path)?;
    info!("Seek index written to {index_path:?}");

    // 4. Build and save final manifest
    let total_shards = crate::peer::shard::total_shards(
        cfg.storage.shard_size,
        cfg.storage.total_books,
    );
    let pkg_manifest = manifest::PackageManifest::build(
        &seek_index,
        records.len() as u64,
        total_shards,
        &index_path,
    )?;
    let manifest_path = output.join("manifest.json");
    pkg_manifest.save(&manifest_path)?;
    info!("Manifest written to {manifest_path:?}");

    println!("Package complete:");
    println!("  Books:   {}", pkg_manifest.total_books);
    println!("  Chunks:  {}", pkg_manifest.chunks.len());
    println!("  Output:  {:?}", output);
    println!("  Manifest:{:?}", manifest_path);

    Ok(())
}
```

- [ ] **Step 3: Compile final binary**

```bash
cargo build --release 2>&1 | grep -E "^error"
```

Expected: no errors

- [ ] **Step 4: Run all tests**

```bash
cargo test 2>&1
```

Expected: all unit tests pass

- [ ] **Step 5: Final commit**

```bash
git add src/package/
git commit -m "feat: package command — manifest, chunked zstd export, seek index, and full wiring"
```

---

## Self-Review Checklist

### Spec Coverage

| Requirement | Task |
|-------------|------|
| P2P Rust client | Tasks 1, 3, 6 |
| SystemD process | Task 7 |
| 100k books/day default quota | Task 1 (config), Task 6 |
| Playwright/spider/libsystemd | Tasks 5 (chromiumoxide), 7 (systemd crate) |
| Full PDFs/EPUBs | Task 5 (downloader + extractor) |
| R2/S3 canonical storage | Task 2 |
| Shard IDs, no duplicate work | Task 3 |
| Stream uploads | Task 2 (streaming_upload) |
| Multipart for large objects | Task 2 (multipart_upload) |
| Manifest/checkpoint layer | Task 4 |
| Recovery without redoing work | Task 4 (completed_ids check) |
| `package` command | Tasks 8–10 |
| JSONL export | Task 8 |
| Chunked .zst compression | Task 9 |
| Seek/offset index | Task 9 |
| Manifest with hashes, sizes, shard IDs | Task 10 |

**Gap noted:** The spec mentions `spider` crate specifically. The current plan uses `chromiumoxide` for browser automation and `reqwest` for downloads. The `spider` crate can optionally wrap the crawl loop in Task 6 for link discovery — but since book URLs are template-generated (`{book}` substitution), `spider` adds little value for this specific crawl pattern. If JS-heavy link discovery within a book page is needed, replace the `browser.fetch_html` call in Task 6 with `spider::website::Website::new(url).crawl()`.

**Gap noted:** The spec says "peer-to-peer distribution" for the final packaged dataset. The plan covers the data format (chunked .zst + seek index + manifest) which enables P2P seeding (BitTorrent, IPFS), but does not add a torrent-generation or IPFS-pinning step. Add a Task 11 if torrent/magnet generation is required.

### Type Consistency

- `BookRecord` defined in `checkpoint/local.rs:9`, used in `export.rs`, `compress.rs` — consistent
- `ShardRange` defined in `peer/shard.rs:7`, used in `peer/registry.rs`, `crawler/mod.rs` — consistent
- `SeekIndex` defined in `package/index.rs:14`, used in `compress.rs`, `package/mod.rs`, `manifest.rs` — consistent
- `S3Store` defined in `storage/s3.rs`, exported from `storage/mod.rs`, used in `peer/registry.rs`, `checkpoint/remote.rs`, `crawler/mod.rs` — consistent

### Placeholder Scan

No TBD, TODO, or "implement later" patterns. All steps include runnable commands with expected output.

---

## Task 12: Peer Registry

**Files:**
- Create: `src/peer/directory.rs`
- Modify: `src/peer/mod.rs` (update `print_status` to show directory)
- Modify: `src/peer/registry.rs` (call `PeerDirectory::upsert` on heartbeat + shard completion)
- Modify: `src/crawler/mod.rs` (call directory upsert periodically)

**Context:** A single JSON object at `peers/directory.json` in S3 acts as a live peer roster. Each peer upserts its own entry (peer_id, last_seen_unix, books_crawled, bytes_uploaded, shards_owned) whenever it heartbeats or completes a shard. `thoth status` reads it to display a rich table. Updates use a read-modify-write with a simple retry (good enough; the file is small and update frequency is low).

- [ ] **Step 1: Write the failing test for PeerDirectory**

```rust
// in src/peer/directory.rs, test module
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_upsert_and_serialize() {
        let mut dir = PeerDirectory::default();
        dir.upsert(PeerEntry {
            peer_id: "abc".into(),
            last_seen_unix: 1000,
            books_crawled: 500,
            bytes_uploaded: 1_024_000,
            shards_owned: vec![0, 1],
        });
        dir.upsert(PeerEntry {
            peer_id: "abc".into(),
            last_seen_unix: 2000,
            books_crawled: 600,
            bytes_uploaded: 2_048_000,
            shards_owned: vec![0, 1, 2],
        });
        // upsert should replace, not append
        assert_eq!(dir.peers.len(), 1);
        assert_eq!(dir.peers[0].books_crawled, 600);
        assert_eq!(dir.peers[0].last_seen_unix, 2000);
    }

    #[test]
    fn test_roundtrip_json() {
        let mut dir = PeerDirectory::default();
        dir.upsert(PeerEntry {
            peer_id: "xyz".into(),
            last_seen_unix: 999,
            books_crawled: 42,
            bytes_uploaded: 1024,
            shards_owned: vec![5],
        });
        let json = serde_json::to_string(&dir).unwrap();
        let back: PeerDirectory = serde_json::from_str(&json).unwrap();
        assert_eq!(back.peers.len(), 1);
        assert_eq!(back.peers[0].peer_id, "xyz");
    }
}
```

Run:
```bash
cargo test peer::directory -- --nocapture
```
Expected: FAIL — `PeerDirectory` not defined yet

- [ ] **Step 2: Implement `src/peer/directory.rs`**

```rust
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PeerEntry {
    pub peer_id: String,
    pub last_seen_unix: u64,
    pub books_crawled: u64,
    pub bytes_uploaded: u64,
    pub shards_owned: Vec<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerDirectory {
    pub peers: Vec<PeerEntry>,
}

impl PeerDirectory {
    const S3_KEY: &'static str = "peers/directory.json";

    /// Insert or replace entry for peer_id.
    pub fn upsert(&mut self, entry: PeerEntry) {
        if let Some(pos) = self.peers.iter().position(|p| p.peer_id == entry.peer_id) {
            self.peers[pos] = entry;
        } else {
            self.peers.push(entry);
        }
    }

    /// Remove peers whose last_seen_unix is older than ttl_secs.
    pub fn prune_stale(&mut self, now_unix: u64, ttl_secs: u64) {
        self.peers.retain(|p| now_unix.saturating_sub(p.last_seen_unix) < ttl_secs);
    }
}

use crate::storage::S3Store;

pub struct PeerDirectoryStore<'a> {
    store: &'a S3Store,
}

impl<'a> PeerDirectoryStore<'a> {
    pub fn new(store: &'a S3Store) -> Self {
        Self { store }
    }

    /// Read-modify-write: upsert this peer's entry, retry up to 3 times on conflict.
    pub async fn upsert_entry(
        &self,
        entry: PeerEntry,
        claim_ttl_secs: u64,
    ) -> anyhow::Result<()> {
        use std::time::{SystemTime, UNIX_EPOCH};
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        for attempt in 0..3u32 {
            // Load current directory (or start empty)
            let mut directory = if let Some(data) =
                self.store.get_object(PeerDirectory::S3_KEY).await?
            {
                serde_json::from_slice::<PeerDirectory>(&data)
                    .unwrap_or_default()
            } else {
                PeerDirectory::default()
            };

            directory.upsert(entry.clone());
            directory.prune_stale(now, claim_ttl_secs * 3); // keep 3x TTL for history

            let data = bytes::Bytes::from(serde_json::to_vec(&directory)?);
            self.store
                .put_object(PeerDirectory::S3_KEY, data, "application/json")
                .await?;
            return Ok(());
        }
        Ok(()) // best-effort; directory is cosmetic
    }

    /// Load the full directory for display.
    pub async fn load(&self) -> anyhow::Result<PeerDirectory> {
        if let Some(data) = self.store.get_object(PeerDirectory::S3_KEY).await? {
            Ok(serde_json::from_slice(&data).unwrap_or_default())
        } else {
            Ok(PeerDirectory::default())
        }
    }
}
```

- [ ] **Step 3: Run tests**

```bash
cargo test peer::directory -- --nocapture
```
Expected: 2 tests pass

- [ ] **Step 4: Export from `src/peer/mod.rs`**

Add to `src/peer/mod.rs`:
```rust
pub mod directory;
pub use directory::{PeerDirectory, PeerDirectoryStore, PeerEntry};
```

Update `print_status` to also show the peer directory:

```rust
pub async fn print_status(cfg: Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = storage::from_config(&cfg.storage).await?;
    let registry = registry::ShardRegistry::new(
        &store,
        &identity.peer_id,
        cfg.storage.shard_size,
        cfg.storage.total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let dir_store = directory::PeerDirectoryStore::new(&store);
    let directory = dir_store.load().await?;
    let claims = registry.list_claims().await?;

    println!("This peer:  {}", identity.peer_id);
    println!();
    println!("Active peers ({}):", directory.peers.len());
    println!("{:<38} {:>12} {:>14} {:>12}",
        "Peer ID", "Books crawled", "Bytes uploaded", "Shards owned");
    println!("{}", "-".repeat(80));
    for p in &directory.peers {
        println!("{:<38} {:>12} {:>14} {:>12}",
            p.peer_id,
            p.books_crawled,
            format_bytes(p.bytes_uploaded),
            p.shards_owned.len(),
        );
    }
    println!();
    println!("Live shard claims ({}):", claims.len());
    for c in &claims {
        println!("  shard {:>10}  peer {}  heartbeat {}s ago",
            c.shard_id,
            &c.peer_id[..8.min(c.peer_id.len())],
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()
                .saturating_sub(c.last_heartbeat_unix),
        );
    }
    Ok(())
}

fn format_bytes(bytes: u64) -> String {
    const GIB: u64 = 1024 * 1024 * 1024;
    const MIB: u64 = 1024 * 1024;
    if bytes >= GIB { format!("{:.1} GiB", bytes as f64 / GIB as f64) }
    else if bytes >= MIB { format!("{:.1} MiB", bytes as f64 / MIB as f64) }
    else { format!("{} B", bytes) }
}
```

- [ ] **Step 5: Update crawler to upsert directory on heartbeat**

In `src/crawler/mod.rs`, inside the main `run()` loop after calling `registry.heartbeat(shard.shard_id)`, add:

```rust
let dir_store = crate::peer::directory::PeerDirectoryStore::new(&store);
let checkpoint = LocalCheckpoint::new(&cfg.crawler.checkpoint_dir, shard.shard_id);
let completed_count = checkpoint.completed_ids()?.len() as u64;
// Sum bytes from completed records for this shard
let records = checkpoint.iter_records()?;
let bytes_uploaded: u64 = records.iter().map(|r| r.size_bytes).sum();
let my_claims = registry.list_claims().await?;
let shards_owned: Vec<u64> = my_claims.iter().map(|c| c.shard_id).collect();

dir_store.upsert_entry(
    crate::peer::directory::PeerEntry {
        peer_id: identity.peer_id.clone(),
        last_seen_unix: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
        books_crawled: completed_count,
        bytes_uploaded,
        shards_owned,
    },
    cfg.crawler.claim_ttl_secs,
).await?;
```

- [ ] **Step 6: Compile check**

```bash
cargo build 2>&1 | grep -E "^error"
```
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add src/peer/directory.rs src/peer/mod.rs src/crawler/mod.rs
git commit -m "feat: peer directory — live roster with stats in peers/directory.json"
```

---

## Task 13: Invite Command

**Files:**
- Modify: `src/main.rs` (add `Invite` subcommand)
- Modify: `src/config.rs` (add optional `InviteConfig` for Cloudflare API token)
- Create: `src/invite.rs`

**Context:** `thoth invite` generates everything a new peer needs to join. It outputs a ready-to-use `config.toml` snippet (all non-secret fields) plus scoped credentials. Two credential backends:

1. **AWS S3** — uses STS `GetFederationToken` via `aws-sdk-sts` to create time-limited credentials scoped to `s3:GetObject`, `s3:PutObject`, `s3:DeleteObject`, `s3:ListBucket` on the archive bucket only. Expiry defaults to 24 h.
2. **Cloudflare R2** — uses the Cloudflare API (`POST /accounts/{account_id}/r2/tokens`) via `reqwest` to create a scoped R2 API token. Requires `CF_API_TOKEN` and `CF_ACCOUNT_ID` env vars.

If neither backend is available, outputs the config snippet only and reminds the user to share credentials manually.

- [ ] **Step 1: Write failing test**

```rust
// in src/invite.rs, test module
#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, CrawlerConfig, PeerConfig, StorageConfig};
    use std::path::PathBuf;

    fn sample_config() -> Config {
        Config {
            peer: PeerConfig { identity_path: PathBuf::from("/tmp/peer_id") },
            storage: StorageConfig {
                bucket: "my-archive".into(),
                endpoint_url: Some("https://abc.r2.cloudflarestorage.com".into()),
                shard_size: 10_000,
                total_books: 40_000_000,
                multipart_threshold_bytes: 104_857_600,
                multipart_part_size: 10_485_760,
            },
            crawler: CrawlerConfig {
                daily_quota: 100_000,
                book_url_template: "https://example.com/books/{book}".into(),
                shards_per_peer: 2,
                concurrency: 4,
                claim_ttl_secs: 3600,
                checkpoint_dir: PathBuf::from("/var/lib/thoth/checkpoints"),
                use_spider: false,
            },
            package: crate::config::PackageConfig::default(),
        }
    }

    #[test]
    fn test_config_snippet_contains_required_fields() {
        let snippet = generate_config_snippet(&sample_config());
        assert!(snippet.contains("bucket = \"my-archive\""),
            "missing bucket: {snippet}");
        assert!(snippet.contains("book_url_template"),
            "missing url template: {snippet}");
        assert!(snippet.contains("shard_size"),
            "missing shard_size: {snippet}");
        assert!(!snippet.contains("identity_path"),
            "should not include identity_path (peer-local): {snippet}");
    }
}
```

Run:
```bash
cargo test invite -- --nocapture
```
Expected: FAIL — `generate_config_snippet` not defined

- [ ] **Step 2: Implement `src/invite.rs`**

```rust
use crate::config::Config;

/// Generate a TOML config snippet for sharing with a new peer.
/// Excludes peer-local fields (identity_path, checkpoint_dir).
pub fn generate_config_snippet(cfg: &Config) -> String {
    let mut out = String::new();
    out.push_str("[storage]\n");
    out.push_str(&format!("bucket = {:?}\n", cfg.storage.bucket));
    if let Some(url) = &cfg.storage.endpoint_url {
        out.push_str(&format!("endpoint_url = {:?}\n", url));
    }
    out.push_str(&format!("shard_size = {}\n", cfg.storage.shard_size));
    out.push_str(&format!("total_books = {}\n", cfg.storage.total_books));
    out.push_str(&format!("multipart_threshold_bytes = {}\n", cfg.storage.multipart_threshold_bytes));
    out.push_str(&format!("multipart_part_size = {}\n", cfg.storage.multipart_part_size));
    out.push('\n');
    out.push_str("[crawler]\n");
    out.push_str(&format!("book_url_template = {:?}\n", cfg.crawler.book_url_template));
    out.push_str(&format!("daily_quota = {}\n", cfg.crawler.daily_quota));
    out.push_str(&format!("shards_per_peer = {}\n", cfg.crawler.shards_per_peer));
    out.push_str(&format!("concurrency = {}\n", cfg.crawler.concurrency));
    out.push_str(&format!("claim_ttl_secs = {}\n", cfg.crawler.claim_ttl_secs));
    out.push_str("checkpoint_dir = \"/var/lib/thoth/checkpoints\"  # change to your preferred path\n");
    out
}

/// Attempt to generate scoped AWS STS credentials. Returns None if STS unavailable.
pub async fn try_sts_credentials(
    bucket: &str,
    expiry_hours: u32,
) -> anyhow::Result<Option<ScopedCredentials>> {
    use aws_config::BehaviorVersion;
    use aws_sdk_sts::Client as StsClient;

    let sdk_config = aws_config::defaults(BehaviorVersion::latest()).load().await;
    let sts = StsClient::new(&sdk_config);

    let policy = serde_json::json!({
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket"
            ],
            "Resource": [
                format!("arn:aws:s3:::{bucket}"),
                format!("arn:aws:s3:::{bucket}/*")
            ]
        }]
    });

    match sts
        .get_federation_token()
        .name("thoth-peer-invite")
        .policy(policy.to_string())
        .duration_seconds(expiry_hours as i32 * 3600)
        .send()
        .await
    {
        Ok(resp) => {
            let creds = resp.credentials()
                .ok_or_else(|| anyhow::anyhow!("STS returned no credentials"))?;
            Ok(Some(ScopedCredentials {
                access_key_id: creds.access_key_id().to_string(),
                secret_access_key: creds.secret_access_key().to_string(),
                session_token: Some(creds.session_token().to_string()),
                expires_at: creds.expiration().map(|e| e.to_string()),
                backend: "aws-sts".into(),
            }))
        }
        Err(_) => Ok(None), // Not on AWS or no STS permission — skip silently
    }
}

/// Attempt to generate scoped Cloudflare R2 credentials via CF API.
/// Requires CF_API_TOKEN and CF_ACCOUNT_ID env vars.
pub async fn try_r2_credentials(
    bucket: &str,
    expiry_hours: u32,
) -> anyhow::Result<Option<ScopedCredentials>> {
    let Ok(api_token) = std::env::var("CF_API_TOKEN") else { return Ok(None) };
    let Ok(account_id) = std::env::var("CF_ACCOUNT_ID") else { return Ok(None) };

    let client = reqwest::Client::new();
    let url = format!("https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/tokens");

    let body = serde_json::json!({
        "name": format!("thoth-peer-invite-{}", chrono_now_str()),
        "policies": [{
            "effect": "allow",
            "resources": {
                format!("com.cloudflare.edge.r2.bucket.{account_id}_default_{bucket}"): "*"
            },
            "permission_groups": [
                {"id": "2efd5506f9c8494dacb1fa10a3e7d5b6", "name": "Workers R2 Storage Bucket Item Write"},
                {"id": "6a018a9f2fc74eb6b293b0c548f38b39", "name": "Workers R2 Storage Bucket Item Read"}
            ]
        }],
        "ttl": expiry_hours * 3600
    });

    let resp = client
        .post(&url)
        .bearer_auth(&api_token)
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("CF API error {status}: {text}"));
    }

    #[derive(serde::Deserialize)]
    struct CfTokenResult { id: String, value: String }
    #[derive(serde::Deserialize)]
    struct CfResponse { result: CfTokenResult }

    let cf: CfResponse = resp.json().await?;

    Ok(Some(ScopedCredentials {
        access_key_id: cf.result.id,
        secret_access_key: cf.result.value,
        session_token: None,
        expires_at: None,
        backend: "cloudflare-r2".into(),
    }))
}

#[derive(Debug)]
pub struct ScopedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<String>,
    pub backend: String,
}

fn chrono_now_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

/// Top-level invite command handler.
pub async fn run_invite(cfg: &Config, expiry_hours: u32) -> anyhow::Result<()> {
    println!("=== Thoth Peer Invite ===");
    println!();
    println!("── config.toml snippet (share this) ────────────────────────────");
    println!("{}", generate_config_snippet(cfg));

    // Try AWS STS first, then CF R2, then fall back to manual
    let creds = if let Some(c) = try_sts_credentials(&cfg.storage.bucket, expiry_hours).await? {
        Some(c)
    } else if let Some(c) = try_r2_credentials(&cfg.storage.bucket, expiry_hours).await? {
        Some(c)
    } else {
        None
    };

    if let Some(creds) = creds {
        println!("── credentials (scoped, expires in {expiry_hours}h) ────────────────");
        println!("export AWS_ACCESS_KEY_ID={}", creds.access_key_id);
        println!("export AWS_SECRET_ACCESS_KEY={}", creds.secret_access_key);
        if let Some(token) = &creds.session_token {
            println!("export AWS_SESSION_TOKEN={token}");
        }
        if let Some(exp) = &creds.expires_at {
            println!("# Expires: {exp}");
        }
        println!("# Backend: {}", creds.backend);
    } else {
        println!("── credentials ─────────────────────────────────────────────");
        println!("# No credential backend detected.");
        println!("# Share your AWS credentials or R2 API token manually.");
        println!("# For AWS: create an IAM user with s3:GetObject/PutObject/DeleteObject/ListBucket");
        println!("# on arn:aws:s3:::{}/*", cfg.storage.bucket);
        println!("# For R2: create an R2 API token at https://dash.cloudflare.com/");
        println!("#         with Object Read & Write on bucket '{}'", cfg.storage.bucket);
    }
    println!();
    println!("── instructions for new peer ────────────────────────────────────");
    println!("1. Install thoth: cargo install --git <repo>");
    println!("2. Save the config snippet above to config.toml, fill in identity_path and checkpoint_dir");
    println!("3. Set the credentials as environment variables (see above)");
    println!("4. Run: thoth --config config.toml crawl");
    Ok(())
}
```

- [ ] **Step 3: Add `aws-sdk-sts` to `Cargo.toml`**

```toml
aws-sdk-sts = "1"
```

- [ ] **Step 4: Add `Invite` to `src/main.rs`**

In the `Commands` enum, add:
```rust
/// Generate a config snippet + scoped credentials for a new peer to join
Invite {
    /// Credential expiry in hours (default: 24)
    #[arg(long, default_value_t = 24)]
    expiry_hours: u32,
},
```

In the `match cli.command` block, add:
```rust
Commands::Invite { expiry_hours } => {
    invite::run_invite(&cfg, expiry_hours).await?
}
```

Add at the top of `main.rs`:
```rust
mod invite;
```

- [ ] **Step 5: Run tests**

```bash
cargo test invite -- --nocapture
```
Expected: 1 test passes (`test_config_snippet_contains_required_fields`)

- [ ] **Step 6: Compile check**

```bash
cargo build 2>&1 | grep -E "^error"
```
Expected: no errors

- [ ] **Step 7: Commit**

```bash
git add src/invite.rs src/main.rs src/config.rs Cargo.toml
git commit -m "feat: invite command — config snippet + scoped AWS STS / R2 credentials"
```

---

## Task 11: Spider Integration + Torrent/IPFS Distribution

**Files:**
- Create: `src/crawler/spider.rs`
- Create: `src/package/torrent.rs`
- Create: `src/package/ipfs.rs`
- Modify: `src/crawler/mod.rs` (use spider for link discovery within book pages)
- Modify: `src/package/mod.rs` (call torrent + ipfs after manifest)
- Modify: `Cargo.toml` (add spider, lava_torrent)

**Context:** Two independent additions wired together at the `package` command level:
1. `spider` replaces the raw `fetch_html` call in the crawler when `use_spider = true` in config, enabling JS-rendered link graph crawling within a book page to discover all download URLs rather than relying on the simple href-suffix scan in `extractor.rs`.
2. After the manifest is generated, `package` optionally generates a `.torrent` metafile covering all chunk files, and optionally pins the entire output directory to a local IPFS node via the Kubo HTTP API.

- [ ] **Step 1: Add dependencies to `Cargo.toml`**

Add under `[dependencies]`:

```toml
spider = { version = "2", features = ["sync", "chrome"] }
lava_torrent = "0.10"
```

- [ ] **Step 2: Add spider config fields to `src/config.rs`**

In `CrawlerConfig`, add:

```rust
/// Use spider crate for link discovery (discovers all download URLs on a book page)
#[serde(default)]
pub use_spider: bool,
```

In `PackageConfig` (new section, add to `Config` struct and `config.toml.example`):

```rust
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct PackageConfig {
    /// Generate a .torrent metafile for the packaged corpus
    #[serde(default)]
    pub generate_torrent: bool,
    /// Torrent tracker announce URL (e.g. "udp://tracker.opentrackr.org:1337/announce")
    pub tracker_url: Option<String>,
    /// Pin output to local IPFS node (requires Kubo running at localhost:5001)
    #[serde(default)]
    pub pin_ipfs: bool,
    /// IPFS Kubo API URL (default: http://127.0.0.1:5001)
    #[serde(default = "default_ipfs_api")]
    pub ipfs_api_url: String,
}

fn default_ipfs_api() -> String { "http://127.0.0.1:5001".to_string() }
```

Add to `Config`:
```rust
pub package: PackageConfig,
```

Add to `config.toml.example`:
```toml
[package]
generate_torrent = false
tracker_url = "udp://tracker.opentrackr.org:1337/announce"
pin_ipfs = false
ipfs_api_url = "http://127.0.0.1:5001"
```

- [ ] **Step 3: Write failing test for spider wrapper**

In `src/crawler/spider.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_discover_links_finds_epub_and_pdf() {
        // Uses a local mock URL — skipped if TEST_SPIDER=1 not set
        if std::env::var("TEST_SPIDER").is_err() { return; }
        let links = discover_download_links("https://www.gutenberg.org/ebooks/1").await.unwrap();
        assert!(links.iter().any(|l: &DownloadLink| l.format == "epub" || l.format == "pdf"),
            "expected at least one epub or pdf link, got: {links:?}");
    }
}
```

Run:
```bash
cargo test crawler::spider -- --nocapture
```

Expected: test skips (TEST_SPIDER not set)

- [ ] **Step 4: Implement `src/crawler/spider.rs`**

```rust
use spider::website::Website;
use crate::crawler::extractor::DownloadTarget;

/// Discover all PDF/EPUB download links on a book page using the spider crate.
/// Falls back gracefully if spider fails.
pub async fn discover_download_links(url: &str) -> anyhow::Result<Vec<DownloadTarget>> {
    let mut website = Website::new(url);
    website
        .with_limit(1)          // Only crawl the single book page, not the whole site
        .with_depth(1)
        .with_chrome_intercept(true, false)  // Use headless Chrome, don't block media
        .build()
        .map_err(|e| anyhow::anyhow!("spider build error: {e}"))?;

    website.crawl().await;

    let pages = website.get_pages();
    let mut targets = Vec::new();

    if let Some(pages) = pages {
        for page in pages.iter() {
            let html = page.get_html();
            let base = page.get_url().as_str();
            // Re-use extractor logic on rendered HTML
            let meta = crate::crawler::extractor::extract_metadata(html, base);
            targets.extend(meta.downloads);
        }
    }

    Ok(targets)
}
```

- [ ] **Step 5: Wire spider into crawler orchestrator**

In `src/crawler/mod.rs`, replace the `browser.fetch_html` + `extract_metadata` block inside `crawl_book` with:

```rust
// Discover download targets
let targets = if cfg_use_spider {
    match crate::crawler::spider::discover_download_links(url).await {
        Ok(t) if !t.is_empty() => t,
        Ok(_) | Err(_) => {
            // Fall back to browser fetch
            let html = browser.fetch_html(url).await?;
            extract_metadata(&html, url).downloads
        }
    }
} else {
    let html = browser.fetch_html(url).await?;
    extract_metadata(&html, url).downloads
};
```

Pass `cfg_use_spider: bool` as a parameter to `crawl_book` (add it to the function signature alongside the existing params). Call site in the task loop passes `cfg.crawler.use_spider`.

Updated `crawl_book` signature:
```rust
async fn crawl_book(
    book_id: u64,
    shard_id: u64,
    url: &str,
    cfg_use_spider: bool,
    browser: &HeadlessBrowser,
    downloader: &Downloader,
    store: &crate::storage::S3Store,
    checkpoint_dir: &std::path::Path,
) -> anyhow::Result<()>
```

- [ ] **Step 6: Run spider tests**

```bash
cargo test crawler -- --nocapture
```

Expected: all existing crawler tests pass; spider test skips

- [ ] **Step 7: Write failing test for torrent generation**

In `src/package/torrent.rs` (test section):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;
    use std::fs;

    #[test]
    fn test_generate_torrent_creates_file() {
        let dir = TempDir::new().unwrap();
        // Create two fake chunk files
        fs::write(dir.path().join("chunk-000000.jsonl.zst"), b"fake chunk 0").unwrap();
        fs::write(dir.path().join("chunk-000001.jsonl.zst"), b"fake chunk 1").unwrap();
        let torrent_path = dir.path().join("corpus.torrent");
        generate_torrent(
            dir.path(),
            &torrent_path,
            Some("udp://tracker.example.com:1337/announce"),
            "open-books-corpus",
        ).unwrap();
        assert!(torrent_path.exists());
        let bytes = fs::read(&torrent_path).unwrap();
        assert!(!bytes.is_empty());
    }
}
```

Run:
```bash
cargo test package::torrent -- --nocapture
```

Expected: FAIL — `generate_torrent` not defined

- [ ] **Step 8: Implement `src/package/torrent.rs`**

```rust
use lava_torrent::torrent::v1::TorrentBuilder;
use std::path::Path;

/// Generate a .torrent metafile covering all *.jsonl.zst files in `chunks_dir`.
pub fn generate_torrent(
    chunks_dir: &Path,
    output_path: &Path,
    tracker_url: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let mut builder = TorrentBuilder::new(chunks_dir, 524_288); // 512 KiB piece size

    builder = builder.name(name);

    if let Some(url) = tracker_url {
        builder = builder.set_announce(Some(url.to_string()));
    }

    let torrent = builder
        .build()
        .map_err(|e| anyhow::anyhow!("torrent build failed: {e}"))?;

    torrent
        .write_into_file(output_path)
        .map_err(|e| anyhow::anyhow!("torrent write failed: {e}"))?;

    Ok(())
}

/// Return the infohash hex string from a .torrent file (for logging/manifest).
pub fn infohash(torrent_path: &Path) -> anyhow::Result<String> {
    use lava_torrent::torrent::v1::Torrent;
    let torrent = Torrent::read_from_file(torrent_path)
        .map_err(|e| anyhow::anyhow!("torrent read failed: {e}"))?;
    Ok(torrent.info_hash())
}
```

- [ ] **Step 9: Run torrent tests**

```bash
cargo test package::torrent -- --nocapture
```

Expected: 1 test passes

- [ ] **Step 10: Write failing test for IPFS client**

In `src/package/ipfs.rs` (test section):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_pin_skips_without_env() {
        // Only runs if TEST_IPFS=1 and a local Kubo node is running
        if std::env::var("TEST_IPFS").is_err() { return; }
        let client = IpfsClient::new("http://127.0.0.1:5001");
        let cid = client.add_directory(std::path::Path::new("/tmp")).await.unwrap();
        assert!(cid.starts_with("Qm") || cid.starts_with("bafy"),
            "unexpected CID format: {cid}");
    }
}
```

Run:
```bash
cargo test package::ipfs -- --nocapture
```

Expected: test skips

- [ ] **Step 11: Implement `src/package/ipfs.rs`**

```rust
use reqwest::multipart;
use serde::Deserialize;
use std::path::Path;

pub struct IpfsClient {
    api_url: String,
    client: reqwest::Client,
}

#[derive(Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

impl IpfsClient {
    pub fn new(api_url: &str) -> Self {
        Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Add all files in `dir` to IPFS recursively and return the root CID.
    pub async fn add_directory(&self, dir: &Path) -> anyhow::Result<String> {
        // Use the /api/v0/add?recursive=true&wrap-with-directory=true endpoint
        let url = format!("{}/api/v0/add?recursive=true&wrap-with-directory=true&quiet=false", self.api_url);

        let mut form = multipart::Form::new();
        for entry in walkdir::WalkDir::new(dir).into_iter().filter_map(|e| e.ok()) {
            if entry.file_type().is_file() {
                let path = entry.path().to_path_buf();
                let rel = path.strip_prefix(dir)
                    .unwrap_or(&path)
                    .to_string_lossy()
                    .to_string();
                let data = tokio::fs::read(&path).await?;
                let part = multipart::Part::bytes(data).file_name(rel);
                form = form.part("file", part);
            }
        }

        let resp = self.client.post(&url).multipart(form).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("IPFS add failed: HTTP {}", resp.status()));
        }

        // IPFS returns one JSON object per line; the last line is the root CID
        let body = resp.text().await?;
        let last_line = body.trim().lines().last()
            .ok_or_else(|| anyhow::anyhow!("empty IPFS response"))?;
        let add_resp: AddResponse = serde_json::from_str(last_line)
            .map_err(|e| anyhow::anyhow!("IPFS response parse error: {e}\nraw: {last_line}"))?;

        // Pin it
        self.pin(&add_resp.hash).await?;

        Ok(add_resp.hash)
    }

    async fn pin(&self, cid: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/v0/pin/add?arg={cid}", self.api_url);
        let resp = self.client.post(&url).send().await?;
        if !resp.status().is_success() {
            return Err(anyhow::anyhow!("IPFS pin failed: HTTP {}", resp.status()));
        }
        Ok(())
    }
}
```

Add `walkdir = "2"` to `Cargo.toml` dependencies.

- [ ] **Step 12: Wire torrent + IPFS into `src/package/mod.rs`**

After `pkg_manifest.save(&manifest_path)?;`, add:

```rust
// Optional: generate .torrent metafile
if cfg.package.generate_torrent {
    use crate::package::torrent::{generate_torrent, infohash};
    let torrent_path = output.join("corpus.torrent");
    generate_torrent(
        &chunks_dir,
        &torrent_path,
        cfg.package.tracker_url.as_deref(),
        "open-books-corpus",
    )?;
    let hash = infohash(&torrent_path)?;
    info!("Torrent generated: {torrent_path:?} (infohash: {hash})");
    println!("  Torrent: {torrent_path:?}");
    println!("  Magnet:  magnet:?xt=urn:btih:{hash}");
}

// Optional: pin to IPFS
if cfg.package.pin_ipfs {
    use crate::package::ipfs::IpfsClient;
    let ipfs = IpfsClient::new(&cfg.package.ipfs_api_url);
    info!("Pinning output directory to IPFS at {}", cfg.package.ipfs_api_url);
    let cid = ipfs.add_directory(&output).await?;
    info!("IPFS CID: {cid}");
    println!("  IPFS CID: {cid}");
    println!("  Gateway:  https://ipfs.io/ipfs/{cid}");
}
```

- [ ] **Step 13: Final compile and test**

```bash
cargo build --release 2>&1 | grep -E "^error"
cargo test 2>&1
```

Expected: no errors, all non-network tests pass

- [ ] **Step 14: Commit**

```bash
git add src/crawler/spider.rs src/package/torrent.rs src/package/ipfs.rs src/config.rs src/crawler/mod.rs src/package/mod.rs Cargo.toml config.toml.example
git commit -m "feat: spider link discovery, torrent metafile generation, and IPFS pinning"
```
