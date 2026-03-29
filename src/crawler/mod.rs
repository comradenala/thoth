pub mod browser;
pub mod downloader;
pub mod extractor;
pub mod spider;

use crate::catalog::{load_shard_records, CatalogManifest};
use crate::checkpoint::{BookRecord, LocalCheckpoint, RemoteCheckpoint, ShardManifest};
use crate::config::Config;
use crate::peer::directory::PeerDirectoryStore;
use crate::peer::registry::ShardRegistry;
use crate::peer::PeerIdentity;
use crate::storage;
use browser::HeadlessBrowser;
use downloader::Downloader;
use extractor::extract_metadata;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use tokio::sync::Semaphore;
use tracing::{error, info, warn};

#[derive(Debug, Clone)]
struct PendingBook {
    book_id: u64,
    source_id: Option<String>,
    token: String,
}

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = Arc::new(storage::from_config(&cfg.storage).await?);
    let (registry_shard_size, registry_total_books) = if cfg.catalog.enabled {
        let manifest = CatalogManifest::load(&store, &cfg.catalog.manifest_key).await?;
        info!(
            "Catalog mode enabled: {} records in {} shards (manifest: {})",
            manifest.total_records, manifest.total_shards, cfg.catalog.manifest_key
        );
        (1, manifest.total_shards)
    } else {
        (cfg.storage.shard_size, cfg.storage.total_books)
    };
    let registry = ShardRegistry::new(
        &*store,
        &identity.peer_id,
        registry_shard_size,
        registry_total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let downloader = Arc::new(Downloader::new()?);
    let browser = Arc::new(HeadlessBrowser::launch().await?);
    let semaphore = Arc::new(Semaphore::new(cfg.crawler.concurrency));
    let dir_store = PeerDirectoryStore::new(&*store);
    if let Err(e) = dir_store.heartbeat(&identity.peer_id, 0).await {
        warn!("peer directory heartbeat failed at startup: {e}");
    }

    crate::systemd::notify_ready();

    let mut total_today: u64 = 0;

    loop {
        // Reset quota at UTC midnight
        if total_today >= cfg.crawler.daily_quota {
            let sleep_secs = seconds_until_midnight();
            info!(
                "Daily quota of {} reached, sleeping {}s until midnight UTC",
                cfg.crawler.daily_quota, sleep_secs
            );
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            total_today = 0;
        }

        let shards = registry.claim_shards(cfg.crawler.shards_per_peer).await?;
        let claimed_count = shards.len() as u64;
        if shards.is_empty() {
            if let Err(e) = dir_store.heartbeat(&identity.peer_id, 0).await {
                warn!("peer directory heartbeat failed on idle loop: {e}");
            }
            info!("No unclaimed shards — corpus may be complete");
            break;
        }
        if let Err(e) = dir_store.heartbeat(&identity.peer_id, claimed_count).await {
            warn!("peer directory heartbeat failed after claim: {e}");
        }

        for shard in shards {
            let checkpoint = LocalCheckpoint::new(&cfg.crawler.checkpoint_dir, shard.shard_id);
            let completed = checkpoint.completed_ids()?;
            let remote_cp = RemoteCheckpoint::new(&*store);
            let (books, shard_total) = pending_books_for_shard(&cfg, &store, &shard, &completed)
                .await
                .map_err(|e| {
                    anyhow::anyhow!("failed to build shard {} book list: {e}", shard.shard_id)
                })?;

            info!(
                "Shard {}: {} books remaining ({} already done)",
                shard.shard_id,
                books.len(),
                completed.len()
            );

            let mut handles = Vec::new();

            for pending in &books {
                if total_today >= cfg.crawler.daily_quota {
                    break;
                }

                let book_id = pending.book_id;
                let source_id = pending.source_id.clone();
                let url = cfg
                    .crawler
                    .book_url_template
                    .replace("{book}", &pending.token);
                let shard_id = shard.shard_id;
                let checkpoint_dir = cfg.crawler.checkpoint_dir.clone();
                let use_spider = cfg.crawler.use_spider;
                let browser = Arc::clone(&browser);
                let downloader = Arc::clone(&downloader);
                let store = Arc::clone(&store);
                let sem = Arc::clone(&semaphore);

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await?;
                    crawl_book(
                        book_id,
                        source_id,
                        shard_id,
                        &url,
                        use_spider,
                        &browser,
                        &downloader,
                        &store,
                        &checkpoint_dir,
                    )
                    .await
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

            // Heartbeat after processing batch
            registry.heartbeat(shard.shard_id).await?;
            if let Err(e) = dir_store.heartbeat(&identity.peer_id, claimed_count).await {
                warn!("peer directory heartbeat failed during shard loop: {e}");
            }

            // Mark shard complete if all books done
            let completed_after = checkpoint.completed_ids()?;
            if completed_after.len() as u64 >= shard_total {
                let manifest = ShardManifest {
                    shard_id: shard.shard_id,
                    peer_id: identity.peer_id.clone(),
                    total_books: shard_total,
                    completed_books: shard_total,
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
    source_id: Option<String>,
    shard_id: u64,
    url: &str,
    use_spider: bool,
    browser: &HeadlessBrowser,
    downloader: &Downloader,
    store: &crate::storage::S3Store,
    checkpoint_dir: &std::path::Path,
) -> anyhow::Result<()> {
    let html = browser.fetch_html(url).await?;
    let meta = extract_metadata(&html, url);

    // Discover download targets. Prefer spider links when enabled; fall back to page metadata.
    let targets = if use_spider {
        match spider::discover_download_links(url).await {
            Ok(t) if !t.is_empty() => t,
            _ => meta.downloads.clone(),
        }
    } else {
        meta.downloads.clone()
    };

    // Prefer epub > pdf > anything else
    let target = targets
        .iter()
        .find(|d| d.format == "epub")
        .or_else(|| targets.iter().find(|d| d.format == "pdf"))
        .or_else(|| targets.first());

    let Some(target) = target else {
        let record = BookRecord {
            book_id,
            source_id,
            shard_id,
            title: meta.title,
            author: meta.author,
            format: "unavailable".to_string(),
            s3_key: String::new(),
            sha256: String::new(),
            size_bytes: 0,
            crawled_at_unix: now_unix(),
        };
        LocalCheckpoint::new(checkpoint_dir, shard_id).append(&record)?;
        warn!("book {book_id}: no downloadable format at {url}");
        return Ok(());
    };

    let result = downloader.download(&target.url).await?;
    let s3_key = format!("books/{shard_id}/{book_id:010}.{}", target.format);

    store
        .put_object(
            &s3_key,
            result.data,
            &format!("application/{}", target.format),
        )
        .await?;

    let record = BookRecord {
        book_id,
        source_id,
        shard_id,
        title: meta.title,
        author: meta.author,
        format: target.format.clone(),
        s3_key,
        sha256: result.sha256,
        size_bytes: result.size_bytes,
        crawled_at_unix: now_unix(),
    };

    LocalCheckpoint::new(checkpoint_dir, shard_id).append(&record)?;
    info!(
        "book {book_id} archived ({} bytes, {})",
        result.size_bytes, target.format
    );
    Ok(())
}

async fn pending_books_for_shard(
    cfg: &Config,
    store: &storage::S3Store,
    shard: &crate::peer::shard::ShardRange,
    completed: &std::collections::HashSet<u64>,
) -> anyhow::Result<(Vec<PendingBook>, u64)> {
    if cfg.catalog.enabled {
        let records = load_shard_records(store, &cfg.catalog.shard_prefix, shard.shard_id).await?;
        let shard_total = records.len() as u64;
        let books = records
            .into_iter()
            .filter(|r| !completed.contains(&r.book_id))
            .map(|r| PendingBook {
                book_id: r.book_id,
                token: r.source_id.clone(),
                source_id: Some(r.source_id),
            })
            .collect();
        Ok((books, shard_total))
    } else {
        let books: Vec<PendingBook> = (shard.start_book_id..shard.end_book_id)
            .filter(|id| !completed.contains(id))
            .map(|book_id| PendingBook {
                book_id,
                token: book_id.to_string(),
                source_id: None,
            })
            .collect();
        Ok((books, shard.end_book_id - shard.start_book_id))
    }
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
