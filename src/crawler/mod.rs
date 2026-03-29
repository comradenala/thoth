pub mod browser;
pub mod downloader;
pub mod extractor;
pub mod spider;

use crate::checkpoint::{BookRecord, LocalCheckpoint, RemoteCheckpoint, ShardManifest};
use crate::config::Config;
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

pub async fn run(cfg: Config) -> anyhow::Result<()> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = Arc::new(storage::from_config(&cfg.storage).await?);
    let registry = ShardRegistry::new(
        &*store,
        &identity.peer_id,
        cfg.storage.shard_size,
        cfg.storage.total_books,
        cfg.crawler.claim_ttl_secs,
    );
    let downloader = Arc::new(Downloader::new()?);
    let browser = Arc::new(HeadlessBrowser::launch().await?);
    let semaphore = Arc::new(Semaphore::new(cfg.crawler.concurrency));

    crate::systemd::notify_ready();

    let mut total_today: u64 = 0;

    loop {
        // Reset quota at UTC midnight
        if total_today >= cfg.crawler.daily_quota {
            let sleep_secs = seconds_until_midnight();
            info!("Daily quota of {} reached, sleeping {}s until midnight UTC",
                cfg.crawler.daily_quota, sleep_secs);
            tokio::time::sleep(std::time::Duration::from_secs(sleep_secs)).await;
            total_today = 0;
        }

        let shards = registry.claim_shards(cfg.crawler.shards_per_peer).await?;
        if shards.is_empty() {
            info!("No unclaimed shards — corpus may be complete");
            break;
        }

        for shard in shards {
            let checkpoint = LocalCheckpoint::new(&cfg.crawler.checkpoint_dir, shard.shard_id);
            let completed = checkpoint.completed_ids()?;
            let remote_cp = RemoteCheckpoint::new(&*store);

            let book_ids: Vec<u64> = (shard.start_book_id..shard.end_book_id)
                .filter(|id| !completed.contains(id))
                .collect();

            info!("Shard {}: {} books remaining ({} already done)",
                shard.shard_id, book_ids.len(), completed.len());

            let mut handles = Vec::new();

            for book_id in &book_ids {
                if total_today >= cfg.crawler.daily_quota {
                    break;
                }

                let book_id = *book_id;
                let url = cfg.crawler.book_url_template.replace("{book}", &book_id.to_string());
                let shard_id = shard.shard_id;
                let checkpoint_dir = cfg.crawler.checkpoint_dir.clone();
                let use_spider = cfg.crawler.use_spider;
                let browser = Arc::clone(&browser);
                let downloader = Arc::clone(&downloader);
                let store = Arc::clone(&store);
                let sem = Arc::clone(&semaphore);

                let handle = tokio::spawn(async move {
                    let _permit = sem.acquire().await?;
                    crawl_book(book_id, shard_id, &url, use_spider, &browser, &downloader, &store, &checkpoint_dir).await
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

            // Mark shard complete if all books done
            let completed_after = checkpoint.completed_ids()?;
            let shard_total = shard.end_book_id - shard.start_book_id;
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
    let target = targets.iter()
        .find(|d| d.format == "epub")
        .or_else(|| targets.iter().find(|d| d.format == "pdf"))
        .or_else(|| targets.first());

    let Some(target) = target else {
        warn!("book {book_id}: no downloadable format at {url}");
        return Ok(());
    };

    let result = downloader.download(&target.url).await?;
    let s3_key = format!("books/{shard_id}/{book_id:010}.{}", target.format);

    store.put_object(
        &s3_key,
        result.data,
        &format!("application/{}", target.format),
    ).await?;

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

    LocalCheckpoint::new(checkpoint_dir, shard_id).append(&record)?;
    info!("book {book_id} archived ({} bytes, {})", result.size_bytes, target.format);
    Ok(())
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
