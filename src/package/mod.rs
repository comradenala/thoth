pub mod compress;
pub mod export;
pub mod index;
pub mod ipfs;
pub mod manifest;
pub mod torrent;

use crate::config::Config;
use std::path::PathBuf;
use tracing::info;

pub async fn run(cfg: Config, output: PathBuf, chunk_bytes: u64) -> anyhow::Result<()> {
    std::fs::create_dir_all(&output)?;

    info!(
        "Scanning checkpoint files in {:?}",
        cfg.crawler.checkpoint_dir
    );
    let records = export::iter_all_records(&cfg.crawler.checkpoint_dir)?;
    info!("Found {} records", records.len());

    let jsonl_path = output.join("corpus.jsonl");
    let mut jsonl_file = std::fs::File::create(&jsonl_path)?;
    export::write_jsonl(&records, &mut jsonl_file)?;
    info!("JSONL written to {:?}", jsonl_path);

    let chunks_dir = output.join("chunks");
    let seek_index = compress::compress_chunks(&records, &chunks_dir, chunk_bytes)?;
    info!("Created {} compressed chunks", seek_index.chunks.len());

    let index_path = output.join("seek_index.json");
    seek_index.save(&index_path)?;
    info!("Seek index written to {:?}", index_path);

    let total_shards =
        crate::peer::shard::total_shards(cfg.storage.shard_size, cfg.storage.total_books);
    let pkg_manifest = manifest::PackageManifest::build(
        &seek_index,
        records.len() as u64,
        total_shards,
        &index_path,
    )?;
    let manifest_path = output.join("manifest.json");
    pkg_manifest.save(&manifest_path)?;
    info!("Manifest written to {:?}", manifest_path);
    println!("Package complete:");
    println!("  Books:   {}", pkg_manifest.total_books);
    println!("  Chunks:  {}", pkg_manifest.chunks.len());
    println!("  Output:  {:?}", output);
    println!("  Manifest:{:?}", manifest_path);

    if cfg.package.generate_torrent {
        let torrent_path = output.join("corpus.torrent");
        torrent::generate_torrent(
            &chunks_dir,
            &torrent_path,
            cfg.package.tracker_url.as_deref(),
            "open-books-corpus",
        )?;
        let hash = torrent::infohash(&torrent_path)?;
        info!("Torrent generated: {:?} (infohash: {hash})", torrent_path);
        println!("  Torrent: {:?}", torrent_path);
        println!("  Magnet:  magnet:?xt=urn:btih:{hash}");
    }

    if cfg.package.pin_ipfs {
        let ipfs = ipfs::IpfsClient::new(&cfg.package.ipfs_api_url);
        info!(
            "Pinning output directory to IPFS at {}",
            cfg.package.ipfs_api_url
        );
        let cid = ipfs.add_directory(&output).await?;
        info!("IPFS CID: {cid}");
        println!("  IPFS CID: {cid}");
        println!("  Gateway:  https://ipfs.io/ipfs/{cid}");
    }

    Ok(())
}
