pub mod compress;
pub mod export;
pub mod index;
pub mod ipfs;
pub mod manifest;
pub mod torrent;

use crate::config::Config;
use std::path::PathBuf;

pub async fn run(_cfg: Config, _output: PathBuf, _chunk_bytes: u64) -> anyhow::Result<()> {
    // Full wiring in Task 10
    Ok(())
}
