pub mod browser;
pub mod downloader;
pub mod extractor;
pub mod spider;
use crate::config::Config;
pub async fn run(_cfg: Config) -> anyhow::Result<()> { Ok(()) }
