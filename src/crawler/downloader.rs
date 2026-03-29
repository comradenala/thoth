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
            .user_agent("thoth-archiver/0.1 (open book archive project)")
            .timeout(std::time::Duration::from_secs(120))
            .build()?;
        Ok(Self { client })
    }

    pub async fn download(&self, url: &str) -> anyhow::Result<DownloadResult> {
        let response = self.client.get(url).send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "download failed: HTTP {} for {url}",
                response.status()
            ));
        }

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

        let data = response.bytes().await?;
        let mut hasher = Sha256::new();
        hasher.update(&data);
        let sha256 = hex::encode(hasher.finalize());
        let size_bytes = data.len() as u64;

        Ok(DownloadResult { data, sha256, content_type, size_bytes })
    }
}
