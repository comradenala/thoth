use reqwest::multipart;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Debug, Deserialize)]
struct AddResponse {
    #[serde(rename = "Hash")]
    hash: String,
}

pub struct IpfsClient {
    api_url: String,
    client: reqwest::Client,
}

fn collect_files_recursive(dir: &Path) -> anyhow::Result<Vec<PathBuf>> {
    let mut files = Vec::new();
    let mut stack = vec![dir.to_path_buf()];
    while let Some(current) = stack.pop() {
        for entry in std::fs::read_dir(&current)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                stack.push(path);
            } else if path.is_file() {
                files.push(path);
            }
        }
    }
    files.sort();
    Ok(files)
}

impl IpfsClient {
    pub fn new(api_url: &str) -> Self {
        Self {
            api_url: api_url.trim_end_matches('/').to_string(),
            client: reqwest::Client::new(),
        }
    }

    /// Add all files in `dir` recursively and return the root CID.
    pub async fn add_directory(&self, dir: &Path) -> anyhow::Result<String> {
        let files = collect_files_recursive(dir)?;
        if files.is_empty() {
            return Err(anyhow::anyhow!(
                "cannot add empty directory to IPFS: {}",
                dir.display()
            ));
        }

        let mut form = multipart::Form::new();
        for path in files {
            let rel = path
                .strip_prefix(dir)
                .unwrap_or(&path)
                .to_string_lossy()
                .replace('\\', "/");
            let bytes = tokio::fs::read(&path).await?;
            let part = multipart::Part::bytes(bytes).file_name(rel);
            form = form.part("file", part);
        }

        let url = format!(
            "{}/api/v0/add?recursive=true&wrap-with-directory=true&quiet=false",
            self.api_url
        );
        let response = self.client.post(url).multipart(form).send().await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "IPFS add failed: HTTP {}",
                response.status()
            ));
        }

        // Kubo returns newline-delimited JSON objects; final parsable "Hash" is the root CID.
        let body = response.text().await?;
        let mut maybe_cid = None;
        for line in body.lines() {
            if line.trim().is_empty() {
                continue;
            }
            if let Ok(parsed) = serde_json::from_str::<AddResponse>(line) {
                maybe_cid = Some(parsed.hash);
            }
        }
        let cid = maybe_cid.ok_or_else(|| anyhow::anyhow!("empty IPFS add response"))?;
        self.pin(&cid).await?;
        Ok(cid)
    }

    async fn pin(&self, cid: &str) -> anyhow::Result<()> {
        let url = format!("{}/api/v0/pin/add?arg={cid}", self.api_url);
        let response = self.client.post(url).send().await?;
        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "IPFS pin failed: HTTP {}",
                response.status()
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_add_and_pin_skips_without_env() {
        if std::env::var("TEST_IPFS").is_err() {
            return;
        }
        let client = IpfsClient::new("http://127.0.0.1:5001");
        let cid = client
            .add_directory(std::path::Path::new("/tmp"))
            .await
            .unwrap();
        assert!(
            cid.starts_with("Qm") || cid.starts_with("bafy"),
            "unexpected CID format: {cid}"
        );
    }
}
