use crate::config::Config;

#[derive(Debug, Clone)]
pub struct ScopedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<String>,
    pub backend: String,
}

/// Generate a shareable TOML snippet for peers. Local-only paths are omitted.
pub fn generate_config_snippet(cfg: &Config) -> String {
    let mut out = String::new();
    out.push_str("[storage]\n");
    out.push_str(&format!("bucket = {:?}\n", cfg.storage.bucket));
    if let Some(url) = &cfg.storage.endpoint_url {
        out.push_str(&format!("endpoint_url = {:?}\n", url));
    }
    out.push_str(&format!("shard_size = {}\n", cfg.storage.shard_size));
    out.push_str(&format!("total_books = {}\n", cfg.storage.total_books));
    out.push_str(&format!(
        "multipart_threshold_bytes = {}\n",
        cfg.storage.multipart_threshold_bytes
    ));
    out.push_str(&format!(
        "multipart_part_size = {}\n",
        cfg.storage.multipart_part_size
    ));
    out.push('\n');

    out.push_str("[crawler]\n");
    out.push_str(&format!(
        "book_url_template = {:?}\n",
        cfg.crawler.book_url_template
    ));
    out.push_str(&format!("daily_quota = {}\n", cfg.crawler.daily_quota));
    out.push_str(&format!(
        "shards_per_peer = {}\n",
        cfg.crawler.shards_per_peer
    ));
    out.push_str(&format!("concurrency = {}\n", cfg.crawler.concurrency));
    out.push_str(&format!("claim_ttl_secs = {}\n", cfg.crawler.claim_ttl_secs));
    out.push_str(&format!("use_spider = {}\n", cfg.crawler.use_spider));
    out.push_str(
        "checkpoint_dir = \"/var/lib/thoth/checkpoints\"  # change per peer host\n",
    );

    out.push('\n');
    out.push_str("[package]\n");
    out.push_str(&format!(
        "generate_torrent = {}\n",
        cfg.package.generate_torrent
    ));
    if let Some(tracker_url) = &cfg.package.tracker_url {
        out.push_str(&format!("tracker_url = {:?}\n", tracker_url));
    }
    out.push_str(&format!("pin_ipfs = {}\n", cfg.package.pin_ipfs));
    out.push_str(&format!("ipfs_api_url = {:?}\n", cfg.package.ipfs_api_url));

    out
}

/// Best-effort AWS STS credentials for short-lived peer onboarding.
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

    let duration_seconds = i32::try_from(expiry_hours)
        .ok()
        .and_then(|h| h.checked_mul(3600))
        .unwrap_or(24 * 3600);

    let resp = match sts
        .get_federation_token()
        .name("thoth-peer-invite")
        .policy(policy.to_string())
        .duration_seconds(duration_seconds)
        .send()
        .await
    {
        Ok(resp) => resp,
        Err(_) => return Ok(None),
    };

    let creds = match resp.credentials() {
        Some(c) => c,
        None => return Ok(None),
    };
    let access_key_id = creds.access_key_id().to_string();
    let secret_access_key = creds.secret_access_key().to_string();
    let session_token = creds.session_token();
    let session_token = if session_token.is_empty() {
        None
    } else {
        Some(session_token.to_string())
    };
    let expires_at = Some(creds.expiration().to_string());

    Ok(Some(ScopedCredentials {
        access_key_id,
        secret_access_key,
        session_token,
        expires_at,
        backend: "aws-sts".to_string(),
    }))
}

/// Best-effort Cloudflare R2 token creation via API.
/// Requires `CF_API_TOKEN` and `CF_ACCOUNT_ID` environment variables.
pub async fn try_r2_credentials(
    bucket: &str,
    expiry_hours: u32,
) -> anyhow::Result<Option<ScopedCredentials>> {
    let Ok(api_token) = std::env::var("CF_API_TOKEN") else {
        return Ok(None);
    };
    let Ok(account_id) = std::env::var("CF_ACCOUNT_ID") else {
        return Ok(None);
    };

    let client = reqwest::Client::new();
    let url = format!(
        "https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/tokens"
    );

    let ttl = expiry_hours.saturating_mul(3600);
    let resource = format!("com.cloudflare.edge.r2.bucket.{account_id}_default_{bucket}");
    let body = serde_json::json!({
        "name": format!("thoth-peer-invite-{}", now_unix_str()),
        "policies": [{
            "effect": "allow",
            "resources": { resource: "*" },
            "permission_groups": [
                {"id": "2efd5506f9c8494dacb1fa10a3e7d5b6", "name": "Workers R2 Storage Bucket Item Write"},
                {"id": "6a018a9f2fc74eb6b293b0c548f38b39", "name": "Workers R2 Storage Bucket Item Read"}
            ]
        }],
        "ttl": ttl
    });

    let resp = client
        .post(&url)
        .bearer_auth(api_token)
        .json(&body)
        .send()
        .await?;

    if !resp.status().is_success() {
        let status = resp.status();
        let text = resp.text().await.unwrap_or_default();
        return Err(anyhow::anyhow!("CF API error {status}: {text}"));
    }

    #[derive(serde::Deserialize)]
    struct CfTokenResult {
        id: String,
        value: String,
    }

    #[derive(serde::Deserialize)]
    struct CfResponse {
        result: CfTokenResult,
    }

    let cf: CfResponse = resp.json().await?;

    Ok(Some(ScopedCredentials {
        access_key_id: cf.result.id,
        secret_access_key: cf.result.value,
        session_token: None,
        expires_at: None,
        backend: "cloudflare-r2".to_string(),
    }))
}

fn now_unix_str() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
        .to_string()
}

pub async fn run_invite(cfg: &Config, expiry_hours: u32) -> anyhow::Result<()> {
    println!("=== Thoth Peer Invite ===");
    println!();
    println!("-- config.toml snippet (share this) --------------------------------");
    println!("{}", generate_config_snippet(cfg));

    let creds = if let Some(c) = try_sts_credentials(&cfg.storage.bucket, expiry_hours).await? {
        Some(c)
    } else {
        try_r2_credentials(&cfg.storage.bucket, expiry_hours).await?
    };

    if let Some(creds) = creds {
        println!("-- credentials (scoped, expires in {expiry_hours}h) -----------------");
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
        println!("-- credentials -------------------------------------------------------");
        println!("# No credential backend detected.");
        println!("# Share AWS credentials or R2 API credentials manually.");
        println!(
            "# Scope AWS/R2 permissions to bucket '{}' read/write/list operations.",
            cfg.storage.bucket
        );
    }

    println!();
    println!("-- instructions for new peer ----------------------------------------");
    println!("1. Install thoth binary on the new host.");
    println!("2. Save the config snippet to config.toml.");
    println!("3. Set credentials via environment variables.");
    println!("4. Run: thoth --config config.toml crawl");
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::{Config, CrawlerConfig, PackageConfig, PeerConfig, StorageConfig};
    use std::path::PathBuf;

    fn sample_config() -> Config {
        Config {
            peer: PeerConfig {
                identity_path: PathBuf::from("/tmp/peer_id"),
            },
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
            package: PackageConfig::default(),
        }
    }

    #[test]
    fn test_config_snippet_contains_required_fields() {
        let snippet = generate_config_snippet(&sample_config());
        assert!(
            snippet.contains("bucket = \"my-archive\""),
            "missing bucket: {snippet}"
        );
        assert!(
            snippet.contains("book_url_template"),
            "missing url template: {snippet}"
        );
        assert!(
            snippet.contains("shard_size"),
            "missing shard_size: {snippet}"
        );
        assert!(
            !snippet.contains("identity_path"),
            "should not include identity_path (peer-local): {snippet}"
        );
    }
}
