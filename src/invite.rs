use crate::config::Config;
use crate::peer::PeerIdentity;
use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::path::Path;

const INVITES_PREFIX: &str = "peers/invites/";
const SHORTCODE_ALPHABET: &[u8] = b"ABCDEFGHJKLMNPQRSTUVWXYZ23456789";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScopedCredentials {
    pub access_key_id: String,
    pub secret_access_key: String,
    pub session_token: Option<String>,
    pub expires_at: Option<String>,
    pub backend: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InviteRecord {
    pub code: String,
    pub created_by_peer_id: String,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
    pub redeemed_by_peer_id: Option<String>,
    pub redeemed_at_unix: Option<u64>,
    pub config_snippet: String,
    pub credentials: Option<ScopedCredentials>,
}

impl InviteRecord {
    pub fn key(code: &str) -> String {
        format!("{INVITES_PREFIX}{}.json", normalize_code(code))
    }

    pub fn state(&self, now_unix: u64) -> &'static str {
        if self.redeemed_by_peer_id.is_some() {
            "redeemed"
        } else if now_unix > self.expires_at_unix {
            "expired"
        } else {
            "active"
        }
    }
}

pub fn now_unix() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn normalize_code(code: &str) -> String {
    code.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .map(|c| c.to_ascii_uppercase())
        .collect()
}

fn format_code(code: &str) -> String {
    let code = normalize_code(code);
    if code.len() <= 4 {
        return code;
    }
    let split = code.len() / 2;
    format!("{}-{}", &code[..split], &code[split..])
}

fn generate_shortcode() -> String {
    let mut value = uuid::Uuid::new_v4().as_u128();
    let mut chars = ['A'; 8];
    for idx in (0..8).rev() {
        let n = (value % SHORTCODE_ALPHABET.len() as u128) as usize;
        chars[idx] = SHORTCODE_ALPHABET[n] as char;
        value /= SHORTCODE_ALPHABET.len() as u128;
    }
    format!(
        "{}{}{}{}-{}{}{}{}",
        chars[0], chars[1], chars[2], chars[3], chars[4], chars[5], chars[6], chars[7]
    )
}

fn render_join_config(cfg: &Config, snippet: &str) -> String {
    let mut out = String::new();
    out.push_str("[peer]\n");
    out.push_str(&format!("identity_path = {:?}\n\n", cfg.peer.identity_path));
    out.push_str(snippet);
    if !out.ends_with('\n') {
        out.push('\n');
    }
    out
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
    out.push_str(&format!(
        "claim_ttl_secs = {}\n",
        cfg.crawler.claim_ttl_secs
    ));
    out.push_str(&format!("use_spider = {}\n", cfg.crawler.use_spider));
    out.push_str("checkpoint_dir = \"/var/lib/thoth/checkpoints\"  # change per peer host\n");

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
    let url = format!("https://api.cloudflare.com/client/v4/accounts/{account_id}/r2/tokens");

    let ttl = expiry_hours.saturating_mul(3600);
    let resource = format!("com.cloudflare.edge.r2.bucket.{account_id}_default_{bucket}");
    let body = serde_json::json!({
        "name": format!("thoth-peer-invite-{}", now_unix()),
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

    #[derive(Deserialize)]
    struct CfTokenResult {
        id: String,
        value: String,
    }

    #[derive(Deserialize)]
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

async fn create_invite_record(cfg: &Config, expiry_hours: u32) -> anyhow::Result<InviteRecord> {
    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    let store = crate::storage::from_config(&cfg.storage).await?;
    let code = loop {
        let candidate = generate_shortcode();
        if store
            .get_object(&InviteRecord::key(&candidate))
            .await?
            .is_none()
        {
            break candidate;
        }
    };
    let created_at_unix = now_unix();
    let expires_at_unix =
        created_at_unix.saturating_add((expiry_hours as u64).saturating_mul(3600));

    let credentials =
        if let Some(c) = try_sts_credentials(&cfg.storage.bucket, expiry_hours).await? {
            Some(c)
        } else {
            try_r2_credentials(&cfg.storage.bucket, expiry_hours).await?
        };

    let invite = InviteRecord {
        code: format_code(&code),
        created_by_peer_id: identity.peer_id,
        created_at_unix,
        expires_at_unix,
        redeemed_by_peer_id: None,
        redeemed_at_unix: None,
        config_snippet: generate_config_snippet(cfg),
        credentials,
    };

    let key = InviteRecord::key(&invite.code);
    let data = Bytes::from(serde_json::to_vec_pretty(&invite)?);
    store.put_object(&key, data, "application/json").await?;
    Ok(invite)
}

pub async fn list_invites(cfg: &Config) -> anyhow::Result<Vec<InviteRecord>> {
    let store = crate::storage::from_config(&cfg.storage).await?;
    let keys = store.list_keys(INVITES_PREFIX).await?;
    let mut invites = Vec::new();
    for key in keys {
        let Some(raw) = store.get_object(&key).await? else {
            continue;
        };
        if let Ok(invite) = serde_json::from_slice::<InviteRecord>(&raw) {
            invites.push(invite);
        }
    }
    invites.sort_by(|a, b| b.created_at_unix.cmp(&a.created_at_unix));
    Ok(invites)
}

async fn load_invite(cfg: &Config, code: &str) -> anyhow::Result<Option<InviteRecord>> {
    let store = crate::storage::from_config(&cfg.storage).await?;
    let key = InviteRecord::key(code);
    let Some(raw) = store.get_object(&key).await? else {
        return Ok(None);
    };
    let invite = serde_json::from_slice::<InviteRecord>(&raw)?;
    Ok(Some(invite))
}

async fn persist_invite(cfg: &Config, invite: &InviteRecord) -> anyhow::Result<()> {
    let store = crate::storage::from_config(&cfg.storage).await?;
    let key = InviteRecord::key(&invite.code);
    let data = Bytes::from(serde_json::to_vec_pretty(invite)?);
    store.put_object(&key, data, "application/json").await
}

pub async fn run_invite(cfg: &Config, expiry_hours: u32) -> anyhow::Result<()> {
    let invite = create_invite_record(cfg, expiry_hours).await?;

    println!("=== Thoth Invite Created ===");
    println!("Shortcode: {}", invite.code);
    println!("Expires:   {}", invite.expires_at_unix);
    println!();
    println!("On the peer machine:");
    println!("1. Copy config.toml with matching [storage] + [peer] paths.");
    println!("2. Run: thoth --config config.toml join {}", invite.code);
    println!("3. Start worker: thoth --config joined.toml crawl");
    println!();
    if invite.credentials.is_none() {
        println!("# Note: no scoped credentials could be minted automatically.");
        println!("# The joining peer will need credentials from your environment/backend.");
    }

    Ok(())
}

pub async fn run_join(
    cfg: &Config,
    shortcode: &str,
    output: &Path,
    force: bool,
) -> anyhow::Result<()> {
    let mut invite = load_invite(cfg, shortcode)
        .await?
        .ok_or_else(|| anyhow::anyhow!("invite code not found: {}", format_code(shortcode)))?;

    let now = now_unix();
    if now > invite.expires_at_unix {
        return Err(anyhow::anyhow!("invite {} is expired", invite.code));
    }
    if invite.redeemed_by_peer_id.is_some() {
        return Err(anyhow::anyhow!(
            "invite {} has already been redeemed",
            invite.code
        ));
    }

    if output.exists() && !force {
        return Err(anyhow::anyhow!(
            "output config already exists: {} (use --force to overwrite)",
            output.display()
        ));
    }

    let identity = PeerIdentity::load_or_create(&cfg.peer.identity_path)?;
    invite.redeemed_by_peer_id = Some(identity.peer_id.clone());
    invite.redeemed_at_unix = Some(now);

    // Best-effort persistence. If this fails, still provide local bootstrap material.
    let _ = persist_invite(cfg, &invite).await;

    if let Some(parent) = output.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let config_toml = render_join_config(cfg, &invite.config_snippet);
    std::fs::write(output, config_toml)?;

    println!("=== Thoth Join Complete ===");
    println!("Peer ID: {}", identity.peer_id);
    println!("Config:  {}", output.display());
    if let Some(creds) = &invite.credentials {
        println!();
        println!("Export these credentials before running crawl:");
        println!("export AWS_ACCESS_KEY_ID={}", creds.access_key_id);
        println!("export AWS_SECRET_ACCESS_KEY={}", creds.secret_access_key);
        if let Some(token) = &creds.session_token {
            println!("export AWS_SESSION_TOKEN={token}");
        }
        if let Some(exp) = &creds.expires_at {
            println!("# Expires: {exp}");
        }
    } else {
        println!();
        println!("# Invite does not contain scoped credentials.");
        println!("# Ensure storage credentials are available in your environment.");
    }
    println!();
    println!("Next: thoth --config {} crawl", output.display());
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

    #[test]
    fn test_shortcode_shape() {
        let code = generate_shortcode();
        assert_eq!(code.len(), 9);
        assert_eq!(code.as_bytes()[4], b'-');
        assert!(code.chars().all(|c| c == '-' || c.is_ascii_alphanumeric()));
    }

    #[test]
    fn test_invite_state() {
        let rec = InviteRecord {
            code: "ABCD-1234".into(),
            created_by_peer_id: "p1".into(),
            created_at_unix: 100,
            expires_at_unix: 200,
            redeemed_by_peer_id: None,
            redeemed_at_unix: None,
            config_snippet: String::new(),
            credentials: None,
        };
        assert_eq!(rec.state(150), "active");
        assert_eq!(rec.state(201), "expired");
    }
}
