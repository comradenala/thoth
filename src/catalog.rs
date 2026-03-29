use crate::storage::S3Store;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogManifest {
    pub version: u32,
    pub created_at_unix: u64,
    pub total_records: u64,
    pub total_shards: u64,
    pub shard_size: u64,
}

impl CatalogManifest {
    pub async fn load(store: &S3Store, key: &str) -> anyhow::Result<Self> {
        let raw = store
            .get_object(key)
            .await?
            .ok_or_else(|| anyhow::anyhow!("catalog manifest not found: {key}"))?;
        let manifest: Self = serde_json::from_slice(&raw)
            .map_err(|e| anyhow::anyhow!("invalid catalog manifest at {key}: {e}"))?;
        if manifest.total_shards == 0 {
            return Err(anyhow::anyhow!(
                "catalog manifest has zero shards (key: {key})"
            ));
        }
        Ok(manifest)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CatalogRecord {
    pub book_id: u64,
    pub source_id: String,
}

pub fn shard_key(prefix: &str, shard_id: u64) -> String {
    format!(
        "{}/shard-{shard_id:010}.ndjson",
        prefix.trim_end_matches('/')
    )
}

pub async fn load_shard_records(
    store: &S3Store,
    shard_prefix: &str,
    shard_id: u64,
) -> anyhow::Result<Vec<CatalogRecord>> {
    let key = shard_key(shard_prefix, shard_id);
    let raw = store
        .get_object(&key)
        .await?
        .ok_or_else(|| anyhow::anyhow!("catalog shard not found: {key}"))?;
    let text = std::str::from_utf8(&raw)
        .map_err(|e| anyhow::anyhow!("catalog shard is not utf-8 ({key}): {e}"))?;

    let mut out = Vec::new();
    for (idx, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rec: CatalogRecord = serde_json::from_str(line).map_err(|e| {
            anyhow::anyhow!(
                "invalid catalog shard record at {} line {}: {e}",
                key,
                idx + 1
            )
        })?;
        out.push(rec);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_key() {
        assert_eq!(
            shard_key("catalog/gbooks/shards", 12),
            "catalog/gbooks/shards/shard-0000000012.ndjson"
        );
        assert_eq!(
            shard_key("catalog/gbooks/shards/", 12),
            "catalog/gbooks/shards/shard-0000000012.ndjson"
        );
    }
}
