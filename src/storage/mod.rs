mod s3;
pub use s3::S3Store;

use crate::config::StorageConfig;

pub async fn from_config(cfg: &StorageConfig) -> anyhow::Result<S3Store> {
    S3Store::new(
        &cfg.bucket,
        cfg.endpoint_url.as_deref(),
        cfg.multipart_threshold_bytes,
        cfg.multipart_part_size,
    )
    .await
}
