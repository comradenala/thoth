mod s3;
pub use s3::S3Store;
use crate::config::StorageConfig;
pub async fn from_config(_cfg: &StorageConfig) -> anyhow::Result<S3Store> {
    unimplemented!()
}
