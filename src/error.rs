use thiserror::Error;

#[derive(Error, Debug)]
pub enum AppError {
    #[error("config error: {0}")]
    Config(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serde error: {0}")]
    Serde(#[from] serde_json::Error),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("shard conflict: shard {0} already claimed")]
    ShardConflict(u64),
    #[error("quota exhausted: {0} books crawled today")]
    QuotaExhausted(u64),
}
