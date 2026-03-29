pub mod local;
pub mod remote;
pub use local::{BookRecord, LocalCheckpoint};
pub use remote::{RemoteCheckpoint, ShardManifest};
