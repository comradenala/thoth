pub mod shard;
pub mod registry;
pub mod directory;
use crate::config::Config;
pub struct PeerIdentity { pub peer_id: String }
impl PeerIdentity {
    pub fn load_or_create(_path: &std::path::Path) -> anyhow::Result<Self> { unimplemented!() }
}
pub async fn print_status(_cfg: Config) -> anyhow::Result<()> { Ok(()) }
