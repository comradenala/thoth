use crate::package::index::SeekIndex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PackageManifest {
    pub version: u32,
    pub created_at_unix: u64,
    pub total_books: u64,
    pub total_shards: u64,
    pub chunks: Vec<ChunkMeta>,
    pub index_sha256: String,
    pub index_size_bytes: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: u64,
    pub filename: String,
    pub sha256: String,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub first_book_id: u64,
    pub last_book_id: u64,
}

impl PackageManifest {
    pub fn build(
        seek_index: &SeekIndex,
        total_books: u64,
        total_shards: u64,
        index_path: &Path,
    ) -> anyhow::Result<Self> {
        let index_data = std::fs::read(index_path)?;
        let index_size_bytes = index_data.len() as u64;
        let mut hasher = Sha256::new();
        hasher.update(index_data);
        let index_sha256 = hex::encode(hasher.finalize());

        let chunks = seek_index
            .chunks
            .iter()
            .map(|c| ChunkMeta {
                chunk_id: c.chunk_id,
                filename: format!("chunk-{:06}.jsonl.zst", c.chunk_id),
                sha256: c.sha256.clone(),
                compressed_size: c.compressed_size,
                uncompressed_size: c.uncompressed_size,
                first_book_id: c.first_book_id,
                last_book_id: c.last_book_id,
            })
            .collect();

        let created_at_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        Ok(Self {
            version: 1,
            created_at_unix,
            total_books,
            total_shards,
            chunks,
            index_sha256,
            index_size_bytes,
        })
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::package::index::{ChunkEntry, SeekIndex};
    use tempfile::TempDir;

    #[test]
    fn test_build_and_save_manifest() {
        let dir = TempDir::new().unwrap();
        let index_path = dir.path().join("seek_index.json");
        let mut seek_index = SeekIndex::new();
        seek_index.add_chunk(ChunkEntry {
            chunk_id: 0,
            first_book_id: 1,
            last_book_id: 10,
            byte_offset: 0,
            compressed_size: 123,
            uncompressed_size: 456,
            sha256: "abc".into(),
        });
        seek_index.save(&index_path).unwrap();

        let manifest = PackageManifest::build(&seek_index, 10, 4_000, &index_path).unwrap();
        assert_eq!(manifest.total_books, 10);
        assert_eq!(manifest.total_shards, 4_000);
        assert_eq!(manifest.chunks.len(), 1);
        assert!(manifest.index_size_bytes > 0);
        assert_eq!(manifest.chunks[0].filename, "chunk-000000.jsonl.zst");

        let manifest_path = dir.path().join("manifest.json");
        manifest.save(&manifest_path).unwrap();
        assert!(manifest_path.exists());
    }
}
