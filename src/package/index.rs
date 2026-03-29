use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChunkEntry {
    pub chunk_id: u64,
    pub first_book_id: u64,
    pub last_book_id: u64,
    pub byte_offset: u64,
    pub compressed_size: u64,
    pub uncompressed_size: u64,
    pub sha256: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SeekIndex {
    pub chunks: Vec<ChunkEntry>,
}

impl SeekIndex {
    pub fn new() -> Self {
        Self { chunks: Vec::new() }
    }

    pub fn add_chunk(&mut self, entry: ChunkEntry) {
        self.chunks.push(entry);
    }

    pub fn save(&self, path: &Path) -> anyhow::Result<()> {
        let data = serde_json::to_vec_pretty(self)?;
        std::fs::write(path, data)?;
        Ok(())
    }

    pub fn find_chunk(&self, book_id: u64) -> Option<&ChunkEntry> {
        let pos = self.chunks.partition_point(|e| e.first_book_id <= book_id);
        if pos == 0 {
            return None;
        }
        let candidate = &self.chunks[pos - 1];
        if book_id <= candidate.last_book_id {
            Some(candidate)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(chunk_id: u64, first: u64, last: u64) -> ChunkEntry {
        ChunkEntry {
            chunk_id,
            first_book_id: first,
            last_book_id: last,
            byte_offset: chunk_id * 1_000,
            compressed_size: 900,
            uncompressed_size: 1_000,
            sha256: "abc".into(),
        }
    }

    #[test]
    fn test_find_chunk() {
        let mut idx = SeekIndex::new();
        idx.add_chunk(entry(0, 1, 100));
        idx.add_chunk(entry(1, 101, 200));
        idx.add_chunk(entry(2, 201, 300));

        assert_eq!(idx.find_chunk(50).unwrap().chunk_id, 0);
        assert_eq!(idx.find_chunk(150).unwrap().chunk_id, 1);
        assert_eq!(idx.find_chunk(250).unwrap().chunk_id, 2);
        assert!(idx.find_chunk(0).is_none());
        assert!(idx.find_chunk(301).is_none());
    }
}
