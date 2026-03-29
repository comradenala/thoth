use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookRecord {
    pub book_id: u64,
    pub shard_id: u64,
    pub title: String,
    pub author: String,
    pub format: String,        // "pdf" | "epub" | "html"
    pub s3_key: String,
    pub sha256: String,
    pub size_bytes: u64,
    pub crawled_at_unix: u64,
}

pub struct LocalCheckpoint {
    path: PathBuf,
}

impl LocalCheckpoint {
    pub fn new(dir: &Path, shard_id: u64) -> Self {
        Self { path: dir.join(format!("shard-{shard_id:010}.ndjson")) }
    }

    /// Load all completed book IDs (for dedup on resume).
    pub fn completed_ids(&self) -> anyhow::Result<HashSet<u64>> {
        if !self.path.exists() {
            return Ok(HashSet::new());
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut ids = HashSet::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            if let Ok(record) = serde_json::from_str::<BookRecord>(&line) {
                ids.insert(record.book_id);
            }
        }
        Ok(ids)
    }

    /// Append one completed book record (append-only, never rewrites).
    pub fn append(&self, record: &BookRecord) -> anyhow::Result<()> {
        use std::fs::OpenOptions;
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)?;
        let line = serde_json::to_string(record)?;
        writeln!(file, "{line}")?;
        Ok(())
    }

    /// Return all records for the package command.
    pub fn iter_records(&self) -> anyhow::Result<Vec<BookRecord>> {
        if !self.path.exists() {
            return Ok(vec![]);
        }
        let file = std::fs::File::open(&self.path)?;
        let reader = BufReader::new(file);
        let mut records = Vec::new();
        for line in reader.lines() {
            let line = line?;
            if line.trim().is_empty() { continue; }
            if let Ok(r) = serde_json::from_str::<BookRecord>(&line) {
                records.push(r);
            }
        }
        Ok(records)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn sample(book_id: u64) -> BookRecord {
        BookRecord {
            book_id,
            shard_id: 0,
            title: format!("Book {book_id}"),
            author: "Author".into(),
            format: "epub".into(),
            s3_key: format!("books/0/{book_id:010}.epub"),
            sha256: "deadbeef".into(),
            size_bytes: 1024,
            crawled_at_unix: 1_700_000_000,
        }
    }

    #[test]
    fn test_append_and_recover() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 0);
        cp.append(&sample(1)).unwrap();
        cp.append(&sample(2)).unwrap();
        let ids = cp.completed_ids().unwrap();
        assert!(ids.contains(&1));
        assert!(ids.contains(&2));
        assert!(!ids.contains(&3));
    }

    #[test]
    fn test_iter_records_order() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 0);
        cp.append(&sample(10)).unwrap();
        cp.append(&sample(20)).unwrap();
        let records = cp.iter_records().unwrap();
        assert_eq!(records.len(), 2);
        assert_eq!(records[0].book_id, 10);
        assert_eq!(records[1].book_id, 20);
    }

    #[test]
    fn test_empty_checkpoint_returns_empty_set() {
        let dir = TempDir::new().unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 99);
        assert!(cp.completed_ids().unwrap().is_empty());
        assert!(cp.iter_records().unwrap().is_empty());
    }

    #[test]
    fn test_append_creates_parent_dirs() {
        let dir = TempDir::new().unwrap();
        let nested = dir.path().join("a").join("b");
        let cp = LocalCheckpoint::new(&nested, 0);
        cp.append(&sample(1)).unwrap(); // must not fail even though a/b/ doesn't exist
        assert_eq!(cp.completed_ids().unwrap().len(), 1);
    }
}
