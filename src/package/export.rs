use crate::checkpoint::{BookRecord, LocalCheckpoint};
use std::path::Path;

/// Walk all shard-*.ndjson files in `checkpoint_dir` and return all BookRecords sorted by book_id.
pub fn iter_all_records(checkpoint_dir: &Path) -> anyhow::Result<Vec<BookRecord>> {
    let mut all = Vec::new();

    if !checkpoint_dir.exists() {
        return Ok(all);
    }

    let entries = std::fs::read_dir(checkpoint_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ndjson") {
            continue;
        }
        if let Some(stem) = path.file_stem().and_then(|s| s.to_str()) {
            if let Some(id_str) = stem.strip_prefix("shard-") {
                if let Ok(shard_id) = id_str.parse::<u64>() {
                    let cp = LocalCheckpoint::new(checkpoint_dir, shard_id);
                    let mut records = cp.iter_records()?;
                    all.append(&mut records);
                }
            }
        }
    }

    all.sort_by_key(|r| r.book_id);
    Ok(all)
}

/// Write records as JSONL (one JSON object per line) to any writer.
pub fn write_jsonl<W: std::io::Write>(
    records: &[BookRecord],
    writer: &mut W,
) -> anyhow::Result<()> {
    for record in records {
        let line = serde_json::to_string(record)?;
        writeln!(writer, "{line}")?;
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::checkpoint::local::LocalCheckpoint;
    use tempfile::TempDir;

    fn sample(book_id: u64, shard_id: u64) -> BookRecord {
        BookRecord {
            book_id,
            shard_id,
            title: format!("Book {book_id}"),
            author: "Author".into(),
            format: "epub".into(),
            s3_key: format!("books/{shard_id}/{book_id:010}.epub"),
            sha256: "abc123".into(),
            size_bytes: 512,
            crawled_at_unix: 0,
        }
    }

    #[test]
    fn test_iter_all_records_from_multiple_shards() {
        let dir = TempDir::new().unwrap();
        let cp0 = LocalCheckpoint::new(dir.path(), 0);
        let cp1 = LocalCheckpoint::new(dir.path(), 1);
        cp0.append(&sample(5, 0)).unwrap();
        cp1.append(&sample(10001, 1)).unwrap();
        let records = iter_all_records(dir.path()).unwrap();
        assert_eq!(records.len(), 2);
        // sorted by book_id
        assert_eq!(records[0].book_id, 5);
        assert_eq!(records[1].book_id, 10001);
    }

    #[test]
    fn test_iter_all_records_empty_dir() {
        let dir = TempDir::new().unwrap();
        let records = iter_all_records(dir.path()).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_iter_all_records_nonexistent_dir() {
        let records = iter_all_records(std::path::Path::new("/tmp/thoth_nonexistent_xyz")).unwrap();
        assert!(records.is_empty());
    }

    #[test]
    fn test_write_jsonl_roundtrip() {
        let records = vec![sample(1, 0), sample(2, 0)];
        let mut buf = Vec::new();
        write_jsonl(&records, &mut buf).unwrap();
        let text = String::from_utf8(buf).unwrap();
        let lines: Vec<&str> = text.lines().collect();
        assert_eq!(lines.len(), 2);
        let parsed: BookRecord = serde_json::from_str(lines[0]).unwrap();
        assert_eq!(parsed.book_id, 1);
        assert_eq!(parsed.format, "epub");
    }

    #[test]
    fn test_iter_ignores_non_ndjson_files() {
        let dir = TempDir::new().unwrap();
        // Create a non-ndjson file that should be ignored
        std::fs::write(dir.path().join("README.txt"), b"ignore me").unwrap();
        std::fs::write(dir.path().join("shard-0000000000.json"), b"{}").unwrap();
        let cp = LocalCheckpoint::new(dir.path(), 1);
        cp.append(&sample(42, 1)).unwrap();
        let records = iter_all_records(dir.path()).unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].book_id, 42);
    }
}
