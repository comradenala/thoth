use crate::checkpoint::BookRecord;
use crate::package::index::{ChunkEntry, SeekIndex};
use sha2::{Digest, Sha256};
use std::io::Write;
use std::path::Path;

fn flush_chunk(
    chunk_id: u64,
    records: &[&BookRecord],
    output_dir: &Path,
    byte_offset: &mut u64,
    index: &mut SeekIndex,
) -> anyhow::Result<()> {
    if records.is_empty() {
        return Ok(());
    }

    let mut jsonl = Vec::new();
    for record in records {
        let line = serde_json::to_string(record)?;
        writeln!(jsonl, "{line}")?;
    }
    let uncompressed_size = jsonl.len() as u64;

    let compressed = zstd::encode_all(jsonl.as_slice(), 3)?;
    let compressed_size = compressed.len() as u64;

    let mut hasher = Sha256::new();
    hasher.update(&compressed);
    let sha256 = hex::encode(hasher.finalize());

    let filename = format!("chunk-{chunk_id:06}.jsonl.zst");
    std::fs::write(output_dir.join(filename), compressed)?;

    let first_book_id = records.first().map(|r| r.book_id).unwrap_or(0);
    let last_book_id = records.last().map(|r| r.book_id).unwrap_or(0);

    index.add_chunk(ChunkEntry {
        chunk_id,
        first_book_id,
        last_book_id,
        byte_offset: *byte_offset,
        compressed_size,
        uncompressed_size,
        sha256,
    });
    *byte_offset += compressed_size;
    Ok(())
}

/// Compress records into chunked zstd JSONL files and return a seek index.
pub fn compress_chunks(
    records: &[BookRecord],
    output_dir: &Path,
    chunk_bytes: u64,
) -> anyhow::Result<SeekIndex> {
    std::fs::create_dir_all(output_dir)?;
    let chunk_bytes = chunk_bytes.max(1);
    let mut index = SeekIndex::new();
    let mut byte_offset = 0_u64;
    let mut chunk_id = 0_u64;
    let mut current_chunk: Vec<&BookRecord> = Vec::new();
    let mut current_size = 0_u64;

    for record in records {
        let line_size = serde_json::to_string(record)?.len() as u64 + 1;
        if current_size + line_size > chunk_bytes && !current_chunk.is_empty() {
            flush_chunk(
                chunk_id,
                &current_chunk,
                output_dir,
                &mut byte_offset,
                &mut index,
            )?;
            chunk_id += 1;
            current_chunk.clear();
            current_size = 0;
        }
        current_chunk.push(record);
        current_size += line_size;
    }

    if !current_chunk.is_empty() {
        flush_chunk(
            chunk_id,
            &current_chunk,
            output_dir,
            &mut byte_offset,
            &mut index,
        )?;
    }

    Ok(index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_record(id: u64) -> BookRecord {
        BookRecord {
            book_id: id,
            source_id: None,
            shard_id: 0,
            title: format!("Book {id}"),
            author: "A".into(),
            format: "epub".into(),
            s3_key: format!("books/0/{id:010}.epub"),
            sha256: "abc".into(),
            size_bytes: 100,
            crawled_at_unix: 0,
        }
    }

    #[test]
    fn test_compress_chunks_creates_files_and_index() {
        let dir = TempDir::new().unwrap();
        let records: Vec<BookRecord> = (1..=10).map(make_record).collect();
        let index = compress_chunks(&records, dir.path(), 200).unwrap();
        assert!(index.chunks.len() > 1, "expected multiple chunks");

        for chunk in &index.chunks {
            let path = dir
                .path()
                .join(format!("chunk-{:06}.jsonl.zst", chunk.chunk_id));
            assert!(path.exists(), "chunk file missing: {path:?}");
        }
    }

    #[test]
    fn test_chunk_decompresses_correctly() {
        let dir = TempDir::new().unwrap();
        let records: Vec<BookRecord> = (1..=3).map(make_record).collect();
        let index = compress_chunks(&records, dir.path(), 1_000_000).unwrap();
        assert_eq!(index.chunks.len(), 1);

        let data = std::fs::read(dir.path().join("chunk-000000.jsonl.zst")).unwrap();
        let decoded = zstd::decode_all(data.as_slice()).unwrap();
        let text = String::from_utf8(decoded).unwrap();
        assert_eq!(text.lines().count(), 3);
    }
}
