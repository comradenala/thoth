use sha1::{Digest, Sha1};
use std::io::Read;
use std::path::{Path, PathBuf};

const PIECE_LENGTH: usize = 524_288;

#[derive(Debug, Clone)]
struct TorrentFile {
    rel_path: PathBuf,
    abs_path: PathBuf,
    length: u64,
}

fn bencode_int(value: i64, out: &mut Vec<u8>) {
    out.push(b'i');
    out.extend_from_slice(value.to_string().as_bytes());
    out.push(b'e');
}

fn bencode_bytes(bytes: &[u8], out: &mut Vec<u8>) {
    out.extend_from_slice(bytes.len().to_string().as_bytes());
    out.push(b':');
    out.extend_from_slice(bytes);
}

fn bencode_str(value: &str, out: &mut Vec<u8>) {
    bencode_bytes(value.as_bytes(), out);
}

fn parse_bencode_bytes(data: &[u8], start: usize) -> anyhow::Result<(usize, usize, usize)> {
    let mut idx = start;
    while idx < data.len() && data[idx].is_ascii_digit() {
        idx += 1;
    }
    if idx == start || idx >= data.len() || data[idx] != b':' {
        return Err(anyhow::anyhow!(
            "invalid bencode byte string at offset {start}"
        ));
    }
    let len = std::str::from_utf8(&data[start..idx])?.parse::<usize>()?;
    let begin = idx + 1;
    let end = begin + len;
    if end > data.len() {
        return Err(anyhow::anyhow!("bencode byte string out of bounds"));
    }
    Ok((begin, end, end))
}

fn parse_bencode_value_end(data: &[u8], start: usize) -> anyhow::Result<usize> {
    if start >= data.len() {
        return Err(anyhow::anyhow!("bencode parse out of bounds"));
    }
    match data[start] {
        b'i' => {
            let mut idx = start + 1;
            while idx < data.len() && data[idx] != b'e' {
                idx += 1;
            }
            if idx >= data.len() {
                return Err(anyhow::anyhow!("unterminated bencode integer"));
            }
            Ok(idx + 1)
        }
        b'l' => {
            let mut idx = start + 1;
            while idx < data.len() && data[idx] != b'e' {
                idx = parse_bencode_value_end(data, idx)?;
            }
            if idx >= data.len() {
                return Err(anyhow::anyhow!("unterminated bencode list"));
            }
            Ok(idx + 1)
        }
        b'd' => {
            let mut idx = start + 1;
            while idx < data.len() && data[idx] != b'e' {
                let (_, _, next) = parse_bencode_bytes(data, idx)?;
                idx = parse_bencode_value_end(data, next)?;
            }
            if idx >= data.len() {
                return Err(anyhow::anyhow!("unterminated bencode dictionary"));
            }
            Ok(idx + 1)
        }
        b'0'..=b'9' => {
            let (_, _, next) = parse_bencode_bytes(data, start)?;
            Ok(next)
        }
        _ => Err(anyhow::anyhow!("invalid bencode token at offset {start}")),
    }
}

fn find_info_span(torrent_data: &[u8]) -> anyhow::Result<(usize, usize)> {
    if torrent_data.first() != Some(&b'd') {
        return Err(anyhow::anyhow!("torrent is not a bencoded dictionary"));
    }
    let mut idx = 1;
    while idx < torrent_data.len() && torrent_data[idx] != b'e' {
        let (key_start, key_end, next) = parse_bencode_bytes(torrent_data, idx)?;
        let value_start = next;
        let value_end = parse_bencode_value_end(torrent_data, value_start)?;
        if &torrent_data[key_start..key_end] == b"info" {
            return Ok((value_start, value_end));
        }
        idx = value_end;
    }
    Err(anyhow::anyhow!("torrent missing info dictionary"))
}

fn collect_chunk_files(chunks_dir: &Path) -> anyhow::Result<Vec<TorrentFile>> {
    let mut files = Vec::new();
    for entry in std::fs::read_dir(chunks_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let Some(name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };
        if !name.ends_with(".jsonl.zst") {
            continue;
        }
        let rel_path = PathBuf::from(name);
        let length = entry.metadata()?.len();
        files.push(TorrentFile {
            rel_path,
            abs_path: path,
            length,
        });
    }
    files.sort_by(|a, b| a.rel_path.cmp(&b.rel_path));
    Ok(files)
}

fn compute_piece_hashes(files: &[TorrentFile]) -> anyhow::Result<Vec<u8>> {
    let mut pieces = Vec::new();
    let mut buf = Vec::with_capacity(PIECE_LENGTH);
    let mut read_buf = vec![0_u8; 64 * 1024];

    for file in files {
        let mut handle = std::fs::File::open(&file.abs_path)?;
        loop {
            let n = handle.read(&mut read_buf)?;
            if n == 0 {
                break;
            }
            let mut offset = 0;
            while offset < n {
                let remaining_piece = PIECE_LENGTH - buf.len();
                let take = remaining_piece.min(n - offset);
                buf.extend_from_slice(&read_buf[offset..offset + take]);
                offset += take;

                if buf.len() == PIECE_LENGTH {
                    let digest = Sha1::digest(&buf);
                    pieces.extend_from_slice(&digest);
                    buf.clear();
                }
            }
        }
    }
    if !buf.is_empty() {
        let digest = Sha1::digest(&buf);
        pieces.extend_from_slice(&digest);
    }

    Ok(pieces)
}

/// Generate a .torrent metafile covering all `*.jsonl.zst` files in `chunks_dir`.
pub fn generate_torrent(
    chunks_dir: &Path,
    output_path: &Path,
    tracker_url: Option<&str>,
    name: &str,
) -> anyhow::Result<()> {
    let files = collect_chunk_files(chunks_dir)?;
    if files.is_empty() {
        return Err(anyhow::anyhow!(
            "no chunk files found in {}",
            chunks_dir.display()
        ));
    }
    let pieces = compute_piece_hashes(&files)?;

    let mut info = Vec::new();
    info.push(b'd');
    bencode_str("files", &mut info);
    info.push(b'l');
    for file in &files {
        info.push(b'd');
        bencode_str("length", &mut info);
        bencode_int(file.length as i64, &mut info);
        bencode_str("path", &mut info);
        info.push(b'l');
        for component in file.rel_path.components() {
            let part = component.as_os_str().to_string_lossy();
            bencode_str(&part, &mut info);
        }
        info.push(b'e');
        info.push(b'e');
    }
    info.push(b'e');
    bencode_str("name", &mut info);
    bencode_str(name, &mut info);
    bencode_str("piece length", &mut info);
    bencode_int(PIECE_LENGTH as i64, &mut info);
    bencode_str("pieces", &mut info);
    bencode_bytes(&pieces, &mut info);
    info.push(b'e');

    let mut torrent = Vec::new();
    torrent.push(b'd');
    if let Some(url) = tracker_url {
        bencode_str("announce", &mut torrent);
        bencode_str(url, &mut torrent);
    }
    bencode_str("info", &mut torrent);
    torrent.extend_from_slice(&info);
    torrent.push(b'e');

    std::fs::write(output_path, torrent)?;
    Ok(())
}

/// Return lowercase hex infohash for a .torrent file.
pub fn infohash(torrent_path: &Path) -> anyhow::Result<String> {
    let data = std::fs::read(torrent_path)?;
    let (start, end) = find_info_span(&data)?;
    let digest = Sha1::digest(&data[start..end]);
    Ok(hex::encode(digest))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_generate_torrent_creates_file() {
        let dir = TempDir::new().unwrap();
        std::fs::write(dir.path().join("chunk-000000.jsonl.zst"), b"fake chunk 0").unwrap();
        std::fs::write(dir.path().join("chunk-000001.jsonl.zst"), b"fake chunk 1").unwrap();

        let torrent_path = dir.path().join("corpus.torrent");
        generate_torrent(
            dir.path(),
            &torrent_path,
            Some("udp://tracker.example.com:1337/announce"),
            "open-books-corpus",
        )
        .unwrap();

        assert!(torrent_path.exists());
        assert!(!std::fs::read(&torrent_path).unwrap().is_empty());
        let hash = infohash(&torrent_path).unwrap();
        assert_eq!(hash.len(), 40);
    }
}
