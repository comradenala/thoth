use serde::{Deserialize, Serialize};

/// A range [start_book_id, end_book_id) owned by one shard.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ShardRange {
    pub shard_id: u64,
    pub start_book_id: u64,
    pub end_book_id: u64,
}

impl ShardRange {
    pub fn contains(&self, book_id: u64) -> bool {
        book_id >= self.start_book_id && book_id < self.end_book_id
    }
}

pub fn shard_for_book_id(book_id: u64, shard_size: u64) -> u64 {
    book_id / shard_size
}

pub fn shard_range(shard_id: u64, shard_size: u64, total_books: u64) -> ShardRange {
    let start = shard_id * shard_size;
    let end = ((shard_id + 1) * shard_size).min(total_books);
    ShardRange {
        shard_id,
        start_book_id: start,
        end_book_id: end,
    }
}

pub fn total_shards(shard_size: u64, total_books: u64) -> u64 {
    (total_books + shard_size - 1) / shard_size
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_shard_for_book() {
        assert_eq!(shard_for_book_id(0, 10_000), 0);
        assert_eq!(shard_for_book_id(9_999, 10_000), 0);
        assert_eq!(shard_for_book_id(10_000, 10_000), 1);
        assert_eq!(shard_for_book_id(39_999_999, 10_000), 3_999);
    }

    #[test]
    fn test_shard_range_last_is_clamped() {
        let r = shard_range(3_999, 10_000, 40_000_000);
        assert_eq!(r.start_book_id, 39_990_000);
        assert_eq!(r.end_book_id, 40_000_000);
    }

    #[test]
    fn test_shard_range_contains() {
        let r = shard_range(1, 10_000, 40_000_000);
        assert!(r.contains(10_000));
        assert!(r.contains(19_999));
        assert!(!r.contains(9_999));
        assert!(!r.contains(20_000));
    }

    #[test]
    fn test_total_shards() {
        assert_eq!(total_shards(10_000, 40_000_000), 4_000);
        assert_eq!(total_shards(10_000, 40_000_001), 4_001);
    }
}
