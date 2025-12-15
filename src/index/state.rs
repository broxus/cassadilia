use std::collections::BTreeMap;
use std::num::NonZeroU64;

use ahash::HashMap;
use thiserror::Error;

use crate::types::{BlobHash, DbStats, WalOp};

#[derive(Error, Debug)]
pub enum IndexStateError {
    #[error("Attempted to decrement zero ref count for hash {hash}")]
    DecrementZeroRefCount { hash: BlobHash },
    #[error("Hash {hash} not found for decrement")]
    HashNotFoundForDecrement { hash: BlobHash },
}

#[derive(Debug, Default, Clone)]
pub(crate) struct IndexState<K> {
    pub(crate) key_to_hash: BTreeMap<K, IndexStateItem>,
    pub(crate) hash_to_ref_count: HashMap<BlobHash, u32>,
    pub(crate) last_persisted_version: Option<NonZeroU64>,
    pub(crate) stats: DbStats,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStateItem {
    pub blob_hash: BlobHash,
    pub blob_size: u64,
}

impl<K> IndexState<K> {
    pub fn new() -> Self {
        IndexState {
            key_to_hash: BTreeMap::new(),
            hash_to_ref_count: HashMap::default(),
            last_persisted_version: None,
            stats: DbStats::default(),
        }
    }

    pub fn recompute_stats(&mut self, index_file_size_bytes: u64) {
        let mut unique =
            HashMap::with_capacity_and_hasher(self.hash_to_ref_count.len(), Default::default());

        for item in self.key_to_hash.values() {
            unique.entry(item.blob_hash).or_insert(item.blob_size);
        }

        let unique_blobs = unique.len() as u64;
        let total_bytes = unique.values().copied().sum::<u64>();

        self.stats.cas.unique_blobs = unique_blobs;
        self.stats.cas.total_bytes = total_bytes;
        self.stats.index.serialized_size_bytes = index_file_size_bytes;
    }

    pub(crate) fn increment_ref(&mut self, hash: &BlobHash) -> bool {
        let entry = self.hash_to_ref_count.entry(*hash).or_default();
        let was_zero = *entry == 0;
        *entry += 1;
        was_zero
    }

    pub(crate) fn decrement_ref(
        &mut self,
        hash_to_decrement: &BlobHash,
    ) -> Result<Option<BlobHash>, IndexStateError> {
        match self.hash_to_ref_count.get_mut(hash_to_decrement) {
            Some(count) => {
                if *count == 0 {
                    return Err(IndexStateError::DecrementZeroRefCount {
                        hash: *hash_to_decrement,
                    });
                }
                *count -= 1;
                if *count == 0 {
                    self.hash_to_ref_count.remove(hash_to_decrement);
                    Ok(Some(*hash_to_decrement))
                } else {
                    Ok(None)
                }
            }
            None => Err(IndexStateError::HashNotFoundForDecrement { hash: *hash_to_decrement }),
        }
    }
}

impl<K> IndexState<K>
where
    K: Clone + Ord,
{
    pub fn apply_logical_op(&mut self, op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexStateError> {
        let mut unreferenced_hashes = Vec::new();

        match op {
            WalOp::Put { key, hash, size } => {
                let new_item = IndexStateItem { blob_hash: *hash, blob_size: *size };

                match self.key_to_hash.insert(key.clone(), new_item) {
                    None => {
                        // New key → bump refcount of the new hash.
                        if self.increment_ref(hash) {
                            self.stats.cas.unique_blobs += 1;
                            self.stats.cas.total_bytes += *size;
                        }
                    }
                    Some(prev) if prev.blob_hash != *hash => {
                        // Repoint to a different blob:
                        // 1) decrement old, collect if it drops to zero
                        if let Some(h) = self.decrement_ref(&prev.blob_hash)? {
                            unreferenced_hashes.push(h);
                            self.stats.cas.unique_blobs -= 1;
                            self.stats.cas.total_bytes -= prev.blob_size;
                        }

                        // 2) increment new
                        if self.increment_ref(hash) {
                            self.stats.cas.unique_blobs += 1;
                            self.stats.cas.total_bytes += *size;
                        }
                    }
                    Some(prev) => {
                        // Same blob hash: refcounts unchanged.
                        // just a second insert of same k -> v pair, noop.
                        // size must match for the same hash.
                        assert_eq!(
                            prev.blob_size, *size,
                            "same hash but different size: prev={} new={}",
                            prev.blob_size, size
                        );
                    }
                }
            }

            WalOp::Remove { keys } => {
                // Remove mappings and decrement each old blob's refcount.
                for key in keys {
                    if let Some(item) = self.key_to_hash.remove(key)
                        && let Some(h) = self.decrement_ref(&item.blob_hash)?
                    {
                        unreferenced_hashes.push(h);
                        self.stats.cas.unique_blobs -= 1;
                        self.stats.cas.total_bytes -= item.blob_size;
                    }
                }
            }
        }

        Ok(unreferenced_hashes)
    }
}
