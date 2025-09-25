use std::collections::BTreeMap;
use std::num::NonZeroU64;

use ahash::HashMap;
use thiserror::Error;

use crate::types::{BlobHash, WalOp};

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
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexStateItem {
    pub blob_hash: BlobHash,
    pub blob_size: u64,
}

impl<K> IndexState<K>
where
    K: Clone + Eq + Ord,
{
    pub fn new() -> Self {
        IndexState {
            key_to_hash: BTreeMap::new(),
            hash_to_ref_count: HashMap::default(),
            last_persisted_version: None,
        }
    }

    pub fn apply_logical_op(&mut self, op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexStateError> {
        let mut unreferenced_hashes = Vec::new();

        match op {
            WalOp::Put { key, hash, size } => {
                let new_item = IndexStateItem { blob_hash: *hash, blob_size: *size };

                match self.key_to_hash.insert(key.clone(), new_item) {
                    None => {
                        // New key → bump refcount of the new hash.
                        self.increment_ref(hash);
                    }
                    Some(prev) if prev.blob_hash != *hash => {
                        // Repoint to a different blob:
                        // 1) decrement old, collect if it drops to zero
                        if let Some(h) = self.decrement_ref(&prev.blob_hash)? {
                            unreferenced_hashes.push(h);
                        }
                        // 2) increment new
                        self.increment_ref(hash);
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
                    if let Some(item) = self.key_to_hash.remove(key) {
                        if let Some(h) = self.decrement_ref(&item.blob_hash)? {
                            unreferenced_hashes.push(h);
                        }
                    }
                }
            }
        }

        Ok(unreferenced_hashes)
    }

    pub(crate) fn increment_ref(&mut self, hash: &BlobHash) {
        *self.hash_to_ref_count.entry(*hash).or_default() += 1;
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
