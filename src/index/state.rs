use std::collections::BTreeMap;

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
    pub(crate) key_to_hash: BTreeMap<K, BlobHash>,
    pub(crate) hash_to_ref_count: HashMap<BlobHash, u32>,
}

impl<K> IndexState<K>
where
    K: Clone + Eq + Ord,
{
    pub fn new() -> Self {
        IndexState { key_to_hash: BTreeMap::new(), hash_to_ref_count: HashMap::default() }
    }

    pub fn apply_logical_op(&mut self, op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexStateError> {
        let mut unreferenced_hashes = Vec::new();

        match op {
            WalOp::Put { key, hash } => {
                // if key already exists, decrement old hash ref count
                let old_hash = self.key_to_hash.get(key).copied();
                if let Some(old_hash) = old_hash {
                    if let Some(unreferenced_hash) = self.decrement_ref(&old_hash)? {
                        unreferenced_hashes.push(unreferenced_hash);
                    }
                }

                // insert new mapping and increment new hash ref count
                self.key_to_hash.insert(key.clone(), *hash);
                self.increment_ref(hash);
            }
            WalOp::Remove { keys } => {
                // remove multiple key->hash mappings and collect unreferenced hashes
                for key in keys {
                    if let Some(hash) = self.key_to_hash.remove(key) {
                        if let Some(unreferenced_hash) = self.decrement_ref(&hash)? {
                            unreferenced_hashes.push(unreferenced_hash);
                        }
                    }
                }
            }
        }

        Ok(unreferenced_hashes)
    }

    pub(crate) fn increment_ref(&mut self, hash: &BlobHash) {
        *self.hash_to_ref_count.entry(*hash).or_insert(0) += 1;
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
