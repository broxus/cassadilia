use std::borrow::Borrow;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::ops::RangeBounds;
use std::path::PathBuf;
use std::sync::Arc;

use ahash::HashMap;
use parking_lot::{Mutex, RwLock};
use persistence::{IndexStatePersister, PersisterError};
use state::{IndexState, IndexStateError};
use thiserror::Error;

pub use self::state::IndexStateItem;
use crate::paths::DbPaths;
use crate::types::{BlobHash, CheckpointReason, Config, DbStats, KeyBytes, WalOp};
use crate::wal::{WalError, WalManager};

mod persistence;
mod state;

#[derive(Debug)]
pub enum IndexIoOperation {
    CreateParentDir,
    CreateDbDir,
}

impl std::fmt::Display for IndexIoOperation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IndexIoOperation::CreateParentDir => write!(f, "create parent directory for DB"),
            IndexIoOperation::CreateDbDir => write!(f, "create DB directory"),
        }
    }
}

#[derive(Error, Debug)]
pub enum IndexError {
    #[error(transparent)]
    PersistFailed(#[from] PersisterError),

    #[error("Key not found: {key}")]
    KeyNotFound { key: String },

    #[error("Initialization: Failed to {operation}")]
    Io {
        operation: IndexIoOperation,
        #[source]
        source: std::io::Error,
    },
    #[error("Initialization: Failed to create WAL manager")]
    InitCreateWalManager(#[source] WalError),

    #[error(transparent)]
    Wal(#[from] WalError),

    #[error("ApplyWalOp: Failed to serialize WalOpRaw")]
    ApplyWalOpSerialize(#[source] crate::serialization::SerializationError),
    #[error("ApplyWalOp: Failed to write WAL entry")]
    ApplyWalOpWriteEntry(#[source] WalError),
    #[error("Failed to serialize WalOp")]
    SerializeWalOp(#[source] crate::serialization::SerializationError),

    #[error("Failed to delete unreferenced blobs")]
    BlobDeletion {
        #[source]
        source: crate::cas_manager::CasManagerError,
    },

    #[error("Refcount integrity issue")]
    RefcountIntegrity {
        #[from]
        source: IndexStateError,
    },
}

#[must_use = "IntentGuard must be committed or will cleanup on drop"]
pub(crate) struct IntentGuard<'a, K>
where
    K: Clone + Eq + Ord + std::hash::Hash,
{
    index: &'a Index<K>,
    key: K,
    hash: BlobHash,
    size: u64,
    replaced_hash: Option<BlobHash>,
    committed: bool,
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct IntentMeta {
    pub blob_hash: BlobHash,
    pub blob_size: u64,
}

impl<K> IntentGuard<'_, K>
where
    K: KeyBytes + Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    /// Commit the intent by applying the WAL operation and removing the intent.
    pub(crate) fn commit(
        mut self,
        delete_fn: &crate::types::DeleteBlobCallFn,
    ) -> Result<(), IndexError> {
        self.index.apply_put_op(self.key.clone(), self.hash, self.size, delete_fn)?;
        self.committed = true;
        Ok(())
    }
}

impl<K> Drop for IntentGuard<'_, K>
where
    K: Clone + Eq + Ord + std::hash::Hash,
{
    fn drop(&mut self) {
        if !self.committed {
            // Revert: Remove our intent from pending_intents
            let mut intents = self.index.pending_intents.lock();

            if let Some(current_hash) = intents.get(&self.key)
                && *current_hash == self.hash
            {
                intents.remove(&self.key);

                // If we had replaced an existing intent, restore it
                if let Some(replaced_hash) = self.replaced_hash {
                    intents.insert(self.key.clone(), replaced_hash);
                }
            }
        }
    }
}

pub(crate) struct Index<K> {
    pub paths: DbPaths,
    pub state: Arc<RwLock<IndexState<K>>>,
    pub wal: Mutex<WalManager>,
    pub pending_intents: Mutex<HashMap<K, BlobHash>>,
}

impl<K> Index<K>
where
    K: KeyBytes + Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub fn load(db_root: PathBuf, config: Config) -> Result<Self, IndexError> {
        Self::initialize_directories(&db_root)?;
        let paths = DbPaths::new(db_root.clone());
        let mut wal_manager = WalManager::new(paths.clone(), config.num_ops_per_wal)
            .map_err(IndexError::InitCreateWalManager)?;

        let persister = IndexStatePersister::new(&paths);
        let mut state = persister.load()?;

        let index_file_size =
            std::fs::metadata(paths.index_file_path()).map(|m| m.len()).unwrap_or(0);
        state.recompute_stats(index_file_size);

        let checkpoint_version = state.last_persisted_version;
        let state = Arc::new(RwLock::new(state));

        let mut replayed_count = 0u64;
        wal_manager.replay_and_prepare(checkpoint_version, |op| {
            replayed_count += 1;
            let mut guard = state.write();
            let _ = guard.apply_logical_op(&op).expect("Index is corrupted");
        })?;

        let index = Self {
            paths,
            state,
            wal: Mutex::new(wal_manager),
            pending_intents: Mutex::new(HashMap::default()),
        };

        // Only checkpoint after replay if we actually replayed something
        if replayed_count > 0 {
            index.checkpoint(CheckpointReason::AfterReplay)?;
        }

        Ok(index)
    }

    pub fn checkpoint(&self, reason: CheckpointReason) -> Result<(), IndexError> {
        tracing::info!(?reason, "Starting checkpoint operation.");

        let mut snapshot = self.state.write();
        let mut wal_guard = self.wal.lock();

        self.checkpoint_inner(reason, &mut wal_guard, &mut *snapshot)
    }

    fn checkpoint_inner(
        &self,
        checkpoint_reason: CheckpointReason,
        wal_guard: &mut WalManager,
        snapshot: &mut IndexState<K>,
    ) -> Result<(), IndexError> {
        tracing::info!(?checkpoint_reason, "Starting checkpoint operation.");
        // 1. Compute the target
        let current_checkpoint = snapshot.last_persisted_version;
        let target_version =
            match wal_guard.compute_checkpoint_target(checkpoint_reason, current_checkpoint) {
                Some(v) => v,
                None => {
                    tracing::debug!(?checkpoint_reason, "Checkpoint was skipped");
                    return Ok(());
                }
            };

        // 2. Set the version we're about to persist
        snapshot.last_persisted_version = Some(target_version);

        let serialized_len = IndexStatePersister::new(&self.paths).save(snapshot)?;
        snapshot.stats.index.serialized_size_bytes = serialized_len;
        // 3. Prune segments up to the target
        wal_guard
            .commit_checkpoint(target_version, current_checkpoint)
            .map_err(IndexError::ApplyWalOpWriteEntry)?;
        tracing::info!(checkpoint_version = target_version, "Checkpoint completed successfully.");

        Ok(())
    }

    pub fn register_intent(
        &self,
        key: K,
        meta: IntentMeta,
    ) -> Result<IntentGuard<'_, K>, IndexError> {
        let mut intents = self.pending_intents.lock();

        // Check if there was a previous intent for this key
        let replaced_hash = intents.get(&key).copied();

        // Insert the new intent
        intents.insert(key.clone(), meta.blob_hash);

        Ok(IntentGuard {
            index: self,
            key,
            hash: meta.blob_hash,
            size: meta.blob_size,
            replaced_hash,
            committed: false,
        })
    }

    fn apply_wal_op_unsafe(
        state: &mut IndexState<K>,
        wal: &mut WalManager,
        logical_op: &WalOp<K>,
    ) -> Result<(Vec<BlobHash>, crate::wal::WalAppendInfo, bool), IndexError> {
        let pre_append_segment_id = wal.get_segment_id_for_previous_op();

        let serialized = crate::serialization::serialize_wal_op_raw(&logical_op.to_raw())
            .map_err(IndexError::SerializeWalOp)?;
        let append_info = wal.append_op(&serialized)?;

        let unreferenced = state.apply_logical_op(logical_op).expect("Index is corrupted");

        let post_append_segment_id = wal.segment_id_for_op_version(append_info.version.get());
        let rolled_over = pre_append_segment_id != post_append_segment_id;

        Ok((unreferenced, append_info, rolled_over))
    }

    pub fn apply_put_op(
        &self,
        key: K,
        hash: BlobHash,
        size: u64,
        delete_fn: &crate::types::DeleteBlobCallFn,
    ) -> Result<(), IndexError> {
        let logical_op = WalOp::Put { key: key.clone(), hash, size };
        let mut intents = self.pending_intents.lock();

        let (mut unreferenced_from_op, rolled_over) = {
            let mut state = self.state.write();
            let mut wal = self.wal.lock();
            let (hashes, _append_info, rolled) =
                Self::apply_wal_op_unsafe(&mut state, &mut wal, &logical_op)?;
            (hashes, rolled)
        };

        intents.remove(&key);

        // Filter out any unreferenced hashes that are still referenced by other intents
        unreferenced_from_op
            .retain(|hash| !intents.values().any(|intent_hash| intent_hash == hash));

        // Delete blobs BEFORE any checkpoint
        if !unreferenced_from_op.is_empty() {
            delete_fn(&unreferenced_from_op).map_err(|e| IndexError::BlobDeletion { source: e })?;
        }

        drop(intents);

        if rolled_over {
            let mut state = self.state.write();
            let mut wal = self.wal.lock();
            self.checkpoint_inner(CheckpointReason::SegmentRollover, &mut wal, &mut state)?;
        }

        Ok(())
    }

    pub fn apply_remove_op(
        &self,
        keys: Vec<K>,
        delete_fn: &crate::types::DeleteBlobCallFn,
    ) -> Result<(), IndexError> {
        let logical_op = WalOp::Remove { keys };
        let intents = self.pending_intents.lock();

        let (mut unreferenced_from_op, rolled_over) = {
            let mut state = self.state.write();
            let mut wal = self.wal.lock();
            let (hashes, _append_info, rolled) =
                Self::apply_wal_op_unsafe(&mut state, &mut wal, &logical_op)?;
            (hashes, rolled)
        };

        // Remove any unreferenced hashes that are still referenced by intents
        unreferenced_from_op
            .retain(|hash| !intents.values().any(|intent_hash| intent_hash == hash));

        // Delete blobs BEFORE any checkpoint
        if !unreferenced_from_op.is_empty() {
            delete_fn(&unreferenced_from_op).map_err(|e| IndexError::BlobDeletion { source: e })?;
        }

        drop(intents);

        if rolled_over {
            let mut state = self.state.write();
            let mut wal = self.wal.lock();
            self.checkpoint_inner(CheckpointReason::SegmentRollover, &mut wal, &mut state)?;
        }

        Ok(())
    }

    fn initialize_directories(db_root: &PathBuf) -> Result<(), IndexError> {
        if let Some(parent_dir) = db_root.parent() {
            std::fs::create_dir_all(parent_dir).map_err(|e| IndexError::Io {
                operation: IndexIoOperation::CreateParentDir,
                source: e,
            })?;
        }
        std::fs::create_dir_all(db_root)
            .map_err(|e| IndexError::Io { operation: IndexIoOperation::CreateDbDir, source: e })?;
        Ok(())
    }
}

impl<K> Index<K> {
    pub fn read_state(&self) -> IndexReadGuard<'_, K> {
        IndexReadGuard { inner: self.state.read() }
    }
}

impl<K: Debug> Debug for Index<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_guard = self.state.read();
        f.debug_struct("Index")
            .field("db_root", &self.paths.db_root_path())
            .field("index_file", &self.paths.index_file_path())
            .field(
                "state_summary",
                &format!(
                    "keys: {}, refs: {}",
                    state_guard.key_to_hash.len(),
                    state_guard.hash_to_ref_count.len()
                ),
            )
            .finish()
    }
}

/// A read-only view of the index state.
/// Holds a shared read lock for the duration of the guard.
/// Supports lookup, iteration, and range queries in key order.
pub struct IndexReadGuard<'a, K> {
    inner: parking_lot::RwLockReadGuard<'a, IndexState<K>>,
}

impl<'a, K> IndexReadGuard<'a, K> {
    #[must_use]
    pub fn stats(&self) -> DbStats {
        self.inner.stats
    }

    /// Returns the number of keys in the index.
    #[must_use]
    pub fn len(&self) -> usize {
        self.inner.key_to_hash.len()
    }

    /// Returns `true` if the index contains no keys.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.inner.key_to_hash.is_empty()
    }

    /// Returns an iterator over known blob hashes and their reference counts.
    #[must_use]
    pub fn known_blobs(&self) -> std::collections::hash_map::Iter<'_, BlobHash, u32> {
        self.inner.hash_to_ref_count.iter()
    }

    /// Returns true if the given blob hash is currently referenced.
    #[must_use]
    pub fn contains_blob_hash(&self, hash: &BlobHash) -> bool {
        self.inner.hash_to_ref_count.contains_key(hash)
    }

    /// Returns an iterator over entries within the specified key range, in ascending order.
    /// The range may use a borrowed form of the key (e.g., `&str` for `String`).
    pub fn range<T, R>(&self, range: R) -> std::collections::btree_map::Range<'_, K, IndexStateItem>
    where
        T: ?Sized + Ord,
        K: Borrow<T> + Ord,
        R: RangeBounds<T>,
    {
        self.inner.key_to_hash.range(range)
    }
}

impl<'a, K> IndexReadGuard<'a, K>
where
    K: Ord,
{
    /// Returns the item for the given key, if it exists.
    pub fn get_item(&self, key: &K) -> Option<IndexStateItem> {
        self.inner.key_to_hash.get(key).copied()
    }

    /// Returns `true` if the index contains the specified key.
    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.key_to_hash.contains_key(key)
    }

    /// Returns an iterator over all entries in ascending key order.
    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, K, IndexStateItem> {
        self.inner.key_to_hash.iter()
    }
}

impl<'a, K> IndexReadGuard<'a, K>
where
    K: Debug + Ord,
{
    /// Returns the item for the given key, or an error if it is not present.
    pub fn require_item(&self, key: &K) -> Result<IndexStateItem, IndexError> {
        self.get_item(key).ok_or(IndexError::KeyNotFound { key: format!("{key:?}") })
    }
}

impl<'a, K> IndexReadGuard<'a, K>
where
    K: Clone,
{
    /// Returns a snapshot of the current key map.
    /// Calls `clone` inside.
    #[must_use]
    pub fn keys_snapshot(&self) -> BTreeMap<K, IndexStateItem> {
        self.inner.key_to_hash.clone()
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;
    use crate::Config;

    fn setup_test_index() -> (tempfile::TempDir, Index<String>) {
        let dir = tempdir().unwrap();
        let index = Index::load(dir.path().to_path_buf(), Config::default()).unwrap();
        (dir, index)
    }

    #[test]
    fn test_intent_guard_cleanup_direct_state() {
        let (_dir, index) = setup_test_index();

        let key = "test_key".to_string();
        let hash = BlobHash::from_bytes([1; 32]);

        // Before registering intent - no intents, no ref count
        assert_eq!(index.pending_intents.lock().len(), 0);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);

        // Register intent and verify state
        {
            let _guard = index
                .register_intent(key.clone(), IntentMeta { blob_hash: hash, blob_size: 1 })
                .unwrap();
            assert_eq!(index.pending_intents.lock().len(), 1);
            assert_eq!(index.pending_intents.lock().get(&key), Some(&hash));
            // No ref count changes from register_intent
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);
        }
        // Guard dropped here - cleanup should happen

        // After guard drop - intent removed, no ref count changes
        assert_eq!(index.pending_intents.lock().len(), 0);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);
    }

    #[test]
    fn test_intent_guard_multiple_intents_same_key_direct_state() {
        let (_dir, index) = setup_test_index();

        let key = "test_key".to_string();
        let hash1 = BlobHash::from_bytes([1; 32]);
        let hash2 = BlobHash::from_bytes([2; 32]);

        // First intent
        let _guard1 = index
            .register_intent(key.clone(), IntentMeta { blob_hash: hash1, blob_size: 1 })
            .unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key), Some(&hash1));
        // No ref count changes from register_intent
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), None);

        // Second intent on same key - should replace first
        {
            let _guard2 = index
                .register_intent(key.clone(), IntentMeta { blob_hash: hash2, blob_size: 1 })
                .unwrap();
            assert_eq!(index.pending_intents.lock().len(), 1);
            assert_eq!(index.pending_intents.lock().get(&key), Some(&hash2));
            // No ref count changes from register_intent
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), None);
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash2), None);
        }
        // guard2 dropped - should restore hash1

        // After guard2 drop - hash1 restored, hash2 removed
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key), Some(&hash1));
        // Dropping guard2 restores the previous intent but does not apply a WAL op,
        // so refcounts in the state remain unchanged.
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), None);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash2), None);
    }

    #[test]
    fn test_intent_guard_reference_counting_direct_state() {
        let (_dir, index) = setup_test_index();

        let key1 = "key1".to_string();
        let key2 = "key2".to_string();
        let hash = BlobHash::from_bytes([1; 32]); // Same hash for both keys

        let meta = IntentMeta { blob_hash: hash, blob_size: 1 };

        // Register first intent
        let _guard1 = index.register_intent(key1.clone(), meta).unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        // No ref count changes from register_intent
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);

        // Register second intent with same hash
        {
            let _guard2 = index.register_intent(key2.clone(), meta).unwrap();
            assert_eq!(index.pending_intents.lock().len(), 2);
            // Still no ref count changes
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);
        }
        // guard2 dropped

        // After guard2 drop - still one intent remaining
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key1), Some(&hash));
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);
    }

    #[test]
    fn test_intent_guard_commit_removes_intent() {
        let (_dir, index) = setup_test_index();

        let key = "test_key".to_string();
        let hash = BlobHash::from_bytes([1; 32]);

        // Register intent
        let guard = index
            .register_intent(key.clone(), IntentMeta { blob_hash: hash, blob_size: 1 })
            .unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        // No ref count from register_intent
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), None);

        // Commit the guard with a no-op delete callback
        let delete_fn =
            |_hashes: &[BlobHash]| -> Result<(), crate::cas_manager::CasManagerError> { Ok(()) };
        guard.commit(&delete_fn).unwrap();

        // After commit - intent removed, ref count now set by WAL operation
        assert_eq!(index.pending_intents.lock().len(), 0);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));
    }

    #[test]
    fn test_index_read_guard_len() {
        let (_tmpdir, index) = setup_test_index();

        assert_eq!(index.read_state().len(), 0);
        assert!(index.read_state().is_empty());

        let guard = index
            .register_intent(
                "a".to_string(),
                IntentMeta { blob_hash: BlobHash::from([1u8; 32]), blob_size: 100 },
            )
            .unwrap();
        let delete_fn =
            |_hashes: &[BlobHash]| -> Result<(), crate::cas_manager::CasManagerError> { Ok(()) };
        guard.commit(&delete_fn).unwrap();

        assert_eq!(index.read_state().len(), 1);
        assert!(!index.read_state().is_empty());
    }
}
