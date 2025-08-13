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
use crate::serialization::serialize_wal_op_raw;
use crate::types::{BlobHash, Config, KeyBytes, WalOp};
use crate::wal::{WalAppendInfo, WalError, WalManager};

mod persistence;
mod state;

pub(crate) const CHECKPOINT_META_FILENAME: &str = "checkpoint.meta";

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
    #[error("Index persister error")]
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

    #[error("WAL operation failed")]
    Wal(#[from] WalError),

    #[error("ApplyWalOp: Failed to serialize WalOpRaw")]
    ApplyWalOpSerialize(#[source] crate::serialization::SerializationError),
    #[error("ApplyWalOp: Failed to write WAL entry")]
    ApplyWalOpWriteEntry(#[source] WalError),

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
    /// Returns a list of blob hashes that are no longer referenced.
    pub(crate) fn commit(mut self) -> Result<Vec<BlobHash>, IndexError> {
        let op = WalOp::Put { key: self.key.clone(), hash: self.hash, size: self.size };
        let result = self.index.apply_put_op(&op, &self.key)?;
        self.committed = true;
        Ok(result)
    }
}

impl<K> Drop for IntentGuard<'_, K>
where
    K: Clone + Eq + Ord + std::hash::Hash,
{
    fn drop(&mut self) {
        if !self.committed {
            //  Revert
            // 1. Remove our intent from pending_intents
            // 2. Decrement ref count for our hash
            // 3. If we replaced an existing intent, restore it

            let mut intents = self.index.pending_intents.lock();
            let mut state = self.index.state.write();

            if let Some(current_hash) = intents.get(&self.key) {
                if *current_hash == self.hash {
                    intents.remove(&self.key);

                    // If we had replaced an existing intent, restore it
                    if let Some(replaced_hash) = self.replaced_hash {
                        intents.insert(self.key.clone(), replaced_hash);
                    }
                    // Decrement ref count for our hash
                    let _ = state.decrement_ref(&self.hash); // Ignore errors in drop

                    // If we restored a replaced intent, increment its ref count
                    if let Some(replaced_hash) = self.replaced_hash {
                        state.increment_ref(&replaced_hash);
                    }
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

        wal_manager.load_checkpoint_metadata()?;

        let persister = IndexStatePersister::new(&paths);
        let state = persister.load()?;
        let state = Arc::new(RwLock::new(state));
        wal_manager.replay_and_prepare(|op| {
            let _ = state.write().apply_logical_op(&op).expect("Index is corrupted");
        })?;

        let index = Self {
            paths,
            state,
            wal: Mutex::new(wal_manager),
            pending_intents: Mutex::new(HashMap::default()),
        };

        index.checkpoint(false)?; // Don't seal segment on startup

        Ok(index)
    }

    pub fn read_state(&self) -> IndexReadGuard<'_, K> {
        IndexReadGuard { inner: self.state.read() }
    }

    pub fn apply_wal_op_unsafe(&self, logical_op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexError> {
        let op_data =
            serialize_wal_op_raw(&logical_op.to_raw()).map_err(IndexError::ApplyWalOpSerialize)?;

        let WalAppendInfo { version, .. } = {
            let mut wal_guard = self.wal.lock();
            wal_guard.append_op(&op_data)?
        };

        tracing::trace!(version, ?logical_op, "Logged WAL op, applying to state");

        // Apply to in-memory state.
        let mut state_guard = self.state.write();
        let unreferenced_hashes =
            state_guard.apply_logical_op(logical_op).expect("Index is corrupted");
        Ok(unreferenced_hashes)
    }

    pub fn checkpoint(&self, seal_segment: bool) -> Result<(), IndexError> {
        tracing::info!("Starting checkpoint operation.");

        {
            let mut wal_guard = self.wal.lock();
            wal_guard.perform_checkpoint(seal_segment)?;
        }
        {
            let intents_guard = self.pending_intents.lock();
            let state_guard = self.state.read();

            // Clone the state and adjust ref counts for pending intents
            let mut snapshot = (*state_guard).clone();
            for (_, hash) in intents_guard.iter() {
                // Decrement ref count in the snapshot (not the actual state)
                // This ensures we persist the "committed" state without inflated intent counts
                if let Some(count) = snapshot.hash_to_ref_count.get_mut(hash) {
                    if *count > 0 {
                        *count -= 1;
                        if *count == 0 {
                            snapshot.hash_to_ref_count.remove(hash);
                        }
                    }
                }
            }

            IndexStatePersister::new(&self.paths).save(&snapshot)?;
        }

        tracing::info!("Checkpoint completed successfully.");
        Ok(())
    }

    pub fn register_intent(
        &self,
        key: K,
        meta: IntentMeta,
    ) -> Result<IntentGuard<'_, K>, IndexError> {
        let mut intents = self.pending_intents.lock();
        let mut state = self.state.write();

        // Check if there was a previous intent for this key and decrement its ref count
        let replaced_hash = if let Some(old_hash) = intents.get(&key) {
            state
                .decrement_ref(old_hash)
                .map_err(|e| IndexError::RefcountIntegrity { source: e })?;
            Some(*old_hash)
        } else {
            None
        };

        // Increment ref count for the new hash
        state.increment_ref(&meta.blob_hash);

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

    pub fn apply_put_op(
        &self,
        logical_op: &WalOp<K>,
        key: &K,
    ) -> Result<Vec<BlobHash>, IndexError> {
        // Atomically apply WAL operation and remove intent to prevent
        // other threads from seeing committed state while intent exists
        let mut intents = self.pending_intents.lock();

        let mut unreferenced_from_op = self.apply_wal_op_unsafe(logical_op)?;

        intents.remove(key);

        // Prevent deletion of blobs still referenced by pending intents
        unreferenced_from_op
            .retain(|hash| !intents.values().any(|intent_hash| intent_hash == hash));

        Ok(unreferenced_from_op)
    }

    pub fn apply_remove_op(&self, logical_op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexError> {
        // Take intent lock before applying remove to prevent deletion
        // of blobs that have pending put operations
        let intents = self.pending_intents.lock();

        let mut unreferenced_from_op = self.apply_wal_op_unsafe(logical_op)?;

        // Filter out blobs that are targets of pending intents
        unreferenced_from_op
            .retain(|hash| !intents.values().any(|intent_hash| intent_hash == hash));

        Ok(unreferenced_from_op)
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

impl<K: Debug> Debug for Index<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let state_guard = self.state.read();
        f.debug_struct("Index")
            .field("db_root", &self.paths.db_root_path())
            .field("index_file", &self.paths.index_file_path())
            .field("checkpoint_meta_file", &self.paths.checkpoint_meta_path())
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

pub struct IndexReadGuard<'a, K> {
    inner: parking_lot::RwLockReadGuard<'a, IndexState<K>>,
}

impl<'a, K> IndexReadGuard<'a, K>
where
    K: Debug + Clone + Ord,
{
    pub fn get_item(&self, key: &K) -> Option<IndexStateItem> {
        self.inner.key_to_hash.get(key).copied()
    }

    pub fn require_item(&self, key: &K) -> Result<IndexStateItem, IndexError> {
        self.get_item(key).ok_or(IndexError::KeyNotFound { key: format!("{key:?}") })
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.inner.key_to_hash.contains_key(key)
    }

    pub fn iter(&self) -> std::collections::btree_map::Iter<'_, K, IndexStateItem> {
        self.inner.key_to_hash.iter()
    }

    pub fn range<T, R>(&self, range: R) -> std::collections::btree_map::Range<'_, K, IndexStateItem>
    where
        T: ?Sized + Ord,
        K: Borrow<T> + Ord,
        R: RangeBounds<T>,
    {
        self.inner.key_to_hash.range(range)
    }

    pub fn keys_snapshot(&self) -> BTreeMap<K, IndexStateItem> {
        self.inner.key_to_hash.clone()
    }

    pub fn len(&self) -> usize {
        self.inner.key_to_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.inner.key_to_hash.is_empty()
    }

    pub fn known_blobs(&self) -> std::collections::hash_map::Iter<'_, BlobHash, u32> {
        self.inner.hash_to_ref_count.iter()
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
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));
        }
        // Guard dropped here - cleanup should happen

        // After guard drop - intent removed, ref count decremented
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
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), Some(&1));

        // Second intent on same key - should replace first
        {
            let _guard2 = index
                .register_intent(key.clone(), IntentMeta { blob_hash: hash2, blob_size: 1 })
                .unwrap();
            assert_eq!(index.pending_intents.lock().len(), 1);
            assert_eq!(index.pending_intents.lock().get(&key), Some(&hash2));
            // hash1 ref count decremented, hash2 incremented
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), None);
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash2), Some(&1));
        }
        // guard2 dropped - should restore hash1

        // After guard2 drop - hash1 restored, hash2 removed
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key), Some(&hash1));
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), Some(&1));
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
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));

        // Register second intent with same hash
        {
            let _guard2 = index.register_intent(key2.clone(), meta).unwrap();
            assert_eq!(index.pending_intents.lock().len(), 2);
            assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&2));
        }
        // guard2 dropped - ref count should decrease but not reach 0

        // After guard2 drop - still one intent remaining
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key1), Some(&hash));
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));
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
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));

        // Commit the guard
        let _to_delete = guard.commit().unwrap();

        // After commit - intent removed but ref count preserved (WAL operation keeps it)
        assert_eq!(index.pending_intents.lock().len(), 0);
        // Note: ref count would normally be managed by the WAL operation in
        // apply_wal_op_with_intent
    }

    #[test]
    fn test_index_read_guard_len() {
        let (_tmpdir, index) = setup_test_index();

        assert_eq!(index.read_state().len(), 0);
        assert!(index.read_state().is_empty());

        index
            .apply_wal_op_unsafe(&WalOp::Put {
                key: "a".to_string(),
                hash: BlobHash::from([1u8; 32]),
                size: 100,
            })
            .unwrap();

        assert_eq!(index.read_state().len(), 1);
        assert!(!index.read_state().is_empty());
    }
}
