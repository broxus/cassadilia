use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use persistence::{IndexStatePersister, PersisterError};
use state::{IndexState, IndexStateError};
use thiserror::Error;

use crate::KeyEncoder;
use crate::paths::DbPaths;
use crate::serialization::serialize_wal_op_raw;
use crate::types::{BlobHash, Config, KeyEncoderError, WalOp};
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

    #[error("WalOpToRaw: Key encoding failed")]
    WalOpToRawEncodeKey(#[source] KeyEncoderError),

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
    replaced_hash: Option<BlobHash>,
    committed: bool,
}

impl<K> IntentGuard<'_, K>
where
    K: Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    /// Commit the intent by applying the WAL operation and removing the intent.
    /// Returns a list of blob hashes that are no longer referenced.
    pub(crate) fn commit(mut self) -> Result<Vec<BlobHash>, IndexError> {
        let op = WalOp::Put { key: self.key.clone(), hash: self.hash };
        let result = self.index.apply_wal_op_with_intent(&op, &self.key)?;
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

                    drop(intents); // Release lock before acquiring state lock

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
    pub key_encoder: Arc<dyn KeyEncoder<K>>,
    pub state: Arc<RwLock<IndexState<K>>>,
    pub wal: Mutex<WalManager>,
    pub pending_intents: Mutex<HashMap<K, BlobHash>>,
}

impl<K> Index<K>
where
    K: Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub fn load(
        db_root: PathBuf,
        key_encoder: Arc<dyn KeyEncoder<K>>,
        config: Config,
    ) -> Result<Self, IndexError> {
        Self::initialize_directories(&db_root)?;
        let paths = DbPaths::new(db_root.clone());
        let mut wal_manager = WalManager::new(paths.clone(), config.num_ops_per_wal)
            .map_err(IndexError::InitCreateWalManager)?;

        wal_manager.load_checkpoint_metadata()?;

        let persister = IndexStatePersister::new(&paths);
        let state = persister.load(key_encoder.as_ref())?;
        let state = Arc::new(RwLock::new(state));
        wal_manager.replay_and_prepare(key_encoder.clone(), |op| {
            let _ = state.write().apply_logical_op(&op).expect("Index is corrupted");
        })?;

        let index = Self {
            paths,
            key_encoder,
            state,
            wal: Mutex::new(wal_manager),
            pending_intents: Mutex::new(HashMap::new()),
        };

        index.checkpoint(false)?; // Don't seal segment on startup

        Ok(index)
    }

    pub fn apply_wal_op(&self, logical_op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexError> {
        let op_data =
            logical_op.to_raw(&self.key_encoder).map_err(IndexError::WalOpToRawEncodeKey)?;
        let op_data = serialize_wal_op_raw(&op_data).map_err(IndexError::ApplyWalOpSerialize)?;

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

            let persister = IndexStatePersister::new(&self.paths);
            persister.save(&snapshot, self.key_encoder.as_ref())?;
        }

        tracing::info!("Checkpoint completed successfully.");
        Ok(())
    }

    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        let state_guard = self.state.read();
        state_guard.key_to_hash.get(key).copied()
    }

    pub fn require_hash_for_key(&self, key: &K) -> Result<BlobHash, IndexError> {
        self.get_hash_for_key(key).ok_or(IndexError::KeyNotFound { key: format!("{key:?}") })
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.get_hash_for_key(key).is_some()
    }

    pub fn known_keys(&self) -> BTreeSet<K> {
        let state_guard = self.state.read();
        state_guard.key_to_hash.keys().cloned().collect()
    }

    pub fn known_blobs(&self) -> BTreeSet<BlobHash> {
        let state_guard = self.state.read();
        state_guard.hash_to_ref_count.keys().copied().collect()
    }

    pub fn index_snapshot(&self) -> BTreeMap<K, BlobHash> {
        let state_guard = self.state.read();
        state_guard.key_to_hash.clone()
    }

    pub fn register_intent(&self, key: K, hash: BlobHash) -> Result<IntentGuard<K>, IndexError> {
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
        state.increment_ref(&hash);

        // Insert the new intent
        intents.insert(key.clone(), hash);

        Ok(IntentGuard { index: self, key, hash, replaced_hash, committed: false })
    }

    pub fn apply_wal_op_with_intent(
        &self,
        logical_op: &WalOp<K>,
        key: &K,
    ) -> Result<Vec<BlobHash>, IndexError> {
        // First apply the WAL operation as usual
        let mut unreferenced_from_op = self.apply_wal_op(logical_op)?;

        // Remove the intent now that it's committed
        let mut intents = self.pending_intents.lock();
        intents.remove(key);

        // Filter out any hashes that are still referenced by pending intents
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use tempfile::tempdir;

    use super::*;
    use crate::Config;
    use crate::tests::utils::encoders::StringEncoder;

    fn setup_test_index() -> (tempfile::TempDir, Index<String>) {
        let dir = tempdir().unwrap();
        let index =
            Index::load(dir.path().to_path_buf(), Arc::new(StringEncoder), Config::default())
                .unwrap();
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
            let _guard = index.register_intent(key.clone(), hash).unwrap();
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
        let _guard1 = index.register_intent(key.clone(), hash1).unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.pending_intents.lock().get(&key), Some(&hash1));
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash1), Some(&1));

        // Second intent on same key - should replace first
        {
            let _guard2 = index.register_intent(key.clone(), hash2).unwrap();
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

        // Register first intent
        let _guard1 = index.register_intent(key1.clone(), hash).unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));

        // Register second intent with same hash
        {
            let _guard2 = index.register_intent(key2.clone(), hash).unwrap();
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
        let guard = index.register_intent(key.clone(), hash).unwrap();
        assert_eq!(index.pending_intents.lock().len(), 1);
        assert_eq!(index.state.read().hash_to_ref_count.get(&hash), Some(&1));

        // Commit the guard
        let _to_delete = guard.commit().unwrap();

        // After commit - intent removed but ref count preserved (WAL operation keeps it)
        assert_eq!(index.pending_intents.lock().len(), 0);
        // Note: ref count would normally be managed by the WAL operation in
        // apply_wal_op_with_intent
    }
}
