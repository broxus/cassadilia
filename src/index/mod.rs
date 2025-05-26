use std::collections::{BTreeMap, BTreeSet};
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
pub enum IndexError<K>
where
    K: Debug,
{
    #[error("Index persister error")]
    PersistFailed(#[from] PersisterError),

    #[error("Key not found: {key:?}")]
    KeyNotFound { key: K },

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

pub(crate) struct Index<K> {
    pub paths: DbPaths,
    pub key_encoder: Arc<dyn KeyEncoder<K>>,
    pub state: Arc<RwLock<IndexState<K>>>,
    pub wal: Mutex<WalManager>,
}

impl<K> Index<K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    pub fn load(
        db_root: PathBuf,
        key_encoder: Arc<dyn KeyEncoder<K>>,
        config: Config,
    ) -> Result<Self, IndexError<K>> {
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

        let index = Self { paths, key_encoder, state, wal: Mutex::new(wal_manager) };

        index.checkpoint(false)?; // Don't seal segment on startup

        Ok(index)
    }

    pub fn apply_wal_op(&self, logical_op: &WalOp<K>) -> Result<Vec<BlobHash>, IndexError<K>> {
        let op_data =
            logical_op.to_raw(&self.key_encoder).map_err(|e| IndexError::WalOpToRawEncodeKey(e))?;
        let op_data =
            serialize_wal_op_raw(&op_data).map_err(|e| IndexError::ApplyWalOpSerialize(e))?;

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

    pub fn checkpoint(&self, seal_segment: bool) -> Result<(), IndexError<K>> {
        tracing::info!("Starting checkpoint operation.");

        {
            let mut wal_guard = self.wal.lock();
            wal_guard.perform_checkpoint(seal_segment)?;
        }
        {
            let state_guard = self.state.read();
            let persister = IndexStatePersister::new(&self.paths);
            persister.save(&*state_guard, self.key_encoder.as_ref())?;
        }

        tracing::info!("Checkpoint completed successfully.");
        Ok(())
    }

    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        let state_guard = self.state.read();
        state_guard.key_to_hash.get(key).copied()
    }

    pub fn require_hash_for_key(&self, key: &K) -> Result<BlobHash, IndexError<K>> {
        self.get_hash_for_key(key).ok_or(IndexError::KeyNotFound { key: key.clone() })
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

    fn initialize_directories(db_root: &PathBuf) -> Result<(), IndexError<K>> {
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
