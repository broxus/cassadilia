use std::collections::{BTreeMap, BTreeSet};
use std::error::Error;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::ops::{Deref, RangeBounds};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use thiserror::Error;

mod cas_manager;

mod io;
mod paths;

mod serialization;

mod types;
mod wal;
pub use types::*;

mod settings;
use settings::{DbSettings, SettingsError, SettingsPersister};

#[cfg(test)]
mod tests;

#[cfg(test)]
pub mod test_utils;

mod index;
use index::Index;

mod transaction;
use cas_manager::{CasManager, CasManagerError};
use index::IndexError;
pub use transaction::Transaction;

#[derive(Debug, Clone)]
pub enum LibIoOperation {
    CreateStagingDir,
    CreateCasDir,
    FileSync,
    CommitFlushWriter,
    CreateStagingFile,
    WriteStagingFile,
}

#[derive(Error, Debug)]
pub enum LibError {
    #[error("IO operation failed: {operation:?} at path {}", path.as_ref().map_or("unknown".to_string(), |p| p.display().to_string()))]
    Io {
        operation: LibIoOperation,
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },

    #[error("CAS operation failed")]
    Cas(CasManagerError),

    #[error("Blob data missing for key {key} with hash {hash}")]
    BlobDataMissing { key: String, hash: BlobHash },

    #[error("Transaction commit: fdatasync send failed")]
    CommitFdatasyncSend(std::sync::mpsc::SendError<File>),
    #[error("Transaction commit: fdatasync IO failed")]
    CommitFdatasyncIo(#[source] std::io::Error),

    #[error("Index operation failed")]
    Index(IndexError),

    #[error("Settings error")]
    Settings(SettingsError),

    #[error("Key encoder operation failed")]
    KeyEncoderError(KeyEncoderError),
    #[error("Types error")]
    TypesError(TypesError),
}

pub fn calculate_blob_hash(blob_data: &[u8]) -> BlobHash {
    BlobHash(blake3::hash(blob_data).into())
}

fn pre_create_all_cas_directories(paths: &paths::DbPaths) -> Result<(), LibError> {
    let cas_root = paths.cas_root_path();

    for i in 0..256 {
        for j in 0..256 {
            let dir = cas_root.join(format!("{i:02x}")).join(format!("{j:02x}"));
            std::fs::create_dir_all(&dir).map_err(|e| LibError::Io {
                operation: LibIoOperation::CreateCasDir,
                path: Some(dir),
                source: e,
            })?;
        }
    }

    Ok(())
}

#[derive(Clone)]
pub struct Cas<K>(Arc<CasInner<K>>);

impl<K> Deref for Cas<K> {
    type Target = CasInner<K>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Cas<K>
where
    K: Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub fn open(
        db_root: impl AsRef<Path>,
        key_encoder: impl KeyEncoder<K> + 'static,
        config: Config,
    ) -> Result<Self, LibError> {
        let inner = CasInner::new(db_root.as_ref().to_path_buf(), key_encoder, config)?;
        Ok(Self(Arc::new(inner)))
    }
}

pub struct CasInner<K> {
    paths: paths::DbPaths,
    index: Index<K>,
    datasync_channel: Option<std::sync::mpsc::Sender<File>>,
    cas_manager: Arc<CasManager>,
}

impl<K> Debug for CasInner<K>
where
    K: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CasInner")
            .field("staging_root", &self.paths.staging_root_path())
            .field("cas_root", &self.paths.cas_root_path())
            .field("index", &self.index)
            .finish()
    }
}

impl<K> CasInner<K>
where
    K: Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    fn new(
        db_root: PathBuf,
        key_encoder: impl KeyEncoder<K> + 'static,
        config: Config,
    ) -> Result<Self, LibError> {
        let key_encoder = Arc::new(key_encoder);

        let fs_lock = FsLock::new();
        let paths = paths::DbPaths::new(db_root.clone());

        std::fs::create_dir_all(paths.staging_root_path()).map_err(|e| LibError::Io {
            operation: LibIoOperation::CreateStagingDir,
            path: Some(paths.staging_root_path().to_path_buf()),
            source: e,
        })?;
        std::fs::create_dir_all(paths.cas_root_path()).map_err(|e| LibError::Io {
            operation: LibIoOperation::CreateCasDir,
            path: Some(paths.cas_root_path().to_path_buf()),
            source: e,
        })?;

        // Load or create settings
        let settings_persister = SettingsPersister::new(paths.settings_path().to_path_buf());
        let dir_tree_is_pre_created = match settings_persister.load().map_err(LibError::Settings)? {
            Some(existing_settings) => {
                // Validate immutable settings
                if existing_settings.num_ops_per_wal != config.num_ops_per_wal {
                    return Err(LibError::Settings(SettingsError::ValidationFailed(format!(
                        "Cannot change num_ops_per_wal from {} to {} after database creation",
                        existing_settings.num_ops_per_wal, config.num_ops_per_wal
                    ))));
                }
                existing_settings.dir_tree_is_pre_created
            }
            None => {
                // First time - create settings from config
                let new_settings = DbSettings {
                    version: settings::CURRENT_DB_VERSION,
                    dir_tree_is_pre_created: config.pre_create_cas_dirs,
                    num_ops_per_wal: config.num_ops_per_wal,
                };

                // Pre-create directories if requested
                if config.pre_create_cas_dirs {
                    tracing::info!("Pre-creating CAS directory tree...");
                    pre_create_all_cas_directories(&paths)?;
                    tracing::info!("Pre-created 65,536 CAS directories");
                }

                settings_persister.save(&new_settings).map_err(LibError::Settings)?;
                new_settings.dir_tree_is_pre_created
            }
        };

        let cas_manager =
            Arc::new(CasManager::new(paths.clone(), fs_lock.clone(), dir_tree_is_pre_created));
        let index = Index::load(db_root, key_encoder, config.clone()).map_err(LibError::Index)?;

        let datasync_channel = match config.sync_mode {
            SyncMode::Sync => None,
            SyncMode::Async => {
                let (sender, receiver) = std::sync::mpsc::channel::<File>();
                std::thread::spawn(move || {
                    for file in receiver {
                        if let Err(e) = file.sync_data() {
                            tracing::error!("Failed to sync file: {e}");
                        }
                    }
                });
                Some(sender)
            }
        };

        Ok(Self { paths, index, datasync_channel, cas_manager })
    }

    /// Start a new transaction for the given key.
    pub fn put(&self, key: K) -> Result<Transaction<K>, LibError> {
        Transaction::new(self, key).map_err(|e| match e {
            transaction::TransactionError::StagingFileIo { operation, path, source } => {
                match operation {
                    transaction::StagingFileOp::Create => LibError::Io {
                        operation: LibIoOperation::CreateStagingFile,
                        path: Some(path),
                        source,
                    },
                    transaction::StagingFileOp::Write => LibError::Io {
                        operation: LibIoOperation::WriteStagingFile,
                        path: Some(path),
                        source,
                    },
                }
            }
        })
    }

    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        self.index.get_hash_for_key(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Result<Option<bytes::Bytes>, LibError> {
        self.with_blob_hash(key, "read", false, |blob_hash| self.cas_manager.read_blob(blob_hash))
    }

    pub fn size(&self, key: &K) -> Result<Option<u64>, LibError> {
        self.with_blob_hash(key, "size check", false, |blob_hash| {
            self.cas_manager.blob_size(blob_hash)
        })
    }

    pub fn raw_bufreader(&self, key: &K) -> Result<BufReader<File>, LibError> {
        let blob_hash = self.index.require_hash_for_key(key).map_err(LibError::Index)?;
        self.cas_manager.blob_bufreader(&blob_hash).map_err(LibError::Cas)
    }

    fn fdatasync(&self, file: File) -> Result<(), LibError> {
        match &self.datasync_channel {
            Some(sender) => {
                sender.send(file).map_err(LibError::CommitFdatasyncSend)?;
                Ok(())
            }
            None => {
                file.sync_data().map_err(LibError::CommitFdatasyncIo)?;
                Ok(())
            }
        }
    }

    pub fn known_keys(&self) -> BTreeSet<K> {
        self.index.known_keys()
    }

    pub fn index_snapshot(&self) -> BTreeMap<K, BlobHash> {
        self.index.index_snapshot()
    }

    pub fn get_range(
        &self,
        key: &K,
        range_start: u64,
        range_end: u64,
    ) -> Result<Option<bytes::Bytes>, LibError> {
        self.with_blob_hash(key, "range read", false, |blob_hash| {
            self.cas_manager.read_blob_range(blob_hash, range_start, range_end)
        })
    }

    pub fn remove(&self, key: &K) -> Result<bool, LibError> {
        if self.index.contains_key(key) {
            let op = WalOp::Remove { keys: vec![key.clone()] };
            let to_delete = self.index.apply_wal_op(&op).map_err(LibError::Index)?;
            let _deleted_hashes =
                self.cas_manager.delete_blobs(&to_delete).map_err(LibError::Cas)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    pub fn known_blobs(&self) -> BTreeSet<BlobHash> {
        self.index.known_blobs()
    }

    pub fn remove_range<R>(&self, range: R) -> Result<usize, LibError>
    where
        R: RangeBounds<K> + Debug + Clone,
    {
        let keys_to_remove: Vec<K> =
            self.index.known_keys().iter().filter(|key| range.contains(key)).cloned().collect();

        if keys_to_remove.is_empty() {
            return Ok(0);
        }

        tracing::debug!("Removing {} keys in range {:?}", keys_to_remove.len(), range);

        let op = WalOp::Remove { keys: keys_to_remove.clone() };
        let to_delete = self.index.apply_wal_op(&op).map_err(LibError::Index)?;
        let _deleted_hashes = self.cas_manager.delete_blobs(&to_delete).map_err(LibError::Cas)?;

        Ok(keys_to_remove.len())
    }

    pub fn checkpoint(&self) -> Result<(), LibError> {
        self.index.checkpoint(true).map_err(LibError::Index)
    }

    fn with_blob_hash<T, F>(
        &self,
        key: &K,
        operation_name: &'static str,
        return_err_on_missing: bool,
        f: F,
    ) -> Result<Option<T>, LibError>
    where
        F: FnOnce(&BlobHash) -> Result<T, CasManagerError>,
    {
        if let Some(blob_hash) = self.index.get_hash_for_key(key) {
            match f(&blob_hash) {
                Ok(result) => Ok(Some(result)),
                Err(cas_error) => {
                    if let Some(io_err) =
                        cas_error.source().and_then(|s| s.downcast_ref::<std::io::Error>())
                    {
                        if io_err.kind() == std::io::ErrorKind::NotFound {
                            tracing::error!(
                                "Index contains key '{:?}' pointing to hash '{}', but CAS file not found during {}!",
                                key,
                                blob_hash,
                                operation_name
                            );
                            return if return_err_on_missing {
                                Err(LibError::BlobDataMissing {
                                    key: format!("{key:?}"),
                                    hash: blob_hash,
                                })
                            } else {
                                Ok(None)
                            };
                        }
                    }
                    Err(LibError::Cas(cas_error))
                }
            }
        } else if return_err_on_missing {
            Err(LibError::Index(IndexError::KeyNotFound { key: format!("{key:?}") }))
        } else {
            Ok(None)
        }
    }
}
