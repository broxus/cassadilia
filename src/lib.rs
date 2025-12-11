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

mod orphan;
pub use orphan::{OrphanStats, RecoveryResult};

mod settings;
use settings::{DbSettings, SettingsError, SettingsPersister};

#[cfg(test)]
mod tests;

mod index;
use index::{Index, IndexError};
pub use index::{IndexReadGuard, IndexStateItem};

mod transaction;
use cas_manager::{CasManager, CasManagerError};
pub use transaction::Transaction;

#[derive(Debug, Clone)]
pub enum LibIoOperation {
    CreateLockFile,
    CreateStagingDir,
    CreateCasDir,
    FileSync,
    CommitFlushWriter,
    CreateStagingFile,
    WriteStagingFile,
    ReadDir,
    RemoveFile,
    ReadContent,
    VerifyPathsSameFs,
}

#[derive(Error, Debug)]
pub enum LibError {
    #[error(
        "IO operation failed: {operation:?} at path {}",
        path.as_ref().map_or("unknown".to_string(), |p| p.display().to_string())
    )]
    Io {
        operation: LibIoOperation,
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },

    #[error("Db instance is already in use")]
    AlreadyOpened,

    #[error(transparent)]
    Cas(#[from] CasManagerError),

    #[error("Blob data missing for key {key} with hash {hash}")]
    BlobDataMissing { key: String, hash: BlobHash },

    #[error("Transaction commit: fdatasync send failed")]
    CommitFdatasyncSend(#[source] std::sync::mpsc::SendError<File>),
    #[error("Transaction commit: fdatasync IO failed")]
    CommitFdatasyncIo(#[source] std::io::Error),

    #[error(transparent)]
    Index(#[from] IndexError),

    #[error(transparent)]
    Settings(#[from] SettingsError),

    #[error(transparent)]
    TypesError(#[from] TypesError),

    #[error("Integrity check failed: {missing} missing blobs, {corrupted} corrupted blobs")]
    IntegrityCheckFailed {
        missing_blobs: Vec<BlobHash>,
        corrupted_blobs: Vec<BlobHash>,
        missing: usize,
        corrupted: usize,
    },
}

#[must_use]
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
    K: KeyBytes + Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub fn open(db_root: impl AsRef<Path>, config: Config) -> Result<Self, LibError> {
        // Use open_with_recover internally and drop the stats
        let fail_on_integrity_errors = config.fail_on_integrity_errors;
        let (cas, orphan_stats) = Self::open_with_recover(db_root, config)?;

        // Log orphan info if stats were collected
        if let Some(stats) = orphan_stats {
            tracing::info!(
                orphans = stats.orphaned_blobs.len(),
                invalid_files = stats.invalid_files.len(),
                missing_blobs = stats.missing_blobs.len(),
                corrupted_blobs = stats.corrupted_blobs.len(),
                staging_files = stats.staging_files.len(),
                scan_time = stats.scan_duration.as_secs_f64(),
                total_blobs = stats.total_blobs,
                "Orphan scan complete"
            );

            // Check for critical issues
            if fail_on_integrity_errors
                && (!stats.missing_blobs.is_empty() || !stats.corrupted_blobs.is_empty())
            {
                return Err(LibError::IntegrityCheckFailed {
                    missing_blobs: stats.missing_blobs.clone(),
                    corrupted_blobs: stats.corrupted_blobs.clone(),
                    missing: stats.missing_blobs.len(),
                    corrupted: stats.corrupted_blobs.len(),
                });
            }
        }

        Ok(cas)
    }

    /// Open database and return orphan stats if scanning is enabled.
    /// The returned stats are a snapshot; subsequent cleanup re-validates against
    /// the live index and `pending_intents` without taking a global filesystem lock.
    pub fn open_with_recover(
        db_root: impl AsRef<Path>,
        config: Config,
    ) -> Result<(Self, Option<OrphanStats<K>>), LibError> {
        let inner = CasInner::new(db_root.as_ref().to_path_buf(), config.clone())?;
        let cas = Self(Arc::new(inner));

        if !cas.paths.verify_all_paths_same_fs().map_err(|source| LibError::Io {
            operation: LibIoOperation::VerifyPathsSameFs,
            path: None,
            source,
        })? {
            return Err(LibError::Settings(SettingsError::InvalidPaths));
        }
        let orphan_stats = if config.scan_orphans_on_startup {
            let stats = orphan::scan_orphans(&cas.0, cas.0.clone(), config.verify_blob_integrity)?;
            Some(stats)
        } else {
            None
        };

        Ok((cas, orphan_stats))
    }

    /// Returns how many space the database occupies on disk.
    /// # NOTE:
    /// It does not include the size of dir in cas
    /// So it can be around 256 * 256(l1) * 256(l2) bytes depending on the number of directories
    /// created and fs used.
    #[must_use]
    pub fn stats(&self) -> DbStats {
        self.0.stats()
    }
}

pub struct CasInner<K> {
    _lockfile: File,
    pub(crate) paths: paths::DbPaths,
    pub(crate) index: Index<K>,
    datasync_channel: Option<std::sync::mpsc::Sender<File>>,
    pub(crate) cas_manager: Arc<CasManager>,
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
    K: KeyBytes + Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    fn new(db_root: PathBuf, config: Config) -> Result<Self, LibError> {
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

        let lockfile = std::fs::OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(paths.lockfile_path())
            .map_err(|e| LibError::Io {
                operation: LibIoOperation::CreateLockFile,
                path: Some(paths.lockfile_path().to_path_buf()),
                source: e,
            })?;

        lockfile.try_lock().map_err(|_e| LibError::AlreadyOpened)?;

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

        let cas_manager = Arc::new(CasManager::new(paths.clone(), dir_tree_is_pre_created));
        let index = Index::load(db_root, config.clone()).map_err(LibError::Index)?;

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

        Ok(Self { _lockfile: lockfile, paths, index, datasync_channel, cas_manager })
    }

    /// Returns a read-only view of the index state.
    /// The returned guard holds a shared read lock until dropped.
    pub fn read_index_state(&self) -> IndexReadGuard<'_, K> {
        self.index.read_state()
    }

    #[must_use]
    pub fn stats(&self) -> DbStats {
        self.index.read_state().stats()
    }

    /// Start a new transaction for the given key.
    pub fn put(&self, key: K) -> Result<Transaction<'_, K>, LibError> {
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

    /// Get a blob by its key.
    /// Returns `Ok(None)` if the key does not exist.
    pub fn get(&self, key: &K) -> Result<Option<bytes::Bytes>, LibError> {
        self.with_blob_item(key, |item| self.cas_manager.read_blob(&item.blob_hash))
    }

    /// Get the size of a blob by its key.
    /// Returns `Ok(None)` if the key does not exist.
    /// Uses index metadata only; no blob I/O.
    pub fn get_size(&self, key: &K) -> Result<Option<u64>, LibError> {
        self.with_blob_item(key, |item| Ok(item.blob_size))
    }

    /// Get a `BufReader` by key.
    /// Returns `Ok(None)` if the key does not exist.
    /// Safe to hold for long periods, will stream data even if the key was deleted.
    pub fn get_reader(&self, key: &K) -> Result<Option<BufReader<File>>, LibError> {
        self.with_blob_item(key, |item| self.cas_manager.blob_bufreader(&item.blob_hash))
    }

    /// Get a range of bytes from a blob.
    /// Returns `Ok(None)` if the key does not exist.
    /// Behavior:
    /// - Returns empty bytes if `range_start == range_end` or `range_start >= blob size`.
    /// - Returns an error if `range_start > range_end`.
    /// - Clamps `range_end` to the blob size if it exceeds it.
    pub fn get_range(
        &self,
        key: &K,
        range_start: u64,
        mut range_end: u64,
    ) -> Result<Option<bytes::Bytes>, LibError> {
        self.with_blob_item(key, |item| {
            if range_start >= item.blob_size {
                return Ok(bytes::Bytes::new());
            }
            range_end = std::cmp::min(range_end, item.blob_size);

            self.cas_manager.read_blob_range(&item.blob_hash, range_start, range_end)
        })
    }

    /// Remove key-value pair from the CAS
    pub fn remove(&self, key: &K) -> Result<bool, LibError> {
        if self.index.read_state().contains_key(key) {
            let delete_fn = |hashes: &[BlobHash]| -> Result<(), CasManagerError> {
                self.cas_manager.delete_blobs(hashes).map(|_| ())
            };

            self.index.apply_remove_op(vec![key.clone()], &delete_fn).map_err(LibError::Index)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Removes all key-value pairs within the specified range.
    ///
    /// Note: This operation provides eventual consistency rather than strict atomicity.
    /// The count returned reflects keys present at the time of initial scan, but:
    /// - Keys added to the range during removal won't be removed
    /// - Keys removed by concurrent operations will be gracefully skipped
    pub fn remove_range<R>(&self, range: R) -> Result<usize, LibError>
    where
        R: RangeBounds<K> + Debug + Clone,
    {
        let keys_to_remove: Vec<K> = {
            let state = self.index.read_state();
            state.range(range.clone()).map(|(key, _)| key.clone()).collect()
        };

        if keys_to_remove.is_empty() {
            return Ok(0);
        }

        let keys_to_remove_count = keys_to_remove.len();

        tracing::debug!("Removing {} keys in range {:?}", keys_to_remove_count, range);

        let delete_fn = |hashes: &[BlobHash]| -> Result<(), CasManagerError> {
            self.cas_manager.delete_blobs(hashes).map(|_| ())
        };

        self.index.apply_remove_op(keys_to_remove, &delete_fn).map_err(LibError::Index)?;

        Ok(keys_to_remove_count)
    }

    /// Checkpoint the index to persist the current state.
    /// Automatically triggered on WAL segment rollover; explicit calls persist the index
    /// and prune old WAL segments. Usually no need to call this manually.
    pub fn checkpoint(&self) -> Result<(), LibError> {
        self.index.checkpoint(CheckpointReason::Explicit).map_err(LibError::Index)
    }

    fn with_blob_item<T, F>(&self, key: &K, f: F) -> Result<Option<T>, LibError>
    where
        F: FnOnce(&IndexStateItem) -> Result<T, CasManagerError>,
    {
        let Some(item) = self.index.read_state().get_item(key) else {
            return Ok(None);
        };

        match f(&item) {
            Ok(result) => Ok(Some(result)),
            Err(cas_error) => {
                if let Some(io_err) =
                    cas_error.source().and_then(|s| s.downcast_ref::<std::io::Error>())
                    && io_err.kind() == std::io::ErrorKind::NotFound
                {
                    return Err(LibError::BlobDataMissing {
                        key: format!("{key:?}"),
                        hash: item.blob_hash,
                    });
                }
                Err(LibError::Cas(cas_error))
            }
        }
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
}
