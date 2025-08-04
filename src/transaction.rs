use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use tempfile::NamedTempFile;
use thiserror::Error;

use crate::CasInner;
use crate::index::IntentMeta;

#[derive(Debug)]
pub enum StagingFileOp {
    Create,
    Write,
}

#[derive(Error, Debug)]
pub enum TransactionError {
    #[error("Staging file IO error during {operation:?} for {path:?}")]
    StagingFileIo {
        operation: StagingFileOp,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}

impl<T> Debug for Transaction<'_, T>
where
    T: Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Transaction")
            .field("temp_file", &self.temp_file.path())
            .field("key", &self.key)
            .finish()
    }
}

#[must_use = "Transaction must be completed by calling finish()"]
pub struct Transaction<'a, K>
where
    K: Debug,
{
    pub(crate) temp_file: NamedTempFile,
    pub(crate) cas_inner: &'a CasInner<K>,
    pub(crate) writer: BufWriter<File>,
    pub(crate) hasher: blake3::Hasher,
    pub(crate) size: u64,
    pub(crate) key: K,
}

impl<'a, K> Transaction<'a, K>
where
    K: Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub(crate) fn new(cas_inner: &'a CasInner<K>, key: K) -> Result<Self, TransactionError> {
        let staging_dir = cas_inner.paths.staging_root_path();

        let temp_file =
            NamedTempFile::new_in(staging_dir).map_err(|e| TransactionError::StagingFileIo {
                operation: StagingFileOp::Create,
                path: staging_dir.to_path_buf(),
                source: e,
            })?;

        let file = temp_file.reopen().map_err(|e| TransactionError::StagingFileIo {
            operation: StagingFileOp::Create,
            path: temp_file.path().to_path_buf(),
            source: e,
        })?;

        tracing::debug!(
            "Starting transaction for key '{:?}' using staging file '{}'",
            key,
            temp_file.path().display()
        );

        Ok(Self {
            writer: BufWriter::new(file),
            temp_file,
            cas_inner,
            hasher: blake3::Hasher::new(),
            size: 0,
            key,
        })
    }

    /// Append `data` to the transaction.
    /// Most common usage is to incrementally compress and write chunks.
    pub fn write(&mut self, data: &[u8]) -> Result<(), TransactionError> {
        self.size += data.len() as u64;
        self.hasher.update(data);
        self.writer.write_all(data).map_err(|e| TransactionError::StagingFileIo {
            operation: StagingFileOp::Write,
            path: self.temp_file.path().to_path_buf(),
            source: e,
        })?;
        Ok(())
    }

    pub fn finish(self) -> Result<(), crate::LibError> {
        tracing::debug!("Finishing transaction for key '{:?}'", self.key);
        self.commit()
    }

    fn commit(self) -> Result<(), crate::LibError> {
        use crate::LibIoOperation;
        use crate::types::BlobHash;

        let file_to_sync = self.writer.into_inner().map_err(|e| crate::LibError::Io {
            operation: LibIoOperation::CommitFlushWriter,
            path: None,
            source: e.into_error(),
        })?;
        self.cas_inner.fdatasync(file_to_sync)?;

        let blob_hash = BlobHash::from_bytes(*self.hasher.finalize().as_bytes());

        // Register intent - returns a guard that will cleanup on drop if not committed
        let intent_guard = self
            .cas_inner
            .index
            .register_intent(self.key.clone(), IntentMeta { blob_hash, blob_size: self.size })
            .map_err(crate::LibError::Index)?;

        let _cas_path = self
            .cas_inner
            .cas_manager
            .commit_blob(self.temp_file.path(), &blob_hash)
            .map_err(crate::LibError::Cas)?;

        // Commit the intent - this applies the WAL operation and removes the intent
        let to_delete = intent_guard.commit().map_err(crate::LibError::Index)?;

        self.cas_inner.cas_manager.delete_blobs(&to_delete).map_err(crate::LibError::Cas)?;

        Ok(())
    }
}
