use std::fmt::Debug;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::ops::Deref;
use std::path::PathBuf;
use std::sync::Arc;

use tempfile::NamedTempFile;
use thiserror::Error;

use crate::index::IntentMeta;
use crate::{CasInner, KeyBytes};

#[must_use = "Transaction must be completed by calling finish()"]
pub struct Transaction<'a, K> {
    pub(crate) temp_file: NamedTempFile,
    cas_inner: CasHandle<'a, K>,
    pub(crate) writer: BufWriter<File>,
    pub(crate) hasher: blake3::Hasher,
    pub(crate) size: u64,
    pub(crate) key: K,
}

impl<'a, K> Transaction<'a, K> {
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
}

impl<'a, K> Transaction<'a, K>
where
    K: KeyBytes + Clone + Eq + Ord + std::hash::Hash + Debug + Send + Sync + 'static,
{
    pub fn finish(self) -> Result<(), crate::LibError> {
        tracing::debug!("Finishing transaction for key '{:?}'", self.key);
        self.commit()
    }

    pub(crate) fn new(cas_inner: &'a CasInner<K>, key: K) -> Result<Self, TransactionError> {
        Self::with_handle(CasHandle::Borrowed(cas_inner), key)
    }

    pub(crate) fn new_owned(
        cas_inner: Arc<CasInner<K>>,
        key: K,
    ) -> Result<OwnedTransaction<K>, TransactionError> {
        Transaction::with_handle(CasHandle::Owned(cas_inner), key)
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

        tracing::debug!(%blob_hash, key = ?self.key, "Committing transaction");
        let _cas_path = self
            .cas_inner
            .cas_manager
            .commit_blob(self.temp_file.path(), &blob_hash)
            .map_err(crate::LibError::Cas)?;

        let delete_fn = |hashes: &[BlobHash]| -> Result<(), crate::cas_manager::CasManagerError> {
            self.cas_inner.cas_manager.delete_blobs(hashes).map(|_| ())
        };

        // Commit the intent - this applies the WAL operation and deletes unreferenced blobs
        intent_guard.commit(&delete_fn).map_err(crate::LibError::Index)?;

        Ok(())
    }

    fn with_handle(cas_inner: CasHandle<'a, K>, key: K) -> Result<Self, TransactionError> {
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

/// A transaction that keeps its CAS alive independently of the handle that created it.
///
/// It can be retained between requests or moved to another thread. Calling `finish()`
/// publishes the blob; dropping it unfinished removes its temporary file, just like
/// a borrowed [`Transaction`]. This does not provide recovery after process termination.
pub type OwnedTransaction<K> = Transaction<'static, K>;

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

impl From<TransactionError> for crate::LibError {
    fn from(error: TransactionError) -> Self {
        let TransactionError::StagingFileIo { operation, path, source } = error;
        Self::Io {
            operation: match operation {
                StagingFileOp::Create => crate::LibIoOperation::CreateStagingFile,
                StagingFileOp::Write => crate::LibIoOperation::WriteStagingFile,
            },
            path: Some(path),
            source,
        }
    }
}

enum CasHandle<'a, K> {
    Borrowed(&'a CasInner<K>),
    Owned(Arc<CasInner<K>>),
}

impl<K> Deref for CasHandle<'_, K> {
    type Target = CasInner<K>;

    fn deref(&self) -> &Self::Target {
        match self {
            Self::Borrowed(inner) => inner,
            Self::Owned(inner) => inner,
        }
    }
}
