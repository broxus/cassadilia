use std::fs::File;
use std::io::BufReader;
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::paths;
use crate::types::{BlobHash, FsLock};

#[derive(Debug, Clone)]
pub enum CasIoOperation {
    ReadContent,
    ReadMetadata,
    OpenBuffered,
    OpenRangeRead,
    ReadRange,
    CreateSubdir,
    MoveStaged,
    RemoveStaged,
    RemoveFile,
}

#[derive(Error, Debug)]
pub enum CasManagerError {
    #[error("CAS IO error during {operation:?} for path {path:?}")]
    FileOperation {
        operation: CasIoOperation,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },

    #[error("Invalid range: start ({start}) > end ({end})")]
    InvalidRangeStartEnd { start: u64, end: u64 },
    #[error("End is after file size: start ({start}) > end ({end})")]
    InvalidCalculatedRangeStartEnd { start: u64, end: u64 },
}

pub struct CasManager {
    paths: paths::DbPaths,
    fs_lock: FsLock,
    dir_tree_is_pre_created: bool,
}

impl CasManager {
    pub fn new(paths: paths::DbPaths, fs_lock: FsLock, dir_tree_is_pre_created: bool) -> Self {
        Self { paths, fs_lock, dir_tree_is_pre_created }
    }

    pub fn read_blob(&self, blob_hash: &BlobHash) -> Result<bytes::Bytes, CasManagerError> {
        let cas_path = self.paths.cas_file_path(blob_hash);
        let bytes = std::fs::read(&cas_path).map_err(|e| CasManagerError::FileOperation {
            operation: CasIoOperation::ReadContent,
            path: cas_path.clone(),
            source: e,
        })?;
        Ok(bytes::Bytes::from(bytes))
    }

    pub fn blob_size(&self, blob_hash: &BlobHash) -> Result<u64, CasManagerError> {
        let cas_path = self.paths.cas_file_path(blob_hash);
        let metadata =
            std::fs::metadata(&cas_path).map_err(|e| CasManagerError::FileOperation {
                operation: CasIoOperation::ReadMetadata,
                path: cas_path.clone(),
                source: e,
            })?;
        Ok(metadata.len())
    }

    pub fn blob_bufreader(&self, blob_hash: &BlobHash) -> Result<BufReader<File>, CasManagerError> {
        let cas_path = self.paths.cas_file_path(blob_hash);
        let file = File::open(&cas_path).map_err(|e| CasManagerError::FileOperation {
            operation: CasIoOperation::OpenBuffered,
            path: cas_path.clone(),
            source: e,
        })?;
        Ok(BufReader::new(file))
    }

    pub fn read_blob_range(
        &self,
        blob_hash: &BlobHash,
        range_start: u64,
        range_end: u64,
    ) -> Result<bytes::Bytes, CasManagerError> {
        if range_start > range_end {
            return Err(CasManagerError::InvalidRangeStartEnd {
                start: range_start,
                end: range_end,
            });
        }

        let cas_path = self.paths.cas_file_path(blob_hash);
        let file = File::open(&cas_path).map_err(|e| CasManagerError::FileOperation {
            operation: CasIoOperation::OpenRangeRead,
            path: cas_path.clone(),
            source: e,
        })?;

        let len = file
            .metadata()
            .map_err(|e| CasManagerError::FileOperation {
                operation: CasIoOperation::ReadMetadata,
                path: cas_path.clone(),
                source: e,
            })?
            .len();

        let start = std::cmp::min(range_start, len);
        let end = std::cmp::min(range_end, len);

        if start > end {
            return Err(CasManagerError::InvalidCalculatedRangeStartEnd { start, end });
        }

        let read_len = end - start;
        if read_len == 0 {
            return Ok(bytes::Bytes::new());
        }

        // Create a Vec with capacity but zero length
        let mut buff = Vec::with_capacity(read_len as usize);

        let mut total_bytes_read = 0;
        let mut current_offset = start;

        // Read in a loop until we've read all requested bytes or hit EOF
        while total_bytes_read < read_len as usize {
            // Get the spare capacity (uninitialized portion) of the vector
            let spare = buff.spare_capacity_mut();
            let remaining = std::cmp::min(spare.len(), read_len as usize - total_bytes_read);

            // SAFETY: We're reading into uninitialized memory, which is safe as long as we
            // only update the length by the amount actually read
            let bytes_read = unsafe {
                let ptr = spare.as_mut_ptr().cast::<u8>();
                let slice = std::slice::from_raw_parts_mut(ptr, remaining);

                file.read_at(slice, current_offset).map_err(|e| CasManagerError::FileOperation {
                    operation: CasIoOperation::ReadRange,
                    path: cas_path.clone(),
                    source: e,
                })?
            };

            // EOF reached
            if bytes_read == 0 {
                break;
            }

            // SAFETY: We've initialized exactly bytes_read bytes in the spare capacity
            unsafe {
                let new_len = buff.len() + bytes_read;
                buff.set_len(new_len);
            }

            total_bytes_read += bytes_read;
            current_offset += bytes_read as u64;
        }

        Ok(bytes::Bytes::from(buff))
    }

    pub fn commit_blob(
        &self,
        staging_path: &Path,
        blob_hash: &BlobHash,
    ) -> Result<PathBuf, CasManagerError> {
        let final_cas_path = self.paths.cas_file_path(blob_hash);

        let _lock = self.fs_lock.lock();

        // Create parent directory inside the lock to prevent races
        // Skip if directory tree was pre-created
        if !self.dir_tree_is_pre_created {
            if let Some(parent) = final_cas_path.parent() {
                std::fs::create_dir_all(parent).map_err(|e| CasManagerError::FileOperation {
                    operation: CasIoOperation::CreateSubdir,
                    path: final_cas_path.clone(),
                    source: e,
                })?;
            }
        }

        if !final_cas_path.exists() {
            std::fs::rename(staging_path, &final_cas_path).map_err(|e| {
                CasManagerError::FileOperation {
                    operation: CasIoOperation::MoveStaged,
                    path: final_cas_path.clone(),
                    source: e,
                }
            })?;
            tracing::debug!("Moved blob {} to CAS path '{}'", blob_hash, final_cas_path.display());
        } else {
            tracing::debug!(
                "Blob {} already exists at CAS path '{}', removing staged file",
                blob_hash,
                final_cas_path.display()
            );
            std::fs::remove_file(staging_path).map_err(|e| CasManagerError::FileOperation {
                operation: CasIoOperation::RemoveStaged,
                path: staging_path.to_path_buf(),
                source: e,
            })?;
        }

        Ok(final_cas_path)
    }

    /// Delete blobs from CAS that are no longer referenced
    pub fn delete_blobs(&self, hashes: &[BlobHash]) -> Result<Vec<BlobHash>, CasManagerError> {
        let mut actually_deleted_hashes = Vec::new();
        let _fs_lock = self.fs_lock.lock();

        for hash in hashes {
            let file_path = self.paths.cas_file_path(hash);
            match std::fs::remove_file(&file_path) {
                Ok(_) => {
                    tracing::debug!(
                        "Successfully deleted unreferenced CAS file: {}",
                        file_path.display()
                    );
                    actually_deleted_hashes.push(*hash);
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::warn!(
                        "CAS file '{}' for unreferenced hash {} not found during deletion, skipping.",
                        file_path.display(),
                        hash
                    );
                }
                Err(e) => {
                    return Err(CasManagerError::FileOperation {
                        operation: CasIoOperation::RemoveFile,
                        path: file_path,
                        source: e,
                    });
                }
            }
        }

        Ok(actually_deleted_hashes)
    }
}
