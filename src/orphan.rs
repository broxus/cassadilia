use std::fmt::Debug;
use std::hash::Hash;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use ahash::HashSet;

use crate::types::BlobHash;
use crate::{CasInner, LibError, LibIoOperation};

fn verify_blob_integrity(path: &Path, expected_hash: &BlobHash) -> Result<bool, LibError> {
    let mut hasher = blake3::Hasher::new();

    hasher.update_mmap_rayon(path).map_err(|e| LibError::Io {
        operation: LibIoOperation::ReadContent,
        path: Some(path.to_path_buf()),
        source: e,
    })?;

    let actual_hash = BlobHash(hasher.finalize().into());
    Ok(actual_hash == *expected_hash)
}

pub struct OrphanStats<K> {
    pub(crate) cas_inner: Arc<crate::CasInner<K>>,
    pub(crate) _fs_lock: parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()>,
    /// Blobs that are not referenced by any keys
    pub orphaned_blobs: Vec<BlobHash>,
    /// Files which name is not a valid blob hash
    pub invalid_files: Vec<PathBuf>,
    pub missing_blobs: Vec<BlobHash>,
    /// Blobs which hash does not match the file content
    pub corrupted_blobs: Vec<BlobHash>,
    /// Staging files that were not cleaned up
    pub staging_files: Vec<PathBuf>,
    pub total_blobs: usize,
    pub scan_duration: Duration,
}

#[derive(Debug, Default)]
pub struct RecoveryResult {
    pub orphans_deleted: usize,
    pub orphans_quarantined: usize,
    pub orphans_skipped: usize,
    pub invalid_files_removed: usize,
    pub staging_files_removed: usize,
    pub errors: Vec<String>,
}

impl<K> OrphanStats<K>
where
    K: Clone + Eq + Ord + Hash + Debug + Send + Sync + 'static,
{
    /// Delete orphaned blobs
    pub fn delete_orphans(&self) -> Result<RecoveryResult, LibError> {
        let mut result = RecoveryResult::default();

        // We already hold the fs_lock from scan time

        for hash in &self.orphaned_blobs {
            let blob_path = self.cas_inner.paths.cas_file_path(hash);
            match std::fs::remove_file(&blob_path) {
                Ok(_) => {
                    result.orphans_deleted += 1;
                    tracing::info!(hash = %hash, "Deleted orphaned blob");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already removed by another process
                    result.orphans_skipped += 1;
                    tracing::warn!(hash = %hash, "Orphaned blob already removed by another process");
                }
                Err(e) => {
                    result.errors.push(format!("Failed to delete {hash}: {e}"));
                }
            }
        }

        // Remove invalid files
        for path in &self.invalid_files {
            if path.exists() {
                match std::fs::remove_file(path) {
                    Ok(_) => result.invalid_files_removed += 1,
                    Err(e) => result.errors.push(format!("Failed to remove {path:?}: {e}")),
                }
            }
        }

        // Remove staging files
        for path in &self.staging_files {
            if path.exists() {
                match std::fs::remove_file(path) {
                    Ok(_) => result.staging_files_removed += 1,
                    Err(e) => result.errors.push(format!("Failed to remove staging {path:?}: {e}")),
                }
            }
        }

        Ok(result)
    }

    /// Quarantine orphaned blobs to a specified directory
    pub fn quarantine_orphans(
        &self,
        quarantine_dir: &std::path::Path,
    ) -> Result<RecoveryResult, crate::LibError> {
        let mut result = RecoveryResult::default();

        // Create quarantine directory
        std::fs::create_dir_all(quarantine_dir).map_err(|e| crate::LibError::Io {
            operation: crate::LibIoOperation::CreateCasDir,
            path: Some(quarantine_dir.to_path_buf()),
            source: e,
        })?;

        // We already hold the fs_lock from scan time

        for hash in &self.orphaned_blobs {
            let src_path = self.cas_inner.paths.cas_file_path(hash);
            let dst_path = quarantine_dir.join(hash.to_string());

            match std::fs::rename(&src_path, &dst_path) {
                Ok(_) => {
                    result.orphans_quarantined += 1;
                    tracing::info!(hash = %hash, dest = ?dst_path, "Quarantined orphaned blob");
                }
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    // Already removed
                    result.orphans_skipped += 1;
                }
                Err(e) => {
                    result.errors.push(format!("Failed to quarantine {hash}: {e}"));
                }
            }
        }

        Ok(result)
    }

    /// Delete a specific orphaned blob
    pub fn delete_orphan(&self, hash: &BlobHash) -> Result<bool, crate::LibError> {
        if !self.orphaned_blobs.contains(hash) {
            return Ok(false); // Not in orphan list
        }

        // We already hold the fs_lock from scan time
        let blob_path = self.cas_inner.paths.cas_file_path(hash);
        match std::fs::remove_file(&blob_path) {
            Ok(_) => Ok(true),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(e) => Err(crate::LibError::Io {
                operation: crate::LibIoOperation::RemoveFile,
                path: Some(blob_path),
                source: e,
            }),
        }
    }
}

pub(crate) fn scan_orphans<K>(
    cas_inner: &CasInner<K>,
    cas_inner_arc: Arc<CasInner<K>>,
    verify_integrity: bool,
) -> Result<OrphanStats<K>, LibError>
where
    K: Clone + Eq + Ord + Hash + Debug + Send + Sync + 'static,
{
    let start_time = std::time::Instant::now();
    let mut orphaned_blobs = Vec::new();
    let mut invalid_files = Vec::new();
    let mut missing_blobs = Vec::new();
    let mut corrupted_blobs = Vec::new();

    // Take the arc lock for the entire operation
    let fs_lock = cas_inner.cas_manager.lock_arc();

    // Get current index state
    let index_blobs =
        cas_inner.index.read_state().known_blobs().map(|(key, _)| *key).collect::<HashSet<_>>();
    let mut seen_blobs = HashSet::default();

    // Single pass through CAS directory
    let cas_root = cas_inner.paths.cas_root_path();

    for l1_entry in std::fs::read_dir(cas_root).map_err(|e| LibError::Io {
        operation: LibIoOperation::ReadDir,
        path: Some(cas_root.to_path_buf()),
        source: e,
    })? {
        let l1_entry = l1_entry.map_err(|e| LibError::Io {
            operation: LibIoOperation::ReadDir,
            path: Some(cas_root.to_path_buf()),
            source: e,
        })?;
        let l1_path = l1_entry.path();

        if !l1_path.is_dir() {
            invalid_files.push(l1_path);
            continue;
        }

        for l2_entry in std::fs::read_dir(&l1_path).map_err(|e| LibError::Io {
            operation: LibIoOperation::ReadDir,
            path: Some(l1_path.clone()),
            source: e,
        })? {
            let l2_entry = l2_entry.map_err(|e| LibError::Io {
                operation: LibIoOperation::ReadDir,
                path: Some(l1_path.clone()),
                source: e,
            })?;
            let l2_path = l2_entry.path();

            if !l2_path.is_dir() {
                invalid_files.push(l2_path);
                continue;
            }

            // Process blob files
            for blob_entry in std::fs::read_dir(&l2_path).map_err(|e| LibError::Io {
                operation: LibIoOperation::ReadDir,
                path: Some(l2_path.clone()),
                source: e,
            })? {
                let blob_entry = blob_entry.map_err(|e| LibError::Io {
                    operation: LibIoOperation::ReadDir,
                    path: Some(l2_path.clone()),
                    source: e,
                })?;
                let blob_path = blob_entry.path();

                // Reconstruct hash from relative path
                let relative_path = blob_path.strip_prefix(cas_root).ok();
                match relative_path.and_then(|p| BlobHash::from_relative_path(p).ok()) {
                    Some(hash) => {
                        seen_blobs.insert(hash);

                        if !index_blobs.contains(&hash) {
                            // Orphaned blob
                            orphaned_blobs.push(hash);
                        } else if verify_integrity {
                            // Optional integrity check
                            match verify_blob_integrity(&blob_path, &hash) {
                                Ok(true) => {} // Valid
                                Ok(false) => {
                                    tracing::error!(hash = %hash, "Corrupted blob detected");
                                    corrupted_blobs.push(hash);
                                }
                                Err(e) => {
                                    tracing::error!(hash = %hash, error = %e, "Failed to verify blob");
                                }
                            }
                        }
                    }
                    None => {
                        // Invalid filename or path structure
                        invalid_files.push(blob_path);
                    }
                }
            }
        }
    }

    // Find missing blobs (in index but not in filesystem)
    for hash in index_blobs {
        if !seen_blobs.contains(&hash) {
            tracing::error!(hash = %hash, "Missing blob in filesystem");
            missing_blobs.push(hash);
        }
    }

    // Scan staging directory for orphaned files
    let staging_files = scan_staging_files(cas_inner)?;

    Ok(OrphanStats {
        cas_inner: cas_inner_arc,
        _fs_lock: fs_lock,
        orphaned_blobs,
        invalid_files,
        missing_blobs,
        corrupted_blobs,
        staging_files,
        total_blobs: seen_blobs.len(),
        scan_duration: start_time.elapsed(),
    })
}

fn scan_staging_files<K>(cas_inner: &CasInner<K>) -> Result<Vec<PathBuf>, LibError>
where
    K: Clone + Eq + Ord + Hash + Debug + Send + Sync + 'static,
{
    let mut staging_files = Vec::new();
    let staging_root = cas_inner.paths.staging_root_path();

    for entry in std::fs::read_dir(staging_root).map_err(|e| LibError::Io {
        operation: LibIoOperation::ReadDir,
        path: Some(staging_root.to_path_buf()),
        source: e,
    })? {
        let entry = entry.map_err(|e| LibError::Io {
            operation: LibIoOperation::ReadDir,
            path: Some(staging_root.to_path_buf()),
            source: e,
        })?;

        if let Ok(metadata) = entry.metadata() {
            if metadata.is_file() {
                staging_files.push(entry.path());
            }
        }
    }

    Ok(staging_files)
}
