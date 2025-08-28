#[cfg(test)]
mod tests;

use std::path::PathBuf;

use replay::WalReplayer;
use storage::{SegmentStorage, SegmentWriter};
use thiserror::Error;

use crate::io::IoError;
use crate::paths::DbPaths;
use crate::serialization::SerializationError;
use crate::types::{BlobHash, CheckpointReason, CheckpointState, TypesError, WalOp};
use crate::{KeyBytes, calculate_blob_hash};

mod replay;
mod storage;

#[derive(Debug)]
pub(crate) struct WalAppendInfo {
    pub version: u64,
    #[allow(unused)]
    pub op_hash: BlobHash,
}

#[derive(Error, Debug)]
pub enum WalError {
    #[error("Failed to parse segment ID from checkpoint metadata")]
    ParseCheckpointMetaSegmentId(#[source] std::num::ParseIntError),
    #[error("Failed to atomically write checkpoint metadata")]
    AtomicWriteCheckpointMeta(#[from] IoError),

    #[error("WAL IO error during {operation:?} (path: {path:?})")]
    Io {
        operation: WalIoOperation,
        path: Option<PathBuf>,
        #[source]
        source: std::io::Error,
    },

    #[error("WAL consistency error: attempting to write to an older segment")]
    WriteToOlderSegment { op_version: u64, target_segment: u64, current_segment: u64 },

    #[error("Failed to write WAL entry data (version {op_version}, segment {segment_id})")]
    WriteWalEntryDataIO {
        op_version: u64,
        segment_id: u64,
        #[source]
        source: std::io::Error,
    },

    #[error("WAL replay IO error during {step:?} for segment {segment_id} (path: {path:?})")]
    ReplayIo {
        step: WalReplayIoStep,
        segment_id: u64,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
    #[error(
        "WAL corruption: checksum mismatch for WAL entry (version {version}, segment {segment_id})"
    )]
    ReplayChecksumMismatch { version: u64, segment_id: u64, expected: BlobHash, actual: BlobHash },
    #[error(
        "WAL corruption: failed to deserialize WalOpRaw (version {version}, segment {segment_id})"
    )]
    ReplayDeserializeWalOpRaw {
        version: u64,
        segment_id: u64,
        #[source]
        source: SerializationError,
    },
    #[error(
        "WAL replay: failed to convert WalOpRaw to WalOp<K> (version {version}, segment {segment_id})"
    )]
    ReplayConvertWalOp {
        version: u64,
        segment_id: u64,
        #[source]
        source: TypesError,
    },
}

#[derive(Debug, Clone, Copy)]
pub enum WalIoOperation {
    CreateDbDir,
    ReadCheckpointMeta,
    OpenSegmentWrite,
    WriteSentinel,
    CreateInitialFile,
    SyncInitialFile,
    FlushWriter,
    SyncData,
    ReadDbDirDiscovery,
    ReadEntryDiscovery,
    RemoveStaleSegment,
}

#[derive(Debug, Clone, Copy)]
pub enum WalReplayIoStep {
    OpenSegment,
    ReadHeader,
    ReadOpData,
}

pub(crate) struct WalManager {
    num_ops_per_wal: u64,
    next_op_version: u64,

    storage: SegmentStorage,

    active_writer: Option<SegmentWriter>,
}

impl WalManager {
    pub(crate) fn new(paths: DbPaths, num_ops_per_wal: u64) -> Result<Self, WalError> {
        std::fs::create_dir_all(paths.db_root_path()).map_err(|e| WalError::Io {
            operation: WalIoOperation::CreateDbDir,
            path: None,
            source: e,
        })?;

        let storage = SegmentStorage::new(paths.clone());
        Ok(WalManager { num_ops_per_wal, next_op_version: 1, storage, active_writer: None })
    }

    /// Decide the checkpoint target version for the given reason without performing any IO.
    /// Returns `Some(version)` when a checkpoint should be performed and `None` when it should be
    /// skipped. The returned version is the highest written op version (i.e., `next_op_version -
    /// 1`).
    pub(crate) fn compute_checkpoint_target(
        &self,
        reason: CheckpointReason,
        last_checkpointed_version: CheckpointState,
    ) -> Option<u64> {
        let should_checkpoint = match reason {
            CheckpointReason::InitialSetup => {
                // Only checkpoint on initial setup if no checkpoint exists
                last_checkpointed_version.is_none()
            }
            CheckpointReason::AfterReplay | CheckpointReason::Explicit => true,
            CheckpointReason::SegmentRollover => {
                // Checkpoint on segment rollover if we have new operations since the last
                // checkpoint
                match last_checkpointed_version {
                    Some(last_checkpoint) => self.next_op_version > last_checkpoint + 1,
                    None => {
                        // No checkpoint exists yet - only checkpoint if we've written data
                        self.next_op_version > 1
                    }
                }
            }
        };

        if !should_checkpoint {
            return None;
        }

        // target the last written op
        Some(self.next_op_version.saturating_sub(1))
    }

    /// Prune stale segments for the provided checkpoint version.
    /// Idempotent: if `version` is less than or equal to the last checkpointed version, this is a
    /// no-op.
    pub(crate) fn commit_checkpoint(
        &mut self,
        version: u64,
        last_checkpointed_version: CheckpointState,
    ) -> Result<(), WalError> {
        // Do not go backwards; allow equal (idempotent).
        if let Some(last) = last_checkpointed_version {
            if version <= last {
                tracing::debug!(
                    last_checkpointed_version = last,
                    commit_version = version,
                    "Skipping checkpoint commit (idempotent/no-op).",
                );
                return Ok(());
            }
        }

        // Prune segments where all operations have version <= checkpoint_version
        let last_checkpointed_segment = self.segment_id_for_op_version(version);
        let _ = self.storage.prune_stale_segments(last_checkpointed_segment);

        tracing::info!(checkpoint_version = version, "Checkpoint committed");
        Ok(())
    }

    pub(crate) fn get_next_op_version(&self) -> u64 {
        self.next_op_version
    }

    /// get and increment the next operation version, returning the version to use for the current
    /// operation
    pub(crate) fn allocate_next_op_version(&mut self) -> u64 {
        let current_version = self.next_op_version;
        self.next_op_version += 1;
        current_version
    }

    pub(crate) fn replay_and_prepare<K>(
        &mut self,
        last_checkpointed_version: CheckpointState,
        apply_op_fn: impl FnMut(WalOp<K>),
    ) -> Result<(), WalError>
    where
        K: KeyBytes + Clone + Eq + Ord + std::fmt::Debug + 'static,
    {
        let replayer = WalReplayer::new(&self.storage, last_checkpointed_version);

        let highest_op_version = replayer.replay(apply_op_fn)?;
        self.next_op_version = highest_op_version + 1;

        // ensure next segment file exists using self.storage
        let next_op_version = self.get_next_op_version();
        let target_segment_id = self.segment_id_for_op_version(next_op_version);
        self.storage.ensure_segment_file_exists(target_segment_id, next_op_version)
    }

    pub(crate) fn append_op(&mut self, op_data: &[u8]) -> Result<WalAppendInfo, WalError> {
        let version = self.allocate_next_op_version();
        let target_segment_id = self.segment_id_for_op_version(version);

        // check if we need to roll over to a new segment file.
        let must_rollover =
            self.active_writer.as_ref().is_none_or(|w| w.segment_id() != target_segment_id);
        if must_rollover {
            if let Some(old_writer) = self.active_writer.take() {
                // when rolling over, the old segment is permanently finished. seal it.
                old_writer.seal()?;
            }
            self.active_writer = Some(self.storage.open_writer(target_segment_id)?);
        }

        let writer = self.active_writer.as_mut().unwrap();
        let op_hash = calculate_blob_hash(op_data);
        writer.write_entry(version, op_hash, op_data)?;

        Ok(WalAppendInfo { version, op_hash })
    }

    /// get the segment ID for the operation that would be placed at the previous operation version
    /// this is used for checkpoint calculations
    pub(crate) fn get_segment_id_for_previous_op(&self) -> u64 {
        self.segment_id_for_op_version(self.next_op_version.saturating_sub(1))
    }

    pub(crate) fn segment_id_for_op_version(&self, op_version: u64) -> u64 {
        (op_version.saturating_sub(1)) / self.num_ops_per_wal
    }
}

impl Drop for WalManager {
    fn drop(&mut self) {
        if let Some(writer) = self.active_writer.take() {
            if let Err(e) = writer.close() {
                tracing::error!("Error closing WAL segment during drop: {:?}", e);
            }
        }
    }
}
