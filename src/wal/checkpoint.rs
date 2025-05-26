use std::io::ErrorKind;
use std::path::PathBuf;

use crate::io::atomically_write_file_bytes;
use crate::paths::DbPaths;
use crate::types::CheckpointState;
use crate::wal::{WalError, WalIoOperation};

pub(crate) struct CheckpointPersister {
    meta_path: PathBuf,
    tmp_meta_path: PathBuf,
}

impl CheckpointPersister {
    pub(crate) fn new(paths: &DbPaths) -> Self {
        Self {
            meta_path: paths.checkpoint_meta_path(),
            tmp_meta_path: paths.checkpoint_meta_tmp_path(),
        }
    }

    pub(crate) fn load(&self) -> Result<CheckpointState, WalError> {
        match std::fs::read_to_string(&self.meta_path) {
            Ok(data) => {
                let checkpoint_state =
                    data.trim().parse::<u64>().map_err(WalError::ParseCheckpointMetaSegmentId)?;
                tracing::info!(
                    "Loaded last_checkpointed_wal_segment_id: {:?} from '{}'",
                    Some(checkpoint_state),
                    self.meta_path.display()
                );
                Ok(Some(checkpoint_state))
            }
            Err(e) if e.kind() == ErrorKind::NotFound => {
                tracing::info!(
                    "Checkpoint metadata file '{}' not found. checkpoint state remains None.",
                    self.meta_path.display()
                );
                Ok(None)
            }
            Err(e) => Err(WalError::Io {
                operation: WalIoOperation::ReadCheckpointMeta,
                path: Some(self.meta_path.clone()),
                source: e,
            }),
        }
    }

    pub(crate) fn save(&self, segment_id: u64) -> Result<(), WalError> {
        let bytes_to_write = segment_id.to_string().into_bytes();

        atomically_write_file_bytes(&self.meta_path, &self.tmp_meta_path, &bytes_to_write)?;

        tracing::info!(
            "Updated checkpoint metadata: last_checkpointed_wal_segment_id is now {}",
            segment_id
        );
        Ok(())
    }
}
