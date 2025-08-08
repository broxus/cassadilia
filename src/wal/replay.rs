use super::storage::SegmentStorage;
use crate::serialization::deserialize_wal_op_raw;
use crate::types::{CheckpointState, KeyBytes, WalOp};
use crate::wal::WalError;

pub(crate) struct WalReplayer<'a> {
    storage: &'a SegmentStorage,
    checkpoint_state: CheckpointState,
    num_ops_per_wal: u64,
}

impl<'a> WalReplayer<'a> {
    pub(crate) fn new(
        storage: &'a SegmentStorage,
        checkpoint_state: CheckpointState,
        num_ops_per_wal: u64,
    ) -> Self {
        Self { storage, checkpoint_state, num_ops_per_wal }
    }

    // replays segments and calls the provided function to update application state.
    // returns the highest operation version found in the WAL.
    // in wal/replay.rs

    pub(crate) fn replay<K>(&self, mut apply_op_fn: impl FnMut(WalOp<K>)) -> Result<u64, WalError>
    where
        K: KeyBytes + Clone + Eq + Ord + std::fmt::Debug + 'static,
    {
        // If a checkpoint exists for segment N, the highest version from segments 0..N-1
        // is N * num_ops_per_wal. This is our baseline.
        let mut highest_op_version = match self.checkpoint_state {
            None => 0,
            Some(checkpoint_id) => checkpoint_id * self.num_ops_per_wal,
        };

        tracing::debug!(
            "Initial highest_op_version (from checkpoint {:?}): {}",
            self.checkpoint_state,
            highest_op_version
        );

        let segments_to_replay: Vec<_> = self
            .storage
            .discover_segments()?
            .into_iter()
            // BUG FIX: Replay must include the checkpointed segment itself, as it may be
            // partially written. The filter must be >=, not >.
            .filter(|segment| match self.checkpoint_state {
                None => true,
                Some(checkpoint_id) => segment.id >= checkpoint_id,
            })
            .collect();

        if segments_to_replay.is_empty() {
            tracing::info!(
                "No WAL segments found to replay (Checkpoint: {:?}). `highest_op_version` remains {}.",
                self.checkpoint_state,
                highest_op_version
            );
        } else {
            tracing::info!(
                "Found {} WAL segments to replay (Checkpoint: {:?}, filter applied). Segments: {:?}",
                segments_to_replay.len(),
                self.checkpoint_state,
                segments_to_replay.iter().map(|segment| segment.id).collect::<Vec<u64>>()
            );

            let mut total_replayed_entries = 0;

            for segment in &segments_to_replay {
                tracing::info!(
                    "Replaying WAL segment {} from '{}'",
                    segment.id,
                    segment.path.display()
                );

                let segment_reader = match self.storage.open_reader(segment.id) {
                    Ok(reader) => reader,
                    Err(e) => {
                        tracing::warn!(
                            "WAL segment {} at '{}' disappeared during replay. Skipping. Error: {:?}",
                            segment.id,
                            segment.path.display(),
                            e
                        );
                        continue;
                    }
                };

                let mut entry_count_in_segment = 0;
                for entry_result in segment_reader {
                    let entry = entry_result?;

                    let op = deserialize_wal_op_raw(&entry.op_data).map_err(|e| {
                        WalError::ReplayDeserializeWalOpRaw {
                            version: entry.version,
                            segment_id: segment.id,
                            source: e,
                        }
                    })?;

                    let op = WalOp::from_raw(op).map_err(|e| WalError::ReplayConvertWalOp {
                        version: entry.version,
                        segment_id: segment.id,
                        source: e,
                    })?;

                    apply_op_fn(op);
                    entry_count_in_segment += 1;

                    highest_op_version = highest_op_version.max(entry.version);

                    tracing::trace!(
                        version = entry.version,
                        segment = segment.id,
                        "Replayed WAL entry"
                    );
                }

                total_replayed_entries += entry_count_in_segment;
                tracing::info!(
                    "Completed replaying WAL segment {} from '{}'. Entries replayed: {}",
                    segment.id,
                    segment.path.display(),
                    entry_count_in_segment
                );
            }

            tracing::info!(
                "WAL replay completed. Total replayed entries: {}. highest_op_version: {}.",
                total_replayed_entries,
                highest_op_version
            );
        }

        Ok(highest_op_version)
    }
}
