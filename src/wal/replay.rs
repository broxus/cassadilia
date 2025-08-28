use super::storage::SegmentStorage;
use crate::serialization::deserialize_wal_op_raw;
use crate::types::{CheckpointState, KeyBytes, WalOp};
use crate::wal::WalError;

pub(crate) struct WalReplayer<'a> {
    storage: &'a SegmentStorage,
    last_checkpointed_op_version: CheckpointState,
}

impl<'a> WalReplayer<'a> {
    pub(crate) fn new(
        storage: &'a SegmentStorage,
        last_checkpointed_op_version: CheckpointState,
    ) -> Self {
        Self { storage, last_checkpointed_op_version }
    }

    // replays segments and calls the provided function to update application state.
    // returns the highest operation version found in the WAL.
    pub(crate) fn replay<K>(&self, mut apply_op_fn: impl FnMut(WalOp<K>)) -> Result<u64, WalError>
    where
        K: KeyBytes + Clone + Eq + Ord + std::fmt::Debug + 'static,
    {
        let checkpoint = self.last_checkpointed_op_version.unwrap_or(0);
        let mut highest = checkpoint;

        let segments = self.storage.discover_segments()?;
        tracing::info!(segments = segments.len(), checkpoint, "Starting WAL replay");

        if segments.is_empty() {
            return Ok(highest);
        }

        let mut total = 0u64;

        for seg in &segments {
            tracing::debug!(segment = seg.id, path = %seg.path.display(), "Replaying segment");
            let reader = self.storage.open_reader(seg.id)?;

            let mut seg_count = 0u64;

            for entry in reader {
                let entry = entry?;
                highest = highest.max(entry.version);

                // Skip already-checkpointed ops
                if entry.version <= checkpoint {
                    continue;
                }

                let raw = deserialize_wal_op_raw(&entry.op_data).map_err(|e| {
                    WalError::ReplayDeserializeWalOpRaw {
                        version: entry.version,
                        segment_id: seg.id,
                        source: e,
                    }
                })?;

                let op = WalOp::from_raw(raw).map_err(|e| WalError::ReplayConvertWalOp {
                    version: entry.version,
                    segment_id: seg.id,
                    source: e,
                })?;

                apply_op_fn(op);
                seg_count += 1;
            }

            total += seg_count;
            tracing::info!(segment = seg.id, entries = seg_count, "Segment replay complete");
        }

        tracing::info!(total_entries = total, highest_op_version = highest, "WAL replay complete");
        Ok(highest)
    }
}
