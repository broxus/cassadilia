use std::fs::File;
use std::io::Write;
use std::sync::Arc;

use tempfile::tempdir;

use super::*;
use crate::paths::DbPaths;
use crate::serialization::serialize_wal_op_raw;
use crate::tests::utils::encoders::{FailingKeyEncoder, TestKeyEncoder};
use crate::types::{BlobHash, WalOpRaw};

// --- Test Setup ---

fn setup_wal_manager(num_ops_per_wal: u64) -> (WalManager, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());
    let wal_manager = WalManager::new(paths, num_ops_per_wal).unwrap();
    (wal_manager, dir)
}

/// Helper to append a number of simple, unique ops.
fn append_ops(wal_manager: &mut WalManager, count: u64) {
    for i in 0..count {
        let op_raw = WalOpRaw::Put {
            key_bytes: i.to_le_bytes().to_vec(),
            hash: BlobHash::from_bytes([0; 32]),
        };
        let op_data = serialize_wal_op_raw(&op_raw).unwrap();
        wal_manager.append_op(&op_data).unwrap();
    }
}

// --- WalManager Tests ---

#[test]
fn wal_manager_new_fails_on_uncreatable_directory() {
    // This test is platform-specific as creating unwritable paths differs.
    #[cfg(unix)]
    {
        // A path inside /proc is not writable by normal users.
        let invalid_path = std::path::PathBuf::from("/proc/test_wal_invalid");
        let paths = DbPaths::new(invalid_path);
        let result = WalManager::new(paths, 100);

        // We expect an I/O error related to creating the directory.
        assert!(matches!(result, Err(WalError::Io { .. })));
    }
}

#[test]
fn append_op_fails_when_segment_rollover_cannot_create_file() {
    let (mut wal_manager, dir) = setup_wal_manager(2);

    // Append two ops to fill segment 0.
    append_ops(&mut wal_manager, 2);
    assert_eq!(wal_manager.get_next_op_version(), 3);

    // Make the directory read-only to prevent creation of the next segment file.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(dir.path()).unwrap().permissions();
        perms.set_mode(0o555); // Read-only for owner, group, and other
        std::fs::set_permissions(dir.path(), perms).unwrap();

        // This append will trigger a rollover to segment 1, which should fail.
        let result = wal_manager.append_op(b"op_data");

        // The error occurs when trying to open the new segment file for writing.
        assert!(
            matches!(result, Err(WalError::Io { operation: WalIoOperation::OpenSegmentWrite, .. })),
            "Expected an OpenSegmentWrite I/O error, but got {result:?}",
        );

        // Cleanup: Restore permissions so the tempdir can be deleted.
        let mut restore_perms = std::fs::metadata(dir.path()).unwrap().permissions();
        restore_perms.set_mode(0o755);
        std::fs::set_permissions(dir.path(), restore_perms).unwrap();
    }
}

#[test]
fn checkpoint_succeeds_and_prunes_old_segments() {
    let (mut wal_manager, _dir) = setup_wal_manager(2);

    // Create segment 0, 1, and an active writer for segment 2
    append_ops(&mut wal_manager, 5); // v1,2 in seg 0; v3,4 in seg 1; v5 in seg 2

    // The last written op (v5) is in segment 2. The checkpoint should target this segment.
    let checkpoint_segment_id = wal_manager.get_segment_id_for_previous_op();
    assert_eq!(checkpoint_segment_id, 2);

    // Perform the checkpoint, which should succeed and remove segments older than 2.
    let result = wal_manager.perform_checkpoint(false);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);

    // Verify segments 0 and 1 are gone, but segment 2 remains.
    let segments = wal_manager.storage.discover_segments().unwrap();
    let segment_ids: Vec<_> = segments.iter().map(|s| s.id).collect();
    assert_eq!(segment_ids, vec![2]);
}

#[test]
fn wal_manager_drop_is_safe_with_active_writer() {
    // This test simply verifies that drop doesn't panic when there's an active
    // writer that needs to be closed. Error handling within drop is hard to test,
    // but we can ensure it completes without crashing.
    {
        let (mut wal_manager, _dir) = setup_wal_manager(5);
        wal_manager.append_op(b"some data").unwrap();
    } // wal_manager is dropped here
}

#[test]
fn replay_fails_on_key_decode_error() {
    let (mut wal_manager, _dir) = setup_wal_manager(10);
    let op_raw =
        WalOpRaw::Put { key_bytes: vec![1], hash: crate::types::BlobHash::from_bytes([0; 32]) };
    let op_data = serialize_wal_op_raw(&op_raw).unwrap();
    wal_manager.append_op(&op_data).unwrap();

    // Explicitly close the writer to ensure data is flushed to disk before replay.
    wal_manager.active_writer.take().unwrap().close().unwrap();

    // Replay with an encoder that is guaranteed to fail decoding.
    let result = wal_manager.replay_and_prepare(Arc::new(FailingKeyEncoder), |_| {});

    assert!(matches!(result, Err(WalError::ReplayConvertWalOp { .. })));
}

#[test]
fn replay_fails_on_corrupted_op_entry() {
    let (mut wal_manager, _dir) = setup_wal_manager(10);

    // Manually write a corrupted entry to the segment file.
    let segment_path = wal_manager.storage.paths.wal_path_for_segment(0);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .open(&segment_path)
        .unwrap();

    let op_data = b"short data";
    let incorrect_len = (op_data.len() + 10) as u32; // Mismatched length

    // Write a valid version and a dummy hash.
    file.write_all(&1u64.to_le_bytes()).unwrap();
    file.write_all(&[0u8; 32]).unwrap();
    // Write the INCORRECT length.
    file.write_all(&incorrect_len.to_le_bytes()).unwrap();
    // Write the op data, which is shorter than the specified length.
    file.write_all(op_data).unwrap();
    file.sync_all().unwrap();
    drop(file);

    // Replay should fail with an I/O error when trying to read past the end of the file.
    let result = wal_manager.replay_and_prepare(Arc::new(TestKeyEncoder), |_| {});

    assert!(
        matches!(result, Err(WalError::ReplayIo { step: WalReplayIoStep::ReadOpData, .. })),
        "Expected a ReplayIo::ReadOpData error, but got {result:?}",
    );
}

#[test]
fn replay_should_ignore_segments_before_checkpoint() {
    let (mut wal_manager, dir) = setup_wal_manager(2);

    // Append ops to create segments 0 and 1.
    append_ops(&mut wal_manager, 4); // v1,2 in seg 0; v3,4 in seg 1

    // Checkpoint after segment 1 is complete. This should prune segment 0.
    wal_manager.perform_checkpoint(false).unwrap();

    // Append more ops to create segment 2.
    append_ops(&mut wal_manager, 2); // v5,6 in seg 2

    // Simulate a restart
    let paths = DbPaths::new(dir.path().to_path_buf());
    let mut new_wal_manager = WalManager::new(paths, 2).unwrap();
    new_wal_manager.load_checkpoint_metadata().unwrap();

    let mut replayed_ops_count = 0;
    let result = new_wal_manager.replay_and_prepare(Arc::new(TestKeyEncoder), |_| {
        replayed_ops_count += 1;
    });
    assert!(result.is_ok());

    // CORRECT BEHAVIOR: Replay must include the checkpointed segment itself to
    // recover any ops that occurred after the last index persistence.
    // Checkpoint is at segment 1. Segments on disk are 1 and 2.
    // Segment 1 has 2 ops (v3, v4). Segment 2 has 2 ops (v5, v6).
    // Therefore, 4 ops should be replayed.
    assert_eq!(
        replayed_ops_count, 4,
        "Replay must include the checkpointed segment and all subsequent segments"
    );

    // The next version should be 7, following the last replayed op (v6).
    assert_eq!(new_wal_manager.get_next_op_version(), 7);
}
#[test]
fn replay_on_completely_empty_directory() {
    let (mut wal_manager, _dir) = setup_wal_manager(5);

    // Immediately call replay on the empty directory.
    wal_manager.load_checkpoint_metadata().unwrap();

    let mut replayed_ops = Vec::new();
    let result = wal_manager.replay_and_prepare(Arc::new(TestKeyEncoder), |op| {
        replayed_ops.push(op);
    });

    assert!(result.is_ok());
    assert_eq!(replayed_ops.len(), 0);
    assert_eq!(wal_manager.get_next_op_version(), 1);
}

// --- CheckpointPersister Tests ---

#[test]
fn checkpoint_load_fails_on_parse_error() {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());
    std::fs::write(paths.checkpoint_meta_path(), "not-a-number").unwrap();

    let persister = CheckpointPersister::new(&paths);
    let result = persister.load();

    assert!(matches!(result, Err(WalError::ParseCheckpointMetaSegmentId(_))));
}

#[test]
fn checkpoint_load_fails_on_read_io_error() {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());

    // Create the checkpoint file.
    let checkpoint_path = paths.checkpoint_meta_path();
    std::fs::write(checkpoint_path, "123").unwrap();

    // Make it unreadable to trigger a read error.
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut perms = std::fs::metadata(checkpoint_path).unwrap().permissions();
        perms.set_mode(0o000); // No permissions.
        std::fs::set_permissions(checkpoint_path, perms).unwrap();

        let persister = CheckpointPersister::new(&paths);
        let result = persister.load();

        assert!(matches!(
            result,
            Err(WalError::Io { operation: WalIoOperation::ReadCheckpointMeta, .. })
        ));
    }
}

// --- SegmentStorage and Calculation Tests ---

#[test]
fn segment_reader_next_fails_on_checksum_mismatch() {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());
    let segment_path = paths.wal_path_for_segment(0);

    let mut file = std::fs::File::create(&segment_path).unwrap();
    let op_data = b"some data";

    // Write a valid header, but with an intentionally incorrect hash.
    file.write_all(&1u64.to_le_bytes()).unwrap(); // version
    file.write_all(&[0u8; 32]).unwrap(); // incorrect hash
    file.write_all(&(op_data.len() as u32).to_le_bytes()).unwrap(); // op length
    file.write_all(op_data).unwrap(); // op data
    file.sync_all().unwrap();
    drop(file);

    let storage = SegmentStorage::new(paths);
    let mut reader = storage.open_reader(0).unwrap();
    let result = reader.next();

    assert!(matches!(result, Some(Err(WalError::ReplayChecksumMismatch { .. }))));
}

#[test]
fn discover_segments_ignores_malformed_filenames() {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());

    // Create a mix of valid and invalid segment file names.
    File::create(paths.wal_path_for_segment(0)).unwrap(); // 0_index.wal
    File::create(paths.wal_path_for_segment(2)).unwrap(); // 2_index.wal
    File::create(dir.path().join("abc_index.wal")).unwrap(); // Invalid name
    File::create(dir.path().join("1_index.wal.bak")).unwrap(); // Invalid extension
    File::create(dir.path().join("checkpoint.meta")).unwrap(); // Not a segment

    let storage = SegmentStorage::new(paths);
    let segments = storage.discover_segments().unwrap();

    // Should only discover the two validly named segments.
    assert_eq!(segments.len(), 2);
    assert_eq!(segments[0].id, 0);
    assert_eq!(segments[1].id, 2);
}

#[test]
fn segment_id_calculation_is_correct() {
    let dir = tempdir().unwrap();
    let paths = DbPaths::new(dir.path().to_path_buf());
    // Use a small, easy-to-reason-about number of ops per segment.
    let wal = WalManager::new(paths, 10).unwrap();

    // Test boundaries and mid-points for various segments.
    // Segment 0: ops 1-10
    assert_eq!(wal.segment_id_for_op_version(1), 0);
    assert_eq!(wal.segment_id_for_op_version(5), 0);
    assert_eq!(wal.segment_id_for_op_version(10), 0);

    // Segment 1: ops 11-20
    assert_eq!(wal.segment_id_for_op_version(11), 1);
    assert_eq!(wal.segment_id_for_op_version(20), 1);

    // Segment 2: ops 21-30
    assert_eq!(wal.segment_id_for_op_version(21), 2);

    // Edge case: op version 0 should be treated as belonging to segment 0.
    assert_eq!(wal.segment_id_for_op_version(0), 0);
}

#[test]
fn segment_reader_stops_at_explicit_sentinel() {
    let (mut wal_manager, _dir) = setup_wal_manager(10);

    // Append 3 ops to segment 0
    append_ops(&mut wal_manager, 3);

    // Get the writer and SEAL it, which should write the sentinel.
    let writer = wal_manager.active_writer.take().unwrap();
    let segment_id = writer.segment_id();
    assert_eq!(segment_id, 0);
    writer.seal().unwrap();

    // Now, open a reader for the same segment.
    let mut reader = wal_manager.storage.open_reader(segment_id).unwrap();

    // We should be able to read exactly 3 entries.
    assert!(reader.next().is_some()); // Entry 1
    assert!(reader.next().is_some()); // Entry 2
    assert!(reader.next().is_some()); // Entry 3

    // The next call should return None because it hits the sentinel.
    assert!(
        reader.next().is_none(),
        "Reader should stop after the last valid entry by hitting the sentinel"
    );
}
