use anyhow::Result;

use crate::tests::utils::*;
use crate::{Config, calculate_blob_hash};

#[test]
fn test_replay_creates_checkpoint_after_restart() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config { num_ops_per_wal: 10, ..Default::default() })?;

    // Write 5 ops, no explicit checkpoint.
    harness.run_session(|cas| {
        populate_cas(cas, 0..5, "data")?;
        assert_key_count(cas, 5);
        // Verify no checkpoint exists yet
        assert!(cas.0.index.state.read().last_persisted_version.is_none());
        Ok(())
    })?;

    // Restart. Should replay the 5 ops and create a checkpoint.
    harness.run_session(|cas| {
        assert_key_count(cas, 5);
        verify_cas_data(cas, 0..5, "data")?;
        verify_refcount_integrity(cas, 5)?; // Verifies no double-replay

        // Checkpoint should now exist at version 5
        assert_eq!(cas.0.index.state.read().last_persisted_version, Some(5));

        // Write 3 more ops
        populate_cas(cas, 5..8, "data")?;
        assert_key_count(cas, 8);

        // Expect 1 WAL segment (seg 0)
        assert_eq!(count_wal_segments(harness.db_path())?, 1);
        Ok(())
    })?;

    // Restart again. Should load from checkpoint + replay from op 5.
    harness.run_session(|cas| {
        assert_key_count(cas, 8);
        verify_cas_data(cas, 0..8, "data")?;
        verify_refcount_integrity(cas, 8)?;

        // Checkpoint = 8 from prior session
        assert_eq!(cas.0.index.state.read().last_persisted_version, Some(8));
        Ok(())
    })?;

    Ok(())
}

#[test]
fn test_checkpoint_persists_overwrites_correctly() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config { num_ops_per_wal: 10, ..Default::default() })?;

    // Write 15 items, creating two WAL segments.
    harness.run_session(|cas| {
        populate_cas(cas, 0..15, "initial_data")?;
        assert_key_count(cas, 15);
        Ok(())
    })?;

    // Restart, replay, overwrite some data, add new data, then checkpoint.
    harness.run_session(|cas| {
        assert_key_count(cas, 15); // Verify replay
        populate_cas(cas, 10..20, "updated_data")?; // Overwrites 10-14, adds 15-19
        assert_key_count(cas, 20);
        cas.checkpoint()?;
        Ok(())
    })?;

    // Restart again, verify all data is correct.
    harness.run_session(|cas| {
        assert_key_count(cas, 20);
        verify_cas_data(cas, 0..10, "initial_data")?;
        verify_cas_data(cas, 10..20, "updated_data")?;
        Ok(())
    })?;

    // One more restart without any writes
    harness.run_session(|cas| {
        assert_key_count(cas, 20);
        verify_cas_data(cas, 0..10, "initial_data")?;
        verify_cas_data(cas, 10..20, "updated_data")?;
        Ok(())
    })?;

    Ok(())
}

#[test]
fn test_checkpoint_prevents_double_replay() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config {
        num_ops_per_wal: 100, // Keep everything in one segment
        ..Default::default()
    })?;

    // Write 3 items and checkpoint.
    harness.run_session(|cas| {
        populate_cas(cas, 0..3, "unique_data")?;
        assert_all_ref_counts_are(cas, 1)?;
        cas.checkpoint()?;
        Ok(())
    })?;

    // Restart. The WAL should NOT be replayed, so ref counts must remain 1.
    harness.run_session(|cas| {
        assert_key_count(cas, 3);
        assert_all_ref_counts_are(cas, 1)?;
        verify_cas_data(cas, 0..3, "unique_data")?;
        Ok(())
    })?;

    Ok(())
}

#[test]
fn test_overwrite_deletes_old_blob_no_orphans() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config {
        num_ops_per_wal: 100,
        scan_orphans_on_startup: true,
        ..Default::default()
    })?;

    let data_a = b"original_data";
    let data_b = b"updated_data";
    let hash_a = calculate_blob_hash(data_a);
    let hash_b = calculate_blob_hash(data_b);

    // Session 1: Write, overwrite, and checkpoint.
    harness.run_session(|cas| {
        let mut tx = cas.put("key1".to_string())?;
        tx.write(data_a)?;
        tx.finish()?;

        let mut tx = cas.put("key1".to_string())?;
        tx.write(data_b)?;
        tx.finish()?;

        // Verify state before shutdown: hash_a is gone, hash_b is active.
        let state = cas.0.index.state.read();
        assert!(
            !state.hash_to_ref_count.contains_key(&hash_a),
            "hash_a still here"
        );
        assert_eq!(state.hash_to_ref_count.get(&hash_b), Some(&1), "hash_b refcnt != 1");
        drop(state);

        cas.checkpoint()?;
        Ok(())
    })?;

    // Session 2: Restart with orphan scan.
    harness.run_session_with_recover(|cas, orphan_stats| {
        let orphans = orphan_stats.expect("no orphan scan?");
        assert_eq!(orphans.orphaned_blobs.len(), 0, "found orphans");

        // Verify final state is still correct.
        let state = cas.0.index.state.read();
        assert_eq!(state.hash_to_ref_count.get(&hash_b), Some(&1), "hash_b refcnt != 1");
        Ok(())
    })?;

    Ok(())
}

#[test]
fn test_wal_segment_rollover_triggers_checkpoint() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config {
        num_ops_per_wal: 5, // Small segments to test rollover
        ..Default::default()
    })?;

    // Write exactly num_ops_per_wal to fill segment 0
    harness.run_session(|cas| {
        populate_cas(cas, 0..5, "segment0")?;
        assert_key_count(cas, 5);
        assert_eq!(count_wal_segments(harness.db_path())?, 1, "need seg 0");
        Ok(())
    })?;

    //  Write one more to trigger rollover to segment 1
    harness.run_session(|cas| {
        assert_key_count(cas, 5);

        // This write should trigger segment rollover
        populate_cas(cas, 5..6, "segment1")?;
        assert_key_count(cas, 6);

        // After rollover, segment 0 is pruned, only segment 1 remains
        assert_eq!(count_wal_segments(harness.db_path())?, 1, "seg0 not pruned");
        assert_eq!(cas.0.index.state.read().last_persisted_version, Some(6), "checkpoint != 6");
        Ok(())
    })?;

    // Session 3: Verify checkpoint persisted correctly
    harness.run_session(|cas| {
        assert_key_count(cas, 6);
        verify_cas_data(cas, 0..5, "segment0")?;
        verify_cas_data(cas, 5..6, "segment1")?;

        // Verify checkpoint is still at version 6
        assert_eq!(cas.0.index.state.read().last_persisted_version, Some(6), "checkpoint lost");
        Ok(())
    })?;

    Ok(())
}

#[test]
fn test_empty_database_checkpoint_on_first_write() -> Result<()> {
    setup_tracing();
    let harness = CasTestHarness::new(Config { num_ops_per_wal: 10, ..Default::default() })?;

    // Empty DB: no checkpoint
    harness.run_session(|cas| {
        assert_key_count(cas, 0);
        assert!(
            cas.0.index.state.read().last_persisted_version.is_none(),
            "has checkpoint?"
        );
        Ok(())
    })?;

    // Still empty
    harness.run_session(|cas| {
        assert_key_count(cas, 0);
        assert!(
            cas.0.index.state.read().last_persisted_version.is_none(),
            "still has checkpoint?"
        );
        Ok(())
    })?;

    // Write something
    harness.run_session(|cas| {
        populate_cas(cas, 0..1, "first")?;
        assert_key_count(cas, 1);
        Ok(())
    })?;

    // Now expect a checkpoint
    harness.run_session(|cas| {
        assert_key_count(cas, 1);
        assert_eq!(cas.0.index.state.read().last_persisted_version, Some(1), "no checkpoint at 1");
        verify_cas_data(cas, 0..1, "first")?;
        Ok(())
    })?;

    Ok(())
}
