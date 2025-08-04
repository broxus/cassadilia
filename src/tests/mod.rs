pub(crate) mod utils;

use std::fs;
use std::os::unix::fs::PermissionsExt;

use anyhow::Result;
use tempfile::tempdir;

use crate::index::CHECKPOINT_META_FILENAME;
use crate::tests::utils::encoders::{StringEncoder, VecU8Encoder};
use crate::{Cas, Config, LibError, LibIoOperation, SyncMode};

fn setup_tracing() {
    let _ = tracing_subscriber::fmt::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
        .try_init();
}

#[test]
fn test_put_get_remove_string_key() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    let key = "my_first_blob".to_string();
    let data = b"Hello, Bubs!";

    let mut tx = cas.put(key.clone())?;
    tx.write(data)?;
    tx.finish()?;

    let retrieved = cas.get(&key)?.expect("blob should exist");
    assert_eq!(retrieved.as_ref(), data);

    assert!(cas.get(&"does_not_exist".to_string())?.is_none());

    assert!(cas.remove(&key)?);
    assert!(cas.get(&key)?.is_none());
    assert!(!cas.remove(&key)?); // false when removing a non-existent key

    Ok(())
}

#[test]
fn test_put_get_remove_bytes_key() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), VecU8Encoder, Config::default())?;

    let key = b"my_bytes_blob".to_vec();
    let data = b"rofls";

    let mut tx = cas.put(key.clone())?;
    tx.write(data)?;
    tx.finish()?;

    let retrieved = cas.get(&key)?.expect("blob should exist");
    assert_eq!(retrieved.as_ref(), data);

    assert!(cas.remove(&key)?);
    assert!(cas.get(&key)?.is_none());

    Ok(())
}

#[test]
fn test_get_range() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    let key = "range_blob".to_string();
    let data = b"0123456789abcdef";

    let mut tx = cas.put(key.clone())?;
    tx.write(data)?;
    tx.finish()?;

    assert_eq!(cas.get_range(&key, 0, 16)?.unwrap().as_ref(), b"0123456789abcdef");
    assert_eq!(cas.get_range(&key, 4, 8)?.unwrap().as_ref(), b"4567");
    assert_eq!(cas.get_range(&key, 10, 16)?.unwrap().as_ref(), b"abcdef");

    // Range extending beyond the end of the data is truncated
    assert_eq!(cas.get_range(&key, 12, 100)?.unwrap().as_ref(), b"cdef");

    // Zero-length or out-of-bounds ranges
    assert!(cas.get_range(&key, 100, 200)?.unwrap().is_empty());
    assert!(cas.get_range(&key, 5, 5)?.unwrap().is_empty());

    // Invalid range (start > end)
    assert!(cas.get_range(&key, 8, 4).is_err());

    Ok(())
}

#[test]
fn test_overwrite_persists_across_reopen() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let db_path = dir.path();
    let key = "overwrite_test".to_string();

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        let mut tx = cas.put(key.clone())?;
        tx.write(b"Version 1")?;
        tx.finish()?;

        assert_eq!(cas.get(&key)?.unwrap().as_ref(), b"Version 1");
    }

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        assert_eq!(cas.get(&key)?.unwrap().as_ref(), b"Version 1");

        let mut tx = cas.put(key.clone())?;
        tx.write(b"Version 2 is better")?;
        tx.finish()?;

        assert_eq!(cas.get(&key)?.unwrap().as_ref(), b"Version 2 is better");
    }

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        let retrieved = cas.get(&key)?.unwrap();
        assert_eq!(retrieved.as_ref(), b"Version 2 is better");
    }
    Ok(())
}

#[test]
fn test_remove_persists_across_reopen() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let db_path = dir.path();
    let key = "remove_persist_test".to_string();

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        let mut tx = cas.put(key.clone())?;
        tx.write(b"Data to be removed")?;
        tx.finish()?;

        assert!(cas.get(&key)?.is_some());
        assert!(cas.remove(&key)?);
        assert!(cas.get(&key)?.is_none());
    }

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        assert!(cas.get(&key)?.is_none(), "data should still be removed after reopen");
    }
    Ok(())
}

#[test]
fn test_transaction_drop_cleans_up_staging_file() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;
    let key = "dropped_tx_key".to_string();

    let staging_path;
    {
        let mut tx = cas.put(key.clone())?;
        tx.write(b"This data won't be saved")?;

        // Capture path before the transaction is dropped and its temp file is removed.
        staging_path = tx.temp_file.path().to_path_buf();
        assert!(staging_path.exists(), "staging file should exist during tx");
    }

    assert!(!staging_path.exists(), "staging file should be removed on drop");
    assert!(cas.get(&key)?.is_none(), "data should not be present if tx was dropped");

    Ok(())
}

#[test]
fn test_checkpoint_persists_index() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let db_path = dir.path();

    let index_file_path = db_path.join("index");
    let checkpoint_meta_path = db_path.join(CHECKPOINT_META_FILENAME);

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        let mut tx = cas.put("key1".to_string())?;
        tx.write(b"data1")?;
        tx.finish()?;

        cas.checkpoint()?;

        assert!(index_file_path.exists(), "index file should be created by checkpoint");
        assert!(index_file_path.metadata()?.len() > 0, "index file should not be empty");
        assert!(checkpoint_meta_path.exists(), "checkpoint meta file must exist");
        assert_eq!(fs::read_to_string(&checkpoint_meta_path)?.trim(), "0");
    }

    {
        // Reopen and ensure data is loaded correctly, proving the checkpoint worked.
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        assert_eq!(cas.get(&"key1".to_string())?.unwrap().as_ref(), b"data1");
    }
    Ok(())
}

#[test]
fn test_wal_rollover_and_cleanup() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let db_path = dir.path();

    // Configure a very small WAL segment size to force rollovers.
    let config = Config {
        sync_mode: SyncMode::Sync,
        num_ops_per_wal: 2,
        pre_create_cas_dirs: false,
        ..Default::default()
    };

    let wal0_path = db_path.join("0_index.wal");
    let wal1_path = db_path.join("1_index.wal");

    {
        let cas = Cas::open(db_path, StringEncoder, config.clone())?;
        let mut tx = cas.put("key1".to_string())?;
        tx.write(b"d1")?;
        tx.finish()?;

        let mut tx = cas.put("key2".to_string())?;
        tx.write(b"d2")?;
        tx.finish()?;

        assert!(wal0_path.exists());
        assert!(!wal1_path.exists());

        // This operation should trigger the rollover to the next WAL segment.
        let mut tx = cas.put("key3".to_string())?;
        tx.write(b"d3")?;
        tx.finish()?;
        assert!(wal1_path.exists());

        // Checkpoint should persist data from all segments and clean up old ones.
        cas.checkpoint()?;

        assert!(!wal0_path.exists(), "stale wal (0) should be cleaned up by checkpoint");
        assert!(wal1_path.exists(), "active wal (1) should remain");
    }

    {
        let cas = Cas::open(db_path, StringEncoder, config)?;
        assert_eq!(cas.get(&"key1".to_string())?.unwrap().as_ref(), b"d1");
        assert_eq!(cas.get(&"key2".to_string())?.unwrap().as_ref(), b"d2");
        assert_eq!(cas.get(&"key3".to_string())?.unwrap().as_ref(), b"d3");
    }

    Ok(())
}

#[test]
fn test_remove_range_persists() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let db_path = dir.path();

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        for i in 1..=4 {
            let mut tx = cas.put(format!("key_{i}"))?;
            tx.write(format!("data_{i}",).as_bytes())?;
            tx.finish()?;
        }

        // remove key_1 and key_2 (exclusive end)
        let count = cas.remove_range("key_1".to_string().."key_3".to_string())?;
        assert_eq!(count, 2);

        assert!(cas.get(&"key_1".to_string())?.is_none());
        assert!(cas.get(&"key_2".to_string())?.is_none());
        assert!(cas.get(&"key_3".to_string())?.is_some());
    }

    {
        let cas = Cas::open(db_path, StringEncoder, Config::default())?;
        assert!(cas.get(&"key_1".to_string())?.is_none());
        assert!(cas.get(&"key_2".to_string())?.is_none());
        assert!(cas.get(&"key_3".to_string())?.is_some());
        assert!(cas.get(&"key_4".to_string())?.is_some());

        assert_eq!(cas.index.read_state().known_blobs().count(), 2);
    }

    Ok(())
}

#[test]
fn test_api_on_nonexistent_key() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;
    let key = "nonexistent_key".to_string();

    let result = cas.get_reader(&key).unwrap();
    match result {
        None => {}
        _ => panic!("Expected KeyNotFound error, got: {result:?}"),
    }

    // High-level APIs should gracefully return None.
    assert!(cas.get(&key)?.is_none());
    assert!(cas.get_size(&key)?.is_none());
    assert!(cas.get_range(&key, 0, 10)?.is_none());

    Ok(())
}

#[test]
fn test_io_error_on_staging_file_creation() -> anyhow::Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    // make the staging directory read-only to force a permissions error.
    let staging_dir = dir.path().join("staging");
    fs::set_permissions(&staging_dir, fs::Permissions::from_mode(0o555))?; // r-xr-xr-x

    let result = cas.put("test_key".to_string());
    assert!(result.is_err());

    match result.unwrap_err() {
        LibError::Io { operation, path, .. } => {
            assert!(matches!(operation, LibIoOperation::CreateStagingFile));
            let path_str = path.unwrap().to_string_lossy().to_string();
            assert!(path_str.contains("staging"));
        }
        other_error => panic!("Expected a specific IO error, but got: {other_error}",),
    }

    Ok(())
}

#[test]
fn test_orphan_detection_and_cleanup() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;

    // First, create a CAS with some valid data
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    // Add some valid data
    let key1 = "valid_key1".to_string();
    let data1 = b"valid data 1";
    let mut tx = cas.put(key1.clone())?;
    tx.write(data1)?;
    tx.finish()?;

    // Get the hash for later
    let _valid_hash = cas.index.read_state().get_hash_for_key(&key1).unwrap();

    // Now create an orphaned blob by writing directly to CAS
    let orphan_data = b"orphaned data";
    let orphan_hash = crate::calculate_blob_hash(orphan_data);
    let orphan_path = dir.path().join("cas").join(orphan_hash.relative_path());

    // Create parent directories
    if let Some(parent) = orphan_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&orphan_path, orphan_data)?;

    // Also create an invalid file in CAS
    let invalid_file_path = dir.path().join("cas").join(".DS_Store");
    fs::write(&invalid_file_path, b"invalid")?;

    // Create an old staging file
    let staging_file = dir.path().join("staging").join("old_file.tmp");
    fs::write(&staging_file, b"old staging data")?;

    // Drop the CAS to ensure clean shutdown
    drop(cas);

    // Reopen with recovery to get orphan stats
    let config = Config { scan_orphans_on_startup: true, ..Default::default() };
    let (cas, orphan_stats) = Cas::open_with_recover(dir.path(), StringEncoder, config)?;

    // Should have orphan stats
    let stats = orphan_stats.expect("Should have orphan stats");
    assert_eq!(stats.orphaned_blobs.len(), 1);
    assert_eq!(stats.orphaned_blobs[0], orphan_hash);
    assert_eq!(stats.invalid_files.len(), 1);

    // Valid blob should still exist
    let retrieved = cas.get(&key1)?.expect("valid blob should still exist");
    assert_eq!(retrieved.as_ref(), data1);

    // Orphan should still exist (not auto-deleted)
    assert!(orphan_path.exists(), "orphaned blob should still exist before cleanup");

    // Now explicitly delete orphans
    let result = stats.delete_orphans()?;
    assert_eq!(result.orphans_deleted, 1);
    assert_eq!(result.invalid_files_removed, 1);

    // Verify orphaned blob was removed
    assert!(!orphan_path.exists(), "orphaned blob should be removed after cleanup");
    assert!(!invalid_file_path.exists(), "invalid file should be removed after cleanup");

    Ok(())
}

#[test]
fn test_orphan_detection_with_integrity_check() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;

    // Create a CAS with some valid data
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    let key1 = "valid_key1".to_string();
    let data1 = b"valid data 1";
    let mut tx = cas.put(key1.clone())?;
    tx.write(data1)?;
    tx.finish()?;

    let valid_hash = cas.index.read_state().get_hash_for_key(&key1).unwrap();
    let valid_path = dir.path().join("cas").join(valid_hash.relative_path());

    // Corrupt the blob by modifying its contents
    fs::write(&valid_path, b"corrupted data")?;

    drop(cas);

    // Reopen with integrity check enabled
    let config = Config {
        scan_orphans_on_startup: true,
        verify_blob_integrity: true,
        fail_on_integrity_errors: true,
        ..Default::default()
    };

    let result = Cas::open(dir.path(), StringEncoder, config);

    // Should fail due to corrupted blob
    match result {
        Err(LibError::IntegrityCheckFailed { corrupted_blobs, .. }) => {
            assert_eq!(corrupted_blobs.len(), 1);
            assert_eq!(corrupted_blobs[0], valid_hash);
        }
        _ => panic!("Expected IntegrityCheckFailed error"),
    }

    Ok(())
}

#[test]
fn test_orphan_detection_with_missing_blobs() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;

    // Create a CAS with some data
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    let key1 = "key1".to_string();
    let data1 = b"data 1";
    let mut tx = cas.put(key1.clone())?;
    tx.write(data1)?;
    tx.finish()?;

    let hash1 = cas.index.read_state().get_hash_for_key(&key1).unwrap();
    let blob_path = dir.path().join("cas").join(hash1.relative_path());

    // Delete the blob file to simulate missing blob
    fs::remove_file(&blob_path)?;

    drop(cas);

    // Reopen with cleanup
    let config = Config {
        scan_orphans_on_startup: true,
        fail_on_integrity_errors: true,
        ..Default::default()
    };

    let result = Cas::open(dir.path(), StringEncoder, config);

    // Should fail due to missing blob
    match result {
        Err(LibError::IntegrityCheckFailed { missing_blobs, .. }) => {
            assert_eq!(missing_blobs.len(), 1);
            assert_eq!(missing_blobs[0], hash1);
        }
        _ => panic!("Expected IntegrityCheckFailed error"),
    }

    Ok(())
}

#[test]
fn test_orphan_quarantine() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let quarantine_dir = dir.path().join("quarantine");

    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    // Add valid data
    let key1 = "key1".to_string();
    let data1 = b"data 1";
    let mut tx = cas.put(key1.clone())?;
    tx.write(data1)?;
    tx.finish()?;

    // Create orphaned blob
    let orphan_data = b"orphan";
    let orphan_hash = crate::calculate_blob_hash(orphan_data);
    let orphan_path = dir.path().join("cas").join(orphan_hash.relative_path());
    if let Some(parent) = orphan_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&orphan_path, orphan_data)?;

    drop(cas);

    // Reopen with recovery
    let config = Config { scan_orphans_on_startup: true, ..Default::default() };
    let (_cas, orphan_stats) = Cas::open_with_recover(dir.path(), StringEncoder, config)?;
    let stats = orphan_stats.expect("Should have orphan stats");

    // Quarantine orphans
    let result = stats.quarantine_orphans(&quarantine_dir)?;
    assert_eq!(result.orphans_quarantined, 1);

    // Verify orphan was moved to quarantine
    assert!(!orphan_path.exists());
    assert!(quarantine_dir.join(orphan_hash.to_string()).exists());

    Ok(())
}

#[test]
fn test_orphan_stats_holds_lock() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;

    // Create CAS with orphaned blob
    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    let orphan_data = b"orphan";
    let orphan_hash = crate::calculate_blob_hash(orphan_data);
    let orphan_path = dir.path().join("cas").join(orphan_hash.relative_path());
    if let Some(parent) = orphan_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&orphan_path, orphan_data)?;

    drop(cas);

    // Reopen with recovery
    let config = Config { scan_orphans_on_startup: true, ..Default::default() };
    let (_cas, orphan_stats) = Cas::open_with_recover(dir.path(), StringEncoder, config)?;
    let stats = orphan_stats.expect("Should have orphan stats");

    assert_eq!(stats.orphaned_blobs.len(), 1);

    // Note: We cannot do any CAS operations while holding OrphanStats
    // because it holds the filesystem lock for its entire lifetime.
    // This is by design to ensure consistency during cleanup.

    // Now delete orphans
    let result = stats.delete_orphans()?;
    assert_eq!(result.orphans_deleted, 1);
    assert!(!orphan_path.exists());

    // Drop stats to release the lock
    drop(stats);

    Ok(())
}

#[test]
fn test_cleanup_disabled() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;

    let cas = Cas::open(dir.path(), StringEncoder, Config::default())?;

    // Create orphaned blob
    let orphan_data = b"orphan";
    let orphan_hash = crate::calculate_blob_hash(orphan_data);
    let orphan_path = dir.path().join("cas").join(orphan_hash.relative_path());
    if let Some(parent) = orphan_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(&orphan_path, orphan_data)?;

    drop(cas);

    // Reopen with cleanup disabled
    let config = Config { scan_orphans_on_startup: false, ..Default::default() };
    let (_cas, orphan_stats) = Cas::open_with_recover(dir.path(), StringEncoder, config)?;

    // Should have no orphan stats
    assert!(orphan_stats.is_none());

    // Orphan should still exist
    assert!(orphan_path.exists(), "orphan should not be removed when cleanup is disabled");

    Ok(())
}
