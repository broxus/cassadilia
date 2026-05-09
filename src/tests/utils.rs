/// Setup tracing for tests with cassadilia debug level
pub fn setup_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("cassadilia=debug".parse().unwrap()),
        )
        .try_init();
}

use std::ops::Range;
use std::path::{Path, PathBuf};

use anyhow::Result;
use tempfile::{TempDir, tempdir};

use crate::{Cas, Config, OrphanStats};

pub struct CasTestHarness {
    _dir: TempDir,
    db_path: PathBuf,
    config: Config,
}

impl CasTestHarness {
    pub fn new(config: Config) -> Result<Self> {
        let dir = tempdir()?;
        let db_path = dir.path().to_path_buf();
        Ok(Self { _dir: dir, db_path, config })
    }

    pub fn run_session<F>(&self, session_fn: F) -> Result<()>
    where
        F: FnOnce(&Cas<String>) -> Result<()>,
    {
        let cas = Cas::<String>::open(&self.db_path, self.config.clone())?;
        session_fn(&cas)
    }

    pub fn run_session_with_recover<F>(&self, session_fn: F) -> Result<()>
    where
        F: FnOnce(&Cas<String>, Option<OrphanStats<String>>) -> Result<()>,
    {
        let (cas, stats) = Cas::<String>::open_with_recover(&self.db_path, self.config.clone())?;
        session_fn(&cas, stats)
    }

    pub fn db_path(&self) -> &Path {
        &self.db_path
    }
}

pub fn populate_cas(cas: &Cas<String>, range: Range<u32>, data_prefix: &str) -> Result<()> {
    for i in range {
        let key = format!("key{:02}", i);
        let data = format!("{}_{}", data_prefix, i);
        let mut tx = cas.put(key)?;
        tx.write(data.as_bytes())?;
        tx.finish()?;
    }
    Ok(())
}

pub fn verify_cas_data(cas: &Cas<String>, range: Range<u32>, data_prefix: &str) -> Result<()> {
    for i in range {
        let key = format!("key{:02}", i);
        let expected_data = format!("{}_{}", data_prefix, i);
        let actual_data = cas.get(&key)?.expect("Key should exist");
        assert_eq!(actual_data.as_ref(), expected_data.as_bytes(), "bad data {}", key);
    }
    Ok(())
}

pub fn assert_key_count(cas: &Cas<String>, expected: usize) {
    let count = cas.read_index_state().len();
    assert_eq!(count, expected, "want {}, got {}", expected, count);
}

pub fn assert_all_ref_counts_are(cas: &Cas<String>, expected_count: u32) -> Result<()> {
    let state = cas.read_index_state();
    for (hash, count) in state.known_blobs() {
        assert_eq!(*count, expected_count, "refcnt {} != {} for {:?}", count, expected_count, hash);
    }
    Ok(())
}

pub fn verify_refcount_integrity(cas: &Cas<String>, expected_keys: usize) -> Result<()> {
    let state = cas.read_index_state();

    assert_eq!(state.known_blobs().len(), expected_keys, "want {} uniq", expected_keys);

    for (hash, count) in state.known_blobs() {
        assert_eq!(*count, 1, "refcnt {} != 1 for {:?}", count, hash);
    }

    Ok(())
}

pub fn count_wal_segments(db_path: &Path) -> Result<usize> {
    let count = std::fs::read_dir(db_path)?
        .filter_map(|entry| entry.ok())
        .filter(|entry| {
            entry.path().extension().and_then(|ext| ext.to_str()).is_some_and(|ext| ext == "wal")
        })
        .count();
    Ok(count)
}

#[cfg(test)]
pub mod directory_helpers;
