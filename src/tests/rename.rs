use std::collections::HashMap;
use std::num::NonZeroU64;
use std::path::Path;

use anyhow::Result;
use tempfile::tempdir;

use super::utils::setup_tracing;
use crate::index::IndexError;
use crate::{BlobHash, Cas, Config, KeyBytes, LibError};

fn put(cas: &Cas<String>, key: &str, data: &[u8]) -> Result<()> {
    let mut tx = cas.put(key.to_owned())?;
    tx.write(data)?;
    tx.finish()?;
    Ok(())
}

fn get(cas: &Cas<String>, key: &str) -> Result<Option<Vec<u8>>> {
    Ok(cas.get(&key.to_owned())?.map(|bytes| bytes.to_vec()))
}

fn pairs(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
    pairs.iter().map(|(from, to)| ((*from).to_owned(), (*to).to_owned())).collect()
}

fn wal_bytes(db_path: &Path) -> Result<u64> {
    let mut total = 0;
    for entry in std::fs::read_dir(db_path)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "wal") {
            total += std::fs::metadata(&path)?.len();
        }
    }
    Ok(total)
}

#[test]
fn rename_many_moves_keys_and_keeps_blobs() -> Result<()> {
    setup_tracing();
    let dir = tempdir()?;
    let cas = Cas::open(dir.path(), Config::default())?;
    put(&cas, "a", b"data a")?;
    put(&cas, "b", b"data b")?;
    put(&cas, "c", b"data a")?;

    let refcounts = |cas: &Cas<String>| -> HashMap<BlobHash, u32> {
        cas.read_index_state().known_blobs().map(|(hash, count)| (*hash, *count)).collect()
    };
    let stats_before = cas.stats();
    let refcounts_before = refcounts(&cas);

    cas.rename_many(pairs(&[("a", "x"), ("b", "y")]))?;

    assert_eq!(get(&cas, "x")?.as_deref(), Some(&b"data a"[..]));
    assert_eq!(get(&cas, "y")?.as_deref(), Some(&b"data b"[..]));
    assert_eq!(get(&cas, "a")?, None);
    assert_eq!(get(&cas, "b")?, None);
    assert_eq!(get(&cas, "c")?.as_deref(), Some(&b"data a"[..]));
    assert_eq!(cas.stats(), stats_before);
    assert_eq!(refcounts(&cas), refcounts_before);

    let wal_before = wal_bytes(dir.path())?;
    cas.rename_many(Vec::new())?;
    assert_eq!(wal_bytes(dir.path())?, wal_before, "an empty batch must not append");
    Ok(())
}

#[test]
fn rename_many_refusals_change_nothing() -> Result<()> {
    setup_tracing();
    // Each batch starts with a valid pair: applied one by one, it would move `a`.
    let cases: [(&[(&str, &str)], &str); 5] = [
        (&[("a", "x"), ("b", "c")], r#"rename target key "c" already exists"#),
        (&[("a", "x"), ("missing", "y")], r#"rename source key "missing" does not exist"#),
        (&[("a", "x"), ("a", "y")], r#"key "a" is renamed twice in one batch"#),
        (&[("a", "x"), ("b", "x")], r#"two keys are renamed to "x" in one batch"#),
        // One by one, `b` would already be free when `a -> b` is checked.
        (&[("b", "z"), ("a", "b")], r#"rename target key "b" already exists"#),
    ];

    for (batch, expected) in cases {
        let dir = tempdir()?;
        let before = {
            let cas = Cas::open(dir.path(), Config::default())?;
            put(&cas, "a", b"data a")?;
            put(&cas, "b", b"data b")?;
            put(&cas, "c", b"data c")?;
            let before = cas.read_index_state().keys_snapshot();

            let err = cas.rename_many(pairs(batch)).expect_err("batch must be refused");
            let LibError::Index(IndexError::Rename(rename_err)) = &err else {
                panic!("{batch:?}: unexpected error {err:?}");
            };
            assert_eq!(rename_err.to_string(), expected, "{batch:?}");
            assert_eq!(get(&cas, "a")?.as_deref(), Some(&b"data a"[..]), "{batch:?}");
            assert_eq!(cas.read_index_state().keys_snapshot(), before, "{batch:?}");
            before
        };

        // A refused batch leaves no WAL entry to replay.
        let cas = Cas::<String>::open(dir.path(), Config::default())?;
        assert_eq!(cas.read_index_state().keys_snapshot(), before, "{batch:?} after reopen");
    }
    Ok(())
}

#[test]
fn rename_many_survives_reopen() -> Result<()> {
    setup_tracing();
    // Default config: the reopen replays the rename from the WAL. Dropping `Cas` leaves
    // the WAL as a crash right after the append would.
    // One op per segment: the rename rolls the segment and lands in the index file. The WAL
    // is deleted before the reopen to prove it.
    let cases = [
        (Config::default(), false),
        (Config { num_ops_per_wal: NonZeroU64::new(1).unwrap(), ..Default::default() }, true),
    ];

    for (config, delete_wal) in cases {
        let dir = tempdir()?;
        {
            let cas = Cas::open(dir.path(), config.clone())?;
            put(&cas, "a", b"data a")?;
            put(&cas, "b", b"data b")?;
            cas.rename_many(pairs(&[("a", "x"), ("b", "y")]))?;
        }
        if delete_wal {
            for entry in std::fs::read_dir(dir.path())? {
                let path = entry?.path();
                if path.extension().is_some_and(|ext| ext == "wal") {
                    std::fs::remove_file(path)?;
                }
            }
        }

        let cas = Cas::open(dir.path(), config.clone())?;
        assert_eq!(get(&cas, "x")?.as_deref(), Some(&b"data a"[..]), "{config:?}");
        assert_eq!(get(&cas, "y")?.as_deref(), Some(&b"data b"[..]), "{config:?}");
        assert_eq!(get(&cas, "a")?, None, "{config:?}");
        assert_eq!(get(&cas, "b")?, None, "{config:?}");
    }
    Ok(())
}

/// Accepts only `new/` keys, like a caller that changes its key format.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
struct NewKey(String);

impl KeyBytes for NewKey {
    type Bytes = Vec<u8>;

    fn to_key_bytes(&self) -> Self::Bytes {
        self.0.as_bytes().to_vec()
    }

    fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        let key = std::str::from_utf8(bytes).ok()?;
        key.starts_with("new/").then(|| Self(key.to_owned()))
    }
}

#[test]
fn opening_with_a_new_key_type_needs_a_checkpoint_after_rename() -> Result<()> {
    setup_tracing();
    for checkpoint_after_rename in [false, true] {
        let dir = tempdir()?;
        {
            let cas = Cas::open(dir.path(), Config::default())?;
            put(&cas, "old/a", b"data a")?;
            // Checkpoint the put, so only the rename is left in the WAL.
            cas.checkpoint()?;
            cas.rename_many(pairs(&[("old/a", "new/a")]))?;
            if checkpoint_after_rename {
                cas.checkpoint()?;
            }
        }

        let opened = Cas::<NewKey>::open(dir.path(), Config::default());
        if checkpoint_after_rename {
            let cas = opened?;
            let data = cas.get(&NewKey("new/a".to_owned()))?;
            assert_eq!(data.as_deref(), Some(&b"data a"[..]));
        } else {
            assert!(
                opened.is_err(),
                "the index file still holds `old/a`, which `NewKey` cannot decode"
            );
        }
    }
    Ok(())
}
