use std::path::{Path, PathBuf};

use crate::index::CHECKPOINT_META_FILENAME;
use crate::types::BlobHash;

#[derive(Debug, Clone)]
pub(crate) struct DbPaths {
    db_root: PathBuf,
    lockfile_path: PathBuf,
    index_path: PathBuf,
    index_tmp_path: PathBuf,
    checkpoint_meta_path: PathBuf,
    checkpoint_meta_tmp_path: PathBuf,
    cas_root_path: PathBuf,
    staging_root_path: PathBuf,
    settings_path: PathBuf,
}

impl DbPaths {
    pub fn new(db_root: PathBuf) -> Self {
        let lockfile_path = db_root.join("LOCK");
        let index_path = db_root.join("index");
        let index_tmp_path = db_root.join("index.tmp");
        let checkpoint_meta_path = db_root.join(CHECKPOINT_META_FILENAME);
        let checkpoint_meta_tmp_path = db_root.join(format!("{CHECKPOINT_META_FILENAME}.tmp"));
        let cas_root_path = db_root.join("cas");
        let staging_root_path = db_root.join("staging");
        let settings_path = db_root.join("db_settings.kdl");

        Self {
            db_root,
            lockfile_path,
            index_path,
            index_tmp_path,
            checkpoint_meta_path,
            checkpoint_meta_tmp_path,
            cas_root_path,
            staging_root_path,
            settings_path,
        }
    }

    pub fn db_root_path(&self) -> &Path {
        &self.db_root
    }

    pub fn lockfile_path(&self) -> &Path {
        &self.lockfile_path
    }

    pub fn wal_path_for_segment(&self, segment_id: u64) -> PathBuf {
        self.db_root.join(format!("{segment_id}_index.wal"))
    }

    pub fn index_file_path(&self) -> &Path {
        &self.index_path
    }

    pub fn index_tmp_path(&self) -> &Path {
        &self.index_tmp_path
    }

    pub fn checkpoint_meta_path(&self) -> &Path {
        &self.checkpoint_meta_path
    }

    pub fn checkpoint_meta_tmp_path(&self) -> &Path {
        &self.checkpoint_meta_tmp_path
    }

    pub fn cas_root_path(&self) -> &Path {
        &self.cas_root_path
    }

    pub fn staging_root_path(&self) -> &Path {
        &self.staging_root_path
    }

    pub fn settings_path(&self) -> &Path {
        &self.settings_path
    }

    pub fn cas_file_path(&self, hash: &BlobHash) -> PathBuf {
        self.cas_root_path().join(hash.relative_path())
    }
}
