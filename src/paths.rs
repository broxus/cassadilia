use std::path::PathBuf;

use crate::index::CHECKPOINT_META_FILENAME;
use crate::types::BlobHash;

#[derive(Debug, Clone)]
pub(crate) struct DbPaths {
    db_root: PathBuf,
}

impl DbPaths {
    pub fn new(db_root: PathBuf) -> Self {
        Self { db_root }
    }

    pub fn db_root_path(&self) -> &PathBuf {
        &self.db_root
    }

    pub fn wal_path_for_segment(&self, segment_id: u64) -> PathBuf {
        self.db_root.join(format!("{segment_id}_index.wal"))
    }

    pub fn index_file_path(&self) -> PathBuf {
        self.db_root.join("index")
    }

    pub fn index_tmp_path(&self) -> PathBuf {
        self.db_root.join("index.tmp")
    }

    pub fn checkpoint_meta_path(&self) -> PathBuf {
        self.db_root.join(CHECKPOINT_META_FILENAME)
    }

    pub fn checkpoint_meta_tmp_path(&self) -> PathBuf {
        self.db_root.join(format!("{CHECKPOINT_META_FILENAME}.tmp"))
    }

    pub fn cas_root_path(&self) -> PathBuf {
        self.db_root.join("cas")
    }

    pub fn staging_root_path(&self) -> PathBuf {
        self.db_root.join("staging")
    }

    pub fn cas_file_path(&self, hash: &BlobHash) -> PathBuf {
        self.cas_root_path().join(hash.relative_path())
    }
}
