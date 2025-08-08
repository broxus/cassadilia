use std::collections::BTreeMap;
use std::fmt::Debug;

use thiserror::Error;

use super::state::IndexState;
use crate::io::{IoError, atomically_write_file_bytes};
use crate::paths::DbPaths;
use crate::serialization::{SerializationError, deserialize_index_state, serialize_index_state};
use crate::types::KeyBytes;

pub(crate) struct IndexStatePersister<'a> {
    paths: &'a DbPaths,
}

impl<'a> IndexStatePersister<'a> {
    pub fn new(paths: &'a DbPaths) -> Self {
        Self { paths }
    }

    pub fn load<K>(&self) -> Result<IndexState<K>, PersisterError>
    where
        K: KeyBytes + Clone + Eq + Ord + Debug + Send + Sync + 'static,
    {
        let index_path = self.paths.index_file_path();
        let mut state = IndexState::new();

        match std::fs::read(index_path) {
            Ok(data) => {
                if data.is_empty() {
                    tracing::info!(
                        "Index file '{}' is empty, starting fresh. State remains new.",
                        index_path.display()
                    );
                    return Ok(state);
                }
                let loaded_key_bytes_to_hash = deserialize_index_state(&data)?;

                state.key_to_hash.clear();
                state.hash_to_ref_count.clear();

                for (key_bytes, item) in loaded_key_bytes_to_hash {
                    let Some(key) = K::from_key_bytes(&key_bytes) else {
                        return Err(PersisterError::DecodeKey);
                    };
                    state.key_to_hash.insert(key, item);
                    state.increment_ref(&item.blob_hash);
                }
                tracing::info!(
                    "Loaded {} entries from index '{}' and populated ref counts.",
                    state.key_to_hash.len(),
                    index_path.display()
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(
                    "Index file '{}' not found, starting fresh. State remains new.",
                    index_path.display()
                );
            }
            Err(e) => {
                return Err(PersisterError::ReadIndexIo(e));
            }
        }
        Ok(state)
    }

    pub fn save<K>(&self, state: &IndexState<K>) -> Result<(), PersisterError>
    where
        K: KeyBytes + Clone + Eq + Ord + Debug + Send + Sync + 'static,
    {
        let index_path = self.paths.index_file_path();
        let index_tmp_path = self.paths.index_tmp_path();

        // convert keys to bytes and build BTreeMap<Vec<u8>, BlobHash>
        let mut key_bytes_to_hash = BTreeMap::new();
        for (key, hash) in &state.key_to_hash {
            let key_bytes = key.to_key_bytes_owned();
            key_bytes_to_hash.insert(key_bytes, *hash);
        }

        let data_bytes = serialize_index_state(&key_bytes_to_hash)?;

        atomically_write_file_bytes(index_path, index_tmp_path, &data_bytes)?;

        tracing::info!(
            "Persisted {} entries to index '{}'",
            state.key_to_hash.len(),
            index_path.display()
        );
        Ok(())
    }
}

#[derive(Error, Debug)]
pub enum PersisterError {
    #[error("Persister: Failed to read index file")]
    ReadIndexIo(#[source] std::io::Error),
    #[error("Persister: Failed to decode index data")]
    DecodeIndex(#[from] SerializationError),
    #[error("Persister: Failed to decode key from index")]
    DecodeKey,
    #[error("Persister: Failed to atomically write index state")]
    AtomicWrite(#[from] IoError),
}
