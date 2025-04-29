use anyhow::{anyhow, Context, Result};
use bincode::{config::standard as bincode_config, Decode, Encode};
use std::collections::BTreeSet;
use std::fmt::Debug;
use std::io::BufReader;
use std::ops::RangeBounds;
use std::{
    collections::BTreeMap,
    fmt,
    fs::{File, OpenOptions},
    hash::{Hash, Hasher},
    io::{BufWriter, Write},
    ops::Deref,
    os::unix::fs::FileExt,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum BubsError {
    #[error("Invalid hash format: {0}")]
    InvalidHashFormat(String),
    #[error("Invalid range requested: {0}")]
    InvalidRange(String),
    #[error("Key not found")]
    KeyNotFound,
}

pub const HASH_SIZE: usize = blake3::OUT_LEN;

#[derive(Clone, Copy, Eq, Encode, Decode)]
pub struct BlobHash([u8; HASH_SIZE]);

impl BlobHash {
    pub fn as_bytes(&self) -> &[u8; HASH_SIZE] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(hex_str: &str) -> Result<Self, BubsError> {
        let mut bytes = [0u8; HASH_SIZE];
        hex::decode_to_slice(hex_str, &mut bytes)
            .map_err(|e| BubsError::InvalidHashFormat(format!("Invalid hex: {}", e)))?;
        Ok(Self(bytes))
    }

    pub fn relative_path(&self) -> PathBuf {
        let hex = self.to_hex();
        if hex.len() >= 4 {
            let prefix1 = &hex[0..2];
            let prefix2 = &hex[2..4];
            PathBuf::from(prefix1).join(prefix2).join(&hex)
        } else {
            PathBuf::from(hex)
        }
    }

    pub fn from_bytes(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }
}

impl PartialEq for BlobHash {
    fn eq(&self, other: &Self) -> bool {
        self.0 == other.0
    }
}

impl Hash for BlobHash {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.0.hash(state);
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("BlobHash").field(&self.to_hex()).finish()
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_hex())
    }
}

impl From<[u8; HASH_SIZE]> for BlobHash {
    fn from(bytes: [u8; HASH_SIZE]) -> Self {
        Self(bytes)
    }
}

impl AsRef<[u8]> for BlobHash {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

#[derive(Clone, Eq, PartialEq, Hash, Debug)]
#[repr(transparent)]
pub struct Bytes(bytes::Bytes);

impl Bytes {
    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn from_slice(slice: &[u8]) -> Self {
        Bytes(bytes::Bytes::copy_from_slice(slice))
    }
}

impl AsRef<[u8]> for Bytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl<T: Into<bytes::Bytes>> From<T> for Bytes {
    fn from(value: T) -> Self {
        Bytes(value.into())
    }
}

impl Encode for Bytes {
    fn encode<E: bincode::enc::Encoder>(
        &self,
        encoder: &mut E,
    ) -> core::result::Result<(), bincode::error::EncodeError> {
        Encode::encode(self.0.as_ref(), encoder)
    }
}

impl<Ctx> Decode<Ctx> for Bytes {
    fn decode<D: bincode::de::Decoder>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let vec: Vec<u8> = Decode::decode(decoder)?;
        Ok(Bytes(bytes::Bytes::from(vec)))
    }
}

impl<'de, Ctx> bincode::BorrowDecode<'de, Ctx> for Bytes {
    fn borrow_decode<D: bincode::de::BorrowDecoder<'de>>(
        decoder: &mut D,
    ) -> core::result::Result<Self, bincode::error::DecodeError> {
        let vec: Vec<u8> = bincode::BorrowDecode::borrow_decode(decoder)?;
        Ok(Bytes(bytes::Bytes::from(vec)))
    }
}

pub fn calculate_blob_hash(blob_data: &[u8]) -> BlobHash {
    BlobHash(blake3::hash(blob_data).into())
}

pub trait KeyEncoder<K>: Send + Sync + 'static {
    fn encode(&self, key: &K) -> Result<Vec<u8>>;
    fn decode(&self, data: &[u8]) -> Result<K>;
}

#[derive(Clone)]
pub struct Bubs<K>(Arc<BubsInner<K>>);

impl<K> Deref for Bubs<K> {
    type Target = BubsInner<K>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<K> Bubs<K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    pub fn open(
        db_root: impl AsRef<Path>,
        key_encoder: impl KeyEncoder<K> + 'static,
    ) -> Result<Self> {
        let inner = BubsInner::new(db_root.as_ref().to_path_buf(), key_encoder)?;
        Ok(Self(Arc::new(inner)))
    }
}

pub struct BubsInner<K> {
    staging_root: PathBuf,
    cas_root: PathBuf,
    index_path: PathBuf,
    index_tmp_path: PathBuf,
    state: RwLock<BTreeMap<K, BlobHash>>,
    key_encoder: Box<dyn KeyEncoder<K>>,
}

impl<K> Debug for BubsInner<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BubsInner")
            .field("staging_root", &self.staging_root)
            .field("cas_root", &self.cas_root)
            .field("index_path", &self.index_path)
            .field("index_tmp_path", &self.index_tmp_path)
            .finish()
    }
}

impl<K> BubsInner<K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    fn new(db_root: PathBuf, key_encoder: impl KeyEncoder<K> + 'static) -> Result<Self> {
        let staging_root = db_root.join("staging");
        let cas_root = db_root.join("cas");
        let index_path = db_root.join("index");
        let index_tmp_path = db_root.join("index.tmp");

        let mut this = Self {
            staging_root,
            cas_root,
            index_path,
            index_tmp_path,
            state: RwLock::new(BTreeMap::new()),
            key_encoder: Box::new(key_encoder),
        };

        this.create_dir_tree()
            .context("Failed to create DB directory structure")?;
        this.load_index().context("Failed to load index")?;

        Ok(this)
    }

    fn create_dir_tree(&self) -> Result<()> {
        std::fs::create_dir_all(&self.staging_root)
            .context("Failed to create staging directory")?;
        std::fs::create_dir_all(&self.cas_root).context("Failed to create CAS directory")?;
        if let Some(parent) = self.index_path.parent() {
            std::fs::create_dir_all(parent).context("Failed to create index parent directory")?;
        }
        Ok(())
    }

    fn load_index(&mut self) -> Result<()> {
        match std::fs::read(&self.index_path) {
            Ok(data) => {
                let (loaded_state_bytes, _len): (BTreeMap<Vec<u8>, BlobHash>, usize) =
                    bincode::decode_from_slice(&data, bincode_config())
                        .context("Failed to decode index data")?;

                let mut state_guard = self
                    .state
                    .write()
                    .map_err(|_| anyhow!("Failed to acquire state write lock during load"))?;

                for (key_bytes, hash) in loaded_state_bytes {
                    let key = self.key_encoder.decode(&key_bytes).with_context(|| {
                        format!("Failed to decode key from index: {:?}", key_bytes)
                    })?;
                    state_guard.insert(key, hash);
                }

                tracing::info!(
                    "Loaded {} entries from index '{}'",
                    state_guard.len(),
                    self.index_path.display()
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!(
                    "Index file '{}' not found, starting fresh.",
                    self.index_path.display()
                );
            }
            Err(e) => {
                return Err(anyhow!(e)).context(format!(
                    "Failed to read index file '{}'",
                    self.index_path.display()
                ));
            }
        }
        Ok(())
    }

    pub fn put(&self, key: K) -> Result<Transaction<K>> {
        Transaction::new(self, key)
    }

    /// Returns the hash for a given key, or None if not present.
    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        let state_guard = self.state.read().expect("mutex poisoned");
        state_guard.get(key).copied()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        let state_guard = self.state.read().expect("mutex poisoned");
        state_guard.contains_key(key)
    }

    pub fn get(&self, key: &K) -> Result<Option<bytes::Bytes>> {
        let hash_opt = self.get_hash_for_key(key);

        if let Some(blob_hash) = hash_opt {
            let path = blob_hash.relative_path();
            let full_path = self.cas_root.join(path);
            match std::fs::read(&full_path) {
                Ok(bytes) => Ok(Some(bytes::Bytes::from(bytes))),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::error!(
                        "Index contains key '{:?}' pointing to hash '{}', but CAS file '{}' not found!",
                        key,
                        blob_hash,
                        full_path.display()
                    );
                    Ok(None)
                }
                Err(e) => Err(anyhow!(e)).context(format!(
                    "Failed to read blob file '{}'",
                    full_path.display()
                )),
            }
        } else {
            Ok(None)
        }
    }

    pub fn size(&self, key: &K) -> Result<Option<u64>> {
        let hash_opt = self.get_hash_for_key(key);

        if let Some(blob_hash) = hash_opt {
            let path = blob_hash.relative_path();
            let full_path = self.cas_root.join(path);
            match std::fs::metadata(&full_path) {
                Ok(metadata) => Ok(Some(metadata.len())),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::error!(
                        "Index contains key '{:?}' pointing to hash '{}', but CAS file '{}' not found!",
                        key,
                        blob_hash,
                        full_path.display()
                    );
                    Ok(None)
                }
                Err(e) => Err(anyhow!(e)).context(format!(
                    "Failed to get metadata for blob file '{}'",
                    full_path.display()
                )),
            }
        } else {
            Ok(None)
        }
    }

    pub fn raw_bufreader(&self, key: &K) -> Result<BufReader<File>> {
        let hash_opt = self.get_hash_for_key(key);

        if let Some(blob_hash) = hash_opt {
            let path = blob_hash.relative_path();
            let full_path = self.cas_root.join(path);
            let file = File::open(&full_path).context(format!(
                "Failed to open blob file '{}'",
                full_path.display()
            ))?;
            Ok(BufReader::new(file))
        } else {
            Err(anyhow!(BubsError::KeyNotFound))
        }
    }

    /// Returns a list of all known keys in the index. Order is not guaranteed.
    pub fn known_keys(&self) -> BTreeSet<K> {
        let state_guard = self.state.read().expect("mutex poisoned");
        state_guard.keys().cloned().collect()
    }

    /// Returns a snapshot of the index
    pub fn index_snapshot(&self) -> BTreeMap<K, BlobHash> {
        let state_guard = self.state.read().expect("mutex poisoned");
        state_guard.clone()
    }

    pub fn get_range(
        &self,
        key: &K,
        range_start: u64,
        range_end: u64,
    ) -> Result<Option<bytes::Bytes>> {
        if range_start > range_end {
            return Err(anyhow!(BubsError::InvalidRange(format!(
                "Start {} > End {}",
                range_start, range_end
            ))));
        }

        let hash_opt = self.get_hash_for_key(key);

        if let Some(blob_hash) = hash_opt {
            let path = blob_hash.relative_path();
            let full_path = self.cas_root.join(path);
            let file = match File::open(&full_path) {
                Ok(f) => f,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                    tracing::error!(
                        "Index contains key '{:?}' pointing to hash '{}', but CAS file '{}' not found!",
                        key,
                        blob_hash,
                        full_path.display()
                    );
                    return Ok(None);
                }
                Err(e) => {
                    return Err(anyhow!(e)).context(format!(
                        "Failed to open blob file '{}' for range read",
                        full_path.display()
                    ));
                }
            };

            let len = file
                .metadata()
                .context("Failed to get blob file metadata")?
                .len();
            let start = std::cmp::min(range_start, len);
            let end = std::cmp::min(range_end, len);

            anyhow::ensure!(
                start <= end,
                BubsError::InvalidRange(format!("Calculated Start {} > End {}", start, end))
            );

            let read_len = end - start;
            let mut buff = vec![0; read_len as usize];

            let bytes_read = file.read_at(&mut buff, start).context(format!(
                "Failed to read range [{}, {}) from blob '{}'",
                start,
                end,
                full_path.display()
            ))?;

            Ok(Some(bytes::Bytes::copy_from_slice(&buff[0..bytes_read])))
        } else {
            Ok(None)
        }
    }

    pub fn remove(&self, key: &K) -> Result<bool> {
        let mut state_guard = self.state.write().expect("mutex poisoned");

        if state_guard.contains_key(key) {
            tracing::debug!("Removing key '{:?}' from index", key);
            state_guard.remove(key);
            self.persist_index(&state_guard)
                .context("Failed to persist index after remove")?;
            Ok(true)
        } else {
            tracing::debug!("Attempted to remove non-existent key '{:?}'", key);
            Ok(false)
        }
    }

    pub fn remove_range<R>(&self, range: R) -> Result<usize>
    where
        R: RangeBounds<K> + Debug, // Add Debug bound for logging
    {
        let mut state_guard = self
            .state
            .write()
            .map_err(|_| anyhow!("Failed to acquire state write lock for remove_range"))?;

        // Identify keys within the range to be removed.
        let keys_to_remove: Vec<K> = state_guard.range(range).map(|(k, _v)| k.clone()).collect();

        let remove_count = keys_to_remove.len();

        if remove_count == 0 {
            return Ok(0);
        }

        tracing::debug!(
            count = remove_count,
            "remove_range: Removing {} keys.",
            remove_count
        );

        // Remove the collected keys from the map
        for key in keys_to_remove {
            // remove() returns the old value, which we don't need here.
            let _ = state_guard.remove(&key);
        }

        // Persist the changes to the index file
        self.persist_index(&state_guard)
            .context("Failed to persist index after removing range")?;

        tracing::debug!(
            count = remove_count,
            "remove_range: Successfully removed {} keys and persisted index.",
            remove_count
        );

        Ok(remove_count)
    }

    pub fn retain<F>(&self, mut predicate: F) -> Result<usize>
    where
        F: FnMut(&K, &mut BlobHash) -> bool,
    {
        let mut state_guard = self
            .state
            .write()
            .map_err(|_| anyhow!("Failed to acquire state write lock for retain"))?;

        let mut retain_count = 0;
        let predicate = |key: &K, hash: &mut BlobHash| {
            if predicate(key, hash) {
                retain_count += 1;
                true
            } else {
                false
            }
        };
        state_guard.retain(predicate);

        Ok(retain_count)
    }

    fn commit_transaction(&self, mut transaction: Transaction<K>) -> Result<()> {
        transaction.writer.flush()?;
        transaction.writer.get_mut().sync_data()?;

        let hash = transaction.hasher.finalize();
        let blob_hash = BlobHash::from_bytes(*hash.as_bytes());

        let relative_cas_path = blob_hash.relative_path();
        let final_cas_path = self.cas_root.join(&relative_cas_path);

        if let Some(parent) = final_cas_path.parent() {
            std::fs::create_dir_all(parent).context(format!(
                "Failed to create CAS subdirectory '{}'",
                parent.display()
            ))?;
        }

        if !final_cas_path.exists() {
            std::fs::rename(&transaction.path, &final_cas_path).context(format!(
                "Failed to move staged file '{}' to CAS '{}'",
                transaction.path.display(),
                final_cas_path.display()
            ))?;
            tracing::debug!(
                "Moved blob {} to CAS path '{}'",
                blob_hash,
                final_cas_path.display()
            );
        } else {
            tracing::debug!(
                "Blob {} already exists at CAS path '{}', removing staged file",
                blob_hash,
                final_cas_path.display()
            );
            std::fs::remove_file(&transaction.path).context(format!(
                "Failed to remove redundant staged file '{}'",
                transaction.path.display()
            ))?;
        }

        {
            let mut state_guard = self.state.write().expect("mutex poisoned");

            let old_hash = state_guard.insert(transaction.key.clone(), blob_hash);
            tracing::debug!(
                "Updated index for key '{:?}' to hash {}",
                transaction.key,
                blob_hash
            );
            self.persist_index(&state_guard)
                .context("Failed to persist index during commit")?;

            if let Some(_old) = old_hash {}
        }

        Ok(())
    }

    fn persist_index(&self, state_guard: &BTreeMap<K, BlobHash>) -> Result<()> {
        tracing::debug!(
            "Persisting index to '{}' (via temporary '{}')",
            self.index_path.display(),
            self.index_tmp_path.display()
        );

        let mut state_bytes_map = BTreeMap::new();
        for (key, hash) in state_guard.iter() {
            let key_bytes = self
                .key_encoder
                .encode(key)
                .with_context(|| format!("Failed to encode key for persistence: {:?}", key))?;
            state_bytes_map.insert(key_bytes, *hash);
        }

        let state_bytes = bincode::encode_to_vec(&state_bytes_map, bincode_config())
            .context("Failed to serialize index state")?;

        let mut temp_file = OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .open(&self.index_tmp_path)
            .context(format!(
                "Failed to create/open temporary index file '{}'",
                self.index_tmp_path.display()
            ))?;

        temp_file.write_all(&state_bytes).context(format!(
            "Failed to write serialized index to temporary file '{}'",
            self.index_tmp_path.display()
        ))?;

        temp_file.sync_data().context(format!(
            "Failed to sync temporary index file '{}'",
            self.index_tmp_path.display()
        ))?;
        drop(temp_file);

        std::fs::rename(&self.index_tmp_path, &self.index_path).context(format!(
            "Failed to atomically rename temporary index '{}' to final index '{}'",
            self.index_tmp_path.display(),
            self.index_path.display()
        ))?;

        tracing::info!(
            "Successfully persisted index with {} entries to '{}'",
            state_guard.len(),
            self.index_path.display()
        );
        Ok(())
    }
}

#[must_use = "Transaction must be completed by calling finish()"]
pub struct Transaction<'a, K>
where
    K: Debug,
{
    path: PathBuf,
    bubs_inner: &'a BubsInner<K>,
    writer: BufWriter<File>,
    hasher: blake3::Hasher,
    key: K,
}

impl<'a, K> Transaction<'a, K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    fn new(bubs_inner: &'a BubsInner<K>, key: K) -> Result<Self> {
        let temp_filename = format!(
            "{}.tmp",
            BlobHash::from_bytes(*blake3::hash(&rand::random::<[u8; 16]>()).as_bytes())
        );
        let path = bubs_inner.staging_root.join(&temp_filename);

        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)
            .context(format!(
                "Failed to create staging file '{}'",
                path.display()
            ))?;

        tracing::debug!(
            "Starting transaction for key '{:?}' using staging file '{}'",
            key,
            path.display()
        );

        Ok(Self {
            writer: BufWriter::new(file),
            path,
            bubs_inner,
            hasher: blake3::Hasher::new(),
            key,
        })
    }

    pub fn write(&mut self, data: &[u8]) -> Result<()> {
        self.hasher.update(data);
        self.writer
            .write_all(data)
            .context("Failed to write data to staging file")?;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        let key = self.key.clone();
        tracing::debug!("Finishing transaction for key '{:?}'", key);
        self.bubs_inner
            .commit_transaction(self)
            .context(format!("Failed to commit transaction for key '{:?}'", key))
    }
}

impl<K> Drop for Transaction<'_, K>
where
    K: Debug,
{
    fn drop(&mut self) {
        if self.path.exists() {
            tracing::warn!(
                "Transaction for ID '{:?}' dropped without calling finish(). Cleaning up staging file: {}",
                self.key,
                self.path.display()
            );
            let _ = std::fs::remove_file(&self.path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[derive(Debug)]
    struct StringEncoder;
    impl KeyEncoder<String> for StringEncoder {
        fn encode(&self, key: &String) -> Result<Vec<u8>> {
            Ok(key.as_bytes().to_vec())
        }
        fn decode(&self, data: &[u8]) -> Result<String> {
            String::from_utf8(data.to_vec()).map_err(|e| anyhow!(e))
        }
    }

    #[derive(Debug)]
    struct VecU8Encoder;
    impl KeyEncoder<Vec<u8>> for VecU8Encoder {
        fn encode(&self, key: &Vec<u8>) -> Result<Vec<u8>> {
            Ok(key.clone())
        }
        fn decode(&self, data: &[u8]) -> Result<Vec<u8>> {
            Ok(data.to_vec())
        }
    }

    #[test]
    fn test_put_get_remove_string_key() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;

        let key1 = "my_first_blob".to_string();
        let data1 = b"Hello, Bubs!";
        let data1_bytes = Bytes::from_slice(data1);

        let mut tx = bubs.put(key1.clone())?;
        tx.write(data1)?;
        tx.finish()?;
        println!("Put completed for {}", key1);

        let retrieved1 = bubs.get(&key1)?.expect("Blob 1 should exist");
        assert_eq!(retrieved1, data1_bytes);
        println!("Get successful for {}", key1);

        let key_nonexist = "does_not_exist".to_string();
        let retrieved_none = bubs.get(&key_nonexist)?;
        assert!(retrieved_none.is_none());
        println!("Get non-existent successful");

        let removed = bubs.remove(&key1)?;
        assert!(removed);
        println!("Remove successful for {}", key1);

        let retrieved_after_remove = bubs.get(&key1)?;
        assert!(retrieved_after_remove.is_none());
        println!("Get after remove successful");

        let removed_again = bubs.remove(&key1)?;
        assert!(!removed_again);
        println!("Remove non-existent successful");

        Ok(())
    }

    #[test]
    fn test_put_get_remove_bytes_key() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), VecU8Encoder)?;

        let key1 = b"my_first_blob".to_vec();
        let data1 = b"Hello, Bubs!";
        let data1_bytes = Bytes::from_slice(data1);

        let mut tx = bubs.put(key1.clone())?;
        tx.write(data1)?;
        tx.finish()?;
        println!("Put completed for {:?}", key1);

        let retrieved1 = bubs.get(&key1)?.expect("Blob 1 should exist");
        assert_eq!(retrieved1, data1_bytes);
        println!("Get successful for {:?}", key1);

        let key_nonexist = b"does_not_exist".to_vec();
        let retrieved_none = bubs.get(&key_nonexist)?;
        assert!(retrieved_none.is_none());
        println!("Get non-existent successful");

        let removed = bubs.remove(&key1)?;
        assert!(removed);
        println!("Remove successful for {:?}", key1);

        let retrieved_after_remove = bubs.get(&key1)?;
        assert!(retrieved_after_remove.is_none());
        println!("Get after remove successful");

        let removed_again = bubs.remove(&key1)?;
        assert!(!removed_again);
        println!("Remove non-existent successful");

        Ok(())
    }

    #[test]
    fn test_get_range() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;

        let key = "range_blob".to_string();
        let data = b"0123456789abcdef";
        let _data_bytes = Bytes::from_slice(data);

        let mut tx = bubs.put(key.clone())?;
        tx.write(data)?;
        tx.finish()?;

        let r1 = bubs.get_range(&key, 0, data.len() as u64)?.unwrap();
        assert_eq!(r1.as_ref(), data);

        let r2 = bubs.get_range(&key, 4, 8)?.unwrap();
        assert_eq!(r2.as_ref(), b"4567");

        let r3 = bubs.get_range(&key, 10, data.len() as u64)?.unwrap();
        assert_eq!(r3.as_ref(), b"abcdef");

        let r4 = bubs.get_range(&key, 12, 100)?.unwrap();
        assert_eq!(r4.as_ref(), b"cdef");

        let r5 = bubs.get_range(&key, 100, 200)?.unwrap();
        assert!(r5.is_empty());

        let r6 = bubs.get_range(&key, 5, 5)?.unwrap();
        assert!(r6.is_empty());

        let r7_result = bubs.get_range(&key, 8, 4);
        assert!(r7_result.is_err());
        assert!(matches!(
            r7_result.unwrap_err().downcast_ref::<BubsError>(),
            Some(BubsError::InvalidRange(_))
        ));

        Ok(())
    }

    #[test]
    fn test_overwrite_persists() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;
        let key = "overwrite_test".to_string();

        let mut tx1 = bubs.put(key.clone())?;
        tx1.write(b"Version 1")?;
        tx1.finish()?;

        let v1 = bubs.get(&key)?.unwrap();
        assert_eq!(v1.as_ref(), b"Version 1");

        drop(bubs);
        let bubs2 = Bubs::open(dir.path(), StringEncoder)?;

        let v1_reloaded = bubs2.get(&key)?.unwrap();
        assert_eq!(v1_reloaded.as_ref(), b"Version 1");

        let mut tx2 = bubs2.put(key.clone())?;
        tx2.write(b"Version 2 is better")?;
        tx2.finish()?;

        let v2 = bubs2.get(&key)?.unwrap();
        assert_eq!(v2.as_ref(), b"Version 2 is better");

        drop(bubs2);
        let bubs3 = Bubs::open(dir.path(), StringEncoder)?;

        let v2_reloaded = bubs3.get(&key)?.unwrap();
        assert_eq!(v2_reloaded.as_ref(), b"Version 2 is better");

        Ok(())
    }

    #[test]
    fn test_transaction_drop_cleanup() -> Result<()> {
        let _ = tracing_subscriber::fmt::try_init();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;
        let key = "dropped_tx".to_string();
        let staging_dir = dir.path().join("staging");

        let staging_file_path;
        {
            let mut tx = bubs.put(key.clone())?;
            tx.write(b"This data won't be saved")?;
            staging_file_path = tx.path.clone();
            assert!(staging_file_path.exists());
        }

        assert!(
            !staging_file_path.exists(),
            "Staging file should be removed on drop"
        );

        let retrieved = bubs.get(&key)?;
        assert!(retrieved.is_none());

        let entries = std::fs::read_dir(staging_dir)?.count();
        assert_eq!(
            entries, 0,
            "Staging directory should be empty after dropped tx cleanup"
        );

        Ok(())
    }
}
