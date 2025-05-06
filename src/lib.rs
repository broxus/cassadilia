use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::fmt::Debug;
use std::fs::{File, OpenOptions};
use std::hash::{Hash, Hasher};
use std::io::{BufReader, BufWriter, Read, Write};
use std::ops::{Deref, RangeBounds};
use std::os::unix::fs::FileExt;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};

use anyhow::{Context, Result, anyhow};
use bincode::config::standard as bincode_config;
use bincode::{Decode, Encode};
use thiserror::Error;

#[derive(Debug, Error, Clone, PartialEq)]
pub enum BubsError {
    #[error("Invalid hash format: {0}")]
    InvalidHashFormat(String),
    #[error("Invalid range requested: {0}")]
    InvalidRange(String),
    #[error("Key not found")]
    KeyNotFound,
    #[error("WAL corrupted: {0}")]
    WalCorruption(String),
    #[error("WAL replay failed: {0}")]
    WalReplayFailed(String),
    #[error("WAL I/O error: {0}")]
    WalIoError(String),
}

const WAL_ENTRY_VERSION_SIZE: usize = std::mem::size_of::<u64>();
const WAL_ENTRY_OP_HASH_SIZE: usize = HASH_SIZE;
const WAL_ENTRY_OP_LEN_SIZE: usize = std::mem::size_of::<u32>();
const WAL_ENTRY_HEADER_SIZE: usize =
    WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE + WAL_ENTRY_OP_LEN_SIZE;

#[derive(Debug, Clone)]
pub enum WalOp<K> {
    Put { key: K, hash: BlobHash },
    Remove { key: K },
    RemoveMultiple { keys: Vec<K> },
}

#[derive(Encode, Decode, Debug, Clone)]
pub enum WalOpRaw {
    Put { key_bytes: Vec<u8>, hash: BlobHash },
    Remove { key_bytes: Vec<u8> },
    RemoveMultiple { key_bytes_list: Vec<Vec<u8>> },
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

struct Index<K> {
    db_root: PathBuf,
    key_encoder: Arc<dyn KeyEncoder<K>>,
    state: RwLock<BTreeMap<K, BlobHash>>,
    wal_file_lock: Mutex<Option<BufWriter<File>>>,
    wal_next_version: AtomicU64,
}

impl<K> Index<K> {
    fn wal_path(&self) -> PathBuf {
        self.db_root.join("index.wal")
    }
    fn index_file_path(&self) -> PathBuf {
        self.db_root.join("index")
    }
    fn index_tmp_path(&self) -> PathBuf {
        self.db_root.join("index.tmp")
    }
}

impl<K> Index<K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    fn new(db_root: PathBuf, key_encoder: Arc<dyn KeyEncoder<K>>) -> Result<Self> {
        if let Some(parent) = db_root.join("index.wal").parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create DB directory '{}'", parent.display()))?;
        }

        let index = Self {
            db_root,
            key_encoder,
            state: RwLock::new(BTreeMap::new()),
            wal_file_lock: Mutex::new(None),
            wal_next_version: AtomicU64::new(1),
        };

        index.load_state_from_index_file().context("Failed to load index from main file")?;

        index.replay_and_open_wal().context("Failed to replay and open WAL")?;
        index.persist_state_to_index_file().context("Failed to persist state to index file")?;
        index.truncate_wal_file().context("Failed to truncate WAL file after replay")?;

        Ok(index)
    }

    fn wal_op_to_raw(&self, op: &WalOp<K>) -> Result<WalOpRaw> {
        match op {
            WalOp::Put { key, hash } => {
                let key_bytes = self.key_encoder.encode(key)?;
                Ok(WalOpRaw::Put { key_bytes, hash: *hash })
            }
            WalOp::Remove { key } => {
                let key_bytes = self.key_encoder.encode(key)?;
                Ok(WalOpRaw::Remove { key_bytes })
            }
            WalOp::RemoveMultiple { keys } => {
                let mut key_bytes_list = Vec::with_capacity(keys.len());
                for key in keys {
                    key_bytes_list.push(self.key_encoder.encode(key)?);
                }
                Ok(WalOpRaw::RemoveMultiple { key_bytes_list })
            }
        }
    }

    fn wal_raw_to_op(&self, raw_op: WalOpRaw) -> Result<WalOp<K>> {
        match raw_op {
            WalOpRaw::Put { key_bytes, hash } => {
                let key = self.key_encoder.decode(&key_bytes)?;
                Ok(WalOp::Put { key, hash })
            }
            WalOpRaw::Remove { key_bytes } => {
                let key = self.key_encoder.decode(&key_bytes)?;
                Ok(WalOp::Remove { key })
            }
            WalOpRaw::RemoveMultiple { key_bytes_list } => {
                let mut keys = Vec::with_capacity(key_bytes_list.len());
                for key_bytes in key_bytes_list {
                    keys.push(self.key_encoder.decode(&key_bytes)?);
                }
                Ok(WalOp::RemoveMultiple { keys })
            }
        }
    }

    fn load_state_from_index_file(&self) -> Result<()> {
        let index_path = self.index_file_path();
        match std::fs::read(&index_path) {
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
                    index_path.display()
                );
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("Index file '{}' not found, starting fresh.", index_path.display());
            }
            Err(e) => {
                return Err(anyhow!(e)
                    .context(format!("Failed to read index file '{}'", index_path.display())));
            }
        }
        Ok(())
    }

    fn replay_and_open_wal(&self) -> Result<()> {
        let wal_path = self.wal_path();
        tracing::info!("Replaying WAL from '{}'", wal_path.display());

        let mut wal_file_for_read = match OpenOptions::new().read(true).open(&wal_path) {
            Ok(file) => file,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                tracing::info!("WAL file not found, starting fresh or assuming clean shutdown.");
                let file =
                    OpenOptions::new().create(true).append(true).open(&wal_path).with_context(
                        || format!("Failed to create WAL file '{}'", wal_path.display()),
                    )?;
                let mut wal_file_guard = self.wal_file_lock.lock().map_err(|_| {
                    anyhow!(BubsError::WalIoError(
                        "Failed to acquire WAL file lock for initial open".to_string()
                    ))
                })?;
                *wal_file_guard = Some(BufWriter::new(file));
                self.wal_next_version.store(1, Ordering::Release);
                return Ok(());
            }
            Err(e) => {
                return Err(BubsError::WalIoError(format!(
                    "Failed to open WAL file for read: {}",
                    e
                )))
                .with_context(|| wal_path.display().to_string());
            }
        };

        let mut state_guard = self.state.write().map_err(|_| {
            anyhow!(BubsError::WalReplayFailed(
                "Failed to acquire state write lock for WAL replay".to_string()
            ))
        })?;

        let mut last_replayed_version = 0;
        let mut entry_count = 0;

        loop {
            let mut header_buf = [0u8; WAL_ENTRY_HEADER_SIZE];
            match wal_file_for_read.read_exact(&mut header_buf) {
                Ok(_) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    let file_len = wal_file_for_read.metadata().map(|m| m.len()).unwrap_or(0);
                    let bytes_read_into_header =
                        header_buf.iter().position(|&x| x == 0).unwrap_or(WAL_ENTRY_HEADER_SIZE);

                    if e.to_string().contains("failed to fill whole buffer")
                        && bytes_read_into_header < WAL_ENTRY_HEADER_SIZE
                    {
                        tracing::debug!(
                            "Reached end of WAL after partial header ({} bytes read of {} required). Valid EOF.",
                            bytes_read_into_header,
                            WAL_ENTRY_HEADER_SIZE
                        );
                        break;
                    }
                    if entry_count == 0 && file_len < WAL_ENTRY_HEADER_SIZE as u64 {
                        tracing::debug!("WAL is empty or smaller than a header, effectively EOF.");
                        break;
                    }
                    return Err(BubsError::WalCorruption(format!(
                        "Unexpected EOF while reading WAL entry header: {}. Might be truncated. File length: {}, Header buffer partially filled: {} bytes",
                        e, file_len, bytes_read_into_header
                    )))
                        .context("WAL replay error");
                }
                Err(e) => {
                    return Err(BubsError::WalIoError(format!(
                        "Failed to read WAL entry header: {}",
                        e
                    )))
                    .context("WAL replay error");
                }
            }

            let version =
                u64::from_le_bytes(header_buf[0..WAL_ENTRY_VERSION_SIZE].try_into().unwrap());
            let mut expected_op_hash_bytes = [0u8; WAL_ENTRY_OP_HASH_SIZE];
            expected_op_hash_bytes.copy_from_slice(
                &header_buf
                    [WAL_ENTRY_VERSION_SIZE..(WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE)],
            );
            let op_len = u32::from_le_bytes(
                header_buf
                    [(WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE)..WAL_ENTRY_HEADER_SIZE]
                    .try_into()
                    .unwrap(),
            ) as usize;

            if op_len == 0 {
                return Err(BubsError::WalCorruption(format!(
                    "WAL entry version {} has zero op length",
                    version
                )))
                .context("WAL replay error");
            }

            let mut op_data_buf = vec![0u8; op_len];
            wal_file_for_read.read_exact(&mut op_data_buf).with_context(|| {
                BubsError::WalCorruption(format!(
                    "Failed to read op data for WAL entry version {}",
                    version
                ))
            })?;

            let actual_op_hash = calculate_blob_hash(&op_data_buf);
            if actual_op_hash.0 != expected_op_hash_bytes {
                return Err(BubsError::WalCorruption(format!(
                    "Checksum mismatch for WAL entry version {}. Expected {}, got {}.",
                    version,
                    BlobHash(expected_op_hash_bytes),
                    actual_op_hash
                )))
                .context("WAL replay error");
            }

            let raw_op: WalOpRaw = bincode::decode_from_slice(&op_data_buf, bincode_config())
                .map_err(|e| {
                    BubsError::WalCorruption(format!(
                        "Failed to decode WalOpRaw for version {}: {}",
                        version, e
                    ))
                })?
                .0;

            let logical_op = self.wal_raw_to_op(raw_op).with_context(|| {
                BubsError::WalReplayFailed(format!(
                    "Failed to convert WalOpRaw to WalOp for version {}",
                    version
                ))
            })?;

            match logical_op {
                WalOp::Put { key, hash } => {
                    state_guard.insert(key, hash);
                }
                WalOp::Remove { key } => {
                    state_guard.remove(&key);
                }
                WalOp::RemoveMultiple { keys } => {
                    for key in keys {
                        state_guard.remove(&key);
                    }
                }
            }
            last_replayed_version = version;
            entry_count += 1;
        }
        drop(state_guard);

        tracing::info!(
            "Successfully replayed {} entries from WAL. Last version: {}.",
            entry_count,
            last_replayed_version
        );
        self.wal_next_version.store(last_replayed_version + 1, Ordering::Release);

        let file =
            OpenOptions::new().create(true).append(true).open(&wal_path).with_context(|| {
                BubsError::WalIoError(format!(
                    "Failed to open WAL file '{}' for appending after replay",
                    wal_path.display()
                ))
            })?;

        let mut wal_file_guard = self.wal_file_lock.lock().map_err(|_| {
            anyhow!(BubsError::WalIoError(
                "Failed to acquire WAL file lock post-replay".to_string()
            ))
        })?;
        *wal_file_guard = Some(BufWriter::new(file));

        Ok(())
    }

    pub fn apply_wal_op(&self, logical_op: &WalOp<K>) -> Result<()> {
        let raw_op = self.wal_op_to_raw(logical_op)?;
        let op_data = bincode::encode_to_vec(&raw_op, bincode_config())
            .map_err(|e| BubsError::WalIoError(format!("Failed to serialize WalOpRaw: {}", e)))?;

        let version = self.wal_next_version.fetch_add(1, Ordering::SeqCst);
        let op_hash = calculate_blob_hash(&op_data);
        let op_len = op_data.len() as u32;

        let mut wal_file_guard = self.wal_file_lock.lock().map_err(|_| {
            anyhow!(BubsError::WalIoError(
                "Failed to acquire WAL file lock for writing".to_string()
            ))
        })?;

        let wal_writer = wal_file_guard
            .as_mut()
            .ok_or_else(|| BubsError::WalIoError("WAL file not open for writing".to_string()))?;

        wal_writer
            .write_all(&version.to_le_bytes())
            .and_then(|_| wal_writer.write_all(&op_hash.0))
            .and_then(|_| wal_writer.write_all(&op_len.to_le_bytes()))
            .and_then(|_| wal_writer.write_all(&op_data))
            .map_err(|e| BubsError::WalIoError(format!("Failed to write WAL entry: {}", e)))?;

        wal_writer
            .flush()
            .map_err(|e| BubsError::WalIoError(format!("Failed to flush WAL writer: {}", e)))?;
        wal_writer
            .get_ref()
            .sync_data()
            .map_err(|e| BubsError::WalIoError(format!("Failed to sync WAL data: {}", e)))?;

        drop(wal_file_guard);

        tracing::trace!(version, ?logical_op, "Logged WAL op, applying to state");

        let mut state_guard = self
            .state
            .write()
            .map_err(|_| anyhow!("Failed to acquire state write lock for applying WAL op"))?;
        match logical_op {
            WalOp::Put { key, hash } => {
                state_guard.insert(key.clone(), *hash);
            }
            WalOp::Remove { key } => {
                state_guard.remove(key);
            }
            WalOp::RemoveMultiple { keys } => {
                for key in keys {
                    state_guard.remove(key);
                }
            }
        }
        Ok(())
    }

    pub fn checkpoint(&self) -> Result<()> {
        tracing::info!("Starting checkpoint operation.");
        self.persist_state_to_index_file()
            .context("Failed to persist state to index file during checkpoint")?;
        self.truncate_wal_file().context("Failed to truncate WAL file during checkpoint")?;
        tracing::info!("Checkpoint completed successfully.");
        Ok(())
    }

    fn persist_state_to_index_file(&self) -> Result<()> {
        let index_path = self.index_file_path();
        let index_tmp_path = self.index_tmp_path();

        tracing::debug!(
            "Persisting index to '{}' (via temporary '{}')",
            index_path.display(),
            index_tmp_path.display()
        );

        let state_guard = self
            .state
            .read()
            .map_err(|_| anyhow!("Failed to acquire state read lock for persisting index"))?;

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
            .open(&index_tmp_path)
            .context(format!(
                "Failed to create/open temporary index file '{}'",
                index_tmp_path.display()
            ))?;

        temp_file.write_all(&state_bytes).context(format!(
            "Failed to write serialized index to temporary file '{}'",
            index_tmp_path.display()
        ))?;

        temp_file.sync_data().context(format!(
            "Failed to sync temporary index file '{}'",
            index_tmp_path.display()
        ))?;
        drop(temp_file);

        std::fs::rename(&index_tmp_path, &index_path).context(format!(
            "Failed to atomically rename temporary index '{}' to final index '{}'",
            index_tmp_path.display(),
            index_path.display()
        ))?;

        tracing::info!(
            "Successfully persisted index with {} entries to '{}'",
            state_guard.len(),
            index_path.display()
        );
        Ok(())
    }

    fn truncate_wal_file(&self) -> Result<()> {
        let wal_path = self.wal_path();
        let mut wal_file_guard = self.wal_file_lock.lock().map_err(|_| {
            anyhow!(BubsError::WalIoError(
                "Failed to acquire WAL file lock for truncation".to_string()
            ))
        })?;

        if let Some(writer) = wal_file_guard.take() {
            drop(writer);
        }

        OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&wal_path)
            .and_then(|file| file.set_len(0))
            .with_context(|| {
                BubsError::WalIoError(format!(
                    "Failed to truncate WAL file '{}'",
                    wal_path.display()
                ))
            })?;

        tracing::debug!("Truncated WAL file: {}", wal_path.display());

        let file =
            OpenOptions::new().create(true).append(true).open(&wal_path).with_context(|| {
                BubsError::WalIoError(format!(
                    "Failed to re-open WAL file '{}' for appending after truncation",
                    wal_path.display()
                ))
            })?;
        *wal_file_guard = Some(BufWriter::new(file));
        self.wal_next_version.store(1, Ordering::Release);

        Ok(())
    }

    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        self.state.read().expect("mutex poisoned").get(key).copied()
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.state.read().expect("mutex poisoned").contains_key(key)
    }

    pub fn known_keys(&self) -> BTreeSet<K> {
        self.state.read().expect("mutex poisoned").keys().cloned().collect()
    }

    pub fn index_snapshot(&self) -> BTreeMap<K, BlobHash> {
        self.state.read().expect("mutex poisoned").clone()
    }
}

impl<K: Debug> Debug for Index<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Index")
            .field("db_root", &self.db_root)
            .field("wal_path", &self.wal_path())
            .field("index_file_path", &self.index_file_path())
            .field("wal_next_version", &self.wal_next_version.load(Ordering::Relaxed))
            .field("state_len (approx)", &self.state.read().map(|s| s.len()).unwrap_or(0))
            .finish()
    }
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
    index: Index<K>,
}

impl<K> Debug for BubsInner<K> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BubsInner")
            .field("staging_root", &self.staging_root)
            .field("cas_root", &self.cas_root)
            .finish()
    }
}

impl<K> BubsInner<K>
where
    K: Clone + Eq + Ord + Debug + Send + Sync + 'static,
{
    fn new(db_root: PathBuf, key_encoder: impl KeyEncoder<K> + 'static) -> Result<Self> {
        let key_encoder = Arc::new(key_encoder);

        let staging_root = db_root.join("staging");
        let cas_root = db_root.join("cas");

        std::fs::create_dir_all(&db_root).with_context(|| {
            format!("Failed to create DB root directory '{}'", db_root.display())
        })?;
        std::fs::create_dir_all(&staging_root).context("Failed to create staging directory")?;
        std::fs::create_dir_all(&cas_root).context("Failed to create CAS directory")?;

        let index = Index::new(db_root, key_encoder)?;

        Ok(Self { staging_root, cas_root, index })
    }

    pub fn put(&self, key: K) -> Result<Transaction<K>> {
        Transaction::new(self, key)
    }

    pub fn get_hash_for_key(&self, key: &K) -> Option<BlobHash> {
        self.index.get_hash_for_key(key)
    }

    pub fn contains_key(&self, key: &K) -> bool {
        self.index.contains_key(key)
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
                Err(e) => Err(anyhow!(e))
                    .context(format!("Failed to read blob file '{}'", full_path.display())),
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
                        "Index contains key '{:?}' pointing to hash '{}', but CAS file '{}' not found for size check!",
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
            let file = File::open(&full_path)
                .context(format!("Failed to open blob file '{}'", full_path.display()))?;
            Ok(BufReader::new(file))
        } else {
            Err(anyhow!(BubsError::KeyNotFound))
        }
    }

    pub fn known_keys(&self) -> BTreeSet<K> {
        self.index.known_keys()
    }

    pub fn index_snapshot(&self) -> BTreeMap<K, BlobHash> {
        self.index.index_snapshot()
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
                        "Index contains key '{:?}' pointing to hash '{}', but CAS file '{}' not found for range read!",
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

            let len = file.metadata().context("Failed to get blob file metadata")?.len();
            let start = std::cmp::min(range_start, len);
            let end = std::cmp::min(range_end, len);

            anyhow::ensure!(
                start <= end,
                BubsError::InvalidRange(format!("Calculated Start {} > End {}", start, end))
            );

            let read_len = end - start;
            if read_len == 0 {
                return Ok(Some(bytes::Bytes::new()));
            }
            let mut buff = vec![0; read_len as usize];

            let bytes_read = file.read_at(&mut buff, start).context(format!(
                "Failed to read range [{}, {}) from blob '{}'",
                start,
                end,
                full_path.display()
            ))?;

            buff.truncate(bytes_read);
            Ok(Some(bytes::Bytes::from(buff)))
        } else {
            Ok(None)
        }
    }

    pub fn remove(&self, key: &K) -> Result<bool> {
        if self.index.contains_key(key) {
            tracing::debug!("Removing key '{:?}' from index via WAL", key);
            let op = WalOp::Remove { key: key.clone() };
            self.index.apply_wal_op(&op).context("Failed to log and apply remove operation")?;
            Ok(true)
        } else {
            tracing::debug!("Attempted to remove non-existent key '{:?}'", key);
            Ok(false)
        }
    }

    pub fn remove_range<R>(&self, range: R) -> Result<usize>
    where
        R: RangeBounds<K> + Debug + Clone,
    {
        let keys_to_remove: Vec<K> = {
            let state_guard = self.index.state.read().map_err(|_| {
                anyhow!("Failed to acquire state read lock for remove_range key collection")
            })?;
            state_guard.range(range).map(|(k, _v)| k.clone()).collect()
        };

        let remove_count = keys_to_remove.len();
        if remove_count == 0 {
            return Ok(0);
        }

        tracing::debug!(
            count = remove_count,
            "remove_range: Logging removal of {} keys via WAL.",
            remove_count
        );

        let op = WalOp::RemoveMultiple { keys: keys_to_remove };
        self.index.apply_wal_op(&op).context("Failed to log and apply remove_range operation")?;

        tracing::debug!(
            count = remove_count,
            "remove_range: Successfully logged and applied removal of {} keys.",
            remove_count
        );
        Ok(remove_count)
    }

    pub fn checkpoint(&self) -> Result<()> {
        self.index.checkpoint()
    }

    fn commit_transaction(&self, mut transaction: Transaction<K>) -> Result<()> {
        transaction.writer.flush()?;
        transaction.writer.get_mut().sync_data()?;

        let hash = transaction.hasher.finalize();
        let blob_hash = BlobHash::from_bytes(*hash.as_bytes());

        let relative_cas_path = blob_hash.relative_path();
        let final_cas_path = self.cas_root.join(&relative_cas_path);

        if let Some(parent) = final_cas_path.parent() {
            std::fs::create_dir_all(parent)
                .context(format!("Failed to create CAS subdirectory '{}'", parent.display()))?;
        }

        if !final_cas_path.exists() {
            std::fs::rename(&transaction.path, &final_cas_path).context(format!(
                "Failed to move staged file '{}' to CAS '{}'",
                transaction.path.display(),
                final_cas_path.display()
            ))?;
            tracing::debug!("Moved blob {} to CAS path '{}'", blob_hash, final_cas_path.display());
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

        let op = WalOp::Put { key: transaction.key.clone(), hash: blob_hash };
        self.index
            .apply_wal_op(&op)
            .context("Failed to log and apply put operation during commit")?;

        tracing::debug!(
            "Updated index for key '{:?}' to hash {} via WAL",
            transaction.key,
            blob_hash
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
            .context(format!("Failed to create staging file '{}'", path.display()))?;

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
        self.writer.write_all(data).context("Failed to write data to staging file")?;
        Ok(())
    }

    pub fn finish(self) -> Result<()> {
        let key_clone_for_debug = self.key.clone();
        tracing::debug!("Finishing transaction for key '{:?}'", key_clone_for_debug);
        self.bubs_inner
            .commit_transaction(self)
            .context(format!("Failed to commit transaction for key '{:?}'", key_clone_for_debug))
    }
}

impl<K> Drop for Transaction<'_, K>
where
    K: Debug,
{
    fn drop(&mut self) {
        if self.path.exists() {
            tracing::warn!(
                "Transaction for key '{:?}' dropped without calling finish(). Cleaning up staging file: {}",
                self.key,
                self.path.display()
            );
            if let Err(e) = std::fs::remove_file(&self.path) {
                tracing::error!(
                    "Failed to clean up staging file '{}' for dropped transaction of key '{:?}': {}",
                    self.path.display(),
                    self.key,
                    e
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use tempfile::tempdir;

    use super::*;

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

    fn setup_tracing() {
        let _ = tracing_subscriber::fmt::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    }

    #[test]
    fn test_put_get_remove_string_key() -> Result<()> {
        setup_tracing();
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
        assert_eq!(retrieved1.as_ref(), data1_bytes.as_ref());
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
        setup_tracing();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), VecU8Encoder)?;

        let key1 = b"my_bytes_blob".to_vec();
        let data1 = b"Binary data here!";
        let data1_bytes = Bytes::from_slice(data1);

        let mut tx = bubs.put(key1.clone())?;
        tx.write(data1)?;
        tx.finish()?;

        let retrieved1 = bubs.get(&key1)?.expect("Blob 1 (bytes key) should exist");
        assert_eq!(retrieved1.as_ref(), data1_bytes.as_ref());

        let removed = bubs.remove(&key1)?;
        assert!(removed);
        assert!(bubs.get(&key1)?.is_none());

        Ok(())
    }

    #[test]
    fn test_get_range() -> Result<()> {
        setup_tracing();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;

        let key = "range_blob".to_string();
        let data = b"0123456789abcdef";

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
    fn test_overwrite_persists_across_reopen() -> Result<()> {
        setup_tracing();
        let dir = tempdir()?;
        let db_path = dir.path();

        {
            let bubs1 = Bubs::open(db_path, StringEncoder)?;
            let key = "overwrite_test".to_string();

            let mut tx1 = bubs1.put(key.clone())?;
            tx1.write(b"Version 1")?;
            tx1.finish()?;

            let v1 = bubs1.get(&key)?.unwrap();
            assert_eq!(v1.as_ref(), b"Version 1");
        }

        {
            let bubs2 = Bubs::open(db_path, StringEncoder)?;
            let key = "overwrite_test".to_string();

            let v1_reloaded = bubs2.get(&key)?.unwrap();
            assert_eq!(v1_reloaded.as_ref(), b"Version 1", "Should load Version 1 after reopen");

            let mut tx2 = bubs2.put(key.clone())?;
            tx2.write(b"Version 2 is better")?;
            tx2.finish()?;

            let v2 = bubs2.get(&key)?.unwrap();
            assert_eq!(v2.as_ref(), b"Version 2 is better");
        }

        {
            let bubs3 = Bubs::open(db_path, StringEncoder)?;
            let key = "overwrite_test".to_string();

            let v2_reloaded = bubs3.get(&key)?.unwrap();
            assert_eq!(
                v2_reloaded.as_ref(),
                b"Version 2 is better",
                "Should load Version 2 after second reopen"
            );
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
            let bubs1 = Bubs::open(db_path, StringEncoder)?;

            let mut tx = bubs1.put(key.clone())?;
            tx.write(b"Data to be removed")?;
            tx.finish()?;
            assert!(bubs1.get(&key)?.is_some(), "Data should exist before removal");

            let removed = bubs1.remove(&key)?;
            assert!(removed, "Remove operation should succeed");
            assert!(bubs1.get(&key)?.is_none(), "Data should be gone after removal");
        }

        {
            let bubs2 = Bubs::open(db_path, StringEncoder)?;
            assert!(bubs2.get(&key)?.is_none(), "Data should remain removed after reopen");
        }
        Ok(())
    }

    #[test]
    fn test_transaction_drop_cleanup() -> Result<()> {
        setup_tracing();
        let dir = tempdir()?;
        let bubs = Bubs::open(dir.path(), StringEncoder)?;
        let key = "dropped_tx_key".to_string();
        let staging_dir = dir.path().join("staging");

        let staging_file_path_opt;
        {
            let mut tx = bubs.put(key.clone())?;
            tx.write(b"This data won't be saved because tx is dropped")?;
            staging_file_path_opt = Some(tx.path.clone());
            assert!(
                staging_file_path_opt.as_ref().unwrap().exists(),
                "Staging file should exist during transaction"
            );
        }

        let staging_file_path = staging_file_path_opt.unwrap();
        assert!(
            !staging_file_path.exists(),
            "Staging file '{}' should be removed on drop",
            staging_file_path.display()
        );

        let retrieved = bubs.get(&key)?;
        assert!(retrieved.is_none(), "Key should not exist in index if transaction was dropped");

        let entries = std::fs::read_dir(staging_dir)?.count();
        assert_eq!(
            entries, 0,
            "Staging directory should be empty after dropped tx cleanup (assuming serial tests)"
        );

        Ok(())
    }

    #[test]
    fn test_checkpoint_functionality() -> Result<()> {
        setup_tracing();
        let dir = tempdir()?;
        let db_path = dir.path();
        let key1 = "cp_key1".to_string();
        let data1 = b"checkpoint_data1";
        let key2 = "cp_key2".to_string();
        let data2 = b"checkpoint_data2";

        {
            let bubs1 = Bubs::open(db_path, StringEncoder)?;
            let mut tx1 = bubs1.put(key1.clone())?;
            tx1.write(data1)?;
            tx1.finish()?;

            let mut tx2 = bubs1.put(key2.clone())?;
            tx2.write(data2)?;
            tx2.finish()?;

            assert_eq!(bubs1.get(&key1)?.unwrap().as_ref(), data1);
            assert_eq!(bubs1.get(&key2)?.unwrap().as_ref(), data2);

            let wal_path = db_path.join("index.wal");
            assert!(wal_path.exists(), "WAL file should exist before checkpoint");
            let wal_size_before = wal_path.metadata()?.len();
            assert!(wal_size_before > 0, "WAL file should have content before checkpoint");

            bubs1.checkpoint()?;

            let wal_size_after = wal_path.metadata()?.len();
            assert!(
                wal_size_after < wal_size_before || wal_size_after == 0,
                "WAL file should be smaller or empty after checkpoint"
            );

            let index_file_path = db_path.join("index");
            assert!(index_file_path.exists(), "Index file should exist after checkpoint");
            assert!(
                index_file_path.metadata()?.len() > 0,
                "Index file should have content after checkpoint"
            );
        }

        {
            let bubs2 = Bubs::open(db_path, StringEncoder)?;
            assert_eq!(
                bubs2.get(&key1)?.unwrap().as_ref(),
                data1,
                "Data1 should persist after checkpoint and reopen"
            );
            assert_eq!(
                bubs2.get(&key2)?.unwrap().as_ref(),
                data2,
                "Data2 should persist after checkpoint and reopen"
            );

            let wal_path = db_path.join("index.wal");
            let wal_metadata = wal_path.metadata()?;
            assert!(
                wal_metadata.len() < WAL_ENTRY_HEADER_SIZE as u64,
                "WAL should be small/empty after reopening post-checkpoint"
            );
        }
        Ok(())
    }

    #[test]
    fn test_remove_range_persists() -> Result<()> {
        setup_tracing();
        let dir = tempdir()?;
        let db_path = dir.path();

        {
            let bubs = Bubs::open(db_path, StringEncoder)?;
            let keys_to_insert = ["alpha", "beta", "gamma", "delta", "epsilon"];
            for (i, key_name) in keys_to_insert.iter().enumerate() {
                let mut tx = bubs.put(key_name.to_string())?;
                tx.write(format!("data for {}", i).as_bytes())?;
                tx.finish()?;
            }

            let count = bubs.remove_range("beta".to_string()..="gamma".to_string())?;
            assert_eq!(count, 4, "Should remove beta, delta, epsilon, and gamma");

            assert!(bubs.get(&"alpha".to_string())?.is_some(), "alpha should remain");
            assert!(bubs.get(&"beta".to_string())?.is_none(), "beta should be removed");
            assert!(bubs.get(&"delta".to_string())?.is_none(), "delta should be removed");
            assert!(bubs.get(&"epsilon".to_string())?.is_none(), "epsilon should be removed");
            assert!(bubs.get(&"gamma".to_string())?.is_none(), "gamma should be removed");
        }

        {
            let bubs = Bubs::open(db_path, StringEncoder)?;
            assert!(
                bubs.get(&"alpha".to_string())?.is_some(),
                "alpha should still exist after reopen"
            );
            assert!(
                bubs.get(&"beta".to_string())?.is_none(),
                "beta should still be removed after reopen"
            );
            assert!(
                bubs.get(&"delta".to_string())?.is_none(),
                "delta should still be removed after reopen"
            );
            assert!(
                bubs.get(&"epsilon".to_string())?.is_none(),
                "epsilon should still be removed after reopen"
            );
            assert!(
                bubs.get(&"gamma".to_string())?.is_none(),
                "gamma should still be removed after reopen"
            );
        }
        Ok(())
    }
}
