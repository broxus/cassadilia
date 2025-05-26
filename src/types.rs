use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::Mutex;
use thiserror::Error;

pub const HASH_SIZE: usize = blake3::OUT_LEN;

#[derive(Clone, Copy, Eq, Ord, PartialOrd)]
pub struct BlobHash(pub [u8; HASH_SIZE]);

impl BlobHash {
    pub fn as_bytes(&self) -> &[u8; HASH_SIZE] {
        &self.0
    }

    pub fn to_hex(&self) -> String {
        hex::encode(self.0)
    }

    pub fn from_hex(hex_str: &str) -> Result<Self, TypesError> {
        let mut bytes = [0u8; HASH_SIZE];
        hex::decode_to_slice(hex_str, &mut bytes)?;
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

impl Debug for BlobHash {
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

#[derive(Error, Debug, Clone, PartialEq)]
pub enum KeyEncoderError {
    #[error("Failed to encode key")]
    EncodeError,
    #[error("Failed to decode key")]
    DecodeError,
}

pub trait KeyEncoder<K>: Send + Sync + 'static {
    fn encode(&self, key: &K) -> Result<Vec<u8>, KeyEncoderError>;
    fn decode(&self, data: &[u8]) -> Result<K, KeyEncoderError>;
}

// ========== config ==========
#[derive(Clone, Copy, Debug, Default)]
pub enum SyncMode {
    /// Waits for fdatasync
    #[default]
    Sync,
    /// Fdatasync are done by a background thread
    Async,
}

#[derive(Clone, Debug)]
pub struct Config {
    pub sync_mode: SyncMode,
    pub num_ops_per_wal: u64,
}

impl Default for Config {
    fn default() -> Self {
        Config { sync_mode: SyncMode::default(), num_ops_per_wal: 100_000 }
    }
}

// ========== core ops ==========

#[derive(Debug, Clone)]
pub enum WalOpRaw {
    Put { key_bytes: Vec<u8>, hash: BlobHash },
    Remove { keys_bytes: Vec<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalOp<K> {
    Put { key: K, hash: BlobHash },
    Remove { keys: Vec<K> },
}

impl<K> WalOp<K> {
    pub fn from_raw(
        raw_op: WalOpRaw,
        key_encoder: &Arc<dyn KeyEncoder<K> + Send + Sync>,
    ) -> Result<Self, TypesError>
    where
        K: 'static,
    {
        match raw_op {
            WalOpRaw::Put { key_bytes, hash } => {
                let key = key_encoder
                    .decode(&key_bytes)
                    .map_err(|e| TypesError::DecodeKeyPutFailed { source: e })?;
                Ok(WalOp::Put { key, hash })
            }
            WalOpRaw::Remove { keys_bytes } => {
                let mut keys = Vec::new();
                for (i, key_bytes) in keys_bytes.iter().enumerate() {
                    let key = key_encoder
                        .decode(key_bytes)
                        .map_err(|e| TypesError::RemoveDecodeKey { index: i, source: e })?;
                    keys.push(key);
                }
                Ok(WalOp::Remove { keys })
            }
        }
    }

    pub fn to_raw(&self, key_encoder: &Arc<dyn KeyEncoder<K>>) -> Result<WalOpRaw, KeyEncoderError>
    where
        K: 'static,
    {
        match self {
            WalOp::Put { key, hash } => {
                let key_bytes = key_encoder.encode(key)?;
                Ok(WalOpRaw::Put { key_bytes, hash: *hash })
            }
            WalOp::Remove { keys } => {
                let mut keys_bytes = Vec::new();
                for key in keys {
                    let key_bytes = key_encoder.encode(key)?;
                    keys_bytes.push(key_bytes);
                }
                Ok(WalOpRaw::Remove { keys_bytes })
            }
        }
    }
}

#[derive(Error, Debug, Clone, PartialEq)]
pub enum TypesError {
    #[error("Invalid hash format")]
    InvalidHashFormat(#[from] hex::FromHexError),

    #[error("Remove: insufficient data for num_keys")]
    RemoveInsufficientDataNumKeys,
    #[error("Remove: failed to convert num_keys bytes")]
    RemoveNumKeysConversion,
    #[error("Remove: insufficient data for key length at index {index}")]
    RemoveInsufficientDataKeyLength { index: usize },
    #[error("Remove: failed to convert key length bytes at index {index}")]
    RemoveKeyLengthConversion { index: usize },
    #[error("Remove: insufficient data for key bytes at index {index}")]
    RemoveInsufficientDataKeyBytes { index: usize },
    #[error("Remove: failed to decode key at index {index}")]
    RemoveDecodeKey {
        index: usize,
        #[source]
        source: KeyEncoderError,
    },

    #[error("Failed to decode key for Put operation")]
    DecodeKeyPutFailed {
        #[source]
        source: KeyEncoderError,
    },
}

// ========== WAL ==========

/// Represents the checkpoint state of the WAL
pub(crate) type CheckpointState = Option<u64>;

pub const WAL_ENTRY_VERSION_SIZE: usize = size_of::<u64>();
pub const WAL_ENTRY_OP_HASH_SIZE: usize = HASH_SIZE;
pub const WAL_ENTRY_OP_LEN_SIZE: usize = size_of::<u32>();
pub const WAL_ENTRY_HEADER_SIZE: usize =
    WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE + WAL_ENTRY_OP_LEN_SIZE;

/// Represents a discovered WAL segment
#[derive(Debug, Clone)]
pub(crate) struct SegmentInfo {
    pub id: u64,
    pub path: PathBuf,
}

impl SegmentInfo {
    pub(crate) fn new(id: u64, path: PathBuf) -> Self {
        Self { id, path }
    }
}

// ==========  Utility Types ==========

#[derive(Clone, Debug)]
pub(crate) struct FsLock {
    lock: Arc<Mutex<()>>,
}

impl FsLock {
    pub(crate) fn new() -> Self {
        FsLock { lock: Arc::new(Mutex::new(())) }
    }

    pub(crate) fn lock(&self) -> parking_lot::MutexGuard<'_, ()> {
        self.lock.lock()
    }
}
