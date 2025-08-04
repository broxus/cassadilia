use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
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

    /// Returns the relative path for this blob hash, suitable for storing in a directory structure.
    /// The path will have 2 levels /h/a/sh
    pub fn relative_path(&self) -> PathBuf {
        let hex = self.to_hex();
        let top_level_dir = &hex[0..2];
        // Use the next 2 characters for the second-level directory.
        let second_level_dir = &hex[2..4];
        // The rest of the hash becomes the filename.
        let truncated_filename = &hex[4..];

        // Join the parts to form the final path: "ab/12/cdef..."
        PathBuf::from(top_level_dir).join(second_level_dir).join(truncated_filename)
    }

    pub fn from_relative_path(path: &Path) -> Result<Self, TypesError> {
        let components: Vec<_> = path.components().collect();
        if components.len() < 3 {
            return Err(TypesError::InvalidHashFormat(hex::FromHexError::InvalidStringLength));
        }
        let last_3 = &components[components.len() - 3..];
        let mut buf = Vec::with_capacity(HASH_SIZE * 2);

        for component in last_3.iter() {
            let component = component.as_os_str().as_encoded_bytes();
            buf.extend_from_slice(component);
        }

        let mut res = [0u8; HASH_SIZE];
        hex::decode_to_slice(&buf, &mut res).map_err(TypesError::InvalidHashFormat)?;

        Ok(Self(res))
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

// === config ===
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
    /// Whether to pre-create directories for CAS blobs
    /// Will create 65k dirs on the first run
    pub pre_create_cas_dirs: bool,
    /// Whether to scan for orphaned blobs on startup
    pub scan_orphans_on_startup: bool,
    /// Also verify blob integrity during the orphan scan
    pub verify_blob_integrity: bool,
    pub fail_on_integrity_errors: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            sync_mode: SyncMode::default(),
            num_ops_per_wal: 100_000,
            pre_create_cas_dirs: false,
            scan_orphans_on_startup: true,
            verify_blob_integrity: false,
            fail_on_integrity_errors: true,
        }
    }
}

// === orphan recovery ===
// Moved to orphan.rs

// === core ops ===

#[derive(Debug, Clone)]
pub enum WalOpRaw {
    Put { key_bytes: Vec<u8>, hash: BlobHash, size: u64 },
    Remove { keys_bytes: Vec<Vec<u8>> },
}

#[derive(Debug, Clone, PartialEq)]
pub enum WalOp<K> {
    Put { key: K, hash: BlobHash, size: u64 },
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
            WalOpRaw::Put { key_bytes, hash, size } => {
                let key = key_encoder
                    .decode(&key_bytes)
                    .map_err(|e| TypesError::DecodeKeyPutFailed { source: e })?;
                Ok(WalOp::Put { key, hash, size })
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
            WalOp::Put { key, hash, size } => {
                let key_bytes = key_encoder.encode(key)?;
                Ok(WalOpRaw::Put { key_bytes, hash: *hash, size: *size })
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

// === WAL ===

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

// ===  Utility Types ===

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

    pub(crate) fn lock_arc(&self) -> parking_lot::ArcMutexGuard<parking_lot::RawMutex, ()> {
        self.lock.clone().lock_arc()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_blob_hash_relative_path_roundtrip() {
        // Test that converting to relative path and back gives the same hash
        let original_hex = "abcdef1234567890abcdef1234567890abcdef1234567890abcdef1234567890";
        let hash = BlobHash::from_hex(original_hex).unwrap();

        let relative_path = hash.relative_path();
        let reconstructed = BlobHash::from_relative_path(&relative_path).unwrap();

        assert_eq!(hash, reconstructed);
        assert_eq!(hash.to_hex(), reconstructed.to_hex());
    }

    #[test]
    fn test_blob_hash_from_relative_path_valid_cases() {
        // Create test hashes using proper generation
        let zero_hash = BlobHash([0u8; HASH_SIZE]);
        let ff_hash = BlobHash([0xffu8; HASH_SIZE]);

        // Create a test hash with known pattern
        let mut test_bytes = [0u8; HASH_SIZE];
        for (i, byte) in test_bytes.iter_mut().enumerate() {
            *byte = (i % 256) as u8;
        }
        let test_hash = BlobHash(test_bytes);

        // Test that from_relative_path correctly reconstructs these hashes
        for hash in &[zero_hash, ff_hash, test_hash] {
            let path = hash.relative_path();
            let reconstructed = BlobHash::from_relative_path(&path).unwrap();
            assert_eq!(hash, &reconstructed);
        }

        // Test with leading path components
        let hash = BlobHash([0x12u8; HASH_SIZE]);
        let relative = hash.relative_path();
        let full_path = PathBuf::from("/var/data/cas").join(&relative);
        let reconstructed = BlobHash::from_relative_path(&full_path).unwrap();
        assert_eq!(hash, reconstructed);
    }

    #[test]
    fn test_blob_hash_from_relative_path_error_cases() {
        // Test invalid path structures
        let invalid_paths = vec![
            // Too few components
            PathBuf::from("ab/cdef1234"),
            PathBuf::from("abcdef1234"),
            PathBuf::from(""),
            // Invalid hex in components
            PathBuf::from("zz/cd/ef1234567890abcdef1234567890abcdef1234567890abcdef12345678"),
            PathBuf::from("ab/zz/ef1234567890abcdef1234567890abcdef1234567890abcdef12345678"),
            PathBuf::from("ab/cd/zz1234567890abcdef1234567890abcdef1234567890abcdef12345678"),
            // Wrong length (not 64 hex chars total)
            PathBuf::from("ab/cd/ef12"),
            PathBuf::from("ab/cd/ef1234567890abcdef1234567890abcdef1234567890abcdef123456789"), /* 65 chars */
        ];

        for path in invalid_paths {
            assert!(
                BlobHash::from_relative_path(&path).is_err(),
                "Expected error for path: {path:?}",
            );
        }
    }

    #[test]
    fn test_blob_hash_relative_path_structure() {
        // Test that relative_path creates the expected structure
        let hash =
            BlobHash::from_hex("1234567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef")
                .unwrap();

        let path = hash.relative_path();
        let components: Vec<_> =
            path.components().map(|c| c.as_os_str().to_str().unwrap()).collect();

        assert_eq!(components.len(), 3);
        assert_eq!(components[0], "12");
        assert_eq!(components[1], "34");
        assert_eq!(components[2], "567890abcdef1234567890abcdef1234567890abcdef1234567890abcdef");
    }

    #[test]
    fn test_blob_hash_from_relative_path_with_prefix() {
        // Create a proper hash using byte array
        let hash = BlobHash([0xabu8; HASH_SIZE]);
        let relative = hash.relative_path();

        // Test that only the last 3 components are used
        let long_path = PathBuf::from("/var/data/cas").join(&relative);
        let short_path = relative.clone();

        let hash1 = BlobHash::from_relative_path(&long_path).unwrap();
        let hash2 = BlobHash::from_relative_path(&short_path).unwrap();

        assert_eq!(hash1, hash2);
        assert_eq!(hash1, hash);
    }
}
