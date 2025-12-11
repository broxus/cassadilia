use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::num::NonZeroU64;
use std::path::{Path, PathBuf};

use thiserror::Error;

use crate::cas_manager::CasManagerError;

pub const HASH_SIZE: usize = blake3::OUT_LEN;

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CasStats {
    pub unique_blobs: u64,
    pub total_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IndexStats {
    pub serialized_size_bytes: u64,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DbStats {
    pub cas: CasStats,
    pub index: IndexStats,
}

#[derive(Clone, Copy, Eq, Ord, PartialOrd)]
pub struct BlobHash(pub [u8; HASH_SIZE]);

impl BlobHash {
    #[must_use]
    pub fn as_bytes(&self) -> &[u8; HASH_SIZE] {
        &self.0
    }

    #[must_use]
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
    #[must_use]
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

    #[must_use]
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

pub trait KeyBytes: Sized {
    type Bytes: AsRef<[u8]> + AsMut<[u8]>;

    fn to_key_bytes(&self) -> Self::Bytes;

    #[inline]
    fn to_key_bytes_owned(&self) -> Vec<u8> {
        self.to_key_bytes().as_ref().to_vec()
    }

    fn from_key_bytes(bytes: &[u8]) -> Option<Self>;
}

impl KeyBytes for String {
    type Bytes = Vec<u8>;

    fn to_key_bytes(&self) -> Self::Bytes {
        self.as_bytes().to_vec()
    }

    fn to_key_bytes_owned(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        str::from_utf8(bytes).map(String::from).ok()
    }
}

impl KeyBytes for Vec<u8> {
    type Bytes = Vec<u8>;

    fn to_key_bytes(&self) -> Self::Bytes {
        self.clone()
    }

    fn to_key_bytes_owned(&self) -> Vec<u8> {
        self.clone()
    }

    fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        Some(Self::from(bytes))
    }
}

impl<const LEN: usize> KeyBytes for [u8; LEN] {
    type Bytes = [u8; LEN];

    fn to_key_bytes(&self) -> Self::Bytes {
        *self
    }

    fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
        Self::try_from(bytes).ok()
    }
}

macro_rules! impl_primitive_key_bytes {
    ($($ty:ty: $n:literal),* $(,)?) => {$(
        impl KeyBytes for $ty {
            type Bytes = [u8; $n];

            #[inline]
            fn to_key_bytes(&self) -> Self::Bytes {
                self.to_le_bytes()
            }

            #[inline]
            fn to_key_bytes_owned(&self) -> Vec<u8> {
                self.to_le_bytes().to_vec()
            }

            #[inline]
            fn from_key_bytes(bytes: &[u8]) -> Option<Self> {
                Self::Bytes::try_from(bytes).map(<$ty>::from_le_bytes).ok()
            }
        }
    )*};
}

impl_primitive_key_bytes!(
    i8: 1, i16: 2, i32: 4, i64: 8, i128: 16,
    u8: 1, u16: 2, u32: 4, u64: 8, u128: 16,
);

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
    pub num_ops_per_wal: NonZeroU64,
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
            num_ops_per_wal: NonZeroU64::new(10_000).unwrap(),
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

impl<K: KeyBytes> WalOp<K> {
    pub fn from_raw(raw_op: WalOpRaw) -> Result<Self, TypesError> {
        match raw_op {
            WalOpRaw::Put { key_bytes, hash, size } => {
                let Some(key) = K::from_key_bytes(&key_bytes) else {
                    return Err(TypesError::DecodeKeyPutFailed);
                };
                Ok(WalOp::Put { key, hash, size })
            }
            WalOpRaw::Remove { keys_bytes } => {
                let mut keys = Vec::new();
                for (i, key_bytes) in keys_bytes.iter().enumerate() {
                    let Some(key) = K::from_key_bytes(key_bytes) else {
                        return Err(TypesError::RemoveDecodeKey { index: i });
                    };
                    keys.push(key);
                }
                Ok(WalOp::Remove { keys })
            }
        }
    }

    pub fn to_raw(&self) -> WalOpRaw {
        match self {
            WalOp::Put { key, hash, size } => {
                let key_bytes = key.to_key_bytes_owned();
                WalOpRaw::Put { key_bytes, hash: *hash, size: *size }
            }
            WalOp::Remove { keys } => {
                let mut keys_bytes = Vec::new();
                for key in keys {
                    keys_bytes.push(key.to_key_bytes_owned());
                }
                WalOpRaw::Remove { keys_bytes }
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
    RemoveDecodeKey { index: usize },
    #[error("Failed to decode key for Put operation")]
    DecodeKeyPutFailed,
}

// === WAL ===

/// At each operation index was checkpointed.
/// None means checkpoint has never been performed.
pub(crate) type CheckpointState = Option<NonZeroU64>;

pub(crate) type DeleteBlobCallFn<'a> = dyn Fn(&[BlobHash]) -> Result<(), CasManagerError> + 'a;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CheckpointReason {
    /// Fresh database initialization
    InitialSetup,
    /// After replaying WAL operations
    AfterReplay,
    /// WAL segment is full or rolling over
    SegmentRollover,
    /// User explicitly requested checkpoint
    Explicit,
}

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
