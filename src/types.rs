use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::error::Error;

use anyhow::Result;
use bincode::{Decode, Encode};

pub const HASH_SIZE: usize = blake3::OUT_LEN;

/// Error type for Bubs operations.
#[derive(Debug, Clone, PartialEq)]
pub enum BubsError {
    InvalidHashFormat(String),
    InvalidRange(String),
    KeyNotFound,
    WalCorruption(String),
    WalReplayFailed(String),
    WalIoError(String),
}

impl fmt::Display for BubsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BubsError::InvalidHashFormat(s) => write!(f, "Invalid hash format: {}", s),
            BubsError::InvalidRange(s) => write!(f, "Invalid range requested: {}", s),
            BubsError::KeyNotFound => write!(f, "Key not found"),
            BubsError::WalCorruption(s) => write!(f, "WAL corrupted: {}", s),
            BubsError::WalReplayFailed(s) => write!(f, "WAL replay failed: {}", s),
            BubsError::WalIoError(s) => write!(f, "WAL I/O error: {}", s),
        }
    }
}

impl Error for BubsError {}

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

#[derive(Clone, Copy, Eq, Encode, Decode)]
pub struct BlobHash(pub [u8; HASH_SIZE]);

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
pub struct Bytes(pub bytes::Bytes);

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
pub const WAL_ENTRY_VERSION_SIZE: usize = std::mem::size_of::<u64>();
pub const WAL_ENTRY_OP_HASH_SIZE: usize = HASH_SIZE;
pub const WAL_ENTRY_OP_LEN_SIZE: usize = std::mem::size_of::<u32>();
pub const WAL_ENTRY_HEADER_SIZE: usize =
    WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE + WAL_ENTRY_OP_LEN_SIZE;
