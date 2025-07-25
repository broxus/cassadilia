#[cfg(test)]
pub mod encoders {
    use crate::{KeyEncoder, KeyEncoderError};

    #[derive(Debug, Default, Clone)]
    pub struct StringEncoder;
    
    impl KeyEncoder<String> for StringEncoder {
        fn encode(&self, key: &String) -> Result<Vec<u8>, KeyEncoderError> {
            Ok(key.as_bytes().to_vec())
        }

        #[allow(clippy::map_err_ignore)]
        fn decode(&self, data: &[u8]) -> Result<String, KeyEncoderError> {
            String::from_utf8(data.to_vec()).map_err(|_| KeyEncoderError::DecodeError)
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct VecU8Encoder;
    
    impl KeyEncoder<Vec<u8>> for VecU8Encoder {
        fn encode(&self, key: &Vec<u8>) -> Result<Vec<u8>, KeyEncoderError> {
            Ok(key.clone())
        }
        
        fn decode(&self, data: &[u8]) -> Result<Vec<u8>, KeyEncoderError> {
            Ok(data.to_vec())
        }
    }

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
    pub struct TestKey(pub u64);
    
    #[derive(Debug, Default, Clone)]
    pub struct TestKeyEncoder;
    
    impl KeyEncoder<TestKey> for TestKeyEncoder {
        fn encode(&self, key: &TestKey) -> Result<Vec<u8>, KeyEncoderError> {
            Ok(key.0.to_le_bytes().to_vec())
        }
        
        fn decode(&self, bytes: &[u8]) -> Result<TestKey, KeyEncoderError> {
            if bytes.len() < 8 {
                return Err(KeyEncoderError::DecodeError);
            }
            Ok(TestKey(u64::from_le_bytes(bytes[..8].try_into().unwrap())))
        }
    }

    #[derive(Debug, Default, Clone)]
    pub struct FailingKeyEncoder;
    
    impl KeyEncoder<TestKey> for FailingKeyEncoder {
        fn encode(&self, _key: &TestKey) -> Result<Vec<u8>, KeyEncoderError> {
            Ok(vec![1, 2, 3])
        }
        
        fn decode(&self, _bytes: &[u8]) -> Result<TestKey, KeyEncoderError> {
            Err(KeyEncoderError::DecodeError)
        }
    }
}