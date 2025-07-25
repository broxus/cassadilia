#[cfg(test)]
pub mod directory_helpers {
    use std::path::Path;

    use anyhow::Result;

    pub fn count_directories_recursive(path: &Path) -> Result<usize> {
        let mut count = 0;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                count += 1;
                count += count_directories_recursive(&path)?;
            }
        }

        Ok(count)
    }

    pub fn count_leaf_directories(path: &Path) -> Result<usize> {
        let mut count = 0;
        let mut has_subdirs = false;

        for entry in std::fs::read_dir(path)? {
            let entry = entry?;
            let path = entry.path();

            if path.is_dir() {
                has_subdirs = true;
                count += count_leaf_directories(&path)?;
            }
        }

        // If no subdirectories, this is a leaf directory
        if !has_subdirs {
            count = 1;
        }

        Ok(count)
    }

    #[derive(Debug)]
    pub struct DirectoryStructure {
        pub depth: usize,
        pub dirs_per_level: Vec<usize>,
    }

    pub fn analyze_directory_structure(root: &Path) -> Result<DirectoryStructure> {
        let mut dirs_per_level = Vec::new();
        let mut max_depth = 0;

        fn analyze_level(
            path: &Path,
            level: usize,
            dirs_per_level: &mut Vec<usize>,
            max_depth: &mut usize,
        ) -> Result<()> {
            if dirs_per_level.len() <= level {
                dirs_per_level.resize(level + 1, 0);
            }

            let mut dir_count = 0;
            for entry in std::fs::read_dir(path)? {
                let entry = entry?;
                let path = entry.path();

                if path.is_dir() {
                    dir_count += 1;
                    *max_depth = (*max_depth).max(level + 1);
                    analyze_level(&path, level + 1, dirs_per_level, max_depth)?;
                }
            }

            dirs_per_level[level] += dir_count;
            Ok(())
        }

        analyze_level(root, 0, &mut dirs_per_level, &mut max_depth)?;

        Ok(DirectoryStructure { depth: max_depth, dirs_per_level })
    }
}

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
