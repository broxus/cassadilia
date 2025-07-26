use std::path::PathBuf;

use thiserror::Error;

use crate::io::atomically_write_file_bytes;

pub const CURRENT_DB_VERSION: u32 = 1;

#[derive(Debug, Clone, PartialEq)]
pub(crate) struct DbSettings {
    pub version: u32,
    pub dir_tree_is_pre_created: bool,
    pub num_ops_per_wal: u64,
}

#[derive(Error, Debug)]
pub enum SettingsError {
    #[error("Failed to read settings file: {0}")]
    ReadFile(#[source] std::io::Error),

    #[error("Failed to write settings file: {0}")]
    WriteFile(#[source] std::io::Error),

    #[error("Failed to parse KDL: {0}")]
    ParseKdl(#[from] kdl::KdlError),

    #[error("Missing required field in settings: {0}")]
    MissingField(&'static str),

    #[error("Invalid field type in settings: {field}")]
    InvalidFieldType { field: String },

    #[error("Settings validation failed: {0}")]
    ValidationFailed(String),

    #[error("Atomic write failed")]
    AtomicWrite(#[from] crate::io::IoError),

    #[error("Unsupported database version: {found}, expected: {expected}")]
    UnsupportedVersion { found: u32, expected: u32 },
}

pub(crate) struct SettingsPersister {
    settings_path: PathBuf,
}

impl SettingsPersister {
    pub fn new(settings_path: PathBuf) -> Self {
        Self { settings_path }
    }

    fn read_int<T: TryFrom<i128>>(
        doc: &kdl::KdlDocument,
        name: &str,
    ) -> Result<Option<T>, SettingsError>
    where
        T::Error: std::fmt::Debug,
    {
        match doc.get(name) {
            Some(node) => {
                let value = node
                    .entries()
                    .first()
                    .and_then(|entry| entry.value().as_integer())
                    .ok_or(SettingsError::InvalidFieldType { field: name.to_string() })?;

                T::try_from(value)
                    .map(Some)
                    .map_err(|_e| SettingsError::InvalidFieldType { field: name.to_string() })
            }
            None => Ok(None),
        }
    }

    fn read_bool(doc: &kdl::KdlDocument, name: &str) -> Result<Option<bool>, SettingsError> {
        match doc.get(name) {
            Some(node) => node
                .entries()
                .first()
                .and_then(|entry| entry.value().as_bool())
                .ok_or(SettingsError::InvalidFieldType { field: name.to_string() })
                .map(Some),
            None => Ok(None),
        }
    }

    pub fn load(&self) -> Result<Option<DbSettings>, SettingsError> {
        match std::fs::read_to_string(&self.settings_path) {
            Ok(content) => {
                let doc = content.parse::<kdl::KdlDocument>()?;

                let version = Self::read_int::<u32>(&doc, "version")?.unwrap_or(CURRENT_DB_VERSION);

                if version > CURRENT_DB_VERSION {
                    return Err(SettingsError::UnsupportedVersion {
                        found: version,
                        expected: CURRENT_DB_VERSION,
                    });
                }

                let dir_tree_is_pre_created = Self::read_bool(&doc, "dir_tree_is_pre_created")?
                    .ok_or(SettingsError::MissingField("dir_tree_is_pre_created"))?;

                let num_ops_per_wal = Self::read_int::<u64>(&doc, "num_ops_per_wal")?
                    .ok_or(SettingsError::MissingField("num_ops_per_wal"))?;

                Ok(Some(DbSettings { version, dir_tree_is_pre_created, num_ops_per_wal }))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SettingsError::ReadFile(e)),
        }
    }

    pub fn save(&self, settings: &DbSettings) -> Result<(), SettingsError> {
        let mut doc = kdl::KdlDocument::new();

        let mut version_node = kdl::KdlNode::new("version");
        version_node.push(kdl::KdlEntry::new(settings.version as i128));
        doc.nodes_mut().push(version_node);

        let mut dir_tree_node = kdl::KdlNode::new("dir_tree_is_pre_created");
        dir_tree_node.push(kdl::KdlEntry::new(settings.dir_tree_is_pre_created));
        doc.nodes_mut().push(dir_tree_node);

        let mut num_ops_node = kdl::KdlNode::new("num_ops_per_wal");
        num_ops_node.push(kdl::KdlEntry::new(settings.num_ops_per_wal as i128));
        doc.nodes_mut().push(num_ops_node);

        // The temp file should be in the same directory as the final file
        let temp_path = self.settings_path.with_extension("kdl.tmp");

        // Write atomically
        atomically_write_file_bytes(&self.settings_path, &temp_path, doc.to_string().as_bytes())?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::*;
    use crate::test_utils::directory_helpers::{
        analyze_directory_structure, count_directories_recursive, count_leaf_directories,
    };
    use crate::test_utils::encoders::StringEncoder;
    use crate::{Cas, Config, LibError, SyncMode};

    #[test]
    fn test_pre_create_cas_dirs() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();

        // Create database with pre-created directories
        let config =
            Config { sync_mode: SyncMode::Sync, num_ops_per_wal: 100, pre_create_cas_dirs: true, ..Default::default() };

        {
            let cas = Cas::open(db_path, StringEncoder, config.clone())?;

            // Verify that all directories exist using recursive counting
            let cas_root = db_path.join("cas");

            // Count all directories (including intermediate ones)
            let total_dir_count = count_directories_recursive(&cas_root)?;
            assert_eq!(
                total_dir_count, 65792,
                "Should have created 256 + 65,536 = 65,792 directories total"
            );

            // Count only leaf directories (where blobs will be stored)
            let leaf_dir_count = count_leaf_directories(&cas_root)?;
            assert_eq!(
                leaf_dir_count, 65536,
                "Should have exactly 65,536 leaf directories for blob storage"
            );

            // Verify the structure: should be 2 levels deep with 256 dirs at each level
            let structure = analyze_directory_structure(&cas_root)?;
            assert_eq!(structure.depth, 2, "Directory structure should be 2 levels deep");
            assert_eq!(structure.dirs_per_level[0], 256, "Should have 256 top-level directories");
            assert_eq!(
                structure.dirs_per_level[1], 65536,
                "Should have 65,536 second-level directories"
            );

            // Write a blob to verify it works
            let mut tx = cas.put("test_key".to_string())?;
            tx.write(b"test data")?;
            tx.finish()?;

            let data = cas.get(&"test_key".to_string())?;
            assert_eq!(data, Some(bytes::Bytes::from("test data")));
        }

        // Re-open database and verify settings are persisted
        {
            let cas = Cas::open(db_path, StringEncoder, config)?;

            // Should not recreate directories, just use existing ones
            let mut tx = cas.put("test_key2".to_string())?;
            tx.write(b"test data 2")?;
            tx.finish()?;

            let data = cas.get(&"test_key2".to_string())?;
            assert_eq!(data, Some(bytes::Bytes::from("test data 2")));
        }

        Ok(())
    }

    #[test]
    fn test_immutable_settings_validation() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();

        // Create database with specific num_ops_per_wal
        let config =
            Config { sync_mode: SyncMode::Sync, num_ops_per_wal: 1000, pre_create_cas_dirs: false, ..Default::default() };

        {
            let _cas = Cas::open(db_path, StringEncoder, config)?;
        }

        // Try to re-open with different num_ops_per_wal
        let config2 = Config {
            sync_mode: SyncMode::Sync,
            num_ops_per_wal: 2000, // Different value
            pre_create_cas_dirs: false,
            ..Default::default()
        };

        let result = Cas::open(db_path, StringEncoder, config2);
        assert!(result.is_err());

        if let Err(LibError::Settings(SettingsError::ValidationFailed(msg))) = result {
            assert!(msg.contains("Cannot change num_ops_per_wal from 1000 to 2000"));
        } else {
            panic!("Expected validation error");
        }

        Ok(())
    }

    #[test]
    fn test_kdl_settings_file() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();

        let config =
            Config { sync_mode: SyncMode::Sync, num_ops_per_wal: 5000, pre_create_cas_dirs: true, ..Default::default() };

        {
            let _cas = Cas::open(db_path, StringEncoder, config)?;
        }

        // Check that settings file was created and can be loaded correctly
        let settings_path = db_path.join("db_settings.kdl");
        assert!(settings_path.exists());

        let persister = SettingsPersister::new(settings_path);
        let loaded_settings =
            persister.load()?.expect("Settings file should exist and be loadable");

        assert_eq!(loaded_settings.version, CURRENT_DB_VERSION);
        assert!(loaded_settings.dir_tree_is_pre_created);
        assert_eq!(loaded_settings.num_ops_per_wal, 5000);

        Ok(())
    }

    #[test]
    fn test_version_backward_compatibility() -> Result<()> {
        let dir = tempdir()?;
        let settings_path = dir.path().join("db_settings.kdl");

        // Write an old-style settings file without version
        let old_content = r#"
dir_tree_is_pre_created #false
num_ops_per_wal 1000
"#;
        std::fs::write(&settings_path, old_content)?;

        // Load should default version to CURRENT_DB_VERSION
        let persister = SettingsPersister::new(settings_path.clone());
        let settings = persister.load()?.unwrap();
        assert_eq!(settings.version, CURRENT_DB_VERSION);
        assert!(!settings.dir_tree_is_pre_created);
        assert_eq!(settings.num_ops_per_wal, 1000);

        Ok(())
    }

    #[test]
    fn test_future_version_rejected() -> Result<()> {
        let dir = tempdir()?;
        let settings_path = dir.path().join("db_settings.kdl");

        // Write a settings file with future version
        let future_content = format!(
            r#"
version {}
dir_tree_is_pre_created #false
num_ops_per_wal 1000
"#,
            CURRENT_DB_VERSION + 1
        );
        std::fs::write(&settings_path, future_content)?;

        // Load should fail with UnsupportedVersion error
        let persister = SettingsPersister::new(settings_path);
        let result = persister.load();

        assert!(matches!(
            result,
            Err(SettingsError::UnsupportedVersion { found, expected })
                if found == CURRENT_DB_VERSION + 1 && expected == CURRENT_DB_VERSION
        ));

        Ok(())
    }
}
