use std::num::NonZeroU64;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::io::atomically_write_file_bytes;

pub const CURRENT_DB_VERSION: u32 = 4;

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct DbSettings {
    pub version: u32,
    pub dir_tree_is_pre_created: bool,
    pub num_ops_per_wal: NonZeroU64,
}

#[derive(Error, Debug)]
pub enum SettingsError {
    #[error("Failed to read settings file: {0}")]
    ReadFile(#[source] std::io::Error),

    #[error("Failed to parse settings: {0}")]
    ParseFailed(#[from] serde_json::Error),

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

    pub fn load(&self) -> Result<Option<DbSettings>, SettingsError> {
        match std::fs::read_to_string(&self.settings_path) {
            Ok(content) => {
                let settings: DbSettings =
                    serde_json::from_str(&content).map_err(SettingsError::ParseFailed)?;

                if settings.version != CURRENT_DB_VERSION {
                    return Err(SettingsError::UnsupportedVersion {
                        found: settings.version,
                        expected: CURRENT_DB_VERSION,
                    });
                }

                Ok(Some(settings))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(SettingsError::ReadFile(e)),
        }
    }

    pub fn save(&self, settings: &DbSettings) -> Result<(), SettingsError> {
        let serialized = serde_json::to_string(settings).unwrap();

        // The temp file should be in the same directory as the final file
        let temp_path = self.settings_path.with_extension("json.tmp");

        // Write atomically
        atomically_write_file_bytes(&self.settings_path, &temp_path, serialized)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use anyhow::Result;
    use tempfile::tempdir;

    use super::*;
    use crate::tests::utils::directory_helpers::{
        analyze_directory_structure, count_directories_recursive, count_leaf_directories,
    };
    use crate::{Cas, Config, LibError, SyncMode};

    #[test]
    fn test_pre_create_cas_dirs() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();

        // Create database with pre-created directories
        let config = Config {
            sync_mode: SyncMode::Sync,
            num_ops_per_wal: NonZeroU64::new(100).unwrap(),
            pre_create_cas_dirs: true,
            ..Default::default()
        };

        {
            let cas = Cas::open(db_path, config.clone())?;

            // Verify that all directories exist using recursive counting
            let cas_root = db_path.join("cas");

            // Count all directories (including intermediate ones)
            let total_dir_count = count_directories_recursive(&cas_root)?;
            assert_eq!(total_dir_count, 65792, "want 65,792 total dirs");

            // Count only leaf directories (where blobs will be stored)
            let leaf_dir_count = count_leaf_directories(&cas_root)?;
            assert_eq!(leaf_dir_count, 65536, "want 65,536 leaf dirs");

            // Verify structure: 2 levels, 256 each
            let structure = analyze_directory_structure(&cas_root)?;
            assert_eq!(structure.depth, 2, "depth != 2");
            assert_eq!(structure.dirs_per_level[0], 256, "need 256 top-level");
            assert_eq!(structure.dirs_per_level[1], 65536, "need 65,536 second-level");

            // Write a blob to verify it works
            let mut tx = cas.put("test_key".to_string())?;
            tx.write(b"test data")?;
            tx.finish()?;

            let data = cas.get(&"test_key".to_string())?;
            assert_eq!(data, Some(bytes::Bytes::from("test data")));
        }

        // Re-open database and verify settings are persisted
        {
            let cas = Cas::open(db_path, config)?;

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
        let config = Config {
            sync_mode: SyncMode::Sync,
            num_ops_per_wal: NonZeroU64::new(1000).unwrap(),
            pre_create_cas_dirs: false,
            ..Default::default()
        };

        {
            let _cas = Cas::<String>::open(db_path, config)?;
        }

        // Try to re-open with different num_ops_per_wal
        let config2 = Config {
            sync_mode: SyncMode::Sync,
            num_ops_per_wal: NonZeroU64::new(2000).unwrap(), // Different value
            pre_create_cas_dirs: false,
            ..Default::default()
        };

        let result = Cas::<String>::open(db_path, config2);
        assert!(result.is_err());

        if let Err(LibError::Settings(SettingsError::ValidationFailed(msg))) = result {
            assert!(msg.contains("Cannot change num_ops_per_wal from 1000 to 2000"));
        } else {
            panic!("Expected validation error");
        }

        Ok(())
    }

    #[test]
    fn test_settings_file() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();

        let config = Config {
            sync_mode: SyncMode::Sync,
            num_ops_per_wal: NonZeroU64::new(5000).unwrap(),
            pre_create_cas_dirs: true,
            ..Default::default()
        };

        {
            let _cas = Cas::<String>::open(db_path, config)?;
        }

        // Check that settings file was created and can be loaded correctly
        let settings_path = db_path.join("db_settings.json");
        assert!(settings_path.exists());

        let persister = SettingsPersister::new(settings_path);
        let loaded_settings =
            persister.load()?.expect("Settings file should exist and be loadable");

        assert_eq!(loaded_settings.version, CURRENT_DB_VERSION);
        assert!(loaded_settings.dir_tree_is_pre_created);
        assert_eq!(loaded_settings.num_ops_per_wal.get(), 5000);

        Ok(())
    }

    #[test]
    fn test_future_version_rejected() -> Result<()> {
        let dir = tempdir()?;
        let settings_path = dir.path().join("db_settings.json");

        // Write a settings file with future version
        let future_content = format!(
            r#"{{"version":{},"dir_tree_is_pre_created":false,"num_ops_per_wal":1000}}"#,
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

    #[test]
    fn test_past_version_rejected() -> Result<()> {
        let dir = tempdir()?;
        let settings_path = dir.path().join("db_settings.json");

        // Write a settings file with past version
        let past_version = CURRENT_DB_VERSION.saturating_sub(1);
        let content = format!(
            r#"{{"version":{},"dir_tree_is_pre_created":false,"num_ops_per_wal":1000}}"#,
            past_version
        );
        std::fs::write(&settings_path, content)?;

        // Load should fail with UnsupportedVersion error
        let persister = SettingsPersister::new(settings_path);
        let result = persister.load();

        assert!(matches!(
            result,
            Err(SettingsError::UnsupportedVersion { found, expected })
                if found == past_version && expected == CURRENT_DB_VERSION
        ));

        Ok(())
    }
}
