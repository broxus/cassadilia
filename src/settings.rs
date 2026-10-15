use std::num::NonZeroU64;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::io::atomically_write_file_bytes;

/// Store version, saved in `db_settings.json`. 0.4 opens only version 4 and refuses
/// version 5 stores.
pub const CURRENT_DB_VERSION: u32 = 5;
/// Version 4, written by 0.4. Its settings have no format fields. Both read as 1.
/// Upgraded to [`CURRENT_DB_VERSION`] on open.
const LEGACY_DB_VERSION: u32 = 4;

/// WAL format. 2 adds `Rename`.
pub(crate) const WAL_FORMAT: u32 = 2;
/// Index file format.
pub(crate) const INDEX_FORMAT: u32 = 1;

#[derive(Error, Debug)]
pub enum SettingsError {
    #[error("Failed to read settings file: {0}")]
    ReadFile(#[source] std::io::Error),

    #[error("Failed to parse settings: {0}")]
    ParseFailed(#[from] serde_json::Error),

    #[error("Settings validation failed: {0}")]
    ValidationFailed(String),

    #[error("Database paths must reside on the same filesystem")]
    InvalidPaths,

    #[error(transparent)]
    AtomicWrite(#[from] crate::io::IoError),

    #[error("Unsupported database version: {found}, expected: {} or {expected}", LEGACY_DB_VERSION)]
    UnsupportedVersion { found: u32, expected: u32 },

    #[error(
        "Unsupported {format} format {found} in db_settings.json: this cassadilia reads up to {supported}"
    )]
    UnsupportedFormat { format: &'static str, found: u32, supported: u32 },
}

#[derive(Debug, Clone, PartialEq, Deserialize, Serialize)]
pub(crate) struct DbSettings {
    pub version: u32,
    pub dir_tree_is_pre_created: bool,
    pub num_ops_per_wal: NonZeroU64,
    #[serde(default = "first_format")]
    pub wal_format: u32,
    #[serde(default = "first_format")]
    pub index_format: u32,
}

impl DbSettings {
    /// The same settings with the versions and formats this cassadilia writes.
    pub fn upgraded(&self) -> Self {
        Self {
            version: CURRENT_DB_VERSION,
            wal_format: WAL_FORMAT,
            index_format: INDEX_FORMAT,
            ..self.clone()
        }
    }
}

fn first_format() -> u32 {
    1
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

                if settings.version != CURRENT_DB_VERSION && settings.version != LEGACY_DB_VERSION {
                    return Err(SettingsError::UnsupportedVersion {
                        found: settings.version,
                        expected: CURRENT_DB_VERSION,
                    });
                }
                for (format, found, supported) in [
                    ("WAL", settings.wal_format, WAL_FORMAT),
                    ("index", settings.index_format, INDEX_FORMAT),
                ] {
                    if found > supported {
                        return Err(SettingsError::UnsupportedFormat { format, found, supported });
                    }
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
    use std::num::NonZeroU64;

    use anyhow::Result;
    use tempfile::tempdir;

    use super::{
        CURRENT_DB_VERSION, INDEX_FORMAT, LEGACY_DB_VERSION, SettingsError, SettingsPersister,
        WAL_FORMAT,
    };
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
            let [top_level, second_level, ..] = structure.dirs_per_level.as_slice() else {
                panic!("Expected at least two directory levels");
            };
            assert_eq!(*top_level, 256, "need 256 top-level");
            assert_eq!(*second_level, 65536, "need 65,536 second-level");

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
        assert_eq!(loaded_settings.wal_format, WAL_FORMAT);
        assert_eq!(loaded_settings.index_format, INDEX_FORMAT);

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

        // Write a version 3 settings file, which is refused
        let past_version = LEGACY_DB_VERSION - 1;
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

    #[test]
    fn legacy_store_is_upgraded_on_open() -> Result<()> {
        let dir = tempdir()?;
        let db_path = dir.path();
        let settings_path = db_path.join("db_settings.json");
        let config =
            Config { num_ops_per_wal: NonZeroU64::new(1000).unwrap(), ..Default::default() };

        {
            let cas = Cas::<String>::open(db_path, config.clone())?;
            let mut tx = cas.put("key".to_owned())?;
            tx.write(b"data")?;
            tx.finish()?;
        }
        // The settings file as cassadilia 0.4.8 writes it.
        std::fs::write(
            &settings_path,
            r#"{"version":4,"dir_tree_is_pre_created":false,"num_ops_per_wal":1000}"#,
        )?;

        let cas = Cas::<String>::open(db_path, config)?;
        assert_eq!(cas.get(&"key".to_owned())?.as_deref(), Some(&b"data"[..]));

        let settings = SettingsPersister::new(settings_path).load()?.expect("settings exist");
        assert_eq!(settings.version, CURRENT_DB_VERSION);
        assert_eq!(settings.wal_format, WAL_FORMAT);
        assert_eq!(settings.index_format, INDEX_FORMAT);
        Ok(())
    }

    #[test]
    fn newer_formats_are_rejected() -> Result<()> {
        for (format, wal_format, index_format, supported) in [
            ("WAL", WAL_FORMAT + 1, INDEX_FORMAT, WAL_FORMAT),
            ("index", WAL_FORMAT, INDEX_FORMAT + 1, INDEX_FORMAT),
        ] {
            let dir = tempdir()?;
            let settings_path = dir.path().join("db_settings.json");
            let content = format!(
                r#"{{"version":{CURRENT_DB_VERSION},"dir_tree_is_pre_created":false,"num_ops_per_wal":1000,"wal_format":{wal_format},"index_format":{index_format}}}"#
            );
            std::fs::write(&settings_path, content)?;

            let result = SettingsPersister::new(settings_path).load();
            assert!(
                matches!(
                    result,
                    Err(SettingsError::UnsupportedFormat { format: f, found, supported: s })
                        if f == format && found == supported + 1 && s == supported
                ),
                "{format}: {result:?}"
            );
        }
        Ok(())
    }
}
