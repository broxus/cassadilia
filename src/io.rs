use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;

use thiserror::Error;

#[derive(Debug, Clone)]
pub enum AtomicWriteStep {
    CreateTemp,
    WriteTemp,
    SyncTemp,
    Rename,
}

#[derive(Error, Debug)]
pub enum IoError {
    #[error("Atomic write failed during {step:?} for target {target_path:?} (temp: {temp_path:?})")]
    AtomicWrite {
        step: AtomicWriteStep,
        target_path: std::path::PathBuf,
        temp_path: std::path::PathBuf,
        #[source]
        source: std::io::Error,
    },
}

pub(crate) fn atomically_write_file_bytes(
    target_path: &Path,
    temp_path: &Path,
    bytes: &[u8],
) -> Result<(), IoError> {
    let mut temp_file =
        OpenOptions::new().write(true).create(true).truncate(true).open(temp_path).map_err(
            |e| IoError::AtomicWrite {
                step: AtomicWriteStep::CreateTemp,
                target_path: target_path.to_path_buf(),
                temp_path: temp_path.to_path_buf(),
                source: e,
            },
        )?;

    temp_file.write_all(bytes).map_err(|e| IoError::AtomicWrite {
        step: AtomicWriteStep::WriteTemp,
        target_path: target_path.to_path_buf(),
        temp_path: temp_path.to_path_buf(),
        source: e,
    })?;
    temp_file.sync_data().map_err(|e| IoError::AtomicWrite {
        step: AtomicWriteStep::SyncTemp,
        target_path: target_path.to_path_buf(),
        temp_path: temp_path.to_path_buf(),
        source: e,
    })?;
    drop(temp_file); // Ensure file is closed before rename

    std::fs::rename(temp_path, target_path).map_err(|e| IoError::AtomicWrite {
        step: AtomicWriteStep::Rename,
        target_path: target_path.to_path_buf(),
        temp_path: temp_path.to_path_buf(),
        source: e,
    })?;
    Ok(())
}
