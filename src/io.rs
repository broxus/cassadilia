use std::fs::{File, OpenOptions};
use std::io::Write;
use std::mem::ManuallyDrop;
use std::os::fd::AsRawFd;
use std::path::Path;

use thiserror::Error;

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

pub(crate) trait FileExt {
    fn lock(self) -> std::io::Result<LockedFile>;
}

impl FileExt for File {
    #[inline]
    fn lock(self) -> std::io::Result<LockedFile> {
        LockedFile::new(self)
    }
}

#[derive(Debug)]
#[repr(transparent)]
pub(crate) struct LockedFile {
    inner: File,
}

impl LockedFile {
    pub fn new(file: File) -> std::io::Result<Self> {
        // SAFETY: An existing file descriptor is used.
        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            return Err(std::io::Error::last_os_error());
        }

        Ok(Self { inner: file })
    }

    #[allow(unused)]
    pub fn unlock(self) -> std::io::Result<File> {
        let res = self.unlock_impl();

        // Avoid calling `Drop` on manual unlock.
        let this = ManuallyDrop::new(self);
        // SAFETY: `this` is a valid pointer.
        let this = unsafe { std::ptr::read(&this.inner) };

        res.map(|_| this)
    }

    fn unlock_impl(&self) -> std::io::Result<()> {
        // SAFETY: An existing file descriptor is used.
        if unsafe { libc::flock(self.inner.as_raw_fd(), libc::LOCK_UN | libc::LOCK_NB) } != 0 {
            Err(std::io::Error::last_os_error())
        } else {
            Ok(())
        }
    }
}

impl Drop for LockedFile {
    fn drop(&mut self) {
        self.unlock_impl().ok();
    }
}

impl std::ops::Deref for LockedFile {
    type Target = File;

    #[inline]
    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl std::ops::DerefMut for LockedFile {
    #[inline]
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.inner
    }
}

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn file_lock() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("tempfile");

        let mut options = std::fs::OpenOptions::new();
        options.create(true).truncate(true).write(true);

        let file = options.open(&path).unwrap().lock().unwrap();

        options.open(&path).unwrap().lock().unwrap_err();

        drop(file);

        options.open(&path).unwrap().lock().unwrap();
    }
}
