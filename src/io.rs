use std::fs::{File, OpenOptions};
use std::io;
use std::io::Write;
use std::mem::ManuallyDrop;
use std::os::fd::AsRawFd;
use std::path::{Path, PathBuf};

use thiserror::Error;

pub(crate) fn atomically_write_file_bytes(
    target_path: &Path,
    temp_path: &Path,
    bytes: impl AsRef<[u8]>,
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

    temp_file.write_all(bytes.as_ref()).map_err(|e| IoError::AtomicWrite {
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
    fn lock(self) -> io::Result<LockedFile>;
}

impl FileExt for File {
    #[inline]
    fn lock(self) -> io::Result<LockedFile> {
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
            return Err(io::Error::last_os_error());
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
            Err(io::Error::last_os_error())
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
        target_path: PathBuf,
        temp_path: PathBuf,
        #[source]
        source: io::Error,
    },
}

#[cfg(target_os = "linux")]
pub fn is_same_fs(paths: &[&Path]) -> Result<bool, io::Error> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    if paths.len() < 2 {
        return Ok(true);
    }

    // Best-effort mount-id via statx (Linux usually ≥ 5.8; may be backported).
    fn mount_id(p: &Path) -> io::Result<Option<u64>> {
        let c = CString::new(p.as_os_str().as_bytes())
            .map_err(|_e| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
        unsafe {
            let mut stx = MaybeUninit::<libc::statx>::uninit();
            let ret = libc::statx(
                libc::AT_FDCWD,
                c.as_ptr(),
                libc::AT_SYMLINK_NOFOLLOW,
                libc::STATX_MNT_ID | libc::STATX_TYPE,
                stx.as_mut_ptr(),
            );
            if ret == 0 {
                let stx = stx.assume_init();
                // Only trust stx_mnt_id if the kernel set the bit.
                if stx.stx_mask & libc::STATX_MNT_ID != 0 {
                    return Ok(Some(stx.stx_mnt_id));
                }
                return Ok(None);
            }
            match io::Error::last_os_error().raw_os_error() {
                Some(libc::ENOSYS | libc::EOPNOTSUPP | libc::EINVAL) => Ok(None),
                _ => Err(io::Error::last_os_error()),
            }
        }
    }

    // Try mount-ids first.
    if let Some(first_mid) = mount_id(paths[0])? {
        for p in paths.iter().skip(1) {
            match mount_id(p) {
                Ok(Some(mid)) if mid == first_mid => {}
                Ok(Some(_)) => return Ok(false),
                Ok(None) => return same_fs_dev(paths),
                Err(e) => return Err(e),
            }
        }
    }

    // Fallback: compare st_dev
    same_fs_dev(paths)
}

#[cfg(all(unix, not(target_os = "linux")))]
pub fn is_same_fs(paths: &[&std::path::Path]) -> Result<bool, io::Error> {
    same_fs_dev(paths)
}

pub fn same_fs_dev(paths: &[&Path]) -> Result<bool, io::Error> {
    use std::os::unix::fs::MetadataExt;

    if paths.len() < 2 {
        return Ok(true);
    }

    let first_dev = std::fs::metadata(paths[0])?.dev();

    for path in paths.iter().skip(1) {
        if std::fs::metadata(path)?.dev() != first_dev {
            return Ok(false);
        }
    }

    Ok(true)
}

#[cfg(not(unix))]
fn is_same_fs(path: &[&Path]) -> Result<bool, IsSameFsError> {
    compile_error!("plz send a pr if you want to use it on non-unix systems");
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

    #[test]
    fn same_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let root = Path::new("/");

        assert!(!is_same_fs(&[tmp.path(), root]).unwrap());
        assert!(is_same_fs(&[tmp.path(), Path::new("/tmp")]).unwrap());
        assert!(is_same_fs(&[tmp.path(), tmp.path()]).unwrap());

        assert!(!same_fs_dev(&[tmp.path(), root]).unwrap());
        assert!(same_fs_dev(&[tmp.path(), Path::new("/tmp")]).unwrap());
        assert!(same_fs_dev(&[tmp.path(), tmp.path()]).unwrap());
    }
}
