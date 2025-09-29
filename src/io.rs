use std::fs::OpenOptions;
use std::io;
use std::io::Write;
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
    if paths.len() < 2 {
        return Ok(true);
    }

    // Try mount-ids first.
    if let Some(first_mid) = mount_id_statx(paths[0])? {
        for p in paths.iter().skip(1) {
            match mount_id_statx(p) {
                Ok(Some(mid)) if mid == first_mid => {}
                Ok(Some(_)) => return Ok(false),
                Ok(None) => return same_fs_dev(paths),
                Err(e) => return Err(e),
            }
        }
        Ok(true)
    } else {
        // Fallback: compare st_dev
        same_fs_dev(paths)
    }
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

// Best-effort mount-id via statx (Linux usually ≥ 5.8; may be backported).
#[cfg(target_os = "linux")]
fn mount_id_statx(p: &Path) -> io::Result<Option<u64>> {
    use std::ffi::CString;
    use std::mem::MaybeUninit;
    use std::os::unix::ffi::OsStrExt;

    let c = CString::new(p.as_os_str().as_bytes())
        .map_err(|_e| io::Error::new(io::ErrorKind::InvalidInput, "path contains NUL"))?;
    unsafe {
        let mut stx = MaybeUninit::<libc::statx>::uninit();
        let ret = libc::statx(
            libc::AT_FDCWD,
            c.as_ptr(),
            libc::AT_NO_AUTOMOUNT,
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

        let err = io::Error::last_os_error();
        match err.raw_os_error() {
            Some(libc::ENOSYS | libc::EOPNOTSUPP | libc::EINVAL) => Ok(None),
            _ => Err(err),
        }
    }
}

#[cfg(not(unix))]
compile_error!("plz send a pr if you want to use it on non-unix systems");

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Cas, LibError};

    #[test]
    fn file_lock() {
        let dir = tempfile::tempdir().unwrap();

        let _cas: Cas<String> = Cas::open(dir.path(), Default::default()).unwrap();

        let cas2: Result<Cas<String>, _> = Cas::<String>::open(dir.path(), Default::default());

        assert!(matches!(cas2, Err(LibError::AlreadyOpened)));
    }

    #[cfg(target_os = "linux")]
    #[test]
    fn same_fs_linux() -> anyhow::Result<()> {
        let root = Path::new("/");
        let dev = Path::new("/dev");
        let shm = Path::new("/dev/shm");
        let null = Path::new("/dev/null");

        assert!(is_same_fs(&[dev, null])?);
        assert!(!is_same_fs(&[root, dev])?);
        assert!(!is_same_fs(&[shm, null])?);

        assert_eq!(mount_id_statx(dev)?, mount_id_statx(null)?);
        assert_ne!(mount_id_statx(root)?, mount_id_statx(dev)?);
        assert_ne!(mount_id_statx(shm)?, mount_id_statx(null)?);

        assert!(same_fs_dev(&[dev, null])?);
        assert!(!same_fs_dev(&[root, dev])?);
        assert!(!same_fs_dev(&[shm, null])?);

        Ok(())
    }

    #[cfg(target_os = "macos")]
    #[test]
    fn same_fs_macos() -> anyhow::Result<()> {
        let temp_root = tempfile::tempdir()?;
        let nested = temp_root.path().join("nested");
        std::fs::create_dir_all(&nested)?;

        assert!(is_same_fs(&[temp_root.path(), nested.as_path()])?);

        // I've no access to macOS, so gues it's enough to test.
        Ok(())
    }
}
