use std::fs::{File, OpenOptions};
use std::io::{BufWriter, ErrorKind, Read, Write};
use std::path::PathBuf;

use crate::calculate_blob_hash;
use crate::paths::DbPaths;
use crate::types::{
    BlobHash, SegmentInfo, WAL_ENTRY_HEADER_SIZE, WAL_ENTRY_OP_HASH_SIZE, WAL_ENTRY_VERSION_SIZE,
};
use crate::wal::{WalError, WalIoOperation, WalReplayIoStep};

pub(crate) struct SegmentWriter {
    writer: BufWriter<File>,
    segment_id: u64,
}

impl SegmentWriter {
    pub(crate) fn new(segment_id: u64, file: File) -> Self {
        Self { writer: BufWriter::new(file), segment_id }
    }

    pub(crate) fn segment_id(&self) -> u64 {
        self.segment_id
    }

    // writes a single, complete entry and syncs it to disk.
    pub(crate) fn write_entry(
        &mut self,
        op_version: u64,
        op_hash: BlobHash,
        op_data: &[u8],
    ) -> Result<(), WalError> {
        let header_bytes_written = WAL_ENTRY_HEADER_SIZE as u32;
        let op_data_len = op_data.len() as u32;

        self.writer.write_all(&op_version.to_le_bytes()).map_err(|io_err| {
            WalError::WriteWalEntryDataIO {
                op_version,
                segment_id: self.segment_id,
                source: io_err,
            }
        })?;
        self.writer.write_all(op_hash.as_bytes()).map_err(|io_err| {
            WalError::WriteWalEntryDataIO {
                op_version,
                segment_id: self.segment_id,
                source: io_err,
            }
        })?;
        self.writer.write_all(&op_data_len.to_le_bytes()).map_err(|io_err| {
            WalError::WriteWalEntryDataIO {
                op_version,
                segment_id: self.segment_id,
                source: io_err,
            }
        })?;

        self.writer.write_all(op_data).map_err(|io_err| WalError::WriteWalEntryDataIO {
            op_version,
            segment_id: self.segment_id,
            source: io_err,
        })?;

        self.writer.flush().map_err(|e| WalError::Io {
            operation: WalIoOperation::FlushWriter,
            path: None,
            source: e,
        })?;
        self.writer.get_ref().sync_data().map_err(|e| WalError::Io {
            operation: WalIoOperation::SyncData,
            path: None,
            source: e,
        })?;

        tracing::trace!(
            version = op_version,
            segment = self.segment_id,
            op_hash = ?op_hash,
            op_len = header_bytes_written + op_data_len,
            "Written WAL entry"
        );
        Ok(())
    }

    // seals the segment by writing a sentinel and then closing.
    // this marks the segment as permanently finished.
    pub(crate) fn seal(mut self) -> Result<(), WalError> {
        tracing::debug!("Sealing WAL segment {}", self.segment_id);
        
        // write an explicit end-of-segment marker.
        let sentinel_header = [0u8; WAL_ENTRY_HEADER_SIZE];
        self.writer.write_all(&sentinel_header).map_err(|e| WalError::Io {
            operation: WalIoOperation::WriteSentinel,
            path: None,
            source: e,
        })?;
        tracing::trace!("Written end-of-segment marker to segment {}", self.segment_id);

        // now, perform the standard close procedure.
        self.close()
    }

    // closes the segment, ensuring all data is flushed and synced.
    // this does NOT write a sentinel marker.
    pub(crate) fn close(mut self) -> Result<(), WalError> {
        tracing::debug!("Closing WAL segment writer for segment {}", self.segment_id);
        self.writer.flush().map_err(|e| WalError::Io {
            operation: WalIoOperation::FlushWriter,
            path: None,
            source: e,
        })?;
        let file = self.writer.into_inner().map_err(WalError::GetInnerFileWalWriter)?;
        file.sync_data().map_err(|e| WalError::Io {
            operation: WalIoOperation::SyncData,
            path: None,
            source: e,
        })?;
        Ok(())
    }
}

pub(crate) struct SegmentReader {
    file: File,
    segment_id: u64,
    path: PathBuf,
}

pub(crate) struct WalEntryRaw {
    pub version: u64,
    pub op_data: Vec<u8>,
}

impl SegmentReader {
    pub(crate) fn new(segment_id: u64, path: PathBuf, file: File) -> Self {
        Self { file, segment_id, path }
    }
}

impl Iterator for SegmentReader {
    type Item = Result<WalEntryRaw, WalError>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut header_buf = [0u8; WAL_ENTRY_HEADER_SIZE];
        match self.file.read_exact(&mut header_buf) {
            Ok(_) => {}
            Err(e) if e.kind() == ErrorKind::UnexpectedEof => {
                tracing::debug!(
                    "Reached end of WAL segment {} at '{}'.",
                    self.segment_id,
                    self.path.display()
                );
                return None;
            }
            Err(e) => {
                return Some(Err(WalError::ReplayIo {
                    step: WalReplayIoStep::ReadHeader,
                    segment_id: self.segment_id,
                    path: self.path.clone(),
                    source: e,
                }));
            }
        }

        let version = u64::from_le_bytes(
            header_buf[0..WAL_ENTRY_VERSION_SIZE]
                .try_into()
                .expect("WAL_ENTRY_VERSION_SIZE: usize = size_of::<u64>();"),
        );
        if version == 0 {
            // This is the explicit end-of-segment marker.
            tracing::debug!(
                "Reached end-of-segment marker (version 0) in segment {}. Cleanly ending replay for this segment.",
                self.segment_id
            );
            return None;
        }

        let mut expected_op_hash_bytes = [0u8; WAL_ENTRY_OP_HASH_SIZE];
        expected_op_hash_bytes.copy_from_slice(
            &header_buf[WAL_ENTRY_VERSION_SIZE..(WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE)],
        );
        let op_len = u32::from_le_bytes(
            header_buf[(WAL_ENTRY_VERSION_SIZE + WAL_ENTRY_OP_HASH_SIZE)..WAL_ENTRY_HEADER_SIZE]
                .try_into()
                .unwrap(),
        ) as usize;

        if op_len == 0 {
            tracing::warn!(
                "WAL entry (version {}) in segment {} has zero op length. Assuming end of valid entries.",
                version,
                self.segment_id
            );
            return None;
        }

        let mut op_data_buf = vec![0u8; op_len];
        if let Err(e) = self.file.read_exact(&mut op_data_buf) {
            return Some(Err(WalError::ReplayIo {
                step: WalReplayIoStep::ReadOpData,
                segment_id: self.segment_id,
                path: self.path.clone(),
                source: e,
            }));
        }

        let actual_op_hash = calculate_blob_hash(&op_data_buf);
        if actual_op_hash.0 != expected_op_hash_bytes {
            return Some(Err(WalError::ReplayChecksumMismatch {
                version,
                segment_id: self.segment_id,
                expected: BlobHash(expected_op_hash_bytes),
                actual: actual_op_hash,
            }));
        }

        Some(Ok(WalEntryRaw { version, op_data: op_data_buf }))
    }
}

pub(crate) struct SegmentStorage {
    pub(crate) paths: DbPaths,
}

impl SegmentStorage {
    pub(crate) fn new(paths: DbPaths) -> Self {
        Self { paths }
    }

    pub(crate) fn open_writer(&self, segment_id: u64) -> Result<SegmentWriter, WalError> {
        let path = self.paths.wal_path_for_segment(segment_id);
        tracing::debug!("Opening WAL segment {} for write at path: {}", segment_id, path.display());
        let file =
            OpenOptions::new().create(true).append(true).open(&path).map_err(|e| WalError::Io {
                operation: WalIoOperation::OpenSegmentWrite,
                path: Some(path),
                source: e,
            })?;
        tracing::info!("Successfully opened writer for WAL segment {}.", segment_id);
        Ok(SegmentWriter::new(segment_id, file))
    }

    pub(crate) fn open_reader(&self, segment_id: u64) -> Result<SegmentReader, WalError> {
        let path = self.paths.wal_path_for_segment(segment_id);
        let file = match OpenOptions::new().read(true).open(&path) {
            Ok(file) => file,
            Err(e) if e.kind() == ErrorKind::NotFound => {
                return Err(WalError::ReplayIo {
                    step: WalReplayIoStep::OpenSegment,
                    segment_id,
                    path,
                    source: e,
                });
            }
            Err(e) => {
                return Err(WalError::ReplayIo {
                    step: WalReplayIoStep::OpenSegment,
                    segment_id,
                    path,
                    source: e,
                });
            }
        };
        Ok(SegmentReader::new(segment_id, path, file))
    }

    pub(crate) fn discover_segments(&self) -> Result<Vec<SegmentInfo>, WalError> {
        let mut segments = Vec::new();
        for entry_res in std::fs::read_dir(self.paths.db_root_path()).map_err(|e| WalError::Io {
            operation: WalIoOperation::ReadDbDirDiscovery,
            path: None,
            source: e,
        })? {
            let entry = entry_res.map_err(|e| WalError::Io {
                operation: WalIoOperation::ReadEntryDiscovery,
                path: None,
                source: e,
            })?;
            let path = entry.path();
            if path.is_file() {
                if let Some(filename_str) = path.file_name().and_then(|name| name.to_str()) {
                    if filename_str.ends_with("_index.wal") {
                        if let Some(id_str) = filename_str.split('_').next() {
                            if let Ok(id) = id_str.parse::<u64>() {
                                segments.push(SegmentInfo::new(id, path.clone()));
                            } else {
                                tracing::warn!(
                                    "Found WAL-like file with non-numeric segment ID: {}",
                                    path.display()
                                );
                            }
                        }
                    }
                }
            }
        }
        segments.sort_by_key(|segment| segment.id);
        tracing::debug!("Discovered WAL segments: {:?}", segments);
        Ok(segments)
    }

    // deletes WAL files older than the checkpointed segment.
    pub(crate) fn prune_stale_segments(
        &self,
        checkpointed_segment_id: u64,
    ) -> Result<(), WalError> {
        tracing::info!(
            "Removing stale WAL segments older than segment ID {}.",
            checkpointed_segment_id
        );

        let mut removal_count = 0;

        for segment in self.discover_segments()? {
            if segment.id < checkpointed_segment_id {
                tracing::debug!(
                    "Removing stale WAL segment {}: {}",
                    segment.id,
                    segment.path.display()
                );
                std::fs::remove_file(&segment.path).map_err(|e| WalError::Io {
                    operation: WalIoOperation::RemoveStaleSegment,
                    path: Some(segment.path),
                    source: e,
                })?;
                removal_count += 1;
            }
        }

        if removal_count > 0 {
            tracing::info!("Successfully removed {} stale WAL segment(s).", removal_count);
        } else {
            tracing::debug!(
                "No stale WAL segments found to remove (older than {}).",
                checkpointed_segment_id
            );
        }
        Ok(())
    }

    // ensures a WAL segment file exists
    pub(crate) fn ensure_segment_file_exists(
        &self,
        segment_id: u64,
        next_op_version_for_logging: u64,
    ) -> Result<(), WalError> {
        let wal_path = self.paths.wal_path_for_segment(segment_id);
        if !wal_path.exists() {
            tracing::debug!(
                "Ensuring WAL segment file {} (for next op version {}) exists at path: {}",
                segment_id,
                next_op_version_for_logging,
                wal_path.display()
            );
            // create the file by opening it in write mode and then immediately closing it.
            File::create(&wal_path)
                .map_err(|e| WalError::Io {
                    operation: WalIoOperation::CreateInitialFile,
                    path: Some(wal_path.clone()),
                    source: e,
                })?
                .sync_all()
                .map_err(|e| WalError::Io {
                    operation: WalIoOperation::SyncInitialFile,
                    path: Some(wal_path),
                    source: e,
                })?;
        }
        Ok(())
    }
}
