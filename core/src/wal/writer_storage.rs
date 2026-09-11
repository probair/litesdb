// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::fs;

use super::{DurablePosition, IoStep, SEGMENT_HEADER_BYTES, SystemWalIo, WalIo, WalWriter};
use crate::{
    Error, ErrorKind, Result,
    fsutil::DbDir,
    wal::{RecordBody, record, segment::parse_segment_name, storage},
};

impl<I: WalIo> WalWriter<I> {
    pub(crate) fn append_requires_checkpoint(&mut self, body: &RecordBody) -> Result<bool> {
        self.ensure_healthy()?;
        let bytes = u64::from(record::encoded_len(body)?);
        self.frame_requires_checkpoint(bytes)
    }

    pub(super) fn frame_requires_checkpoint(&mut self, frame_bytes: u64) -> Result<bool> {
        #[cfg(feature = "archive")]
        self.archive_admit(frame_bytes)?;
        let limit = u64::from(self.config.wal_limit);
        let minimum = frame_bytes
            .checked_add(SEGMENT_HEADER_BYTES as u64)
            .and_then(|bytes| bytes.checked_add(SEGMENT_HEADER_BYTES as u64))
            .ok_or_else(|| Error::limit("wal_storage_bytes", u64::MAX, limit))?;
        if minimum > limit {
            return Err(Error::limit("wal_storage_bytes", minimum, limit));
        }
        if self.needs_rotation {
            return Ok(true);
        }
        let required = self.append_growth(frame_bytes)?;
        Ok(!self.reserve_capacity(required)?)
    }

    pub(super) fn reserve_capacity(&mut self, required: u64) -> Result<bool> {
        let limit = u64::from(self.config.wal_limit);
        if self
            .storage_bytes
            .checked_add(required)
            .is_some_and(|total| total <= limit)
        {
            return Ok(true);
        }
        match storage::reclaim(&self.wal_directory, required, limit) {
            Ok(bytes) => {
                self.storage_bytes = bytes;
                Ok(true)
            }
            Err(error) if error.kind() == ErrorKind::ResourceExhausted => {
                self.storage_bytes = storage::measure(&self.wal_directory)?;
                Ok(false)
            }
            Err(error) => Err(error),
        }
    }

    pub(super) fn reserve_transition_header(&mut self, recovery: bool) -> Result<()> {
        let header = SEGMENT_HEADER_BYTES as u64;
        if self.reserve_capacity(header)? || recovery {
            return Ok(());
        }
        Err(Error::limit(
            "wal_storage_bytes",
            self.storage_bytes.saturating_add(header),
            u64::from(self.config.wal_limit),
        ))
    }

    pub(super) fn append_growth(&self, frame_bytes: u64) -> Result<u64> {
        let header = SEGMENT_HEADER_BYTES as u64;
        let end = self.offset.checked_add(frame_bytes).ok_or_else(|| {
            Error::limit(
                "wal_storage_bytes",
                u64::MAX,
                u64::from(self.config.wal_limit),
            )
        })?;
        frame_bytes
            .checked_add(header)
            .and_then(|bytes| {
                bytes.checked_add(if end > u64::from(self.config.segment_limit) {
                    header
                } else {
                    0
                })
            })
            .ok_or_else(|| {
                Error::limit(
                    "wal_storage_bytes",
                    u64::MAX,
                    u64::from(self.config.wal_limit),
                )
            })
    }

    pub(crate) fn checkpoint(&mut self) -> Result<()> {
        self.ensure_healthy()?;
        #[cfg(feature = "archive")]
        self.archive_protected()?;
        let mut removed = false;
        for entry in fs::read_dir(&self.wal_directory)? {
            let entry = entry?;
            if entry.file_name() == "damaged" {
                continue;
            }
            let name = entry
                .file_name()
                .into_string()
                .map_err(|_| Error::corruption("WAL directory", "file name is not UTF-8"))?;
            if parse_segment_name(&name)? < self.segment_first_seq {
                fs::remove_file(entry.path())?;
                removed = true;
            }
        }
        if removed {
            self.io
                .sync_directory(IoStep::WalDirectorySync, &self.wal_directory)?;
        }
        self.wal_bytes = SEGMENT_HEADER_BYTES as u64;
        self.unsynced_bytes = 0;
        self.storage_bytes =
            storage::reclaim(&self.wal_directory, 0, u64::from(self.config.wal_limit))?;
        Ok(())
    }

    pub(crate) const fn storage_bytes(&self) -> u64 {
        self.storage_bytes
    }

    pub(crate) const fn needs_rotation(&self) -> bool {
        self.needs_rotation || self.offset > SEGMENT_HEADER_BYTES as u64
    }

    pub(crate) fn mark_poisoned(&mut self) {
        self.poisoned = true;
    }
}

impl WalWriter<SystemWalIo> {
    pub(crate) fn prepare_checkpoint(&mut self, directory: &DbDir) -> Result<DurablePosition> {
        self.ensure_healthy()?;
        self.sync()?;
        let recovery = self.recovery_headroom;
        self.recovery_headroom = false;
        if self.needs_rotation && self.offset == SEGMENT_HEADER_BYTES as u64 {
            self.rebuild_empty_segment(directory, self.writer_epoch, recovery)?;
        } else if self.offset > SEGMENT_HEADER_BYTES as u64 {
            self.roll_segment_with_headroom(self.next_seq, recovery)?;
        }
        self.durable = DurablePosition {
            seq: self.last_seq,
            segment: self.segment_first_seq,
            offset: self.offset,
        };
        Ok(self.durable)
    }
}
