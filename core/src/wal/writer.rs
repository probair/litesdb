// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by the database writer facade later in M3/M6"
)]

use std::{
    fs::{File, OpenOptions},
    io::{self, Read},
    path::{Path, PathBuf},
};

use crate::{
    Error, Result,
    fsutil::{Area, DbDir},
    wal::{
        RecordBody,
        record::{self},
        recover::RecoveryOutcome,
        segment::{SEGMENT_HEADER_BYTES, SegmentHeader, segment_name},
        storage,
    },
};

#[cfg(feature = "archive")]
#[path = "writer_archive.rs"]
mod writer_archive;

#[path = "writer_io.rs"]
mod writer_io;

#[path = "writer_config.rs"]
mod writer_config;
#[path = "writer_epoch.rs"]
mod writer_epoch;
#[path = "writer_storage.rs"]
mod writer_storage;

pub(crate) use writer_config::WriterConfig;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DurablePosition {
    seq: u64,
    segment: u64,
    offset: u64,
}

impl DurablePosition {
    #[must_use]
    pub const fn seq(self) -> u64 {
        self.seq
    }

    #[must_use]
    pub const fn segment(self) -> u64 {
        self.segment
    }

    #[must_use]
    pub const fn offset(self) -> u64 {
        self.offset
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AppendOutcome {
    seq: u64,
    seal_recommended: bool,
}

impl AppendOutcome {
    pub(crate) const fn seq(self) -> u64 {
        self.seq
    }

    pub(crate) const fn seal_recommended(self) -> bool {
        self.seal_recommended
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum IoStep {
    SegmentHeaderWrite,
    SegmentDataSync,
    WalDirectorySync,
    RecordWrite,
    ExplicitDataSync,
    RollOldDataSync,
}

pub(crate) trait WalIo {
    fn create_segment(&mut self, path: &Path) -> io::Result<File>;

    fn write_all(&mut self, step: IoStep, file: &mut File, bytes: &[u8]) -> io::Result<()>;

    fn sync_data(&mut self, step: IoStep, file: &File) -> io::Result<()>;

    fn sync_directory(&mut self, step: IoStep, path: &Path) -> io::Result<()>;
}

#[derive(Debug, Default)]
pub(crate) struct SystemWalIo;

#[derive(Debug)]
pub(crate) struct WalWriter<I = SystemWalIo> {
    io: I,
    wal_directory: PathBuf,
    file: File,
    config: WriterConfig,
    shard_id: u64,
    writer_epoch: u64,
    segment_epoch: u64,
    segment_first_seq: u64,
    offset: u64,
    next_seq: u64,
    last_seq: u64,
    wal_bytes: u64,
    storage_bytes: u64,
    needs_rotation: bool,
    recovery_headroom: bool,
    unsynced_bytes: u64,
    durable: DurablePosition,
    poisoned: bool,
    #[cfg(feature = "archive")]
    archive: Option<crate::archive::ArchiveStore>,
}

impl WalWriter<SystemWalIo> {
    pub(crate) fn create(
        directory: &DbDir,
        config: WriterConfig,
        shard_id: u64,
        writer_epoch: u64,
        next_seq: u64,
    ) -> Result<Self> {
        Self::create_with_io(
            directory,
            config,
            shard_id,
            writer_epoch,
            next_seq,
            SystemWalIo,
        )
    }

    pub(crate) fn resume(
        directory: &DbDir,
        config: WriterConfig,
        shard_id: u64,
        writer_epoch: u64,
        recovery: RecoveryOutcome,
    ) -> Result<Self> {
        let wal_directory = directory.path(Area::Wal);
        let path = wal_directory.join(segment_name(recovery.active_segment()));
        let needs_rotation = recovery.needs_rotation();
        let mut file = OpenOptions::new()
            .read(true)
            .append(!needs_rotation)
            .open(path)?;
        let physical_len = file.metadata()?.len();
        if physical_len < recovery.active_offset()
            || (!needs_rotation && physical_len != recovery.active_offset())
        {
            return Err(Error::corruption(
                "WAL resume",
                "active file length changed after recovery",
            ));
        }
        let mut header = [0_u8; SEGMENT_HEADER_BYTES];
        file.read_exact(&mut header)?;
        let header =
            SegmentHeader::decode(&header, recovery.active_segment(), shard_id, writer_epoch)?;
        if header.writer_epoch() != recovery.active_epoch() {
            return Err(Error::corruption(
                "WAL resume",
                "active segment epoch changed after recovery",
            ));
        }
        let last_seq = recovery
            .next_seq()
            .checked_sub(1)
            .ok_or_else(|| Error::corruption("WAL resume", "next sequence is zero"))?;
        Ok(Self {
            io: SystemWalIo,
            wal_directory,
            file,
            config,
            shard_id,
            writer_epoch,
            segment_epoch: header.writer_epoch(),
            segment_first_seq: recovery.active_segment(),
            offset: recovery.active_offset(),
            next_seq: recovery.next_seq(),
            last_seq,
            wal_bytes: recovery.wal_bytes(),
            storage_bytes: recovery.storage_bytes(),
            needs_rotation,
            recovery_headroom: true,
            unsynced_bytes: 0,
            durable: DurablePosition {
                seq: last_seq,
                segment: recovery.active_segment(),
                offset: recovery.active_offset(),
            },
            poisoned: false,
            #[cfg(feature = "archive")]
            archive: None,
        })
    }
}

impl<I: WalIo> WalWriter<I> {
    fn create_with_io(
        directory: &DbDir,
        config: WriterConfig,
        shard_id: u64,
        writer_epoch: u64,
        next_seq: u64,
        mut io: I,
    ) -> Result<Self> {
        if next_seq == 0 {
            return Err(Error::invalid("next_seq", "WAL sequence starts at one"));
        }
        let wal_directory = directory.path(Area::Wal);
        let storage_bytes = storage::reclaim(
            &wal_directory,
            SEGMENT_HEADER_BYTES as u64,
            u64::from(config.wal_limit),
        )?;
        let path = wal_directory.join(segment_name(next_seq));
        let mut file = io.create_segment(&path)?;
        let header = SegmentHeader::new(next_seq, shard_id, writer_epoch).encode();
        io.write_all(IoStep::SegmentHeaderWrite, &mut file, &header)?;
        io.sync_data(IoStep::SegmentDataSync, &file)?;
        io.sync_directory(IoStep::WalDirectorySync, &wal_directory)?;
        let offset = u64::try_from(SEGMENT_HEADER_BYTES)
            .map_err(|_| Error::invalid("segment header", "length does not fit u64"))?;
        let last_seq = next_seq
            .checked_sub(1)
            .ok_or_else(|| Error::invalid("next_seq", "WAL sequence starts at one"))?;
        Ok(Self {
            io,
            wal_directory,
            file,
            config,
            shard_id,
            writer_epoch,
            segment_epoch: writer_epoch,
            segment_first_seq: next_seq,
            offset,
            next_seq,
            last_seq,
            wal_bytes: offset,
            storage_bytes: storage_bytes.checked_add(offset).ok_or_else(|| {
                Error::limit("wal_storage_bytes", u64::MAX, u64::from(config.wal_limit))
            })?,
            needs_rotation: false,
            recovery_headroom: false,
            unsynced_bytes: 0,
            durable: DurablePosition {
                seq: last_seq,
                segment: next_seq,
                offset,
            },
            poisoned: false,
            #[cfg(feature = "archive")]
            archive: None,
        })
    }

    pub(crate) fn append(&mut self, body: &RecordBody) -> Result<AppendOutcome> {
        self.ensure_healthy()?;
        let following_seq = self
            .next_seq
            .checked_add(1)
            .ok_or_else(|| Error::limit("wal_sequence", u64::MAX, u64::MAX))?;
        let frame = record::encode(self.next_seq, body)?;
        let frame_bytes = u64::try_from(frame.len())
            .map_err(|_| Error::limit("wal_record_bytes", u64::MAX, u64::MAX))?;
        if self.frame_requires_checkpoint(frame_bytes)? {
            return Err(Error::limit(
                "wal_storage_bytes",
                self.storage_bytes
                    .saturating_add(self.append_growth(frame_bytes)?),
                u64::from(self.config.wal_limit),
            ));
        }
        let segment_end = self
            .offset
            .checked_add(frame_bytes)
            .ok_or_else(|| Error::limit("wal_segment_bytes", u64::MAX, u64::from(u32::MAX)))?;
        let rolls = segment_end > u64::from(self.config.segment_limit);
        let header_bytes = if rolls {
            u64::try_from(SEGMENT_HEADER_BYTES)
                .map_err(|_| Error::limit("wal_bytes", u64::MAX, u64::from(u32::MAX)))?
        } else {
            0
        };
        let next_wal_bytes = self
            .wal_bytes
            .checked_add(header_bytes)
            .and_then(|bytes| bytes.checked_add(frame_bytes))
            .ok_or_else(|| Error::limit("wal_bytes", u64::MAX, u64::from(self.config.wal_limit)))?;
        if rolls {
            self.roll_segment(self.next_seq)?;
        }
        #[cfg(feature = "archive")]
        self.archive_stage(
            self.offset
                .checked_add(frame_bytes)
                .ok_or_else(|| Error::corruption("archive", "position overflow"))?,
            &frame,
        )?;
        if self
            .io
            .write_all(IoStep::RecordWrite, &mut self.file, &frame)
            .is_err()
        {
            return self.poison();
        }
        self.offset = self
            .offset
            .checked_add(frame_bytes)
            .ok_or_else(|| Error::limit("wal_segment_bytes", u64::MAX, u64::from(u32::MAX)))?;
        self.wal_bytes = next_wal_bytes;
        self.storage_bytes = self.storage_bytes.checked_add(frame_bytes).ok_or_else(|| {
            Error::limit(
                "wal_storage_bytes",
                u64::MAX,
                u64::from(self.config.wal_limit),
            )
        })?;
        self.unsynced_bytes = self
            .unsynced_bytes
            .checked_add(frame_bytes)
            .ok_or_else(|| {
                Error::limit("unsynced_bytes", u64::MAX, u64::from(self.config.wal_limit))
            })?;
        let seq = self.next_seq;
        self.next_seq = following_seq;
        self.last_seq = seq;
        Ok(AppendOutcome {
            seq,
            seal_recommended: self.wal_bytes >= u64::from(self.config.seal_threshold),
        })
    }

    pub(crate) fn sync(&mut self) -> Result<DurablePosition> {
        self.ensure_healthy()?;
        if self
            .io
            .sync_data(IoStep::ExplicitDataSync, &self.file)
            .is_err()
        {
            return self.poison();
        }
        #[cfg(feature = "archive")]
        self.archive_commit()?;
        self.durable = DurablePosition {
            seq: self.last_seq,
            segment: self.segment_first_seq,
            offset: self.offset,
        };
        self.unsynced_bytes = 0;
        Ok(self.durable)
    }

    fn roll_segment(&mut self, first_seq: u64) -> Result<()> {
        self.roll_segment_with_headroom(first_seq, false)
    }

    fn roll_segment_with_headroom(&mut self, first_seq: u64, recovery: bool) -> Result<()> {
        self.reserve_transition_header(recovery)?;
        if self
            .io
            .sync_data(IoStep::RollOldDataSync, &self.file)
            .is_err()
        {
            return self.poison();
        }
        #[cfg(feature = "archive")]
        self.archive_commit()?;
        self.durable = DurablePosition {
            seq: self.last_seq,
            segment: self.segment_first_seq,
            offset: self.offset,
        };

        let path = self.wal_directory.join(segment_name(first_seq));
        let mut file = self.io.create_segment(&path)?;
        let header = SegmentHeader::new(first_seq, self.shard_id, self.writer_epoch).encode();
        if self
            .io
            .write_all(IoStep::SegmentHeaderWrite, &mut file, &header)
            .is_err()
            || self.io.sync_data(IoStep::SegmentDataSync, &file).is_err()
            || self
                .io
                .sync_directory(IoStep::WalDirectorySync, &self.wal_directory)
                .is_err()
        {
            return self.poison();
        }
        self.file = file;
        self.segment_first_seq = first_seq;
        self.segment_epoch = self.writer_epoch;
        self.offset = u64::try_from(SEGMENT_HEADER_BYTES)
            .map_err(|_| Error::limit("wal_segment_bytes", u64::MAX, u64::from(u32::MAX)))?;
        self.wal_bytes = self
            .wal_bytes
            .checked_add(self.offset)
            .ok_or_else(|| Error::limit("wal_bytes", u64::MAX, u64::from(self.config.wal_limit)))?;
        self.storage_bytes = self.storage_bytes.checked_add(self.offset).ok_or_else(|| {
            Error::limit(
                "wal_storage_bytes",
                u64::MAX,
                u64::from(self.config.wal_limit),
            )
        })?;
        self.needs_rotation = false;
        self.unsynced_bytes = 0;
        Ok(())
    }

    pub(crate) const fn unsynced_bytes(&self) -> u64 {
        self.unsynced_bytes
    }

    pub(crate) const fn wal_bytes(&self) -> u64 {
        self.wal_bytes
    }

    pub(crate) const fn durable_position(&self) -> DurablePosition {
        self.durable
    }

    pub(crate) const fn ensure_healthy(&self) -> Result<()> {
        if self.poisoned {
            Err(Error::Poisoned)
        } else {
            Ok(())
        }
    }

    fn poison<T>(&mut self) -> Result<T> {
        self.poisoned = true;
        Err(Error::Poisoned)
    }
}

#[cfg(test)]
#[path = "writer_tests.rs"]
mod tests;
