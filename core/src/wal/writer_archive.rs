// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SEGMENT_HEADER_BYTES, SystemWalIo, WalIo, WalWriter};
use crate::{
    Result,
    archive::{ArchiveCursor, ArchiveOptions, ArchiveStatus, ArchiveStore, ExportChunk},
    fsutil::{Area, DbDir},
    wal::{
        record,
        segment::{SegmentHeader, parse_segment_name},
        storage,
    },
};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom},
    sync::Arc,
};

impl WalWriter<SystemWalIo> {
    pub(crate) fn attach_archive(
        &mut self,
        directory: &Arc<DbDir>,
        options: ArchiveOptions,
        loaded: Option<ArchiveStore>,
    ) -> Result<()> {
        let mut archive = if let Some(store) = loaded {
            store
        } else {
            ArchiveStore::create(
                Arc::clone(directory),
                options,
                self.segment_epoch,
                self.durable,
            )?
        };
        if archive.durable().seq > self.last_seq {
            return Err(crate::archive::invalid(
                "primary lost a committed archived prefix",
            ));
        }
        if archive.durable().seq < self.last_seq {
            for path in storage::live_segments(&directory.path(Area::Wal))? {
                let name = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .ok_or_else(|| crate::archive::invalid("WAL name"))?;
                let first = parse_segment_name(name)?;
                if first > self.segment_first_seq {
                    break;
                }
                let mut file = File::open(&path)?;
                let mut header = [0; SEGMENT_HEADER_BYTES];
                file.read_exact(&mut header)?;
                let header =
                    SegmentHeader::decode(&header, first, self.shard_id, self.writer_epoch)?;
                let bound = if first == self.segment_first_seq {
                    self.offset
                } else {
                    storage::frozen_boundary(&directory.path(Area::Wal), first)?
                        .map_or(file.metadata()?.len(), |b| b.offset)
                };
                let mut offset = SEGMENT_HEADER_BYTES as u64;
                while offset < bound {
                    let mut prefix = [0; 4];
                    file.read_exact(&mut prefix)?;
                    let length = record::framed_len(&prefix)?;
                    let end = offset
                        .checked_add(length as u64)
                        .ok_or_else(|| crate::archive::invalid("WAL end overflow"))?;
                    if end > bound {
                        return Err(crate::archive::invalid(
                            "archive catchup crossed recovered prefix",
                        ));
                    }
                    let mut raw = vec![0; length];
                    raw[..4].copy_from_slice(&prefix);
                    file.read_exact(&mut raw[4..])?;
                    let seq = record::inspect(&raw)?;
                    if seq > archive.durable().seq {
                        archive.stage(header.writer_epoch(), first, end, &raw)?;
                    }
                    offset = end;
                }
                file.seek(SeekFrom::Start(bound))?;
            }
        }
        self.archive = Some(archive);
        self.sync()?;
        Ok(())
    }
}
impl<I: WalIo> WalWriter<I> {
    pub(super) fn archive_admit(&self, length: u64) -> Result<()> {
        if let Some(archive) = &self.archive {
            archive.admit(length)?;
        }
        Ok(())
    }
    pub(super) fn archive_stage(&mut self, offset: u64, raw: &[u8]) -> Result<()> {
        if let Some(archive) = &mut self.archive
            && archive
                .stage(self.writer_epoch, self.segment_first_seq, offset, raw)
                .is_err()
        {
            return self.poison();
        }
        Ok(())
    }
    pub(super) fn archive_commit(&mut self) -> Result<()> {
        if let Some(archive) = &mut self.archive
            && archive.commit(self.last_seq).is_err()
        {
            return self.poison();
        }
        Ok(())
    }
    pub(super) fn archive_protected(&self) -> Result<()> {
        if let Some(archive) = &self.archive {
            archive.ensure_protected(self.last_seq)?;
        }
        Ok(())
    }
    pub(crate) fn archive_status(&self) -> Result<ArchiveStatus> {
        self.archive
            .as_ref()
            .map(|a| a.status(!self.poisoned))
            .ok_or_else(|| crate::Error::unsupported("archive", "archive is disabled"))
    }
    pub(crate) fn export_durable(&self, after: ArchiveCursor, maximum: u32) -> Result<ExportChunk> {
        self.ensure_healthy()?;
        self.archive
            .as_ref()
            .ok_or_else(|| crate::Error::unsupported("archive", "archive is disabled"))?
            .export(after, maximum)
    }
    pub(crate) fn release_archive(&mut self, through: ArchiveCursor) -> Result<()> {
        self.ensure_healthy()?;
        let result = self
            .archive
            .as_mut()
            .ok_or_else(|| crate::Error::unsupported("archive", "archive is disabled"))?
            .release(through);
        if result
            .as_ref()
            .is_err_and(|e| e.kind() == crate::ErrorKind::Io)
        {
            self.poisoned = true;
        }
        result
    }
}
