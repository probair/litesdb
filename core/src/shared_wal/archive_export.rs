// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{SharedMember, SharedWal, advance, format};
use crate::shared_wal::{format::Pointer, member::FrameReader};
use crate::{
    ArchiveCursor, Error, ExportChunk, Result,
    archive::{
        format::encode_record,
        types::{EXPORT_HEADER, MAX_EXPORT_BYTES},
    },
    fsutil::Area,
};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

pub(crate) struct ExportIndex {
    owner: SharedWal,
    path: PathBuf,
    charged: u64,
    cursor: ArchiveCursor,
    end: ArchiveCursor,
    remaining: u64,
}
impl Drop for ExportIndex {
    fn drop(&mut self) {
        if self.release_scratch().is_err() {
            self.owner.poison();
        }
    }
}
impl ExportIndex {
    fn release_scratch(&mut self) -> Result<()> {
        match fs::remove_file(&self.path) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(error.into()),
        }
        let mut state = self.owner.inner.state.lock().map_err(|_| Error::Poisoned)?;
        state.scratch_bytes = state.scratch_bytes.saturating_sub(self.charged);
        self.charged = 0;
        Ok(())
    }

    fn build(member: &SharedMember, baseline: ArchiveCursor) -> Result<Self> {
        let (mut pointer, end, budget) = {
            let state = member.owner.lock()?;
            let entry = state
                .members
                .get(&member.id)
                .ok_or_else(|| format::invalid("export member absent"))?;
            let archive = entry
                .archive
                .as_ref()
                .ok_or_else(|| Error::unsupported("archive", "member archive disabled"))?;
            if entry.lsn <= state.durable {
                (
                    entry.latest,
                    archive.latest,
                    member.owner.inner.options.max_bytes,
                )
            } else {
                (
                    entry.durable_pointer,
                    archive.durable,
                    member.owner.inner.options.max_bytes,
                )
            }
        };
        if !end.covers(baseline)? {
            return Err(Error::invalid(
                "export cursor",
                "start exceeds durable prefix",
            ));
        }
        let count = end.seq.saturating_sub(baseline.seq);
        let charge = count
            .checked_mul(16)
            .ok_or_else(|| format::invalid("export scratch overflow"))?;
        let path = member.owner.inner.directory.file(
            Area::Temporary,
            &format!("EXPORT-{}", crate::shared_wal::registry::name(member.id)),
        );
        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&path)?;
        let mut index = Self {
            owner: member.owner.clone(),
            path,
            charged: 0,
            cursor: baseline,
            end,
            remaining: count,
        };
        {
            let mut state = member.owner.lock()?;
            let total = format::add(state.scratch_bytes, charge)?;
            if total > budget {
                return Err(Error::limit("shared_export_scratch_bytes", total, budget));
            }
            state.scratch_bytes = total;
            index.charged = charge;
        }
        let mut reader = FrameReader {
            root: member.owner.inner.directory.path(Area::Wal),
            current: None,
        };
        let mut seq = end.seq;
        while seq > baseline.seq {
            let bytes = reader.read(pointer)?;
            let frame = format::inspect(&bytes)?;
            if frame.id != member.id
                || frame.seq != seq
                || (frame.previous != Pointer::default() && frame.previous >= pointer)
            {
                return Err(format::invalid("export predecessor conflict"));
            }
            file.write_all(&pointer.segment.to_le_bytes())?;
            file.write_all(&pointer.offset.to_le_bytes())?;
            pointer = frame.previous;
            seq = seq.saturating_sub(1);
        }
        Ok(index)
    }
    fn peek(&self, file: &mut File, reader: &mut FrameReader) -> Result<Vec<u8>> {
        if self.remaining == 0 {
            return Err(format::invalid("export index exhausted"));
        }
        let offset = self
            .remaining
            .saturating_sub(1)
            .checked_mul(16)
            .ok_or_else(|| format::invalid("export index offset"))?;
        file.seek(SeekFrom::Start(offset))?;
        let mut pointer = [0; 16];
        file.read_exact(&mut pointer)?;
        let bytes = reader.read(Pointer {
            segment: format::u64_at(&pointer, 0)?,
            offset: format::u64_at(&pointer, 8)?,
        })?;
        let frame = format::inspect(&bytes)?;
        Ok(frame.raw.to_vec())
    }
    fn consume(&mut self, raw: &[u8]) -> Result<()> {
        self.cursor = advance(self.cursor, raw)?;
        self.remaining = self.remaining.saturating_sub(1);
        if self.remaining == 0 {
            if self.cursor != self.end {
                return Err(format::invalid(
                    "export final hash disagrees with durable chain",
                ));
            }
            self.release_scratch()?;
        }
        Ok(())
    }
}
impl SharedMember {
    fn ensure_export_index(&mut self, after: ArchiveCursor) -> Result<()> {
        let status = self.archive_status()?;
        if !after.covers(status.earliest())? || !status.durable_end().covers(after)? {
            return Err(Error::invalid(
                "export cursor",
                "cursor outside retained durable history",
            ));
        }
        let cached = self
            .export_index
            .as_ref()
            .is_some_and(|index| index.cursor == after);
        if cached
            && self
                .export_index
                .as_ref()
                .is_some_and(|index| index.remaining > 0 || index.end == status.durable_end())
        {
            return Ok(());
        }
        let baseline = if cached { after } else { status.earliest() };
        self.export_index = None;
        let mut index = ExportIndex::build(self, baseline)?;
        if index.cursor != after {
            let mut file = File::open(&index.path)?;
            let mut reader = FrameReader {
                root: self.owner.inner.directory.path(Area::Wal),
                current: None,
            };
            while index.cursor.seq < after.seq {
                let raw = index.peek(&mut file, &mut reader)?;
                index.consume(&raw)?;
            }
            if index.cursor != after {
                return Err(Error::invalid(
                    "export cursor",
                    "cursor digest is not a retained record boundary",
                ));
            }
        }
        self.export_index = Some(index);
        Ok(())
    }
    pub(super) fn verify_cursor(&mut self, cursor: ArchiveCursor) -> Result<()> {
        let status = self.archive_status()?;
        if cursor == status.earliest() || cursor == status.durable_end() {
            return Ok(());
        }
        self.ensure_export_index(cursor)
    }
    pub(super) fn export_shared(
        &mut self,
        after: ArchiveCursor,
        max_bytes: u32,
    ) -> Result<ExportChunk> {
        if max_bytes > MAX_EXPORT_BYTES || (max_bytes as usize) < EXPORT_HEADER {
            return Err(Error::invalid(
                "export budget",
                "requires header through 4 MiB",
            ));
        }
        let status = self.archive_status()?;
        if after == status.durable_end() {
            return Ok(ExportChunk {
                start: after,
                end: after,
                records: Vec::new(),
            });
        }
        self.ensure_export_index(after)?;
        let index = self
            .export_index
            .as_mut()
            .ok_or_else(|| format::invalid("export index missing"))?;
        let mut file = File::open(&index.path)?;
        let mut reader = FrameReader {
            root: self.owner.inner.directory.path(Area::Wal),
            current: None,
        };
        let mut records = Vec::new();
        while index.remaining > 0 {
            let raw = index.peek(&mut file, &mut reader)?;
            let growth = raw.len().saturating_add(28);
            if EXPORT_HEADER
                .saturating_add(records.len())
                .saturating_add(growth)
                > max_bytes as usize
            {
                if records.is_empty() {
                    return Err(Error::limit(
                        "export_bytes",
                        EXPORT_HEADER.saturating_add(growth) as u64,
                        max_bytes.into(),
                    ));
                }
                break;
            }
            index.consume(&raw)?;
            records.extend_from_slice(&encode_record(
                index.cursor.epoch,
                index.cursor.segment,
                index.cursor.offset,
                &raw,
            )?);
        }
        Ok(ExportChunk {
            start: after,
            end: index.cursor,
            records,
        })
    }
}
