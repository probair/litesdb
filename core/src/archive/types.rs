// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::format::{Reader, read_record};
use crate::{Error, Result};
use sha2::{Digest, Sha256};

pub(crate) const CURSOR_BYTES: usize = 120;
pub(crate) const EXPORT_HEADER: usize = 8 + CURSOR_BYTES * 2;
pub(crate) const MAX_EXPORT_BYTES: u32 = 4_194_304;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArchiveOptions {
    pub max_bytes: u64,
}
impl Default for ArchiveOptions {
    fn default() -> Self {
        Self {
            max_bytes: 67_108_864,
        }
    }
}
impl ArchiveOptions {
    pub(crate) fn validate(self) -> Result<()> {
        if !(131_072..=8_589_934_592).contains(&self.max_bytes) {
            return Err(Error::invalid(
                "archive max_bytes",
                "requires 128 KiB..=8 GiB",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArchiveCursor {
    pub(crate) database: [u8; 16],
    pub(crate) generation: [u8; 16],
    pub(crate) branch: [u8; 16],
    pub(crate) epoch: u64,
    pub(crate) segment: u64,
    pub(crate) offset: u64,
    pub(crate) seq: u64,
    pub(crate) digest: [u8; 32],
}
impl ArchiveCursor {
    #[must_use]
    pub fn to_bytes(self) -> Vec<u8> {
        self.encoded().to_vec()
    }
    fn encoded(self) -> [u8; CURSOR_BYTES] {
        let mut bytes = [0; CURSOR_BYTES];
        bytes[..8].copy_from_slice(b"LSAC\x02\0\0\0");
        bytes[8..24].copy_from_slice(&self.database);
        bytes[24..40].copy_from_slice(&self.generation);
        bytes[40..56].copy_from_slice(&self.branch);
        bytes[56..64].copy_from_slice(&self.epoch.to_le_bytes());
        bytes[64..72].copy_from_slice(&self.segment.to_le_bytes());
        bytes[72..80].copy_from_slice(&self.offset.to_le_bytes());
        bytes[80..88].copy_from_slice(&self.seq.to_le_bytes());
        bytes[88..].copy_from_slice(&self.digest);
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        let mut reader = Reader::new(bytes);
        reader.magic(b"LSAC\x02\0\0\0")?;
        let cursor = Self {
            database: reader.array()?,
            generation: reader.array()?,
            branch: reader.array()?,
            epoch: reader.u64()?,
            segment: reader.u64()?,
            offset: reader.u64()?,
            seq: reader.u64()?,
            digest: reader.array()?,
        };
        reader.finish()?;
        if cursor.segment == 0 || cursor.offset < 32 {
            return Err(super::invalid("invalid cursor position"));
        }
        Ok(cursor)
    }
    pub fn covers(self, other: Self) -> Result<bool> {
        if !self.same_history(other) {
            return Err(Error::invalid(
                "archive cursor",
                "cursors belong to different histories",
            ));
        }
        if self.seq == other.seq {
            if self != other {
                return Err(Error::invalid(
                    "archive cursor",
                    "equal sequences have conflicting positions",
                ));
            }
            return Ok(true);
        }
        if self.seq < other.seq {
            return Ok(false);
        }
        if self.epoch < other.epoch
            || self.segment < other.segment
            || (self.segment == other.segment && self.offset <= other.offset)
        {
            return Err(Error::invalid(
                "archive cursor",
                "cursor positions contradict sequence order",
            ));
        }
        Ok(true)
    }
    pub(crate) fn same_history(self, other: Self) -> bool {
        self.database == other.database
            && self.generation == other.generation
            && self.branch == other.branch
    }
    pub(crate) fn advance(self, epoch: u64, segment: u64, offset: u64, raw: &[u8]) -> Result<Self> {
        let seq = crate::wal::record::inspect(raw)?;
        let expected_offset = if segment == self.segment {
            self.offset
        } else {
            32
        }
        .checked_add(raw.len() as u64);
        if self.seq.checked_add(1) != Some(seq)
            || epoch < self.epoch
            || segment < self.segment
            || (segment != self.segment && segment != seq)
            || (segment == self.segment && epoch != self.epoch && self.offset != 32)
            || expected_offset != Some(offset)
        {
            return Err(super::invalid("archive record position is not continuous"));
        }
        let mut hash = Sha256::new();
        hash.update(self.encoded());
        for value in [epoch, segment, offset] {
            hash.update(value.to_le_bytes());
        }
        hash.update(raw);
        Ok(Self {
            epoch,
            segment,
            offset,
            seq,
            digest: hash.finalize().into(),
            ..self
        })
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExportChunk {
    pub(crate) start: ArchiveCursor,
    pub(crate) end: ArchiveCursor,
    pub(crate) records: Vec<u8>,
}
impl ExportChunk {
    #[must_use]
    pub const fn start(&self) -> ArchiveCursor {
        self.start
    }
    #[must_use]
    pub const fn end(&self) -> ArchiveCursor {
        self.end
    }
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }
    pub fn suffix_after(&self, after: ArchiveCursor) -> Result<Option<Self>> {
        if !after.covers(self.start)? || !self.end.covers(after)? {
            return Err(super::invalid("suffix cursor is outside export range"));
        }
        let mut cursor = self.start;
        let mut reader = Reader::new(&self.records);
        let mut suffix = (after == cursor).then_some(self.records.as_slice());
        while !reader.remaining().is_empty() {
            let (epoch, segment, offset, raw) = read_record(&mut reader)?;
            cursor = cursor.advance(epoch, segment, offset, raw)?;
            if cursor == after {
                suffix = Some(reader.remaining());
            }
        }
        if cursor != self.end {
            return Err(super::invalid("export cursor digest mismatch"));
        }
        let records =
            suffix.ok_or_else(|| super::invalid("suffix cursor is not a record boundary"))?;
        Ok((!records.is_empty()).then(|| Self {
            start: after,
            end: self.end,
            records: records.to_vec(),
        }))
    }
    #[must_use]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(EXPORT_HEADER.saturating_add(self.records.len()));
        bytes.extend_from_slice(b"LSEX\x02\0\0\0");
        bytes.extend_from_slice(&self.start.to_bytes());
        bytes.extend_from_slice(&self.end.to_bytes());
        bytes.extend_from_slice(&self.records);
        bytes
    }
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() > MAX_EXPORT_BYTES as usize {
            return Err(Error::limit(
                "export_bytes",
                bytes.len() as u64,
                u64::from(MAX_EXPORT_BYTES),
            ));
        }
        let mut reader = Reader::new(bytes);
        reader.magic(b"LSEX\x02\0\0\0")?;
        let start = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
        let end = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
        let records = reader.remaining();
        let mut cursor = start;
        let mut stream = Reader::new(records);
        while !stream.remaining().is_empty() {
            let (epoch, segment, offset, raw) = read_record(&mut stream)?;
            cursor = cursor.advance(epoch, segment, offset, raw)?;
        }
        if cursor != end {
            return Err(super::invalid("export cursor digest mismatch"));
        }
        Ok(Self {
            start,
            end,
            records: records.to_vec(),
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ArchiveStatus {
    pub(crate) earliest: ArchiveCursor,
    pub(crate) durable_end: ArchiveCursor,
    pub(crate) bytes: u64,
    pub(crate) healthy: bool,
}
impl ArchiveStatus {
    #[must_use]
    pub const fn earliest(self) -> ArchiveCursor {
        self.earliest
    }
    #[must_use]
    pub const fn durable_end(self) -> ArchiveCursor {
        self.durable_end
    }
    #[must_use]
    pub const fn bytes(self) -> u64 {
        self.bytes
    }
    #[must_use]
    pub const fn healthy(self) -> bool {
        self.healthy
    }
}
