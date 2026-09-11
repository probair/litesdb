// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    ArchiveCursor, ArchiveOptions, ArchiveStatus, ExportChunk,
    format::{Reader, encode_record, read_record},
    types::{CURSOR_BYTES, EXPORT_HEADER, LOG_TARGET_BYTES, MAX_EXPORT_BYTES},
};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_atomically, sync_directory},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Write},
    path::PathBuf,
    sync::Arc,
};

#[path = "store_open.rs"]
mod opening;
const LOG_HEADER: usize = 8 + CURSOR_BYTES;
const METADATA_RESERVE: u64 = 8192;
const MAX_CHUNKS: usize = 8192;

#[derive(Debug)]
struct Log {
    path: PathBuf,
    start: ArchiveCursor,
    end: ArchiveCursor,
    length: u64,
    dirty: bool,
}
#[derive(Debug)]
pub(crate) struct ArchiveStore {
    directory: Arc<DbDir>,
    options: ArchiveOptions,
    initial: ArchiveCursor,
    released: ArchiveCursor,
    durable: ArchiveCursor,
    staged: ArchiveCursor,
    logs: Vec<Log>,
    active: Option<File>,
    bytes: u64,
}
impl ArchiveStore {
    pub(crate) fn create(
        directory: Arc<DbDir>,
        options: ArchiveOptions,
        epoch: u64,
        position: crate::DurablePosition,
    ) -> Result<Self> {
        options.validate()?;
        let path = directory.path(Area::Root).join("archive");
        fs::create_dir_all(&path)?;
        if fs::read_dir(&path)?.next().is_some() {
            return Err(super::invalid(
                "archive objects without authoritative metadata",
            ));
        }
        sync_directory(&path)?;
        directory.sync(Area::Root)?;
        let mut identity = [0u8; 32];
        File::open("/dev/urandom")?.read_exact(&mut identity)?;
        let mut cursor = ArchiveCursor {
            database: identity[..16]
                .try_into()
                .map_err(|_| super::invalid("identity"))?,
            branch: identity[16..]
                .try_into()
                .map_err(|_| super::invalid("identity"))?,
            epoch,
            segment: position.segment(),
            offset: position.offset(),
            seq: position.seq(),
            digest: [0; 32],
        };
        let origin = directory.file(Area::Root, "ARCHIVE-ORIGIN");
        if origin.try_exists()? {
            let metadata = fs::symlink_metadata(&origin)?;
            if !metadata.file_type().is_file() || metadata.len() != 144 {
                return Err(super::invalid("invalid restored archive origin"));
            }
            let bytes = fs::read(origin)?;
            if Sha256::digest(&bytes[..112]).as_slice() != &bytes[112..] {
                return Err(super::invalid("restored archive origin checksum"));
            }
            let mut reader = Reader::new(&bytes[..112]);
            reader.magic(b"LSAO\x01\0\0\0")?;
            cursor.database = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?.database;
            reader.finish()?;
        }
        cursor.digest = Sha256::digest(cursor.to_bytes()).into();
        let store = Self {
            directory,
            options,
            initial: cursor,
            released: cursor,
            durable: cursor,
            staged: cursor,
            logs: Vec::new(),
            active: None,
            bytes: METADATA_RESERVE,
        };
        store.publish(cursor, cursor)?;
        Ok(store)
    }
    pub(crate) fn admit(&self, raw_length: u64) -> Result<()> {
        let record_bytes = raw_length
            .checked_add(28)
            .ok_or_else(|| super::invalid("record size overflow"))?;
        let rotate = self
            .logs
            .last()
            .is_none_or(|log| log.length.saturating_add(record_bytes) > LOG_TARGET_BYTES);
        let growth = record_bytes.saturating_add(if rotate { LOG_HEADER as u64 } else { 0 });
        if self
            .bytes
            .checked_add(growth)
            .is_none_or(|n| n > self.options.max_bytes)
            || (rotate && self.logs.len() >= MAX_CHUNKS)
        {
            return Err(Error::limit(
                "archive_bytes",
                self.bytes.saturating_add(growth),
                self.options.max_bytes,
            ));
        }
        Ok(())
    }
    pub(crate) fn stage(
        &mut self,
        epoch: u64,
        segment: u64,
        offset: u64,
        raw: &[u8],
    ) -> Result<()> {
        self.admit(raw.len() as u64)?;
        let next = self.staged.advance(epoch, segment, offset, raw)?;
        let bytes = encode_record(epoch, segment, offset, raw)?;
        if self
            .logs
            .last()
            .is_none_or(|log| log.length.saturating_add(bytes.len() as u64) > LOG_TARGET_BYTES)
        {
            let path = self
                .directory
                .path(Area::Root)
                .join("archive")
                .join(format!("{:020}.lar", next.seq));
            let mut file = OpenOptions::new()
                .write(true)
                .create_new(true)
                .open(&path)?;
            file.write_all(b"LSAL\x01\0\0\0")?;
            file.write_all(&self.staged.to_bytes())?;
            self.logs.push(Log {
                path,
                start: self.staged,
                end: self.staged,
                length: LOG_HEADER as u64,
                dirty: true,
            });
            self.bytes = self.bytes.saturating_add(LOG_HEADER as u64);
            self.active = Some(file);
        }
        self.active
            .as_mut()
            .ok_or_else(|| super::invalid("missing archive file"))?
            .write_all(&bytes)?;
        let log = self
            .logs
            .last_mut()
            .ok_or_else(|| super::invalid("missing archive log"))?;
        log.length = log.length.saturating_add(bytes.len() as u64);
        log.end = next;
        log.dirty = true;
        self.bytes = self.bytes.saturating_add(bytes.len() as u64);
        self.staged = next;
        Ok(())
    }
    pub(crate) fn commit(&mut self, primary_seq: u64) -> Result<()> {
        if self.staged.seq != primary_seq {
            return Err(super::invalid("archive and primary sequence disagree"));
        }
        if self.staged == self.durable {
            return Ok(());
        }
        for log in &self.logs {
            if log.dirty {
                File::open(&log.path)?.sync_all()?;
            }
        }
        sync_directory(&self.directory.path(Area::Root).join("archive"))?;
        self.publish(self.released, self.staged)?;
        self.durable = self.staged;
        for log in &mut self.logs {
            log.dirty = false;
        }
        Ok(())
    }
    pub(crate) fn ensure_protected(&self, seq: u64) -> Result<()> {
        if seq > self.durable.seq {
            return Err(super::invalid("unprotected WAL reclamation"));
        }
        Ok(())
    }
    pub(crate) const fn status(&self, healthy: bool) -> ArchiveStatus {
        ArchiveStatus {
            earliest: self.released,
            durable_end: self.durable,
            bytes: self.bytes,
            healthy,
        }
    }
    pub(crate) const fn durable(&self) -> ArchiveCursor {
        self.durable
    }
    pub(crate) fn export(&self, after: ArchiveCursor, maximum: u32) -> Result<ExportChunk> {
        let maximum = maximum.min(MAX_EXPORT_BYTES) as usize;
        if maximum < EXPORT_HEADER {
            return Err(Error::invalid("max_bytes", "cannot hold an export header"));
        }
        if !after.same_history(self.durable)
            || after.seq < self.released.seq
            || after.seq > self.durable.seq
        {
            return Err(Error::invalid(
                "archive cursor",
                "cursor is outside retained history",
            ));
        }
        if after == self.durable {
            return Ok(ExportChunk {
                start: after,
                end: after,
                records: Vec::new(),
            });
        }
        let mut records = Vec::new();
        let mut cursor = after;
        let mut found = after == self.released;
        for log in &self.logs {
            if log.end.seq < after.seq || log.start.seq >= self.durable.seq {
                continue;
            }
            let bytes = opening::read_log(&log.path)?;
            opening::verify_log(log, self.durable, &bytes)?;
            let mut reader = Reader::new(&bytes);
            reader.magic(b"LSAL\x01\0\0\0")?;
            let mut current = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
            if current.seq == after.seq {
                if current != after {
                    return Err(Error::invalid(
                        "archive cursor",
                        "cursor is not a retained record boundary",
                    ));
                }
                found = true;
            }
            while current.seq < self.durable.seq && !reader.remaining().is_empty() {
                let before = reader.remaining();
                let (epoch, segment, offset, raw) = read_record(&mut reader)?;
                current = current.advance(epoch, segment, offset, raw)?;
                if current.seq == after.seq {
                    if current != after {
                        return Err(Error::invalid(
                            "archive cursor",
                            "cursor is not a retained record boundary",
                        ));
                    }
                    found = true;
                }
                if current.seq <= after.seq {
                    continue;
                }
                if !found {
                    return Err(super::invalid("archive cursor is absent"));
                }
                let consumed = before.len().saturating_sub(reader.remaining().len());
                if EXPORT_HEADER
                    .saturating_add(records.len())
                    .saturating_add(consumed)
                    > maximum
                {
                    if records.is_empty() {
                        return Err(Error::invalid(
                            "max_bytes",
                            "cannot hold the next complete WAL record",
                        ));
                    }
                    return Ok(ExportChunk {
                        start: after,
                        end: cursor,
                        records,
                    });
                }
                records.extend_from_slice(&before[..consumed]);
                cursor = current;
            }
            if cursor == self.durable {
                break;
            }
        }
        if cursor == after {
            return Err(super::invalid("retained archive has a gap"));
        }
        Ok(ExportChunk {
            start: after,
            end: cursor,
            records,
        })
    }
    fn validate_cursor(&self, requested: ArchiveCursor) -> Result<()> {
        if !requested.same_history(self.durable)
            || requested.seq < self.released.seq
            || requested.seq > self.durable.seq
        {
            return Err(Error::invalid(
                "archive cursor",
                "cursor is outside retained history",
            ));
        }
        if requested == self.released || requested == self.durable {
            return Ok(());
        }
        for log in &self.logs {
            if requested.seq < log.start.seq || requested.seq > log.end.seq {
                continue;
            }
            let bytes = opening::read_log(&log.path)?;
            opening::verify_log(log, self.durable, &bytes)?;
            let mut reader = Reader::new(&bytes);
            reader.magic(b"LSAL\x01\0\0\0")?;
            let mut cursor = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
            while cursor.seq < requested.seq {
                let (epoch, segment, offset, raw) = read_record(&mut reader)?;
                cursor = cursor.advance(epoch, segment, offset, raw)?;
            }
            if cursor == requested {
                return Ok(());
            }
            return Err(Error::invalid(
                "archive cursor",
                "cursor is not a retained record boundary",
            ));
        }
        Err(super::invalid("retained archive has a gap"))
    }
    pub(crate) fn release(&mut self, through: ArchiveCursor) -> Result<()> {
        for pin in super::base::read_pins(&self.directory.path(Area::Root))? {
            if !pin.same_history(through) || through.seq > pin.seq {
                return Err(Error::limit(
                    "archive_release_sequence",
                    through.seq,
                    pin.seq,
                ));
            }
        }
        self.validate_cursor(through)?;
        if through.seq < self.released.seq {
            return Err(Error::invalid("release cursor", "release regressed"));
        }
        self.publish(through, self.durable)?;
        self.released = through;
        self.reap()
    }
    fn reap(&mut self) -> Result<()> {
        let count = self
            .logs
            .iter()
            .take_while(|log| log.end.seq <= self.released.seq)
            .count();
        if count == self.logs.len() {
            self.active = None;
        }
        for log in self.logs.drain(..count) {
            fs::remove_file(&log.path)?;
            self.bytes = self.bytes.saturating_sub(log.length);
        }
        sync_directory(&self.directory.path(Area::Root).join("archive"))?;
        Ok(())
    }
    fn publish(&self, released: ArchiveCursor, durable: ArchiveCursor) -> Result<()> {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(b"LSAM\x01\0\0\0");
        for cursor in [self.initial, released, durable] {
            bytes.extend_from_slice(&cursor.to_bytes());
        }
        let digest = Sha256::digest(&bytes);
        bytes.extend_from_slice(&digest);
        publish_atomically(&self.directory, Area::Root, "ARCHIVE", &bytes)
    }
}

pub(crate) fn encode_origin(cursor: ArchiveCursor) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"LSAO\x01\0\0\0");
    bytes.extend_from_slice(&cursor.to_bytes());
    let digest = Sha256::digest(&bytes);
    bytes.extend_from_slice(&digest);
    bytes
}
