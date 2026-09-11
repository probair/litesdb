// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    ArchiveCursor, ArchiveOptions, ArchiveStore, CURSOR_BYTES, LOG_TARGET_BYTES, Log, MAX_CHUNKS,
    METADATA_RESERVE, Reader, read_record,
};
use crate::{
    Error, Result,
    fsutil::{Area, DbDir, sync_directory},
};
use sha2::{Digest, Sha256};
use std::{
    fs::{self, OpenOptions},
    path::{Path, PathBuf},
    sync::Arc,
};

impl ArchiveStore {
    pub(crate) fn load(directory: Arc<DbDir>, options: ArchiveOptions) -> Result<Option<Self>> {
        options.validate()?;
        let Some((initial, released, durable)) = read_metadata(&directory)? else {
            return Ok(None);
        };
        let path = directory.path(Area::Root).join("archive");
        if !fs::symlink_metadata(&path)?.file_type().is_dir() {
            return Err(super::super::invalid("archive directory is not regular"));
        }
        let paths = discover_logs(&path)?;
        let mut logs = Vec::new();
        let mut charged = METADATA_RESERVE;
        let mut previous = None;
        let mut released_found = released == durable;
        for (first, path) in paths {
            if first > durable.seq {
                fs::remove_file(path)?;
                continue;
            }
            let bytes = read_log(&path)?;
            let mut reader = Reader::new(&bytes);
            reader.magic(b"LSAL\x01\0\0\0")?;
            let start = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
            if !start.same_history(durable)
                || start.seq.checked_add(1) != Some(first)
                || previous.is_some_and(|end| end != start)
            {
                return Err(super::super::invalid("archive log chain gap"));
            }
            if previous.is_none() && start.seq > released.seq {
                return Err(super::super::invalid("missing oldest archive prefix"));
            }
            let mut cursor = start;
            if cursor == released {
                released_found = true;
            }
            while cursor.seq < durable.seq && !reader.remaining().is_empty() {
                let (epoch, segment, offset, raw) = read_record(&mut reader)?;
                cursor = cursor.advance(epoch, segment, offset, raw)?;
                if cursor == released {
                    released_found = true;
                }
            }
            let length = bytes.len().saturating_sub(reader.remaining().len()) as u64;
            if length != bytes.len() as u64 {
                let file = OpenOptions::new().write(true).open(&path)?;
                file.set_len(length)?;
                file.sync_all()?;
            }
            charged = charged
                .checked_add(length)
                .ok_or_else(|| super::super::invalid("archive size overflow"))?;
            logs.push(Log {
                path,
                start,
                end: cursor,
                length,
                dirty: false,
            });
            previous = Some(cursor);
        }
        if (durable != released && previous != Some(durable))
            || previous.is_some_and(|cursor| cursor != durable)
            || !released_found
        {
            return Err(super::super::invalid(
                "committed archive prefix is incomplete",
            ));
        }
        sync_directory(&path)?;
        let active = logs
            .last()
            .map(|log| OpenOptions::new().append(true).open(&log.path))
            .transpose()?;
        let mut store = Self {
            directory,
            options,
            initial,
            released,
            durable,
            staged: durable,
            logs,
            active,
            bytes: charged,
        };
        store.reap()?;
        if store.bytes > options.max_bytes {
            return Err(Error::limit(
                "archive_bytes",
                store.bytes,
                options.max_bytes,
            ));
        }
        Ok(Some(store))
    }
}

pub(super) fn read_log(path: &std::path::Path) -> Result<Vec<u8>> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() || metadata.len() > LOG_TARGET_BYTES.saturating_add(131_072)
    {
        return Err(super::super::invalid("archive log exceeds bounded extent"));
    }
    Ok(fs::read(path)?)
}

pub(super) fn verify_log(log: &Log, durable: ArchiveCursor, bytes: &[u8]) -> Result<()> {
    let mut reader = Reader::new(bytes);
    reader.magic(b"LSAL\x01\0\0\0")?;
    let mut cursor = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
    if cursor != log.start {
        return Err(super::super::invalid("archive header changed"));
    }
    let expected = if log.end.seq > durable.seq {
        durable
    } else {
        log.end
    };
    while cursor.seq < expected.seq {
        let (epoch, segment, offset, raw) = read_record(&mut reader)?;
        cursor = cursor.advance(epoch, segment, offset, raw)?;
    }
    if cursor != expected {
        return Err(super::super::invalid("archive log digest changed"));
    }
    Ok(())
}

fn read_metadata(
    directory: &DbDir,
) -> Result<Option<(ArchiveCursor, ArchiveCursor, ArchiveCursor)>> {
    let metadata = directory.file(Area::Root, "ARCHIVE");
    if !metadata.try_exists()? {
        let objects = directory.path(Area::Root).join("archive");
        if objects.try_exists()? && fs::read_dir(objects)?.next().is_some() {
            return Err(super::super::invalid(
                "archive objects lost authoritative metadata",
            ));
        }
        return Ok(None);
    }
    let extent = fs::symlink_metadata(&metadata)?;
    if !extent.file_type().is_file() || extent.len() != 352 {
        return Err(super::super::invalid("invalid archive metadata length"));
    }
    let bytes = fs::read(metadata)?;
    let end = bytes.len().saturating_sub(32);
    if Sha256::digest(&bytes[..end]).as_slice() != &bytes[end..] {
        return Err(super::super::invalid("archive metadata checksum"));
    }
    let mut reader = Reader::new(&bytes[..end]);
    reader.magic(b"LSAM\x01\0\0\0")?;
    let initial = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
    let released = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
    let durable = ArchiveCursor::from_bytes(reader.take(CURSOR_BYTES)?)?;
    reader.finish()?;
    if !initial.same_history(released)
        || !initial.same_history(durable)
        || initial.seq > released.seq
        || released.seq > durable.seq
    {
        return Err(super::super::invalid("archive metadata ordering"));
    }
    Ok(Some((initial, released, durable)))
}

fn discover_logs(path: &Path) -> Result<Vec<(u64, PathBuf)>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() || paths.len() >= MAX_CHUNKS {
            return Err(super::super::invalid("invalid archive log directory"));
        }
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| super::super::invalid("archive filename"))?;
        let first: u64 = name
            .strip_suffix(".lar")
            .ok_or_else(|| super::super::invalid("archive filename"))?
            .parse()
            .map_err(|_| super::super::invalid("archive filename"))?;
        if format!("{first:020}.lar") != name {
            return Err(super::super::invalid("noncanonical archive filename"));
        }
        paths.push((first, entry.path()));
    }
    paths.sort_unstable_by_key(|entry| entry.0);
    Ok(paths)
}
