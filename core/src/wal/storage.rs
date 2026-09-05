// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    collections::BTreeSet,
    fs::{self, File},
    io::{self, Read},
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
};

use crc32fast::Hasher;

use super::segment::{parse_segment_name, segment_name};
use crate::{Error, Result, fsutil::sync_directory, limits::MAX_LIVE_UNITS};

pub(crate) const DAMAGED_DIRECTORY: &str = "damaged";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FrozenBoundary {
    pub(crate) first_seq: u64,
    pub(crate) offset: u64,
    pub(crate) next_seq: u64,
    checksum: u32,
}

impl FrozenBoundary {
    fn name(self) -> String {
        format!(
            "{:020}-{:020}-{:020}-{:08x}.wal",
            self.first_seq, self.offset, self.next_seq, self.checksum
        )
    }

    fn parse(name: &str) -> Result<Self> {
        let parts: Vec<_> = name.trim_end_matches(".wal").split('-').collect();
        if parts.len() != 4 {
            return Err(invalid_archive());
        }
        let boundary = Self {
            first_seq: parts[0].parse().map_err(|_| invalid_archive())?,
            offset: parts[1].parse().map_err(|_| invalid_archive())?,
            next_seq: parts[2].parse().map_err(|_| invalid_archive())?,
            checksum: u32::from_str_radix(parts[3], 16).map_err(|_| invalid_archive())?,
        };
        if boundary.name() != name || boundary.first_seq > boundary.next_seq {
            return Err(invalid_archive());
        }
        Ok(boundary)
    }
}

struct Archived {
    path: PathBuf,
    boundary: FrozenBoundary,
}

pub(crate) fn measure(wal_directory: &Path) -> Result<u64> {
    let mut inodes = BTreeSet::new();
    let mut bytes = 0_u64;
    for path in live_segments(wal_directory)?
        .into_iter()
        .chain(archives(wal_directory)?.into_iter().map(|entry| entry.path))
    {
        let metadata = regular_metadata(&path)?;
        if inodes.insert((metadata.dev(), metadata.ino())) {
            bytes = bytes
                .checked_add(metadata.len())
                .ok_or_else(|| Error::limit("wal_storage_bytes", u64::MAX, u64::MAX))?;
        }
    }
    Ok(bytes)
}

pub(crate) fn reclaim(wal_directory: &Path, required: u64, limit: u64) -> Result<u64> {
    let mut bytes = measure(wal_directory)?;
    if fits(bytes, required, limit) {
        return Ok(bytes);
    }
    for archived in archives(wal_directory)? {
        let canonical = wal_directory.join(segment_name(archived.boundary.first_seq));
        if canonical.try_exists()? {
            let original = regular_metadata(&canonical)?;
            let retained = regular_metadata(&archived.path)?;
            if (original.dev(), original.ino()) == (retained.dev(), retained.ino())
                || digest(&canonical, archived.boundary)? == archived.boundary.checksum
            {
                continue;
            }
        }
        fs::remove_file(&archived.path)?;
        sync_directory(&wal_directory.join(DAMAGED_DIRECTORY))?;
        bytes = measure(wal_directory)?;
        if fits(bytes, required, limit) {
            return Ok(bytes);
        }
    }
    Err(Error::limit(
        "wal_storage_bytes",
        bytes.saturating_add(required),
        limit,
    ))
}

pub(crate) fn frozen_boundary(
    wal_directory: &Path,
    first_seq: u64,
) -> Result<Option<FrozenBoundary>> {
    let original = wal_directory.join(segment_name(first_seq));
    for archived in archives(wal_directory)? {
        let boundary = archived.boundary;
        if boundary.first_seq == first_seq
            && boundary.offset <= regular_metadata(&original)?.len()
            && digest(&original, boundary)? == boundary.checksum
        {
            if digest(&archived.path, boundary)? != boundary.checksum {
                return Err(Error::corruption(
                    "WAL archive",
                    "retained original changed",
                ));
            }
            sync_directory(wal_directory)?;
            sync_directory(&wal_directory.join(DAMAGED_DIRECTORY))?;
            return Ok(Some(boundary));
        }
    }
    Ok(None)
}

pub(crate) fn preserve(
    wal_directory: &Path,
    first_seq: u64,
    offset: u64,
    next_seq: u64,
) -> Result<bool> {
    preserve_with_hook(wal_directory, first_seq, offset, next_seq, |_| Ok(()))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PreserveStep {
    ParentSync,
    OriginalSync,
    Link,
    ArchiveSync,
}

fn preserve_with_hook<F>(
    wal_directory: &Path,
    first_seq: u64,
    offset: u64,
    next_seq: u64,
    mut before: F,
) -> Result<bool>
where
    F: FnMut(PreserveStep) -> io::Result<()>,
{
    let source = wal_directory.join(segment_name(first_seq));
    let mut boundary = FrozenBoundary {
        first_seq,
        offset,
        next_seq,
        checksum: 0,
    };
    if offset > regular_metadata(&source)?.len() || first_seq > next_seq {
        return Err(invalid_archive());
    }
    boundary.checksum = digest(&source, boundary)?;
    let archive_directory = wal_directory.join(DAMAGED_DIRECTORY);
    match fs::create_dir(&archive_directory) {
        Ok(()) => {}
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if !fs::symlink_metadata(&archive_directory)?
                .file_type()
                .is_dir()
            {
                return Err(invalid_archive());
            }
        }
        Err(error) => return Err(error.into()),
    }
    before(PreserveStep::ParentSync)?;
    sync_directory(wal_directory)?;
    let target = archive_directory.join(boundary.name());
    before(PreserveStep::OriginalSync)?;
    File::open(&source)?.sync_all()?;
    before(PreserveStep::Link)?;
    match fs::hard_link(&source, &target) {
        Ok(()) => {
            before(PreserveStep::ArchiveSync)?;
            sync_directory(&archive_directory)?;
            Ok(true)
        }
        Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
            if regular_metadata(&target)?.len() != regular_metadata(&source)?.len()
                || digest(&target, boundary)? != boundary.checksum
            {
                return Err(Error::corruption(
                    "WAL archive",
                    "archive identity collision",
                ));
            }
            before(PreserveStep::ArchiveSync)?;
            sync_directory(&archive_directory)?;
            Ok(false)
        }
        Err(error) => Err(error.into()),
    }
}

pub(crate) fn live_segments(wal_directory: &Path) -> Result<Vec<PathBuf>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(wal_directory)? {
        let entry = entry?;
        if entry.file_name() == DAMAGED_DIRECTORY {
            if !entry.file_type()?.is_dir() {
                return Err(invalid_archive());
            }
            continue;
        }
        if !entry.file_type()?.is_file() {
            return Err(Error::corruption(
                "WAL directory",
                "entry is not a regular file",
            ));
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(invalid_archive)?;
        parse_segment_name(name)?;
        paths.push(entry.path());
        check_count(paths.len())?;
    }
    paths.sort_unstable();
    Ok(paths)
}

fn archives(wal_directory: &Path) -> Result<Vec<Archived>> {
    let path = wal_directory.join(DAMAGED_DIRECTORY);
    let metadata = match fs::symlink_metadata(&path) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(error) => return Err(error.into()),
    };
    if !metadata.file_type().is_dir() {
        return Err(invalid_archive());
    }
    let mut output = Vec::new();
    for entry in fs::read_dir(path)? {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            return Err(invalid_archive());
        }
        let name = entry.file_name();
        let name = name.to_str().ok_or_else(invalid_archive)?;
        output.push(Archived {
            path: entry.path(),
            boundary: FrozenBoundary::parse(name)?,
        });
        check_count(output.len())?;
    }
    output.sort_unstable_by_key(|entry| {
        (
            entry.boundary.first_seq,
            entry.boundary.next_seq,
            entry.boundary.offset,
            entry.boundary.checksum,
        )
    });
    Ok(output)
}

fn digest(path: &Path, boundary: FrozenBoundary) -> Result<u32> {
    let mut hasher = Hasher::new();
    hasher.update(b"LSWR1");
    hasher.update(&boundary.first_seq.to_le_bytes());
    hasher.update(&boundary.offset.to_le_bytes());
    hasher.update(&boundary.next_seq.to_le_bytes());
    let mut file = File::open(path)?;
    let mut buffer = [0_u8; 16_384];
    loop {
        let read = file.read(&mut buffer)?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
    }
    Ok(hasher.finalize())
}

fn regular_metadata(path: &Path) -> Result<fs::Metadata> {
    let metadata = fs::symlink_metadata(path)?;
    if !metadata.file_type().is_file() {
        return Err(invalid_archive());
    }
    Ok(metadata)
}

fn fits(bytes: u64, required: u64, limit: u64) -> bool {
    bytes
        .checked_add(required)
        .is_some_and(|total| total <= limit)
}

fn check_count(count: usize) -> Result<()> {
    if count > usize::try_from(MAX_LIVE_UNITS).unwrap_or(usize::MAX) {
        return Err(Error::limit(
            "wal_segments",
            u64::try_from(count).unwrap_or(u64::MAX),
            u64::from(MAX_LIVE_UNITS),
        ));
    }
    Ok(())
}

fn invalid_archive() -> Error {
    Error::corruption("WAL archive", "invalid recovery evidence")
}

#[cfg(test)]
#[path = "storage_tests.rs"]
mod tests;
