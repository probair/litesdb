// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};

use std::{
    collections::BTreeMap,
    fs::File,
    io::{Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::{Arc, Mutex, MutexGuard},
};

use crc32fast::Hasher;

use crate::{
    Error, Result, TableId,
    limits::SECTION_WORKING_MEMORY_BYTES,
    manifest::UnitMeta,
    unit::{DirectoryCache, TableDirectoryEntry, format},
};

const CRC_CHUNK_BYTES: usize = 65_536;

struct FileUnit {
    meta: UnitMeta,
    path: PathBuf,
    integrity: Mutex<Integrity>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Integrity {
    Unchecked,
    Valid,
    Invalid,
}

pub(crate) struct FileUnitSource {
    units: BTreeMap<u64, FileUnit>,
    directories: Arc<Mutex<DirectoryCache>>,
}

impl FileUnitSource {
    pub(crate) fn open(root: &Path, units: &[UnitMeta], budget: u32) -> Result<Self> {
        Self::with_cache(
            root,
            units,
            Arc::new(Mutex::new(DirectoryCache::new(budget)?)),
        )
    }

    pub(crate) fn reopen(&self, root: &Path, units: &[UnitMeta]) -> Result<Self> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::SourceReopen);
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(
            Counter::ReopenedUnits,
            u64::try_from(units.len()).unwrap_or(u64::MAX),
        );
        Self::with_cache(root, units, Arc::clone(&self.directories))
    }

    fn with_cache(
        root: &Path,
        units: &[UnitMeta],
        directories: Arc<Mutex<DirectoryCache>>,
    ) -> Result<Self> {
        let mut loaded = BTreeMap::new();
        for unit in units {
            let path = root.join(format::unit_name(unit.unit_id()));
            let file = File::open(&path)?;
            if file.metadata()?.len() != unit.file_len() {
                return Err(Error::corruption(
                    "unit source",
                    "file length disagrees with MANIFEST",
                ));
            }
            loaded.insert(
                unit.unit_id(),
                FileUnit {
                    meta: *unit,
                    path,
                    integrity: Mutex::new(Integrity::Unchecked),
                },
            );
        }
        Ok(Self {
            units: loaded,
            directories,
        })
    }
}

pub(crate) trait UnitSource {
    fn table_sections(&self, unit: UnitMeta, table: TableId) -> Result<Vec<TableDirectoryEntry>>;

    fn section_bytes(&self, unit: UnitMeta, section: TableDirectoryEntry) -> Result<Vec<u8>>;
}

impl UnitSource for FileUnitSource {
    fn table_sections(&self, unit: UnitMeta, table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        let stored = self.unit(unit)?;
        if let Some(entries) = self
            .lock_directories()?
            .table_sections(unit.unit_id(), table)
        {
            return Ok(entries.to_vec());
        }
        let entries = load_directory(&stored.path, stored.meta)?;
        let selected = select_table(&entries, table).to_vec();
        let _ = self.lock_directories()?.insert(unit.unit_id(), entries)?;
        Ok(selected)
    }

    fn section_bytes(&self, unit: UnitMeta, section: TableDirectoryEntry) -> Result<Vec<u8>> {
        let stored = self.unit(unit)?;
        if !self.directory_contains(stored, section)? {
            return Err(Error::corruption(
                "unit source",
                "requested section is absent from validated directory",
            ));
        }
        if u64::from(section.section_len()) > u64::from(SECTION_WORKING_MEMORY_BYTES) {
            return Err(Error::limit(
                "section_working_memory_bytes",
                u64::from(section.section_len()),
                u64::from(SECTION_WORKING_MEMORY_BYTES),
            ));
        }
        ensure_body_integrity(stored)?;
        let length = usize::try_from(section.section_len())
            .map_err(|_| Error::corruption("unit source", "section length does not fit usize"))?;
        let mut bytes = vec![0_u8; length];
        let mut file = File::open(&stored.path)?;
        file.seek(SeekFrom::Start(section.section_offset()))?;
        file.read_exact(&mut bytes)?;
        Ok(bytes)
    }
}

impl FileUnitSource {
    fn unit(&self, unit: UnitMeta) -> Result<&FileUnit> {
        let stored = self
            .units
            .get(&unit.unit_id())
            .ok_or_else(|| Error::corruption("unit source", "unit is absent"))?;
        if stored.meta != unit {
            return Err(Error::corruption(
                "unit source",
                "MANIFEST metadata changed for immutable unit",
            ));
        }
        Ok(stored)
    }

    fn directory_contains(&self, stored: &FileUnit, section: TableDirectoryEntry) -> Result<bool> {
        if let Some(entries) = self.lock_directories()?.get(stored.meta.unit_id()) {
            return Ok(entries.contains(&section));
        }
        let entries = load_directory(&stored.path, stored.meta)?;
        let contains = entries.contains(&section);
        let _ = self
            .lock_directories()?
            .insert(stored.meta.unit_id(), entries)?;
        Ok(contains)
    }

    fn lock_directories(&self) -> Result<MutexGuard<'_, DirectoryCache>> {
        self.directories.lock().map_err(|_| Error::Poisoned)
    }

    pub(crate) fn validate_all(&self) -> Result<()> {
        for unit in self.units.values() {
            load_directory(&unit.path, unit.meta)?;
            ensure_body_integrity(unit)?;
        }
        Ok(())
    }

    pub(crate) fn directory_cache_bytes(&self) -> Result<u64> {
        Ok(self.lock_directories()?.used_bytes())
    }

    #[cfg(test)]
    fn cache_usage(&self) -> Result<(usize, u64)> {
        let cache = self.lock_directories()?;
        Ok((cache.len(), cache.used_bytes()))
    }
}

fn load_directory(path: &Path, expected: UnitMeta) -> Result<Vec<TableDirectoryEntry>> {
    let mut file = File::open(path)?;
    let file_len = file.metadata()?.len();
    if file_len != expected.file_len() {
        return Err(Error::corruption(
            "unit source",
            "file length disagrees with MANIFEST",
        ));
    }
    let directory_len = usize::try_from(expected.section_count())
        .ok()
        .and_then(|count| count.checked_mul(format::TABLE_DIRECTORY_ENTRY_BYTES))
        .ok_or_else(|| Error::corruption("unit source", "directory length overflow"))?;
    let prefix_len = format::UNIT_HEADER_BYTES
        .checked_add(directory_len)
        .ok_or_else(|| Error::corruption("unit source", "directory end overflow"))?;
    let mut prefix = vec![0_u8; prefix_len];
    file.read_exact(&mut prefix)?;

    let footer_bytes = u64::try_from(format::UNIT_FOOTER_BYTES)
        .map_err(|_| Error::corruption("unit source", "footer length overflow"))?;
    let footer_start = file_len
        .checked_sub(footer_bytes)
        .ok_or_else(|| Error::corruption("unit source", "footer offset underflow"))?;
    file.seek(SeekFrom::Start(footer_start))?;
    let mut footer = [0_u8; format::UNIT_FOOTER_BYTES];
    file.read_exact(&mut footer)?;
    let layout = format::decode_parts(
        &prefix,
        &footer,
        expected.unit_id(),
        file_len,
        expected.body_crc32(),
    )?;
    let header = layout.header();
    if header.level() != expected.level()
        || header.min_ts() != expected.min_ts()
        || header.max_ts() != expected.max_ts()
        || header.section_count() != expected.section_count()
        || header.total_rows() != expected.total_rows()
        || layout.body_crc32() != expected.body_crc32()
    {
        return Err(Error::corruption(
            "unit source",
            "unit envelope disagrees with MANIFEST metadata",
        ));
    }
    Ok(layout.sections().to_vec())
}

fn select_table(entries: &[TableDirectoryEntry], table: TableId) -> &[TableDirectoryEntry] {
    let first = entries.partition_point(|entry| entry.table() < table);
    let last = entries.partition_point(|entry| entry.table() <= table);
    &entries[first..last]
}

fn ensure_body_integrity(unit: &FileUnit) -> Result<()> {
    let mut state = unit.integrity.lock().map_err(|_| Error::Poisoned)?;
    match *state {
        Integrity::Valid => return Ok(()),
        Integrity::Invalid => {
            return Err(Error::corruption(
                "unit source",
                "unit body previously failed integrity validation",
            ));
        }
        Integrity::Unchecked => {}
    }
    let actual = body_crc32(&unit.path, unit.meta.file_len())?;
    if actual != unit.meta.body_crc32() {
        *state = Integrity::Invalid;
        return Err(Error::corruption("unit source", "unit body CRC32 mismatch"));
    }
    *state = Integrity::Valid;
    Ok(())
}

fn body_crc32(path: &Path, file_len: u64) -> Result<u32> {
    let mut file = File::open(path)?;
    let header_bytes = u64::try_from(format::UNIT_HEADER_BYTES)
        .map_err(|_| Error::corruption("unit source", "header length overflow"))?;
    let footer_bytes = u64::try_from(format::UNIT_FOOTER_BYTES)
        .map_err(|_| Error::corruption("unit source", "footer length overflow"))?;
    let mut remaining = file_len
        .checked_sub(header_bytes)
        .and_then(|length| length.checked_sub(footer_bytes))
        .ok_or_else(|| Error::corruption("unit source", "body length underflow"))?;
    file.seek(SeekFrom::Start(header_bytes))?;
    let mut hasher = Hasher::new();
    let mut chunk = vec![0_u8; CRC_CHUNK_BYTES].into_boxed_slice();
    while remaining > 0 {
        let take = usize::try_from(remaining.min(CRC_CHUNK_BYTES as u64))
            .map_err(|_| Error::corruption("unit source", "CRC chunk length overflow"))?;
        file.read_exact(&mut chunk[..take])?;
        hasher.update(&chunk[..take]);
        remaining = remaining
            .checked_sub(u64::try_from(take).unwrap_or(u64::MAX))
            .ok_or_else(|| Error::corruption("unit source", "CRC remaining underflow"))?;
    }
    Ok(hasher.finalize())
}

#[cfg(test)]
#[path = "source_tests.rs"]
mod tests;
