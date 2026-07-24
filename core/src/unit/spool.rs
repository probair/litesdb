// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    fs::{self, File, OpenOptions},
    io::{Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crc32fast::Hasher;

use crate::{
    Error, Result,
    fsutil::{Area, DbDir, publish_streaming},
    limits::{Limit, MAX_OPERATION_MEMORY_BYTES, SECTION_WORKING_MEMORY_BYTES, ensure_at_most},
    manifest::UnitMeta,
    unit::{
        format::{self, TableDirectoryEntry, UnitHeader},
        seal::EncodedSection,
    },
};

const COPY_BUFFER_BYTES: usize = 65_536;

struct SpooledSection {
    table: crate::TableId,
    version_no: u32,
    min_ts: i64,
    max_ts: i64,
    row_count: u32,
    payload_len: u32,
}

pub(crate) struct UnitSpool {
    level: u8,
    unit_id: u64,
    path: PathBuf,
    file: File,
    sections: Vec<SpooledSection>,
    payload_bytes: u64,
    total_rows: u64,
    min_ts: i64,
    max_ts: i64,
}

impl UnitSpool {
    pub(crate) fn new(directory: &DbDir, level: u8, unit_id: u64) -> Result<Self> {
        let name = format!(".{}.spool", format::unit_name(unit_id));
        let path = directory.path(Area::Temporary).join(name);
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)?;
        Ok(Self {
            level,
            unit_id,
            path,
            file,
            sections: Vec::new(),
            payload_bytes: 0,
            total_rows: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        })
    }

    pub(crate) fn push(&mut self, section: &EncodedSection) -> Result<()> {
        if section.payload.len()
            > usize::try_from(SECTION_WORKING_MEMORY_BYTES).unwrap_or(usize::MAX)
        {
            return Err(Error::limit(
                "section_working_memory_bytes",
                u64::try_from(section.payload.len()).unwrap_or(u64::MAX),
                u64::from(SECTION_WORKING_MEMORY_BYTES),
            ));
        }
        ensure_at_most(
            Limit::UnitSections,
            u64::try_from(self.sections.len().saturating_add(1)).unwrap_or(u64::MAX),
        )?;
        if self.sections.last().is_some_and(|previous| {
            (previous.table, previous.min_ts) >= (section.table, section.min_ts)
                || (previous.table == section.table && previous.max_ts >= section.min_ts)
        }) {
            return Err(Error::corruption(
                "unit spool",
                "sections are unordered or overlapping",
            ));
        }
        let payload_len = u32::try_from(section.payload.len()).map_err(|_| {
            Error::limit(
                "section_bytes",
                u64::try_from(section.payload.len()).unwrap_or(u64::MAX),
                u64::from(u32::MAX),
            )
        })?;
        self.file.write_all(&section.payload)?;
        self.payload_bytes = self
            .payload_bytes
            .checked_add(u64::from(payload_len))
            .ok_or_else(|| {
                Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
            })?;
        self.total_rows = self
            .total_rows
            .checked_add(u64::from(section.row_count))
            .ok_or_else(|| Error::limit("unit_rows", u64::MAX, u64::MAX))?;
        self.min_ts = self.min_ts.min(section.min_ts);
        self.max_ts = self.max_ts.max(section.max_ts);
        self.sections.push(SpooledSection {
            table: section.table,
            version_no: section.version_no,
            min_ts: section.min_ts,
            max_ts: section.max_ts,
            row_count: section.row_count,
            payload_len,
        });
        Ok(())
    }

    pub(crate) fn publish(mut self, directory: &DbDir) -> Result<UnitMeta> {
        if self.sections.is_empty() {
            return Err(Error::invalid("unit spool", "unit contains no sections"));
        }
        let name = format::unit_name(self.unit_id);
        if directory.file(Area::Units, &name).try_exists()? {
            return Err(Error::invalid(
                "unit_id",
                "immutable unit file already exists",
            ));
        }
        let section_count = u32::try_from(self.sections.len())
            .map_err(|_| Error::limit("unit_sections", u64::MAX, Limit::UnitSections.maximum()))?;
        let header = UnitHeader::new(
            self.level,
            self.unit_id,
            self.min_ts,
            self.max_ts,
            section_count,
            self.total_rows,
        )?;
        let entries = self.directory_entries()?;
        let footer_start = entries
            .last()
            .map(|entry| {
                entry
                    .section_offset()
                    .saturating_add(u64::from(entry.section_len()))
            })
            .ok_or_else(|| Error::corruption("unit spool", "directory is empty"))?;
        let file_len = footer_start
            .checked_add(u64::try_from(format::UNIT_FOOTER_BYTES).unwrap_or(u64::MAX))
            .ok_or_else(|| {
                Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
            })?;
        ensure_at_most(Limit::UnitFileBytes, file_len)?;
        format::validate_stream_plan(header, &entries, footer_start)?;
        self.file.flush()?;
        self.file.seek(SeekFrom::Start(0))?;

        let mut published_crc = None;
        publish_streaming(directory, Area::Units, &name, |output| {
            output.write_all(&header.encode())?;
            let mut hasher = Hasher::new();
            for entry in &entries {
                let bytes = entry.encode();
                output.write_all(&bytes)?;
                hasher.update(&bytes);
            }
            let mut remaining = self.payload_bytes;
            let mut buffer = vec![0_u8; COPY_BUFFER_BYTES].into_boxed_slice();
            while remaining > 0 {
                let take = usize::try_from(remaining.min(COPY_BUFFER_BYTES as u64))
                    .map_err(|_| Error::corruption("unit spool", "copy length overflow"))?;
                self.file.read_exact(&mut buffer[..take])?;
                output.write_all(&buffer[..take])?;
                hasher.update(&buffer[..take]);
                remaining = remaining
                    .checked_sub(u64::try_from(take).unwrap_or(u64::MAX))
                    .ok_or_else(|| Error::corruption("unit spool", "copy length underflow"))?;
            }
            let body_crc32 = hasher.finalize();
            output.write_all(&format::encode_footer(body_crc32, file_len))?;
            published_crc = Some(body_crc32);
            Ok(())
        })?;
        let body_crc32 = published_crc
            .ok_or_else(|| Error::corruption("unit spool", "publisher produced no checksum"))?;
        UnitMeta::new(
            self.unit_id,
            self.level,
            self.min_ts,
            self.max_ts,
            section_count,
            self.total_rows,
            file_len,
            body_crc32,
        )
    }

    fn directory_entries(&self) -> Result<Vec<TableDirectoryEntry>> {
        let directory_bytes = self
            .sections
            .len()
            .checked_mul(format::TABLE_DIRECTORY_ENTRY_BYTES)
            .ok_or_else(|| {
                Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
            })?;
        let mut offset = format::UNIT_HEADER_BYTES
            .checked_add(directory_bytes)
            .and_then(|value| u64::try_from(value).ok())
            .ok_or_else(|| {
                Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
            })?;
        let mut entries = Vec::with_capacity(self.sections.len());
        for section in &self.sections {
            entries.push(TableDirectoryEntry::new(
                section.table,
                section.version_no,
                section.min_ts,
                section.max_ts,
                section.row_count,
                offset,
                section.payload_len,
            )?);
            offset = offset
                .checked_add(u64::from(section.payload_len))
                .ok_or_else(|| {
                    Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
                })?;
        }
        Ok(entries)
    }
}

impl Drop for UnitSpool {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.path);
    }
}

const _: () = assert!(COPY_BUFFER_BYTES <= MAX_OPERATION_MEMORY_BYTES as usize);
