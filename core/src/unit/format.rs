// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by unit sealing and decoding later in M4"
)]

use crc32fast::hash;

use crate::{
    Error, Result, TableId,
    limits::{MAX_SECTION_ROWS, MAX_UNIT_FILE_BYTES, MAX_UNIT_SECTIONS},
};

#[path = "format_decode.rs"]
mod decode_impl;

pub(crate) const UNIT_HEADER_BYTES: usize = 48;
pub(crate) const TABLE_DIRECTORY_ENTRY_BYTES: usize = 44;
pub(crate) const UNIT_FOOTER_BYTES: usize = 16;
const UNIT_MAGIC: [u8; 4] = *b"LSU1";
const UNIT_END_MAGIC: [u8; 4] = *b"1USL";
const UNIT_FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnitHeader {
    level: u8,
    unit_id: u64,
    min_ts: i64,
    max_ts: i64,
    section_count: u32,
    total_rows: u64,
}

impl UnitHeader {
    pub(crate) fn new(
        level: u8,
        unit_id: u64,
        min_ts: i64,
        max_ts: i64,
        section_count: u32,
        total_rows: u64,
    ) -> Result<Self> {
        if level > 2 || min_ts > max_ts || section_count == 0 {
            return Err(Error::invalid(
                "unit header",
                "level, range, or section count is invalid",
            ));
        }
        if section_count > MAX_UNIT_SECTIONS {
            return Err(Error::limit(
                "unit_sections",
                u64::from(section_count),
                u64::from(MAX_UNIT_SECTIONS),
            ));
        }
        Ok(Self {
            level,
            unit_id,
            min_ts,
            max_ts,
            section_count,
            total_rows,
        })
    }

    pub(crate) const fn level(self) -> u8 {
        self.level
    }

    pub(crate) const fn unit_id(self) -> u64 {
        self.unit_id
    }

    pub(crate) const fn min_ts(self) -> i64 {
        self.min_ts
    }

    pub(crate) const fn max_ts(self) -> i64 {
        self.max_ts
    }

    pub(crate) const fn section_count(self) -> u32 {
        self.section_count
    }

    pub(crate) const fn total_rows(self) -> u64 {
        self.total_rows
    }

    pub(crate) fn encode(self) -> [u8; UNIT_HEADER_BYTES] {
        let mut bytes = [0_u8; UNIT_HEADER_BYTES];
        bytes[0..4].copy_from_slice(&UNIT_MAGIC);
        bytes[4..6].copy_from_slice(&UNIT_FORMAT_VERSION.to_le_bytes());
        bytes[6] = self.level;
        bytes[8..16].copy_from_slice(&self.unit_id.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.min_ts.to_le_bytes());
        bytes[24..32].copy_from_slice(&self.max_ts.to_le_bytes());
        bytes[32..36].copy_from_slice(&self.section_count.to_le_bytes());
        bytes[36..44].copy_from_slice(&self.total_rows.to_le_bytes());
        let crc = hash(&bytes[..44]);
        bytes[44..48].copy_from_slice(&crc.to_le_bytes());
        bytes
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct TableDirectoryEntry {
    table: TableId,
    version_no: u32,
    min_ts: i64,
    max_ts: i64,
    row_count: u32,
    section_offset: u64,
    section_len: u32,
}

impl TableDirectoryEntry {
    #[allow(
        clippy::too_many_arguments,
        reason = "fields mirror one fixed directory entry"
    )]
    pub(crate) fn new(
        table: TableId,
        version_no: u32,
        min_ts: i64,
        max_ts: i64,
        row_count: u32,
        section_offset: u64,
        section_len: u32,
    ) -> Result<Self> {
        if version_no == 0 || min_ts > max_ts || row_count == 0 || section_len == 0 {
            return Err(Error::invalid(
                "table directory",
                "version, range, row count, or section length is invalid",
            ));
        }
        if row_count > MAX_SECTION_ROWS {
            return Err(Error::limit(
                "section_rows",
                u64::from(row_count),
                u64::from(MAX_SECTION_ROWS),
            ));
        }
        Ok(Self {
            table,
            version_no,
            min_ts,
            max_ts,
            row_count,
            section_offset,
            section_len,
        })
    }

    pub(crate) const fn table(self) -> TableId {
        self.table
    }

    pub(crate) const fn version_no(self) -> u32 {
        self.version_no
    }

    pub(crate) const fn min_ts(self) -> i64 {
        self.min_ts
    }

    pub(crate) const fn max_ts(self) -> i64 {
        self.max_ts
    }

    pub(crate) const fn row_count(self) -> u32 {
        self.row_count
    }

    pub(crate) const fn section_offset(self) -> u64 {
        self.section_offset
    }

    pub(crate) const fn section_len(self) -> u32 {
        self.section_len
    }

    pub(crate) fn encode(self) -> [u8; TABLE_DIRECTORY_ENTRY_BYTES] {
        let mut bytes = [0_u8; TABLE_DIRECTORY_ENTRY_BYTES];
        bytes[0..4].copy_from_slice(&self.table.get().to_le_bytes());
        bytes[4..8].copy_from_slice(&self.version_no.to_le_bytes());
        bytes[8..16].copy_from_slice(&self.min_ts.to_le_bytes());
        bytes[16..24].copy_from_slice(&self.max_ts.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.row_count.to_le_bytes());
        bytes[28..36].copy_from_slice(&self.section_offset.to_le_bytes());
        bytes[36..40].copy_from_slice(&self.section_len.to_le_bytes());
        bytes
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct UnitLayout {
    header: UnitHeader,
    sections: Vec<TableDirectoryEntry>,
    body_crc32: u32,
    file_len: u64,
}

impl UnitLayout {
    pub(crate) const fn header(&self) -> UnitHeader {
        self.header
    }

    pub(crate) fn sections(&self) -> &[TableDirectoryEntry] {
        &self.sections
    }

    pub(crate) const fn body_crc32(&self) -> u32 {
        self.body_crc32
    }

    pub(crate) const fn file_len(&self) -> u64 {
        self.file_len
    }
}

pub(crate) fn unit_name(unit_id: u64) -> String {
    format!("{unit_id:016x}.lsu")
}

pub(crate) fn parse_unit_name(name: &str) -> Result<u64> {
    let Some(digits) = name.strip_suffix(".lsu") else {
        return Err(Error::corruption("unit name", "suffix is not .lsu"));
    };
    if digits.len() != 16
        || !digits
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(Error::corruption(
            "unit name",
            "identifier is not 16 lowercase hexadecimal digits",
        ));
    }
    let unit_id = u64::from_str_radix(digits, 16)
        .map_err(|_| Error::corruption("unit name", "identifier exceeds u64"))?;
    if unit_name(unit_id) != name {
        return Err(Error::corruption("unit name", "name is not canonical"));
    }
    Ok(unit_id)
}

pub(crate) fn decode(bytes: &[u8], name_unit_id: u64) -> Result<UnitLayout> {
    decode_impl::decode(bytes, name_unit_id)
}

pub(crate) fn decode_parts(
    prefix: &[u8],
    footer: &[u8; UNIT_FOOTER_BYTES],
    name_unit_id: u64,
    file_len: u64,
    body_crc32: u32,
) -> Result<UnitLayout> {
    decode_impl::decode_parts(prefix, footer, name_unit_id, file_len, body_crc32)
}

pub(crate) fn encode(
    header: UnitHeader,
    sections: &[TableDirectoryEntry],
    payloads: &[&[u8]],
) -> Result<Vec<u8>> {
    if sections.len() != payloads.len()
        || sections.len() != usize::try_from(header.section_count).unwrap_or(usize::MAX)
    {
        return Err(Error::invalid(
            "unit sections",
            "header, directory, and payload counts differ",
        ));
    }
    validate_directory(header, sections, None)?;
    for (entry, payload) in sections.iter().zip(payloads) {
        if usize::try_from(entry.section_len).ok() != Some(payload.len()) {
            return Err(Error::invalid(
                "section payload",
                "payload length differs from directory",
            ));
        }
    }
    let footer_start = section_end(sections)?;
    let file_len = footer_start
        .checked_add(
            u64::try_from(UNIT_FOOTER_BYTES)
                .map_err(|_| Error::limit("unit_file_bytes", u64::MAX, MAX_UNIT_FILE_BYTES))?,
        )
        .ok_or_else(|| Error::limit("unit_file_bytes", u64::MAX, MAX_UNIT_FILE_BYTES))?;
    if file_len > MAX_UNIT_FILE_BYTES {
        return Err(Error::limit(
            "unit_file_bytes",
            file_len,
            MAX_UNIT_FILE_BYTES,
        ));
    }
    let capacity = usize::try_from(file_len)
        .map_err(|_| Error::limit("unit_file_bytes", file_len, MAX_UNIT_FILE_BYTES))?;
    let mut bytes = Vec::with_capacity(capacity);
    bytes.extend_from_slice(&header.encode());
    for entry in sections {
        bytes.extend_from_slice(&entry.encode());
    }
    for payload in payloads {
        bytes.extend_from_slice(payload);
    }
    let body_crc = hash(&bytes[UNIT_HEADER_BYTES..]);
    bytes.extend_from_slice(&body_crc.to_le_bytes());
    bytes.extend_from_slice(&file_len.to_le_bytes());
    bytes.extend_from_slice(&UNIT_END_MAGIC);
    if bytes.len() != capacity {
        return Err(Error::corruption(
            "unit",
            "encoded file differs from measured length",
        ));
    }
    Ok(bytes)
}

pub(crate) fn encode_footer(body_crc32: u32, file_len: u64) -> [u8; UNIT_FOOTER_BYTES] {
    let mut bytes = [0_u8; UNIT_FOOTER_BYTES];
    bytes[..4].copy_from_slice(&body_crc32.to_le_bytes());
    bytes[4..12].copy_from_slice(&file_len.to_le_bytes());
    bytes[12..16].copy_from_slice(&UNIT_END_MAGIC);
    bytes
}

pub(crate) fn validate_stream_plan(
    header: UnitHeader,
    sections: &[TableDirectoryEntry],
    footer_start: u64,
) -> Result<()> {
    validate_directory(header, sections, Some(footer_start))
}

fn validate_directory(
    header: UnitHeader,
    sections: &[TableDirectoryEntry],
    expected_footer_start: Option<u64>,
) -> Result<()> {
    let directory_bytes = sections
        .len()
        .checked_mul(TABLE_DIRECTORY_ENTRY_BYTES)
        .and_then(|bytes| bytes.checked_add(UNIT_HEADER_BYTES))
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| Error::corruption("table directory", "directory end overflow"))?;
    let mut expected_offset = directory_bytes;
    let mut rows = 0_u64;
    let mut global_min = i64::MAX;
    let mut global_max = i64::MIN;
    let mut previous: Option<TableDirectoryEntry> = None;
    for entry in sections {
        if entry.version_no == 0
            || entry.min_ts > entry.max_ts
            || entry.row_count == 0
            || entry.row_count > MAX_SECTION_ROWS
            || entry.section_len == 0
            || entry.section_offset != expected_offset
        {
            return Err(Error::corruption(
                "table directory",
                "entry range, count, or extent is invalid",
            ));
        }
        if let Some(prior) = previous
            && ((prior.table, prior.min_ts) >= (entry.table, entry.min_ts)
                || (prior.table == entry.table && prior.max_ts >= entry.min_ts))
        {
            return Err(Error::corruption(
                "table directory",
                "entries are unordered or overlap",
            ));
        }
        expected_offset = expected_offset
            .checked_add(u64::from(entry.section_len))
            .ok_or_else(|| Error::corruption("table directory", "section end overflow"))?;
        rows = rows
            .checked_add(u64::from(entry.row_count))
            .ok_or_else(|| Error::corruption("table directory", "row total overflow"))?;
        global_min = global_min.min(entry.min_ts);
        global_max = global_max.max(entry.max_ts);
        previous = Some(*entry);
    }
    if rows != header.total_rows || global_min != header.min_ts || global_max != header.max_ts {
        return Err(Error::corruption(
            "table directory",
            "directory totals or global range disagree with header",
        ));
    }
    if expected_footer_start.is_some_and(|expected| expected_offset != expected) {
        return Err(Error::corruption(
            "table directory",
            "sections do not end at footer",
        ));
    }
    Ok(())
}

fn section_end(sections: &[TableDirectoryEntry]) -> Result<u64> {
    let Some(last) = sections.last() else {
        return Err(Error::invalid(
            "unit sections",
            "unit must contain a section",
        ));
    };
    last.section_offset
        .checked_add(u64::from(last.section_len))
        .ok_or_else(|| Error::limit("unit_file_bytes", u64::MAX, MAX_UNIT_FILE_BYTES))
}

#[cfg(test)]
#[path = "format_tests.rs"]
mod tests;
