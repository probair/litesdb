// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crc32fast::hash;

use super::{
    TABLE_DIRECTORY_ENTRY_BYTES, TableDirectoryEntry, UNIT_END_MAGIC, UNIT_FOOTER_BYTES,
    UNIT_FORMAT_VERSION, UNIT_HEADER_BYTES, UNIT_MAGIC, UnitHeader, UnitLayout, validate_directory,
};
use crate::{Error, Result, TableId, limits::MAX_UNIT_FILE_BYTES, limits::MAX_UNIT_SECTIONS};

pub(crate) fn decode(bytes: &[u8], name_unit_id: u64) -> Result<UnitLayout> {
    let minimum = UNIT_HEADER_BYTES
        .checked_add(UNIT_FOOTER_BYTES)
        .ok_or_else(|| Error::corruption("unit", "minimum file length overflow"))?;
    if bytes.len() < minimum {
        return Err(Error::corruption("unit", "file is shorter than envelope"));
    }
    let file_len = u64::try_from(bytes.len())
        .map_err(|_| Error::limit("unit_file_bytes", u64::MAX, MAX_UNIT_FILE_BYTES))?;
    if file_len > MAX_UNIT_FILE_BYTES {
        return Err(Error::limit(
            "unit_file_bytes",
            file_len,
            MAX_UNIT_FILE_BYTES,
        ));
    }
    let header = decode_header(&bytes[..UNIT_HEADER_BYTES], name_unit_id)?;
    let footer_start = bytes
        .len()
        .checked_sub(UNIT_FOOTER_BYTES)
        .ok_or_else(|| Error::corruption("unit", "footer offset underflow"))?;
    let footer = bytes
        .get(footer_start..)
        .ok_or_else(|| Error::corruption("unit", "footer is absent"))?;
    let body_crc32 = read_u32(footer, 0, "unit footer")?;
    if read_u64(footer, 4, "unit footer")? != file_len {
        return Err(Error::corruption("unit footer", "file length mismatch"));
    }
    if footer.get(12..16) != Some(UNIT_END_MAGIC.as_slice()) {
        return Err(Error::corruption("unit footer", "end magic mismatch"));
    }
    if hash(&bytes[UNIT_HEADER_BYTES..footer_start]) != body_crc32 {
        return Err(Error::corruption("unit footer", "body CRC32 mismatch"));
    }
    let sections = decode_directory(bytes, header, footer_start)?;
    validate_directory(
        header,
        &sections,
        Some(u64::try_from(footer_start).unwrap_or(u64::MAX)),
    )?;
    Ok(UnitLayout {
        header,
        sections,
        body_crc32,
        file_len,
    })
}

pub(crate) fn decode_parts(
    prefix: &[u8],
    footer: &[u8; UNIT_FOOTER_BYTES],
    name_unit_id: u64,
    file_len: u64,
    computed_body_crc32: u32,
) -> Result<UnitLayout> {
    if file_len > MAX_UNIT_FILE_BYTES || prefix.len() < UNIT_HEADER_BYTES {
        return Err(Error::limit(
            "unit_file_bytes",
            file_len,
            MAX_UNIT_FILE_BYTES,
        ));
    }
    let header = decode_header(&prefix[..UNIT_HEADER_BYTES], name_unit_id)?;
    let body_crc32 = read_u32(footer, 0, "unit footer")?;
    if read_u64(footer, 4, "unit footer")? != file_len
        || footer.get(12..16) != Some(UNIT_END_MAGIC.as_slice())
        || body_crc32 != computed_body_crc32
    {
        return Err(Error::corruption(
            "unit footer",
            "file length, end magic, or body CRC32 is invalid",
        ));
    }
    let footer_start = file_len
        .checked_sub(u64::try_from(UNIT_FOOTER_BYTES).unwrap_or(u64::MAX))
        .and_then(|offset| usize::try_from(offset).ok())
        .ok_or_else(|| Error::corruption("unit footer", "footer offset is invalid"))?;
    let sections = decode_directory(prefix, header, footer_start)?;
    let expected_prefix = UNIT_HEADER_BYTES
        .checked_add(
            sections
                .len()
                .checked_mul(TABLE_DIRECTORY_ENTRY_BYTES)
                .ok_or_else(|| Error::corruption("table directory", "length overflow"))?,
        )
        .ok_or_else(|| Error::corruption("table directory", "end overflow"))?;
    if prefix.len() != expected_prefix {
        return Err(Error::corruption(
            "table directory",
            "streamed prefix length is not exact",
        ));
    }
    validate_directory(
        header,
        &sections,
        Some(u64::try_from(footer_start).unwrap_or(u64::MAX)),
    )?;
    Ok(UnitLayout {
        header,
        sections,
        body_crc32,
        file_len,
    })
}

fn decode_header(bytes: &[u8], name_unit_id: u64) -> Result<UnitHeader> {
    if bytes.get(0..4) != Some(UNIT_MAGIC.as_slice())
        || read_u16(bytes, 4, "unit header")? != UNIT_FORMAT_VERSION
        || bytes.get(7) != Some(&0)
        || hash(&bytes[..44]) != read_u32(bytes, 44, "unit header")?
    {
        return Err(Error::corruption(
            "unit header",
            "magic, version, reserved byte, or CRC is invalid",
        ));
    }
    let level = *bytes
        .get(6)
        .ok_or_else(|| Error::corruption("unit header", "level is absent"))?;
    let unit_id = read_u64(bytes, 8, "unit header")?;
    let min_ts = read_i64(bytes, 16, "unit header")?;
    let max_ts = read_i64(bytes, 24, "unit header")?;
    let section_count = read_u32(bytes, 32, "unit header")?;
    let total_rows = read_u64(bytes, 36, "unit header")?;
    if unit_id != name_unit_id
        || level > 2
        || min_ts > max_ts
        || section_count == 0
        || section_count > MAX_UNIT_SECTIONS
    {
        return Err(Error::corruption(
            "unit header",
            "identity, level, range, or section count is invalid",
        ));
    }
    Ok(UnitHeader {
        level,
        unit_id,
        min_ts,
        max_ts,
        section_count,
        total_rows,
    })
}

fn decode_directory(
    bytes: &[u8],
    header: UnitHeader,
    footer_start: usize,
) -> Result<Vec<TableDirectoryEntry>> {
    let count = usize::try_from(header.section_count)
        .map_err(|_| Error::corruption("table directory", "section count does not fit usize"))?;
    let directory_len = count
        .checked_mul(TABLE_DIRECTORY_ENTRY_BYTES)
        .ok_or_else(|| Error::corruption("table directory", "directory length overflow"))?;
    let end = UNIT_HEADER_BYTES
        .checked_add(directory_len)
        .ok_or_else(|| Error::corruption("table directory", "directory end overflow"))?;
    if end > footer_start {
        return Err(Error::corruption(
            "table directory",
            "directory exceeds unit body",
        ));
    }
    let mut sections = Vec::with_capacity(count);
    for index in 0..count {
        let start = UNIT_HEADER_BYTES
            .checked_add(
                index
                    .checked_mul(TABLE_DIRECTORY_ENTRY_BYTES)
                    .ok_or_else(|| Error::corruption("table directory", "entry offset overflow"))?,
            )
            .ok_or_else(|| Error::corruption("table directory", "entry start overflow"))?;
        let entry_end = start
            .checked_add(TABLE_DIRECTORY_ENTRY_BYTES)
            .ok_or_else(|| Error::corruption("table directory", "entry end overflow"))?;
        let entry = bytes
            .get(start..entry_end)
            .ok_or_else(|| Error::corruption("table directory", "truncated entry"))?;
        if entry.get(40..44) != Some([0_u8; 4].as_slice()) {
            return Err(Error::corruption(
                "table directory",
                "reserved bytes are nonzero",
            ));
        }
        sections.push(TableDirectoryEntry {
            table: TableId::new(read_u32(entry, 0, "table directory")?),
            version_no: read_u32(entry, 4, "table directory")?,
            min_ts: read_i64(entry, 8, "table directory")?,
            max_ts: read_i64(entry, 16, "table directory")?,
            row_count: read_u32(entry, 24, "table directory")?,
            section_offset: read_u64(entry, 28, "table directory")?,
            section_len: read_u32(entry, 36, "table directory")?,
        });
    }
    Ok(sections)
}

fn read_u16(bytes: &[u8], offset: usize, context: &'static str) -> Result<u16> {
    read_array::<2>(bytes, offset, context).map(u16::from_le_bytes)
}

fn read_u32(bytes: &[u8], offset: usize, context: &'static str) -> Result<u32> {
    read_array::<4>(bytes, offset, context).map(u32::from_le_bytes)
}

fn read_u64(bytes: &[u8], offset: usize, context: &'static str) -> Result<u64> {
    read_array::<8>(bytes, offset, context).map(u64::from_le_bytes)
}

fn read_i64(bytes: &[u8], offset: usize, context: &'static str) -> Result<i64> {
    read_array::<8>(bytes, offset, context).map(i64::from_le_bytes)
}

fn read_array<const N: usize>(
    bytes: &[u8],
    offset: usize,
    context: &'static str,
) -> Result<[u8; N]> {
    let end = offset
        .checked_add(N)
        .ok_or_else(|| Error::corruption(context, "scalar offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption(context, "truncated scalar"))?;
    <[u8; N]>::try_from(source).map_err(|_| Error::corruption(context, "invalid scalar width"))
}
