// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crc32fast::hash;

use crate::{
    Error, FieldId, FieldSchema, Result, SeriesId, TableId, TableVersion, Validity, ValueType,
    limits::{MAX_LIVE_UNITS, MAX_LOGICAL_STREAMS, MAX_MANIFEST_BODY_BYTES, MAX_TABLES},
    manifest::{
        catalog::{
            FieldRetirement, Manifest, ManifestIdentity, RetentionState, SeriesRetirement,
            TableCatalog, UnitMeta,
        },
        format::{MANIFEST_FORMAT_VERSION, MANIFEST_HEADER_BYTES, MANIFEST_MAGIC},
    },
    wal::Checkpoint,
};

pub(super) fn decode(bytes: &[u8]) -> Result<Manifest> {
    if bytes.len() < MANIFEST_HEADER_BYTES {
        return Err(Error::corruption("MANIFEST", "truncated header"));
    }
    if bytes.get(0..4) != Some(MANIFEST_MAGIC.as_slice()) {
        return Err(Error::corruption("MANIFEST", "magic mismatch"));
    }
    if read_at_u16(bytes, 4)? != MANIFEST_FORMAT_VERSION {
        return Err(Error::corruption("MANIFEST", "format version mismatch"));
    }
    if read_at_u16(bytes, 6)? != 0 {
        return Err(Error::corruption("MANIFEST", "reserved field is nonzero"));
    }
    let generation = read_at_u64(bytes, 8)?;
    let body_len = read_at_u32(bytes, 16)?;
    if body_len > MAX_MANIFEST_BODY_BYTES {
        return Err(Error::limit(
            "manifest_body_bytes",
            u64::from(body_len),
            u64::from(MAX_MANIFEST_BODY_BYTES),
        ));
    }
    let expected_len = usize::try_from(body_len)
        .ok()
        .and_then(|length| length.checked_add(MANIFEST_HEADER_BYTES))
        .ok_or_else(|| Error::corruption("MANIFEST", "file length overflow"))?;
    if bytes.len() != expected_len {
        return Err(Error::corruption("MANIFEST", "file length mismatch"));
    }
    let body = bytes
        .get(MANIFEST_HEADER_BYTES..)
        .ok_or_else(|| Error::corruption("MANIFEST", "body is absent"))?;
    if hash(body) != read_at_u32(bytes, 20)? {
        return Err(Error::corruption("MANIFEST", "body CRC32 mismatch"));
    }
    decode_body(generation, body)
}

fn decode_body(header_generation: u64, body: &[u8]) -> Result<Manifest> {
    let mut cursor = Cursor::new(body);
    let checkpoint = Checkpoint::new(cursor.read_u64()?, cursor.read_u64()?, cursor.read_u64()?)?;
    let unit_high_water = cursor.read_u64()?;
    let table_high_water = cursor.read_u32()?;
    let body_generation = cursor.read_u64()?;
    if body_generation != header_generation {
        return Err(Error::corruption(
            "MANIFEST",
            "header and body generations disagree",
        ));
    }
    let identity = ManifestIdentity::new(
        header_generation,
        unit_high_water,
        table_high_water,
        cursor.read_u64()?,
        cursor.read_u64()?,
    );
    let retention = RetentionState::new(cursor.read_option_i64()?, cursor.read_option_u64()?);
    let table_count = cursor.read_count(MAX_TABLES, 17, "tables")?;
    let mut tables = Vec::with_capacity(table_count);
    for _ in 0..table_count {
        tables.push(decode_table(&mut cursor)?);
    }
    let unit_count = cursor.read_count(MAX_LIVE_UNITS, 49, "live_units")?;
    let mut units = Vec::with_capacity(unit_count);
    for _ in 0..unit_count {
        units.push(UnitMeta::new(
            cursor.read_u64()?,
            cursor.read_u8()?,
            cursor.read_i64()?,
            cursor.read_i64()?,
            cursor.read_u32()?,
            cursor.read_u64()?,
            cursor.read_u64()?,
            cursor.read_u32()?,
        )?);
    }
    if !cursor.is_exhausted() {
        return Err(Error::corruption("MANIFEST", "trailing body bytes"));
    }
    Manifest::restore(identity, checkpoint, retention, tables, units)
}

fn decode_table(cursor: &mut Cursor<'_>) -> Result<TableCatalog> {
    let table = TableId::new(cursor.read_u32()?);
    let last_ts = cursor.read_option_i64()?;
    let version_count = cursor.read_count(MAX_LOGICAL_STREAMS, 10, "table_versions")?;
    let mut versions = Vec::with_capacity(version_count);
    for _ in 0..version_count {
        versions.push(decode_version(cursor)?);
    }
    let series_count = cursor.read_count(MAX_LOGICAL_STREAMS, 16, "retired_series")?;
    let mut retired_series = Vec::with_capacity(series_count);
    for _ in 0..series_count {
        retired_series.push(SeriesRetirement::new(
            SeriesId::new(cursor.read_u64()?),
            cursor.read_i64()?,
        ));
    }
    let field_count = cursor.read_count(MAX_LOGICAL_STREAMS, 10, "retired_fields")?;
    let mut retired_fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        retired_fields.push(FieldRetirement::new(
            FieldId::new(cursor.read_u16()?),
            cursor.read_i64()?,
        ));
    }
    TableCatalog::restore(table, last_ts, versions, retired_series, retired_fields)
}

fn decode_version(cursor: &mut Cursor<'_>) -> Result<TableVersion> {
    let version_no = cursor.read_u32()?;
    let validity = match cursor.read_u8()? {
        0 => Validity::duration_seconds(cursor.read_u32()?)
            .map_err(|_| Error::corruption("MANIFEST", "finite validity is zero"))?,
        1 => Validity::Forever,
        _ => return Err(Error::corruption("MANIFEST", "unknown validity tag")),
    };
    let effective_from = cursor.read_option_i64()?;
    let field_count = cursor.read_count(MAX_LOGICAL_STREAMS, 3, "fields")?;
    let mut fields = Vec::with_capacity(field_count);
    for _ in 0..field_count {
        fields.push(FieldSchema::new(
            FieldId::new(cursor.read_u16()?),
            decode_value_type(cursor.read_u8()?)?,
        ));
    }
    TableVersion::restore(version_no, validity, fields, effective_from)
}

fn decode_value_type(tag: u8) -> Result<ValueType> {
    match tag {
        0 => Ok(ValueType::UInt),
        1 => Ok(ValueType::Sq1),
        2 => Ok(ValueType::F32Bits),
        _ => Err(Error::corruption("MANIFEST", "unknown value type tag")),
    }
}

fn read_at_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    let end = offset
        .checked_add(2)
        .ok_or_else(|| Error::corruption("MANIFEST", "u16 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("MANIFEST", "truncated u16"))?;
    let value = <[u8; 2]>::try_from(source)
        .map_err(|_| Error::corruption("MANIFEST", "invalid u16 width"))?;
    Ok(u16::from_le_bytes(value))
}

fn read_at_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    let end = offset
        .checked_add(4)
        .ok_or_else(|| Error::corruption("MANIFEST", "u32 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("MANIFEST", "truncated u32"))?;
    let value = <[u8; 4]>::try_from(source)
        .map_err(|_| Error::corruption("MANIFEST", "invalid u32 width"))?;
    Ok(u32::from_le_bytes(value))
}

fn read_at_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    let end = offset
        .checked_add(8)
        .ok_or_else(|| Error::corruption("MANIFEST", "u64 offset overflow"))?;
    let source = bytes
        .get(offset..end)
        .ok_or_else(|| Error::corruption("MANIFEST", "truncated u64"))?;
    let value = <[u8; 8]>::try_from(source)
        .map_err(|_| Error::corruption("MANIFEST", "invalid u64 width"))?;
    Ok(u64::from_le_bytes(value))
}

struct Cursor<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Cursor<'a> {
    const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn is_exhausted(&self) -> bool {
        self.offset == self.bytes.len()
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    fn read_count(&mut self, maximum: u32, width: usize, name: &'static str) -> Result<usize> {
        let count = self.read_u32()?;
        if count > maximum {
            return Err(Error::limit(name, u64::from(count), u64::from(maximum)));
        }
        let count = usize::try_from(count)
            .map_err(|_| Error::corruption("MANIFEST", "count does not fit usize"))?;
        let needed = count
            .checked_mul(width)
            .ok_or_else(|| Error::corruption("MANIFEST", "count byte size overflow"))?;
        if needed > self.remaining() {
            return Err(Error::corruption(
                "MANIFEST",
                "count exceeds remaining body",
            ));
        }
        Ok(count)
    }

    fn read_option_i64(&mut self) -> Result<Option<i64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_i64().map(Some),
            _ => Err(Error::corruption("MANIFEST", "unknown Option<i64> tag")),
        }
    }

    fn read_option_u64(&mut self) -> Result<Option<u64>> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u64().map(Some),
            _ => Err(Error::corruption("MANIFEST", "unknown Option<u64> tag")),
        }
    }

    fn read_u8(&mut self) -> Result<u8> {
        let value = *self
            .bytes
            .get(self.offset)
            .ok_or_else(|| Error::corruption("MANIFEST", "truncated byte"))?;
        self.offset = self
            .offset
            .checked_add(1)
            .ok_or_else(|| Error::corruption("MANIFEST", "cursor overflow"))?;
        Ok(value)
    }

    fn read_u16(&mut self) -> Result<u16> {
        self.read_array::<2>().map(u16::from_le_bytes)
    }

    fn read_u32(&mut self) -> Result<u32> {
        self.read_array::<4>().map(u32::from_le_bytes)
    }

    fn read_u64(&mut self) -> Result<u64> {
        self.read_array::<8>().map(u64::from_le_bytes)
    }

    fn read_i64(&mut self) -> Result<i64> {
        self.read_array::<8>().map(i64::from_le_bytes)
    }

    fn read_array<const N: usize>(&mut self) -> Result<[u8; N]> {
        let end = self
            .offset
            .checked_add(N)
            .ok_or_else(|| Error::corruption("MANIFEST", "cursor overflow"))?;
        let source = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(|| Error::corruption("MANIFEST", "truncated scalar"))?;
        let value = <[u8; N]>::try_from(source)
            .map_err(|_| Error::corruption("MANIFEST", "invalid scalar width"))?;
        self.offset = end;
        Ok(value)
    }
}
