// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by retention folding and Db publication later in M6"
)]

use crc32fast::hash;

use crate::{
    CellValue, Error, F32Bits, FieldId, Result, SeriesId, Sq1, StreamKey, TableId,
    fsutil::{Area, DbDir, publish_atomically},
    limits::MAX_RETENTION_HEADS,
};

const HEADER_BYTES: usize = 24;
const TABLE_HEADER_BYTES: usize = 8;
const ENTRY_BYTES: usize = 27;
const FOOTER_BYTES: usize = 4;
const MAGIC: [u8; 4] = *b"LSR1";
const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct RetentionHead {
    table: TableId,
    series: SeriesId,
    field: FieldId,
    fact_ts: i64,
    value: CellValue,
}

impl RetentionHead {
    pub(crate) const fn new(
        table: TableId,
        series: SeriesId,
        field: FieldId,
        fact_ts: i64,
        value: CellValue,
    ) -> Self {
        Self {
            table,
            series,
            field,
            fact_ts,
            value,
        }
    }

    pub(crate) const fn table(self) -> TableId {
        self.table
    }

    pub(crate) const fn series(self) -> SeriesId {
        self.series
    }

    pub(crate) const fn field(self) -> FieldId {
        self.field
    }

    pub(crate) const fn fact_ts(self) -> i64 {
        self.fact_ts
    }

    pub(crate) const fn value(self) -> CellValue {
        self.value
    }

    const fn key(self) -> (TableId, SeriesId, FieldId) {
        (self.table, self.series, self.field)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RetentionHeads {
    floor: i64,
    entries: Vec<RetentionHead>,
}

impl RetentionHeads {
    pub(crate) fn new(floor: i64, entries: Vec<RetentionHead>) -> Result<Self> {
        let count = u32::try_from(entries.len()).map_err(|_| {
            Error::limit("retention_heads", u64::MAX, u64::from(MAX_RETENTION_HEADS))
        })?;
        if count > MAX_RETENTION_HEADS {
            return Err(Error::limit(
                "retention_heads",
                u64::from(count),
                u64::from(MAX_RETENTION_HEADS),
            ));
        }
        if entries.iter().any(|entry| entry.fact_ts() >= floor)
            || entries
                .windows(2)
                .any(|pair| pair[0].key() >= pair[1].key())
        {
            return Err(Error::invalid(
                "retention heads",
                "Facts must predate the floor and keys must be strictly ordered",
            ));
        }
        Ok(Self { floor, entries })
    }

    pub(crate) const fn floor(&self) -> i64 {
        self.floor
    }

    pub(crate) fn entries(&self) -> &[RetentionHead] {
        &self.entries
    }

    pub(crate) fn find(&self, key: StreamKey) -> Option<RetentionHead> {
        self.entries
            .binary_search_by_key(&(key.table(), key.series(), key.field()), |entry| {
                entry.key()
            })
            .ok()
            .and_then(|index| self.entries.get(index).copied())
    }
}

pub(crate) fn encode(heads: &RetentionHeads) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let mut start = 0_usize;
    while start < heads.entries.len() {
        let table = heads.entries[start].table();
        let end = heads.entries[start..]
            .partition_point(|entry| entry.table() == table)
            .checked_add(start)
            .ok_or_else(file_limit)?;
        body.extend_from_slice(&table.get().to_le_bytes());
        let count = u32::try_from(end.saturating_sub(start)).map_err(|_| file_limit())?;
        body.extend_from_slice(&count.to_le_bytes());
        for entry in &heads.entries[start..end] {
            encode_entry(*entry, &mut body);
        }
        start = end;
    }
    let total = u32::try_from(heads.entries.len()).map_err(|_| file_limit())?;
    let file_len = HEADER_BYTES
        .checked_add(body.len())
        .and_then(|bytes| bytes.checked_add(FOOTER_BYTES))
        .ok_or_else(file_limit)?;
    let mut bytes = Vec::with_capacity(file_len);
    bytes.extend_from_slice(&MAGIC);
    bytes.extend_from_slice(&FORMAT_VERSION.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&heads.floor.to_le_bytes());
    bytes.extend_from_slice(&total.to_le_bytes());
    bytes.extend_from_slice(&hash(&bytes[..20]).to_le_bytes());
    bytes.extend_from_slice(&body);
    bytes.extend_from_slice(&hash(&body).to_le_bytes());
    if bytes.len() != file_len {
        return Err(Error::corruption(
            "retention heads",
            "encoded length differs from measurement",
        ));
    }
    Ok(bytes)
}

pub(crate) fn decode(bytes: &[u8], expected_floor: i64) -> Result<RetentionHeads> {
    if bytes.len() < HEADER_BYTES.saturating_add(FOOTER_BYTES)
        || bytes.get(0..4) != Some(MAGIC.as_slice())
        || read_u16(bytes, 4)? != FORMAT_VERSION
        || read_u16(bytes, 6)? != 0
        || hash(bytes.get(..20).ok_or_else(truncated)?) != read_u32(bytes, 20)?
    {
        return Err(Error::corruption(
            "retention heads",
            "header, version, reserved bytes, or CRC is invalid",
        ));
    }
    let floor = read_i64(bytes, 8)?;
    if floor != expected_floor {
        return Err(Error::corruption(
            "retention heads",
            "floor disagrees with MANIFEST",
        ));
    }
    let total = read_u32(bytes, 16)?;
    if total > MAX_RETENTION_HEADS {
        return Err(Error::limit(
            "retention_heads",
            u64::from(total),
            u64::from(MAX_RETENTION_HEADS),
        ));
    }
    let footer = bytes
        .len()
        .checked_sub(FOOTER_BYTES)
        .ok_or_else(truncated)?;
    let body = bytes.get(HEADER_BYTES..footer).ok_or_else(truncated)?;
    if hash(body) != read_u32(bytes, footer)? {
        return Err(Error::corruption("retention heads", "body CRC mismatch"));
    }
    let minimum = usize::try_from(total)
        .ok()
        .and_then(|count| count.checked_mul(ENTRY_BYTES))
        .ok_or_else(file_limit)?;
    if minimum > body.len() {
        return Err(truncated());
    }
    let entries = decode_body(body, total, floor)?;
    RetentionHeads::new(floor, entries)
        .map_err(|_| Error::corruption("retention heads", "entry ordering or floor is invalid"))
}

pub(crate) fn publish(
    directory: &DbDir,
    generation: u64,
    heads: &RetentionHeads,
) -> Result<String> {
    let name = head_name(generation);
    if directory.file(Area::Heads, &name).try_exists()? {
        return Err(Error::invalid(
            "heads_generation",
            "retention-head file already exists",
        ));
    }
    publish_atomically(directory, Area::Heads, &name, &encode(heads)?)?;
    Ok(name)
}

pub(crate) fn head_name(generation: u64) -> String {
    format!("{generation:016x}.lsr")
}

fn encode_entry(entry: RetentionHead, output: &mut Vec<u8>) {
    output.extend_from_slice(&entry.series().get().to_le_bytes());
    output.extend_from_slice(&entry.field().get().to_le_bytes());
    output.extend_from_slice(&entry.fact_ts().to_le_bytes());
    let (tag, payload) = encode_value(entry.value());
    output.push(tag);
    output.extend_from_slice(&payload);
}

fn encode_value(value: CellValue) -> (u8, [u8; 8]) {
    let mut payload = [0_u8; 8];
    let tag = match value {
        CellValue::Null => 0,
        CellValue::UInt(value) => {
            payload = value.to_le_bytes();
            1
        }
        CellValue::Sq1(value) => {
            payload[0] = value.code();
            2
        }
        CellValue::F32Bits(value) => {
            payload[..4].copy_from_slice(&value.bits().to_le_bytes());
            3
        }
    };
    (tag, payload)
}

fn decode_body(body: &[u8], total: u32, floor: i64) -> Result<Vec<RetentionHead>> {
    let mut entries = Vec::with_capacity(usize::try_from(total).map_err(|_| file_limit())?);
    let mut offset = 0_usize;
    let mut previous_table = None;
    while offset < body.len() {
        let table_end = offset
            .checked_add(TABLE_HEADER_BYTES)
            .ok_or_else(truncated)?;
        if table_end > body.len() {
            return Err(truncated());
        }
        let table = TableId::new(read_u32(body, offset)?);
        let count = read_u32(body, offset.saturating_add(4))?;
        if count == 0 || previous_table.is_some_and(|previous| previous >= table) {
            return Err(Error::corruption(
                "retention heads",
                "table groups are empty or unordered",
            ));
        }
        offset = table_end;
        let mut previous_stream = None;
        for _ in 0..count {
            let end = offset.checked_add(ENTRY_BYTES).ok_or_else(truncated)?;
            let raw = body.get(offset..end).ok_or_else(truncated)?;
            let series = SeriesId::new(read_u64(raw, 0)?);
            let field = FieldId::new(read_u16(raw, 8)?);
            if previous_stream.is_some_and(|previous| previous >= (series, field)) {
                return Err(Error::corruption(
                    "retention heads",
                    "stream keys are not strictly ordered",
                ));
            }
            let fact_ts = read_i64(raw, 10)?;
            if fact_ts >= floor {
                return Err(Error::corruption(
                    "retention heads",
                    "Fact does not predate floor",
                ));
            }
            let payload = <[u8; 8]>::try_from(&raw[19..27])
                .map_err(|_| Error::corruption("retention heads", "payload is truncated"))?;
            entries.push(RetentionHead::new(
                table,
                series,
                field,
                fact_ts,
                decode_value(raw[18], payload)?,
            ));
            previous_stream = Some((series, field));
            offset = end;
        }
        previous_table = Some(table);
    }
    if entries.len() != usize::try_from(total).map_err(|_| file_limit())? {
        return Err(Error::corruption(
            "retention heads",
            "entry total disagrees with table groups",
        ));
    }
    Ok(entries)
}

fn decode_value(tag: u8, payload: [u8; 8]) -> Result<CellValue> {
    match tag {
        0 if payload == [0; 8] => Ok(CellValue::Null),
        1 => Ok(CellValue::UInt(u64::from_le_bytes(payload))),
        2 if payload[1..] == [0; 7] && payload[0] != u8::MAX => {
            Ok(CellValue::Sq1(Sq1::new(payload[0]).ok_or_else(|| {
                Error::corruption("retention heads", "SQ1 payload is Null code")
            })?))
        }
        3 if payload[4..] == [0; 4] => {
            Ok(CellValue::F32Bits(F32Bits::from_bits(u32::from_le_bytes(
                payload[..4]
                    .try_into()
                    .map_err(|_| Error::corruption("retention heads", "F32 payload is invalid"))?,
            ))))
        }
        _ => Err(Error::corruption(
            "retention heads",
            "value tag or payload padding is invalid",
        )),
    }
}

fn read_u16(bytes: &[u8], offset: usize) -> Result<u16> {
    read_array::<2>(bytes, offset).map(u16::from_le_bytes)
}

fn read_u32(bytes: &[u8], offset: usize) -> Result<u32> {
    read_array::<4>(bytes, offset).map(u32::from_le_bytes)
}

fn read_u64(bytes: &[u8], offset: usize) -> Result<u64> {
    read_array::<8>(bytes, offset).map(u64::from_le_bytes)
}

fn read_i64(bytes: &[u8], offset: usize) -> Result<i64> {
    read_array::<8>(bytes, offset).map(i64::from_le_bytes)
}

fn read_array<const N: usize>(bytes: &[u8], offset: usize) -> Result<[u8; N]> {
    let end = offset.checked_add(N).ok_or_else(truncated)?;
    bytes
        .get(offset..end)
        .ok_or_else(truncated)?
        .try_into()
        .map_err(|_| truncated())
}

const fn truncated() -> Error {
    Error::corruption("retention heads", "file is truncated")
}

fn file_limit() -> Error {
    Error::limit("retention_head_bytes", u64::MAX, u64::from(u32::MAX))
}

#[cfg(test)]
#[path = "head_tests.rs"]
mod tests;
