// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::mem::size_of;

use crate::{
    CellValue, Error, FieldId, Result, SeriesId, TableVersion, ValueType,
    codec::{presence, selector, timestamp},
    limits::{Limit, SECTION_DECODE_MEMORY_BYTES, ensure_at_most},
    unit::{format::TableDirectoryEntry, section::COLUMN_DIRECTORY_ENTRY_BYTES},
};

const TIMESTAMP_HEADER_BYTES: usize = 5;
const COLUMN_COUNT_BYTES: usize = 4;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedColumn {
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    cells: Vec<Option<CellValue>>,
}

impl DecodedColumn {
    pub(crate) const fn series(&self) -> SeriesId {
        self.series
    }

    pub(crate) const fn field(&self) -> FieldId {
        self.field
    }

    pub(crate) const fn value_type(&self) -> ValueType {
        self.value_type
    }

    pub(crate) fn cells(&self) -> &[Option<CellValue>] {
        &self.cells
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DecodedSection {
    timestamps: Vec<i64>,
    columns: Vec<DecodedColumn>,
}

impl DecodedSection {
    pub(crate) fn timestamps(&self) -> &[i64] {
        &self.timestamps
    }

    pub(crate) fn columns(&self) -> &[DecodedColumn] {
        &self.columns
    }
}

#[derive(Clone, Copy)]
struct ParsedColumn {
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    presence_tag: u8,
    value_tag: u8,
    presence_len: u32,
    value_len: u32,
    value_count: u32,
    null_count: u32,
    presence_start: usize,
    value_start: usize,
}

pub(crate) fn decode(
    bytes: &[u8],
    entry: TableDirectoryEntry,
    version: &TableVersion,
) -> Result<DecodedSection> {
    decode_filtered(bytes, entry, version, None)
}

pub(crate) fn decode_stream(
    bytes: &[u8],
    entry: TableDirectoryEntry,
    version: &TableVersion,
    series: SeriesId,
    field: FieldId,
) -> Result<DecodedSection> {
    decode_filtered(bytes, entry, version, Some(&[(series, field)]))
}

pub(crate) fn decode_streams(
    bytes: &[u8],
    entry: TableDirectoryEntry,
    version: &TableVersion,
    streams: &[(SeriesId, FieldId)],
) -> Result<DecodedSection> {
    if streams.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(Error::invalid(
            "streams",
            "decoded stream selection must be strictly ordered",
        ));
    }
    decode_filtered(bytes, entry, version, Some(streams))
}

fn decode_filtered(
    bytes: &[u8],
    entry: TableDirectoryEntry,
    version: &TableVersion,
    selected: Option<&[(SeriesId, FieldId)]>,
) -> Result<DecodedSection> {
    if version.version_no() != entry.version_no() {
        return Err(Error::corruption(
            "table section",
            "directory references a different table version",
        ));
    }
    let timestamp_tag = *bytes
        .first()
        .ok_or_else(|| Error::corruption("table section", "timestamp tag is absent"))?;
    let timestamp_len = to_usize(read_u32(bytes, 1, "table section")?, "timestamp length")?;
    let timestamp_end = TIMESTAMP_HEADER_BYTES
        .checked_add(timestamp_len)
        .ok_or_else(|| Error::corruption("table section", "timestamp extent overflow"))?;
    let count_end = timestamp_end
        .checked_add(COLUMN_COUNT_BYTES)
        .ok_or_else(|| Error::corruption("table section", "column count offset overflow"))?;
    if count_end > bytes.len() {
        return Err(Error::corruption(
            "table section",
            "timestamp stream or column count is truncated",
        ));
    }
    let column_count = read_u32(bytes, timestamp_end, "table section")?;
    if column_count == 0 {
        return Err(Error::corruption(
            "table section",
            "column count must be positive",
        ));
    }
    ensure_at_most(Limit::LogicalStreams, u64::from(column_count))?;
    let count = to_usize(column_count, "column count")?;
    preflight_decode_memory(bytes.len(), entry.row_count(), count, 0, 0)?;
    let directory_len = count
        .checked_mul(COLUMN_DIRECTORY_ENTRY_BYTES)
        .ok_or_else(|| Error::corruption("column directory", "directory length overflow"))?;
    let directory_end = count_end
        .checked_add(directory_len)
        .ok_or_else(|| Error::corruption("column directory", "directory end overflow"))?;
    if directory_end > bytes.len() {
        return Err(Error::corruption(
            "column directory",
            "directory exceeds section",
        ));
    }

    let mut columns = parse_directory(bytes, count_end, count, entry, version)?;
    preflight_payloads(&mut columns, directory_end, bytes.len())?;
    let selected_columns = columns
        .iter()
        .filter(|column| {
            selected.is_none_or(|keys| keys.binary_search(&(column.series, column.field)).is_ok())
        })
        .collect::<Vec<_>>();
    let maximum_facts = selected_columns
        .iter()
        .map(|column| column.value_count.saturating_add(column.null_count))
        .max()
        .unwrap_or_default();
    preflight_decode_memory(
        bytes.len(),
        entry.row_count(),
        count,
        selected_columns.len(),
        maximum_facts,
    )?;
    let timestamp_bytes = bytes
        .get(TIMESTAMP_HEADER_BYTES..timestamp_end)
        .ok_or_else(|| Error::corruption("table section", "timestamp extent is invalid"))?;
    let timestamps = timestamp::decode(
        timestamp_tag,
        timestamp_bytes,
        entry.row_count(),
        entry.min_ts(),
        entry.max_ts(),
    )?;
    let decoded_columns = selected_columns
        .into_iter()
        .map(|column| decode_column(bytes, entry.row_count(), *column))
        .collect::<Result<Vec<_>>>()?;
    Ok(DecodedSection {
        timestamps,
        columns: decoded_columns,
    })
}

fn preflight_decode_memory(
    input_bytes: usize,
    row_count: u32,
    parsed_columns: usize,
    selected_columns: usize,
    maximum_facts: u32,
) -> Result<()> {
    let rows = to_usize(row_count, "row count")?;
    let facts = to_usize(maximum_facts, "Fact count")?;
    let parsed = parsed_columns
        .checked_mul(size_of::<ParsedColumn>())
        .ok_or_else(decode_memory_limit)?;
    let timestamps = rows.checked_mul(32).ok_or_else(decode_memory_limit)?;
    let cells = rows
        .checked_mul(selected_columns)
        .and_then(|count| count.checked_mul(size_of::<Option<CellValue>>()))
        .ok_or_else(decode_memory_limit)?;
    let fact_scratch = facts.checked_mul(40).ok_or_else(decode_memory_limit)?;
    let output_metadata = selected_columns
        .checked_mul(size_of::<DecodedColumn>())
        .ok_or_else(decode_memory_limit)?;
    let total = input_bytes
        .checked_add(parsed)
        .and_then(|bytes| bytes.checked_add(timestamps))
        .and_then(|bytes| bytes.checked_add(cells))
        .and_then(|bytes| bytes.checked_add(fact_scratch))
        .and_then(|bytes| bytes.checked_add(output_metadata))
        .ok_or_else(decode_memory_limit)?;
    if total > usize::try_from(SECTION_DECODE_MEMORY_BYTES).unwrap_or(usize::MAX) {
        return Err(Error::limit(
            "section_decode_memory_bytes",
            u64::try_from(total).unwrap_or(u64::MAX),
            u64::from(SECTION_DECODE_MEMORY_BYTES),
        ));
    }
    Ok(())
}

fn decode_memory_limit() -> Error {
    Error::limit(
        "section_decode_memory_bytes",
        u64::MAX,
        u64::from(SECTION_DECODE_MEMORY_BYTES),
    )
}

fn parse_directory(
    bytes: &[u8],
    start: usize,
    count: usize,
    entry: TableDirectoryEntry,
    version: &TableVersion,
) -> Result<Vec<ParsedColumn>> {
    let mut columns = Vec::with_capacity(count);
    let mut previous = None;
    for index in 0..count {
        let offset = start
            .checked_add(
                index
                    .checked_mul(COLUMN_DIRECTORY_ENTRY_BYTES)
                    .ok_or_else(|| {
                        Error::corruption("column directory", "entry offset overflow")
                    })?,
            )
            .ok_or_else(|| Error::corruption("column directory", "entry start overflow"))?;
        let end = offset
            .checked_add(COLUMN_DIRECTORY_ENTRY_BYTES)
            .ok_or_else(|| Error::corruption("column directory", "entry end overflow"))?;
        let raw = bytes
            .get(offset..end)
            .ok_or_else(|| Error::corruption("column directory", "entry is truncated"))?;
        if raw.get(13..16) != Some([0_u8; 3].as_slice()) {
            return Err(Error::corruption(
                "column directory",
                "reserved bytes are nonzero",
            ));
        }
        let series = SeriesId::new(read_u64(raw, 0, "column directory")?);
        let field = FieldId::new(read_u16(raw, 8, "column directory")?);
        if previous.is_some_and(|prior| prior >= (series, field)) {
            return Err(Error::corruption(
                "column directory",
                "stream keys are not strictly increasing",
            ));
        }
        let value_type = parse_value_type(raw[10])?;
        validate_schema(version, field, value_type)?;
        let value_count = read_u32(raw, 24, "column directory")?;
        let null_count = read_u32(raw, 28, "column directory")?;
        let fact_count = value_count
            .checked_add(null_count)
            .ok_or_else(|| Error::corruption("column directory", "Fact count overflow"))?;
        if fact_count == 0 || fact_count > entry.row_count() {
            return Err(Error::corruption(
                "column directory",
                "Fact count is zero or exceeds section rows",
            ));
        }
        columns.push(ParsedColumn {
            series,
            field,
            value_type,
            presence_tag: raw[11],
            value_tag: raw[12],
            presence_len: read_u32(raw, 16, "column directory")?,
            value_len: read_u32(raw, 20, "column directory")?,
            value_count,
            null_count,
            presence_start: 0,
            value_start: 0,
        });
        previous = Some((series, field));
    }
    Ok(columns)
}

fn preflight_payloads(
    columns: &mut [ParsedColumn],
    start: usize,
    section_len: usize,
) -> Result<()> {
    let mut offset = start;
    for column in columns {
        column.presence_start = offset;
        offset = offset
            .checked_add(to_usize(column.presence_len, "presence length")?)
            .ok_or_else(|| Error::corruption("column payload", "presence extent overflow"))?;
        column.value_start = offset;
        offset = offset
            .checked_add(to_usize(column.value_len, "value length")?)
            .ok_or_else(|| Error::corruption("column payload", "value extent overflow"))?;
        if offset > section_len {
            return Err(Error::corruption(
                "column payload",
                "payload exceeds section",
            ));
        }
    }
    if offset != section_len {
        return Err(Error::corruption(
            "column payload",
            "payloads do not consume the exact section",
        ));
    }
    Ok(())
}

fn decode_column(bytes: &[u8], row_count: u32, column: ParsedColumn) -> Result<DecodedColumn> {
    let presence_end = column
        .presence_start
        .checked_add(to_usize(column.presence_len, "presence length")?)
        .ok_or_else(|| Error::corruption("column payload", "presence end overflow"))?;
    let value_end = column
        .value_start
        .checked_add(to_usize(column.value_len, "value length")?)
        .ok_or_else(|| Error::corruption("column payload", "value end overflow"))?;
    let fact_count = column
        .value_count
        .checked_add(column.null_count)
        .ok_or_else(|| Error::corruption("column payload", "Fact count overflow"))?;
    let presence_bytes = bytes
        .get(column.presence_start..presence_end)
        .ok_or_else(|| Error::corruption("column payload", "presence range is invalid"))?;
    let value_bytes = bytes
        .get(column.value_start..value_end)
        .ok_or_else(|| Error::corruption("column payload", "value range is invalid"))?;
    let presence = presence::decode(column.presence_tag, row_count, fact_count, presence_bytes)?;
    let facts = selector::decode(
        column.value_type,
        column.value_tag,
        value_bytes,
        fact_count,
        column.null_count,
    )?;
    let capacity = to_usize(row_count, "row count")?;
    let mut cells = Vec::with_capacity(capacity);
    let mut fact_index = 0_usize;
    for row in 0..row_count {
        if presence
            .is_fact(row)
            .ok_or_else(|| Error::corruption("column payload", "row exceeds presence"))?
        {
            let fact = facts
                .get(fact_index)
                .copied()
                .ok_or_else(|| Error::corruption("column payload", "value stream is short"))?;
            cells.push(Some(fact));
            fact_index = fact_index
                .checked_add(1)
                .ok_or_else(|| Error::corruption("column payload", "Fact index overflow"))?;
        } else {
            cells.push(None);
        }
    }
    if fact_index != facts.len() || cells.len() != capacity {
        return Err(Error::corruption(
            "column payload",
            "decoded counts disagree with directory",
        ));
    }
    Ok(DecodedColumn {
        series: column.series,
        field: column.field,
        value_type: column.value_type,
        cells,
    })
}

fn validate_schema(version: &TableVersion, field: FieldId, value_type: ValueType) -> Result<()> {
    let index = version
        .fields()
        .binary_search_by_key(&field, |schema| schema.field())
        .map_err(|_| Error::corruption("column directory", "field is absent from schema"))?;
    if version.fields()[index].value_type() != value_type {
        return Err(Error::corruption(
            "column directory",
            "physical value type disagrees with schema",
        ));
    }
    Ok(())
}

fn parse_value_type(tag: u8) -> Result<ValueType> {
    match tag {
        0 => Ok(ValueType::UInt),
        1 => Ok(ValueType::Sq1),
        2 => Ok(ValueType::F32Bits),
        _ => Err(Error::corruption(
            "column directory",
            "unknown value type tag",
        )),
    }
}

fn to_usize(value: u32, context: &'static str) -> Result<usize> {
    usize::try_from(value).map_err(|_| Error::corruption(context, "value does not fit usize"))
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

#[cfg(test)]
#[path = "section_decode_tests.rs"]
mod tests;
