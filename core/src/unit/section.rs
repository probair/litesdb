// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::mem::size_of;

use crate::{
    CellValue, Error, FieldId, Result, SeriesId, TableVersion, ValueType,
    codec::{
        FactShape,
        presence::{self, PresenceEncoding},
        selector::{self, ValueEncoding},
        timestamp,
    },
    limits::{Limit, SECTION_WORKING_MEMORY_BYTES, ensure_at_most},
    wal::{TailRow, TailTable},
};

pub(crate) const COLUMN_DIRECTORY_ENTRY_BYTES: usize = 32;
const TIMESTAMP_HEADER_BYTES: usize = 5;
const COLUMN_COUNT_BYTES: usize = 4;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ColumnMeta {
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    presence_encoding: PresenceEncoding,
    value_encoding: ValueEncoding,
    presence_len: u32,
    value_len: u32,
    value_count: u32,
    null_count: u32,
}

impl ColumnMeta {
    fn encode(self) -> [u8; COLUMN_DIRECTORY_ENTRY_BYTES] {
        let mut bytes = [0_u8; COLUMN_DIRECTORY_ENTRY_BYTES];
        bytes[0..8].copy_from_slice(&self.series.get().to_le_bytes());
        bytes[8..10].copy_from_slice(&self.field.get().to_le_bytes());
        bytes[10] = self.value_type.tag();
        bytes[11] = self.presence_encoding.tag();
        bytes[12] = self.value_encoding.tag();
        bytes[16..20].copy_from_slice(&self.presence_len.to_le_bytes());
        bytes[20..24].copy_from_slice(&self.value_len.to_le_bytes());
        bytes[24..28].copy_from_slice(&self.value_count.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.null_count.to_le_bytes());
        bytes
    }
}

struct ColumnPayload {
    meta: ColumnMeta,
    presence: Vec<u8>,
    values: Vec<u8>,
}

pub(crate) struct MaterializedColumn {
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    cells: Vec<Option<CellValue>>,
}

impl MaterializedColumn {
    pub(crate) const fn new(
        series: SeriesId,
        field: FieldId,
        value_type: ValueType,
        cells: Vec<Option<CellValue>>,
    ) -> Self {
        Self {
            series,
            field,
            value_type,
            cells,
        }
    }
}

pub(crate) fn encode(
    table: &TailTable,
    start: usize,
    end: usize,
    version: &TableVersion,
) -> Result<Vec<u8>> {
    let rows = table
        .rows()
        .get(start..end)
        .ok_or_else(|| Error::corruption("Seal section", "row range is out of bounds"))?;
    let row_count = u32::try_from(rows.len())
        .map_err(|_| Error::limit("section_rows", u64::MAX, Limit::SectionRows.maximum()))?;
    ensure_at_most(Limit::SectionRows, u64::from(row_count))?;
    if rows.is_empty()
        || rows
            .iter()
            .any(|row| row.version_no() != version.version_no())
    {
        return Err(Error::corruption(
            "Seal section",
            "row range is empty or crosses a table version",
        ));
    }
    preflight_tail(table, rows, start, end)?;

    let timestamps: Vec<i64> = rows.iter().map(TailRow::timestamp).collect();
    let timestamp_plan = timestamp::select(&timestamps)?;
    let mut timestamp_bytes = vec![0; to_usize(timestamp_plan.byte_len(), "timestamp bytes")?];
    timestamp::encode(&timestamps, timestamp_plan, &mut timestamp_bytes)?;

    let mut columns = Vec::new();
    let start_row = u32::try_from(start)
        .map_err(|_| Error::limit("tail_rows", u64::MAX, u64::from(u32::MAX)))?;
    let end_row =
        u32::try_from(end).map_err(|_| Error::limit("tail_rows", u64::MAX, u64::from(u32::MAX)))?;
    for ((series, field), postings) in table.streams() {
        let first = postings.partition_point(|row| *row < start_row);
        let last = postings.partition_point(|row| *row < end_row);
        let selected = postings
            .get(first..last)
            .ok_or_else(|| Error::corruption("Seal section", "posting range is invalid"))?;
        if selected.is_empty() {
            continue;
        }
        let value_type = field_type(version, field)?;
        columns.push(encode_column(
            table, rows, start, series, field, value_type, selected,
        )?);
    }
    ensure_at_most(
        Limit::LogicalStreams,
        u64::try_from(columns.len()).unwrap_or(u64::MAX),
    )?;
    assemble(timestamp_plan.encoding().tag(), &timestamp_bytes, columns)
}

pub(crate) fn encode_materialized(
    timestamps: &[i64],
    columns: &[MaterializedColumn],
) -> Result<Vec<u8>> {
    let row_count = u32::try_from(timestamps.len())
        .map_err(|_| Error::limit("section_rows", u64::MAX, Limit::SectionRows.maximum()))?;
    ensure_at_most(Limit::SectionRows, u64::from(row_count))?;
    if timestamps.is_empty() || timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(Error::corruption(
            "compacted section",
            "timestamps are empty or not strictly increasing",
        ));
    }
    preflight_materialized(timestamps, columns)?;
    ensure_at_most(
        Limit::LogicalStreams,
        u64::try_from(columns.len()).unwrap_or(u64::MAX),
    )?;
    let mut previous = None;
    let mut encoded = Vec::with_capacity(columns.len());
    for column in columns {
        let key = (column.series, column.field);
        if previous.is_some_and(|prior| prior >= key)
            || column.cells.len() != timestamps.len()
            || column.cells.iter().all(Option::is_none)
            || column
                .cells
                .iter()
                .flatten()
                .any(|value| !value.matches(column.value_type))
        {
            return Err(Error::corruption(
                "compacted section",
                "column order, row count, presence, or type is invalid",
            ));
        }
        let mut shapes = Vec::with_capacity(column.cells.len());
        let mut facts = Vec::new();
        for cell in &column.cells {
            match cell {
                None => shapes.push(FactShape::Absent),
                Some(CellValue::Null) => {
                    shapes.push(FactShape::Null);
                    facts.push(CellValue::Null);
                }
                Some(value) => {
                    shapes.push(FactShape::Value);
                    facts.push(*value);
                }
            }
        }
        encoded.push(encode_shapes(
            column.series,
            column.field,
            column.value_type,
            &shapes,
            &facts,
        )?);
        previous = Some(key);
    }
    if encoded.is_empty() {
        return Err(Error::corruption(
            "compacted section",
            "section has no Fact columns",
        ));
    }
    let timestamp_plan = timestamp::select(timestamps)?;
    let mut timestamp_bytes = vec![0; to_usize(timestamp_plan.byte_len(), "timestamp bytes")?];
    timestamp::encode(timestamps, timestamp_plan, &mut timestamp_bytes)?;
    assemble(timestamp_plan.encoding().tag(), &timestamp_bytes, encoded)
}

fn field_type(version: &TableVersion, field: FieldId) -> Result<ValueType> {
    let index = version
        .fields()
        .binary_search_by_key(&field, |schema| schema.field())
        .map_err(|_| Error::corruption("Seal section", "posting field is absent from schema"))?;
    Ok(version.fields()[index].value_type())
}

fn encode_column(
    table: &TailTable,
    rows: &[TailRow],
    global_start: usize,
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    postings: &[u32],
) -> Result<ColumnPayload> {
    let mut shapes = vec![FactShape::Absent; rows.len()];
    let mut facts = Vec::with_capacity(postings.len());
    for row_index in postings {
        let local = usize::try_from(*row_index)
            .map_err(|_| Error::corruption("Seal section", "posting does not fit usize"))?
            .checked_sub(global_start)
            .ok_or_else(|| Error::corruption("Seal section", "posting precedes section"))?;
        let row = rows
            .get(local)
            .ok_or_else(|| Error::corruption("Seal section", "posting exceeds section"))?;
        let entries = table
            .row_entries(row)
            .ok_or_else(|| Error::corruption("Seal section", "row entries are invalid"))?;
        let entry_index = entries
            .binary_search_by_key(&(series, field), |entry| (entry.series(), entry.field()))
            .map_err(|_| Error::corruption("Seal section", "posting has no matching Fact"))?;
        let value = entries[entry_index].value();
        let shape = if value == CellValue::Null {
            FactShape::Null
        } else {
            FactShape::Value
        };
        let target = shapes
            .get_mut(local)
            .ok_or_else(|| Error::corruption("Seal section", "shape offset is invalid"))?;
        if *target != FactShape::Absent {
            return Err(Error::corruption(
                "Seal section",
                "duplicate stream posting",
            ));
        }
        *target = shape;
        facts.push(value);
    }

    encode_shapes(series, field, value_type, &shapes, &facts)
}

fn encode_shapes(
    series: SeriesId,
    field: FieldId,
    value_type: ValueType,
    shapes: &[FactShape],
    facts: &[CellValue],
) -> Result<ColumnPayload> {
    let presence_plan = presence::measure(shapes)?;
    let value_plan = selector::select(facts, value_type)?;
    if presence_plan.fact_count() != value_plan.fact_count() {
        return Err(Error::corruption(
            "Seal section",
            "presence and value Fact counts disagree",
        ));
    }
    let value_count = value_plan
        .fact_count()
        .checked_sub(value_plan.null_count())
        .ok_or_else(|| Error::corruption("Seal section", "Null count exceeds Fact count"))?;
    let mut presence_bytes = vec![0; to_usize(presence_plan.byte_len(), "presence bytes")?];
    let mut value_bytes = vec![0; to_usize(value_plan.byte_len(), "value bytes")?];
    presence::encode(shapes, presence_plan, &mut presence_bytes)?;
    selector::encode(facts, value_type, value_plan, &mut value_bytes)?;
    Ok(ColumnPayload {
        meta: ColumnMeta {
            series,
            field,
            value_type,
            presence_encoding: presence_plan.encoding(),
            value_encoding: value_plan.encoding(),
            presence_len: presence_plan.byte_len(),
            value_len: value_plan.byte_len(),
            value_count,
            null_count: value_plan.null_count(),
        },
        presence: presence_bytes,
        values: value_bytes,
    })
}

fn assemble(
    timestamp_tag: u8,
    timestamp_bytes: &[u8],
    columns: Vec<ColumnPayload>,
) -> Result<Vec<u8>> {
    let directory_len = columns
        .len()
        .checked_mul(COLUMN_DIRECTORY_ENTRY_BYTES)
        .ok_or_else(|| Error::limit("section_bytes", u64::MAX, u64::from(u32::MAX)))?;
    let mut total = TIMESTAMP_HEADER_BYTES
        .checked_add(timestamp_bytes.len())
        .and_then(|value| value.checked_add(COLUMN_COUNT_BYTES))
        .and_then(|value| value.checked_add(directory_len))
        .ok_or_else(|| Error::limit("section_bytes", u64::MAX, u64::from(u32::MAX)))?;
    for column in &columns {
        total = total
            .checked_add(column.presence.len())
            .and_then(|value| value.checked_add(column.values.len()))
            .ok_or_else(|| Error::limit("section_bytes", u64::MAX, u64::from(u32::MAX)))?;
    }
    let _ = u32::try_from(total).map_err(|_| {
        Error::limit(
            "section_bytes",
            u64::try_from(total).unwrap_or(u64::MAX),
            u64::from(u32::MAX),
        )
    })?;
    let timestamp_len = u32::try_from(timestamp_bytes.len())
        .map_err(|_| Error::limit("timestamp_bytes", u64::MAX, u64::from(u32::MAX)))?;
    let column_count = u32::try_from(columns.len())
        .map_err(|_| Error::limit("logical_streams", u64::MAX, Limit::LogicalStreams.maximum()))?;

    let mut output = Vec::with_capacity(total);
    output.push(timestamp_tag);
    output.extend_from_slice(&timestamp_len.to_le_bytes());
    output.extend_from_slice(timestamp_bytes);
    output.extend_from_slice(&column_count.to_le_bytes());
    for column in &columns {
        output.extend_from_slice(&column.meta.encode());
    }
    for column in columns {
        output.extend_from_slice(&column.presence);
        output.extend_from_slice(&column.values);
    }
    if output.len() != total {
        return Err(Error::corruption(
            "Seal section",
            "encoded length differs from measurement",
        ));
    }
    Ok(output)
}

fn preflight_tail(table: &TailTable, rows: &[TailRow], start: usize, end: usize) -> Result<()> {
    let start_row = u32::try_from(start)
        .map_err(|_| Error::limit("tail_rows", u64::MAX, u64::from(u32::MAX)))?;
    let end_row =
        u32::try_from(end).map_err(|_| Error::limit("tail_rows", u64::MAX, u64::from(u32::MAX)))?;
    let mut streams = 0_usize;
    let mut facts = 0_usize;
    for (_, postings) in table.streams() {
        let first = postings.partition_point(|row| *row < start_row);
        let last = postings.partition_point(|row| *row < end_row);
        let count = last
            .checked_sub(first)
            .ok_or_else(|| Error::corruption("Seal section", "posting range underflow"))?;
        if count > 0 {
            streams = streams.checked_add(1).ok_or_else(section_memory_limit)?;
            facts = facts.checked_add(count).ok_or_else(section_memory_limit)?;
        }
    }
    preflight_bytes(rows.len(), streams, facts)
}

fn preflight_materialized(timestamps: &[i64], columns: &[MaterializedColumn]) -> Result<()> {
    let facts = columns.iter().try_fold(0_usize, |total, column| {
        total
            .checked_add(column.cells.iter().filter(|cell| cell.is_some()).count())
            .ok_or_else(section_memory_limit)
    })?;
    preflight_bytes(timestamps.len(), columns.len(), facts)
}

fn preflight_bytes(rows: usize, streams: usize, facts: usize) -> Result<()> {
    let total = working_set_bytes(rows, streams, facts)?;
    if total > usize::try_from(SECTION_WORKING_MEMORY_BYTES).unwrap_or(usize::MAX) {
        return Err(Error::limit(
            "section_working_memory_bytes",
            u64::try_from(total).unwrap_or(u64::MAX),
            u64::from(SECTION_WORKING_MEMORY_BYTES),
        ));
    }
    Ok(())
}

pub(crate) fn working_set_bytes(rows: usize, streams: usize, facts: usize) -> Result<usize> {
    let row_state = rows
        .checked_mul(size_of::<i64>().saturating_mul(3))
        .ok_or_else(section_memory_limit)?;
    let shape_state = rows
        .checked_mul(streams)
        .and_then(|count| count.checked_mul(size_of::<Option<CellValue>>().saturating_add(3)))
        .ok_or_else(section_memory_limit)?;
    let fact_state = facts
        .checked_mul(size_of::<CellValue>().saturating_add(16))
        .ok_or_else(section_memory_limit)?;
    let metadata = streams.checked_mul(128).ok_or_else(section_memory_limit)?;
    let total = row_state
        .checked_add(shape_state)
        .and_then(|bytes| bytes.checked_add(fact_state))
        .and_then(|bytes| bytes.checked_add(metadata))
        .ok_or_else(section_memory_limit)?;
    Ok(total)
}

fn section_memory_limit() -> Error {
    Error::limit(
        "section_working_memory_bytes",
        u64::MAX,
        u64::from(SECTION_WORKING_MEMORY_BYTES),
    )
}

fn to_usize(length: u32, context: &'static str) -> Result<usize> {
    usize::try_from(length).map_err(|_| Error::corruption(context, "length does not fit usize"))
}
