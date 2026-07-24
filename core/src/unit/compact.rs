// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::collections::BTreeMap;

use crate::{
    CellValue, Error, FieldId, Result, SeriesId, TableId, ValueType,
    fsutil::DbDir,
    limits::{MAX_OPERATION_MEMORY_BYTES, MAX_SECTION_ROWS, SECTION_WORKING_MEMORY_BYTES},
    manifest::UnitMeta,
    unit::{
        UnitSource, UnitSpool,
        seal::EncodedSection,
        section::{self, MaterializedColumn},
        section_decode::{DecodedColumn, DecodedSection, decode},
    },
    wal::TailIndex,
};

#[cfg(test)]
use crate::unit::seal::{SealedUnit, assemble_sections};

type Stream = (SeriesId, FieldId);

struct ColumnBuffer {
    value_type: ValueType,
    cells: Vec<Option<CellValue>>,
}

struct SectionBuffer {
    table: TableId,
    version_no: u32,
    timestamps: Vec<i64>,
    columns: BTreeMap<Stream, ColumnBuffer>,
    facts: usize,
}

trait SectionSink {
    fn push_section(&mut self, section: EncodedSection) -> Result<()>;
}

#[cfg(test)]
impl SectionSink for Vec<EncodedSection> {
    fn push_section(&mut self, section: EncodedSection) -> Result<()> {
        self.push(section);
        Ok(())
    }
}

impl SectionSink for UnitSpool {
    fn push_section(&mut self, section: EncodedSection) -> Result<()> {
        self.push(&section)
    }
}

impl SectionBuffer {
    fn new(table: TableId, version_no: u32) -> Self {
        Self {
            table,
            version_no,
            timestamps: Vec::new(),
            columns: BTreeMap::new(),
            facts: 0,
        }
    }

    fn can_accept(&self, columns: &[DecodedColumn], row: usize) -> Result<bool> {
        let rows = self
            .timestamps
            .len()
            .checked_add(1)
            .ok_or_else(operation_limit)?;
        if rows > usize::try_from(MAX_SECTION_ROWS).unwrap_or(usize::MAX) {
            return Ok(false);
        }
        let added = columns
            .iter()
            .filter(|column| {
                !self
                    .columns
                    .contains_key(&(column.series(), column.field()))
            })
            .count();
        let streams = self
            .columns
            .len()
            .checked_add(added)
            .ok_or_else(operation_limit)?;
        let added_facts = columns.iter().try_fold(0_usize, |count, column| {
            let present = column
                .cells()
                .get(row)
                .ok_or_else(|| Error::corruption("compaction", "decoded row is absent"))?
                .is_some();
            count
                .checked_add(usize::from(present))
                .ok_or_else(operation_limit)
        })?;
        let facts = self
            .facts
            .checked_add(added_facts)
            .ok_or_else(operation_limit)?;
        let total = section::working_set_bytes(rows, streams, facts)?;
        Ok(total <= usize::try_from(SECTION_WORKING_MEMORY_BYTES).unwrap_or(usize::MAX))
    }

    fn push(&mut self, decoded: &DecodedSection, row: usize) -> Result<()> {
        let timestamp = *decoded
            .timestamps()
            .get(row)
            .ok_or_else(|| Error::corruption("compaction", "decoded row is absent"))?;
        if self
            .timestamps
            .last()
            .is_some_and(|previous| *previous >= timestamp)
        {
            return Err(Error::corruption(
                "compaction",
                "source rows are not strictly increasing",
            ));
        }
        let previous_rows = self.timestamps.len();
        let added_facts = decoded
            .columns()
            .iter()
            .try_fold(0_usize, |count, column| {
                let present = column
                    .cells()
                    .get(row)
                    .ok_or_else(|| Error::corruption("compaction", "decoded row is absent"))?
                    .is_some();
                count
                    .checked_add(usize::from(present))
                    .ok_or_else(operation_limit)
            })?;
        for column in self.columns.values_mut() {
            column.cells.push(None);
        }
        for column in decoded.columns() {
            let key = (column.series(), column.field());
            let cell = *column
                .cells()
                .get(row)
                .ok_or_else(|| Error::corruption("compaction", "column row is absent"))?;
            if let Some(buffer) = self.columns.get_mut(&key) {
                if buffer.value_type != column.value_type() {
                    return Err(Error::corruption(
                        "compaction",
                        "stream type changed within a table version",
                    ));
                }
                let target = buffer
                    .cells
                    .last_mut()
                    .ok_or_else(|| Error::corruption("compaction", "new row is absent"))?;
                *target = cell;
            } else {
                let mut cells = vec![None; previous_rows];
                cells.push(cell);
                self.columns.insert(
                    key,
                    ColumnBuffer {
                        value_type: column.value_type(),
                        cells,
                    },
                );
            }
        }
        self.timestamps.push(timestamp);
        self.facts = self
            .facts
            .checked_add(added_facts)
            .ok_or_else(operation_limit)?;
        Ok(())
    }

    fn finish(self) -> Result<EncodedSection> {
        let min_ts = *self
            .timestamps
            .first()
            .ok_or_else(|| Error::corruption("compaction", "output section is empty"))?;
        let max_ts = *self
            .timestamps
            .last()
            .ok_or_else(|| Error::corruption("compaction", "output section is empty"))?;
        let row_count = u32::try_from(self.timestamps.len())
            .map_err(|_| Error::limit("section_rows", u64::MAX, u64::from(MAX_SECTION_ROWS)))?;
        let columns: Vec<MaterializedColumn> = self
            .columns
            .into_iter()
            .map(|((series, field), column)| {
                MaterializedColumn::new(series, field, column.value_type, column.cells)
            })
            .collect();
        let payload = section::encode_materialized(&self.timestamps, &columns)?;
        EncodedSection::new(
            self.table,
            self.version_no,
            min_ts,
            max_ts,
            row_count,
            payload,
        )
    }
}

#[cfg(test)]
pub(crate) fn assemble(
    inputs: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    unit_id: u64,
) -> Result<SealedUnit> {
    let level = validate_inputs(inputs, unit_id)?;
    let mut output = Vec::new();
    merge(inputs, source, schemas, &mut output)?;
    assemble_sections(level, unit_id, output)
}

pub(crate) fn assemble_and_publish(
    directory: &DbDir,
    inputs: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    unit_id: u64,
) -> Result<UnitMeta> {
    let level = validate_inputs(inputs, unit_id)?;
    let mut spool = UnitSpool::new(directory, level, unit_id)?;
    merge(inputs, source, schemas, &mut spool)?;
    spool.publish(directory)
}

fn merge<S: SectionSink>(
    inputs: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    output: &mut S,
) -> Result<()> {
    let mut seen_sections = vec![0_u32; inputs.len()];
    let mut seen_rows = vec![0_u64; inputs.len()];
    for (table, state) in schemas.tables() {
        let mut current = None;
        let mut previous_max = None;
        for (input_index, unit) in inputs.iter().copied().enumerate() {
            for entry in source.table_sections(unit, table)? {
                if entry.table() != table
                    || entry.min_ts() < unit.min_ts()
                    || entry.max_ts() > unit.max_ts()
                    || previous_max.is_some_and(|maximum| maximum >= entry.min_ts())
                {
                    return Err(Error::corruption(
                        "compaction",
                        "source section contradicts unit or table ordering",
                    ));
                }
                seen_sections[input_index] = seen_sections[input_index]
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("compaction", "section count overflow"))?;
                seen_rows[input_index] = seen_rows[input_index]
                    .checked_add(u64::from(entry.row_count()))
                    .ok_or_else(|| Error::corruption("compaction", "row count overflow"))?;
                let version_index = state
                    .versions()
                    .binary_search_by_key(&entry.version_no(), crate::TableVersion::version_no)
                    .map_err(|_| Error::corruption("compaction", "table version is absent"))?;
                let bytes = source.section_bytes(unit, entry)?;
                let decoded = decode(&bytes, entry, &state.versions()[version_index])?;
                append_section(table, entry.version_no(), &decoded, &mut current, output)?;
                previous_max = Some(entry.max_ts());
            }
        }
        flush(&mut current, output)?;
    }
    for (index, input) in inputs.iter().enumerate() {
        if seen_sections[index] != input.section_count() || seen_rows[index] != input.total_rows() {
            return Err(Error::corruption(
                "compaction",
                "source directory totals disagree with MANIFEST",
            ));
        }
    }
    Ok(())
}

fn append_section<S: SectionSink>(
    table: TableId,
    version_no: u32,
    decoded: &DecodedSection,
    current: &mut Option<SectionBuffer>,
    output: &mut S,
) -> Result<()> {
    for row in 0..decoded.timestamps().len() {
        if current
            .as_ref()
            .is_some_and(|buffer| buffer.version_no != version_no)
        {
            flush(current, output)?;
        }
        if current.is_none() {
            *current = Some(SectionBuffer::new(table, version_no));
        }
        let fits = current
            .as_ref()
            .ok_or_else(|| Error::corruption("compaction", "section buffer is absent"))?
            .can_accept(decoded.columns(), row)?;
        if !fits {
            flush(current, output)?;
            *current = Some(SectionBuffer::new(table, version_no));
            if !current
                .as_ref()
                .ok_or_else(|| Error::corruption("compaction", "section buffer is absent"))?
                .can_accept(decoded.columns(), row)?
            {
                return Err(operation_limit());
            }
        }
        current
            .as_mut()
            .ok_or_else(|| Error::corruption("compaction", "section buffer is absent"))?
            .push(decoded, row)?;
    }
    Ok(())
}

fn flush<S: SectionSink>(current: &mut Option<SectionBuffer>, output: &mut S) -> Result<()> {
    if let Some(buffer) = current.take() {
        output.push_section(buffer.finish()?)?;
    }
    Ok(())
}

fn validate_inputs(inputs: &[UnitMeta], unit_id: u64) -> Result<u8> {
    let first = inputs
        .first()
        .ok_or_else(|| Error::invalid("compaction", "input unit set is empty"))?;
    if first.level() >= 2 {
        return Err(Error::invalid(
            "compaction",
            "at least one L0 or L1 unit is required",
        ));
    }
    if inputs.iter().any(|unit| unit.level() != first.level())
        || inputs.windows(2).any(|pair| {
            (pair[0].min_ts(), pair[0].unit_id()) >= (pair[1].min_ts(), pair[1].unit_id())
        })
        || inputs.iter().any(|unit| unit.unit_id() >= unit_id)
    {
        return Err(Error::invalid(
            "compaction",
            "inputs must be ordered, same-level, and older than the output id",
        ));
    }
    first
        .level()
        .checked_add(1)
        .ok_or_else(|| Error::invalid("compaction", "output level overflow"))
}

fn operation_limit() -> Error {
    Error::limit(
        "operation_memory_bytes",
        u64::MAX,
        u64::from(MAX_OPERATION_MEMORY_BYTES),
    )
}

#[cfg(test)]
#[path = "compact_tests.rs"]
mod tests;
