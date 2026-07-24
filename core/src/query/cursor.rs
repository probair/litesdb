// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by query primitives later in M5")]

use crate::{
    CellValue, Error, Result, StreamKey, TableVersion,
    manifest::UnitMeta,
    query::plan::{self, PlannedSection},
    unit::{UnitSource, decode_stream},
    wal::{TailIndex, TailRow, TailTable},
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Fact {
    timestamp: i64,
    value: CellValue,
}

impl Fact {
    pub(crate) const fn new(timestamp: i64, value: CellValue) -> Self {
        Self { timestamp, value }
    }

    #[must_use]
    pub const fn timestamp(self) -> i64 {
        self.timestamp
    }

    #[must_use]
    pub const fn value(self) -> CellValue {
        self.value
    }
}

pub struct FactCursor<'a> {
    source: &'a dyn UnitSource,
    schemas: &'a TailIndex,
    key: StreamKey,
    start: i64,
    end: i64,
    planned: Vec<PlannedSection>,
    next_section: usize,
    current: Vec<Fact>,
    current_index: usize,
    tail_table: Option<&'a TailTable>,
    tail_postings: &'a [u32],
    tail_index: usize,
    last_emitted: Option<i64>,
    failed: bool,
}

impl<'a> FactCursor<'a> {
    pub(crate) fn new(
        units: &[UnitMeta],
        source: &'a dyn UnitSource,
        schemas: &'a TailIndex,
        key: StreamKey,
        start: i64,
        end: i64,
    ) -> Result<Self> {
        let table = validate_key(schemas, key)?;
        let planned = plan::range(units, key.table(), start, end, |unit, table| {
            source.table_sections(unit, table)
        })?;
        let tail_postings = tail_postings(table, key, start, end)?;
        validate_join(&planned, table)?;
        Ok(Self {
            source,
            schemas,
            key,
            start,
            end,
            planned,
            next_section: 0,
            current: Vec::new(),
            current_index: 0,
            tail_table: Some(table),
            tail_postings,
            tail_index: 0,
            last_emitted: None,
            failed: false,
        })
    }

    pub fn next_fact(&mut self) -> Result<Option<Fact>> {
        if self.failed {
            return Err(Error::corruption("Fact cursor", "cursor already failed"));
        }
        match self.advance() {
            Ok(fact) => Ok(fact),
            Err(error) => {
                self.failed = true;
                Err(error)
            }
        }
    }

    fn advance(&mut self) -> Result<Option<Fact>> {
        loop {
            if let Some(fact) = self.current.get(self.current_index).copied() {
                self.current_index = self
                    .current_index
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("Fact cursor", "section index overflow"))?;
                return self.emit(fact).map(Some);
            }
            if let Some(planned) = self.planned.get(self.next_section).copied() {
                self.next_section = self
                    .next_section
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("Fact cursor", "plan index overflow"))?;
                self.load_section(planned)?;
                continue;
            }
            if let Some(fact) = self.next_tail_fact()? {
                return self.emit(fact).map(Some);
            }
            return Ok(None);
        }
    }

    fn load_section(&mut self, planned: PlannedSection) -> Result<()> {
        let section = planned.section();
        let table = self
            .schemas
            .table(section.table())
            .ok_or_else(|| Error::corruption("Fact cursor", "section table disappeared"))?;
        let version_index = table
            .versions()
            .binary_search_by_key(&section.version_no(), TableVersion::version_no)
            .map_err(|_| Error::corruption("Fact cursor", "section version disappeared"))?;
        let bytes = self.source.section_bytes(planned.unit(), section)?;
        let decoded = decode_stream(
            &bytes,
            section,
            &table.versions()[version_index],
            self.key.series(),
            self.key.field(),
        )?;
        if decoded.columns().len() > 1 {
            return Err(Error::corruption(
                "Fact cursor",
                "stream decoder returned multiple columns",
            ));
        }
        self.current.clear();
        self.current_index = 0;
        if let Some(column) = decoded.columns().first() {
            for (timestamp, cell) in decoded.timestamps().iter().zip(column.cells()) {
                if *timestamp >= self.start
                    && *timestamp < self.end
                    && let Some(value) = cell
                {
                    self.current.push(Fact {
                        timestamp: *timestamp,
                        value: *value,
                    });
                }
            }
        }
        Ok(())
    }

    fn next_tail_fact(&mut self) -> Result<Option<Fact>> {
        let Some(row_index) = self.tail_postings.get(self.tail_index).copied() else {
            return Ok(None);
        };
        self.tail_index = self
            .tail_index
            .checked_add(1)
            .ok_or_else(|| Error::corruption("Fact cursor", "tail posting index overflow"))?;
        let table = self
            .tail_table
            .ok_or_else(|| Error::corruption("Fact cursor", "tail table is absent"))?;
        let row = posted_row(table, row_index)
            .ok_or_else(|| Error::corruption("Fact cursor", "tail posting exceeds rows"))?;
        let entries = table
            .row_entries(row)
            .ok_or_else(|| Error::corruption("Fact cursor", "tail row entries are invalid"))?;
        let entry_index = entries
            .binary_search_by_key(&(self.key.series(), self.key.field()), |entry| {
                (entry.series(), entry.field())
            })
            .map_err(|_| Error::corruption("Fact cursor", "tail posting has no Fact"))?;
        Ok(Some(Fact {
            timestamp: row.timestamp(),
            value: entries[entry_index].value(),
        }))
    }

    fn emit(&mut self, fact: Fact) -> Result<Fact> {
        if self
            .last_emitted
            .is_some_and(|previous| previous >= fact.timestamp)
        {
            return Err(Error::corruption(
                "Fact cursor",
                "sources emitted unordered or duplicate timestamps",
            ));
        }
        self.last_emitted = Some(fact.timestamp);
        Ok(fact)
    }
}

pub(crate) fn predecessor_fact(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    key: StreamKey,
    at: i64,
) -> Result<Option<Fact>> {
    let mut facts = super::batch::predecessors(units, source, schemas, &[key], at)?;
    Ok(facts.pop().flatten())
}

pub(crate) fn validate_key(schemas: &TailIndex, key: StreamKey) -> Result<&TailTable> {
    let table = schemas
        .table(key.table())
        .ok_or_else(|| Error::invalid("key", "table is absent"))?;
    let latest = table
        .versions()
        .last()
        .ok_or_else(|| Error::corruption("Fact cursor", "table has no version"))?;
    if latest
        .fields()
        .binary_search_by_key(&key.field(), |field| field.field())
        .is_err()
    {
        return Err(Error::invalid("key", "field is absent"));
    }
    Ok(table)
}

pub(crate) fn tail_predecessor(table: &TailTable, key: StreamKey, at: i64) -> Result<Option<Fact>> {
    let Some(postings) = table.stream_rows(key.series(), key.field()) else {
        return Ok(None);
    };
    let upper = postings.partition_point(|index| {
        posted_row(table, *index).is_some_and(|row| row.timestamp() <= at)
    });
    let Some(posting_index) = upper.checked_sub(1) else {
        return Ok(None);
    };
    let row_index = *postings
        .get(posting_index)
        .ok_or_else(|| Error::corruption("Fact cursor", "tail predecessor is absent"))?;
    let row = posted_row(table, row_index)
        .ok_or_else(|| Error::corruption("Fact cursor", "tail predecessor exceeds rows"))?;
    let entries = table
        .row_entries(row)
        .ok_or_else(|| Error::corruption("Fact cursor", "tail row entries are invalid"))?;
    let entry_index = entries
        .binary_search_by_key(&(key.series(), key.field()), |entry| {
            (entry.series(), entry.field())
        })
        .map_err(|_| Error::corruption("Fact cursor", "tail predecessor has no Fact"))?;
    Ok(Some(Fact {
        timestamp: row.timestamp(),
        value: entries[entry_index].value(),
    }))
}

fn tail_postings(table: &TailTable, key: StreamKey, start: i64, end: i64) -> Result<&[u32]> {
    let Some(postings) = table.stream_rows(key.series(), key.field()) else {
        return Ok(&[]);
    };
    let first = postings.partition_point(|index| {
        posted_row(table, *index).is_some_and(|row| row.timestamp() < start)
    });
    let last = postings.partition_point(|index| {
        posted_row(table, *index).is_some_and(|row| row.timestamp() < end)
    });
    postings
        .get(first..last)
        .ok_or_else(|| Error::corruption("Fact cursor", "tail posting range is invalid"))
}

pub(crate) fn posted_row(table: &TailTable, index: u32) -> Option<&TailRow> {
    usize::try_from(index)
        .ok()
        .and_then(|offset| table.rows().get(offset))
}

fn validate_join(planned: &[PlannedSection], tail: &TailTable) -> Result<()> {
    let unit_max = planned.last().map(|entry| entry.section().max_ts());
    let tail_min = tail.rows().first().map(TailRow::timestamp);
    if unit_max
        .zip(tail_min)
        .is_some_and(|(unit, tail)| unit >= tail)
    {
        return Err(Error::corruption(
            "Fact cursor",
            "Seal and WAL tail ranges overlap",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "cursor_tests.rs"]
mod tests;
