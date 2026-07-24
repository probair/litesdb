// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::collections::BTreeMap;

use crate::{
    Error, Result, StreamKey, TableId, TableVersion,
    manifest::UnitMeta,
    query::{
        cursor::{Fact, posted_row, tail_predecessor, validate_key},
        plan,
    },
    unit::{TableDirectoryEntry, UnitSource, decode_streams},
    wal::{TailIndex, TailRow},
};

pub(crate) struct TableKeyGroup {
    table: TableId,
    keys: Vec<StreamKey>,
    destinations: Vec<Vec<usize>>,
}

impl TableKeyGroup {
    pub(crate) const fn table(&self) -> TableId {
        self.table
    }

    pub(crate) fn keys(&self) -> &[StreamKey] {
        &self.keys
    }

    pub(crate) fn destinations(&self) -> &[Vec<usize>] {
        &self.destinations
    }
}

pub(crate) fn group_keys(keys: &[StreamKey]) -> Vec<TableKeyGroup> {
    let mut tables: BTreeMap<TableId, BTreeMap<StreamKey, Vec<usize>>> = BTreeMap::new();
    for (index, key) in keys.iter().copied().enumerate() {
        tables
            .entry(key.table())
            .or_default()
            .entry(key)
            .or_default()
            .push(index);
    }
    tables
        .into_iter()
        .map(|(table, streams)| {
            let (keys, destinations) = streams.into_iter().unzip();
            TableKeyGroup {
                table,
                keys,
                destinations,
            }
        })
        .collect()
}

pub(crate) fn scatter<T: Clone>(
    output: &mut [Option<T>],
    destinations: &[Vec<usize>],
    values: Vec<T>,
) -> Result<()> {
    if destinations.len() != values.len() {
        return Err(Error::corruption("batch query", "result shape mismatch"));
    }
    for (positions, value) in destinations.iter().zip(values) {
        for position in positions {
            let target = output
                .get_mut(*position)
                .ok_or_else(|| Error::corruption("batch query", "output position is invalid"))?;
            *target = Some(value.clone());
        }
    }
    Ok(())
}

pub(crate) fn collect_output<T>(output: Vec<Option<T>>) -> Result<Vec<T>> {
    output
        .into_iter()
        .map(|value| value.ok_or_else(|| Error::corruption("batch query", "result is absent")))
        .collect()
}

pub(crate) fn predecessors(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
    at: i64,
) -> Result<Vec<Option<Fact>>> {
    let Some(first) = keys.first().copied() else {
        return Ok(Vec::new());
    };
    validate_group(schemas, first.table(), keys)?;
    plan::validate_units(units)?;
    let table = validate_key(schemas, first)?;
    let mut facts = keys
        .iter()
        .map(|key| tail_predecessor(table, *key, at))
        .collect::<Result<Vec<_>>>()?;
    let mut unresolved = facts.iter().filter(|fact| fact.is_none()).count();
    let tail_min = table.rows().first().map(TailRow::timestamp);
    let upper = units.partition_point(|unit| unit.min_ts() <= at);
    let mut newer = None;
    for unit in units[..upper].iter().rev().copied() {
        let sections = source.table_sections(unit, first.table())?;
        for section in sections.iter().rev().copied() {
            validate_reverse_section(unit, first.table(), section, newer, tail_min)?;
            newer = Some(section);
            if unresolved == 0 {
                return Ok(facts);
            }
            if section.min_ts() > at {
                continue;
            }
            let selected = keys
                .iter()
                .zip(&facts)
                .filter(|(_, fact)| fact.is_none())
                .map(|(key, _)| (key.series(), key.field()))
                .collect::<Vec<_>>();
            let version = section_version(table.versions(), section)?;
            let bytes = source.section_bytes(unit, section)?;
            let decoded = decode_streams(&bytes, section, version, &selected)?;
            for column in decoded.columns() {
                let key = StreamKey::new(first.table(), column.series(), column.field());
                let index = keys.binary_search(&key).map_err(|_| {
                    Error::corruption("batch predecessor", "decoded key was not requested")
                })?;
                for (timestamp, cell) in decoded.timestamps().iter().zip(column.cells()).rev() {
                    if *timestamp <= at
                        && let Some(value) = cell
                    {
                        facts[index] = Some(Fact::new(*timestamp, *value));
                        unresolved = unresolved.saturating_sub(1);
                        break;
                    }
                }
            }
            if unresolved == 0 {
                return Ok(facts);
            }
        }
    }
    Ok(facts)
}

pub(crate) fn visit_facts<F>(
    units: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    keys: &[StreamKey],
    start: i64,
    end: i64,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(usize, Fact) -> Result<()>,
{
    let Some(first) = keys.first().copied() else {
        return Ok(());
    };
    validate_group(schemas, first.table(), keys)?;
    let table = validate_key(schemas, first)?;
    let planned = plan::range(units, first.table(), start, end, |unit, table| {
        source.table_sections(unit, table)
    })?;
    validate_tail_join(&planned, table.rows().first().map(TailRow::timestamp))?;
    let selected = keys
        .iter()
        .map(|key| (key.series(), key.field()))
        .collect::<Vec<_>>();
    for planned in planned {
        let section = planned.section();
        let version = section_version(table.versions(), section)?;
        let bytes = source.section_bytes(planned.unit(), section)?;
        let decoded = decode_streams(&bytes, section, version, &selected)?;
        for column in decoded.columns() {
            let key = StreamKey::new(first.table(), column.series(), column.field());
            let index = keys
                .binary_search(&key)
                .map_err(|_| Error::corruption("batch scan", "decoded key was not requested"))?;
            for (timestamp, cell) in decoded.timestamps().iter().zip(column.cells()) {
                if *timestamp >= start
                    && *timestamp < end
                    && let Some(value) = cell
                {
                    visit(index, Fact::new(*timestamp, *value))?;
                }
            }
        }
    }
    visit_tail(table, keys, start, end, visit)
}

fn validate_group(schemas: &TailIndex, table: TableId, keys: &[StreamKey]) -> Result<()> {
    if keys.windows(2).any(|pair| pair[0] >= pair[1]) || keys.iter().any(|key| key.table() != table)
    {
        return Err(Error::invalid(
            "keys",
            "batch execution keys must be unique, ordered, and from one table",
        ));
    }
    for key in keys {
        let _ = validate_key(schemas, *key)?;
    }
    Ok(())
}

fn validate_reverse_section(
    unit: UnitMeta,
    table: TableId,
    section: TableDirectoryEntry,
    newer: Option<TableDirectoryEntry>,
    tail_min: Option<i64>,
) -> Result<()> {
    plan::validate_loaded(unit, table, section)?;
    if let Some(newer_section) = newer {
        plan::validate_after(Some(section), newer_section)
    } else if tail_min.is_some_and(|minimum| section.max_ts() >= minimum) {
        Err(Error::corruption(
            "batch predecessor",
            "Seal and WAL tail ranges overlap",
        ))
    } else {
        Ok(())
    }
}

fn section_version(
    versions: &[TableVersion],
    section: TableDirectoryEntry,
) -> Result<&TableVersion> {
    let index = versions
        .binary_search_by_key(&section.version_no(), TableVersion::version_no)
        .map_err(|_| Error::corruption("batch query", "section version disappeared"))?;
    Ok(&versions[index])
}

fn validate_tail_join(planned: &[plan::PlannedSection], tail_min: Option<i64>) -> Result<()> {
    if planned
        .last()
        .map(|entry| entry.section().max_ts())
        .zip(tail_min)
        .is_some_and(|(maximum, minimum)| maximum >= minimum)
    {
        return Err(Error::corruption(
            "batch scan",
            "Seal and WAL tail ranges overlap",
        ));
    }
    Ok(())
}

fn visit_tail<F>(
    table: &crate::wal::TailTable,
    keys: &[StreamKey],
    start: i64,
    end: i64,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(usize, Fact) -> Result<()>,
{
    for (index, key) in keys.iter().copied().enumerate() {
        let Some(postings) = table.stream_rows(key.series(), key.field()) else {
            continue;
        };
        let first = postings.partition_point(|row| {
            posted_row(table, *row).is_some_and(|entry| entry.timestamp() < start)
        });
        let last = postings.partition_point(|row| {
            posted_row(table, *row).is_some_and(|entry| entry.timestamp() < end)
        });
        for row_index in postings
            .get(first..last)
            .ok_or_else(|| Error::corruption("batch scan", "tail posting range is invalid"))?
        {
            let row = posted_row(table, *row_index)
                .ok_or_else(|| Error::corruption("batch scan", "tail posting exceeds rows"))?;
            let entries = table
                .row_entries(row)
                .ok_or_else(|| Error::corruption("batch scan", "tail row entries are invalid"))?;
            let entry_index = entries
                .binary_search_by_key(&(key.series(), key.field()), |entry| {
                    (entry.series(), entry.field())
                })
                .map_err(|_| Error::corruption("batch scan", "tail posting has no Fact"))?;
            visit(
                index,
                Fact::new(row.timestamp(), entries[entry_index].value()),
            )?;
        }
    }
    Ok(())
}
