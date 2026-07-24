// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(dead_code, reason = "consumed by query primitives later in M5")]

use std::{collections::BTreeSet, mem::size_of};

use crate::{
    Error, Result, TableId,
    limits::{Limit, ensure_at_most},
    manifest::UnitMeta,
    unit::TableDirectoryEntry,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct PlannedSection {
    unit: UnitMeta,
    section: TableDirectoryEntry,
}

impl PlannedSection {
    pub(crate) const fn unit(self) -> UnitMeta {
        self.unit
    }

    pub(crate) const fn section(self) -> TableDirectoryEntry {
        self.section
    }
}

pub(crate) fn range<F>(
    units: &[UnitMeta],
    table: TableId,
    start: i64,
    end: i64,
    mut load: F,
) -> Result<Vec<PlannedSection>>
where
    F: FnMut(UnitMeta, TableId) -> Result<Vec<TableDirectoryEntry>>,
{
    validate_units(units)?;
    if start > end {
        return Err(Error::invalid("range", "start must not exceed end"));
    }
    if start == end {
        return Ok(Vec::new());
    }
    let upper = units.partition_point(|unit| unit.min_ts() < end);
    let mut planned = Vec::new();
    for unit in &units[..upper] {
        if unit.max_ts() < start {
            continue;
        }
        for section in load(*unit, table)? {
            validate_loaded(*unit, table, section)?;
            if section.min_ts() < end && section.max_ts() >= start {
                push_ordered(
                    &mut planned,
                    PlannedSection {
                        unit: *unit,
                        section,
                    },
                )?;
            }
        }
    }
    Ok(planned)
}

pub(crate) fn validate_units(units: &[UnitMeta]) -> Result<()> {
    if units
        .windows(2)
        .any(|pair| (pair[0].min_ts(), pair[0].unit_id()) >= (pair[1].min_ts(), pair[1].unit_id()))
    {
        return Err(Error::corruption(
            "query plan",
            "units are not in routing order",
        ));
    }
    let mut ids = BTreeSet::new();
    if units.iter().any(|unit| !ids.insert(unit.unit_id())) {
        return Err(Error::corruption(
            "query plan",
            "unit identifier is duplicated",
        ));
    }
    Ok(())
}

pub(crate) fn validate_loaded(
    unit: UnitMeta,
    table: TableId,
    section: TableDirectoryEntry,
) -> Result<()> {
    if section.table() != table
        || section.min_ts() < unit.min_ts()
        || section.max_ts() > unit.max_ts()
    {
        return Err(Error::corruption(
            "query plan",
            "loaded section contradicts table or unit range",
        ));
    }
    Ok(())
}

fn push_ordered(planned: &mut Vec<PlannedSection>, candidate: PlannedSection) -> Result<()> {
    validate_after(planned.last().map(|entry| entry.section), candidate.section)?;
    let new_len = planned.len().checked_add(1).ok_or_else(|| {
        Error::limit(
            "operation_memory_bytes",
            u64::MAX,
            Limit::OperationMemoryBytes.maximum(),
        )
    })?;
    let bytes = new_len
        .checked_mul(size_of::<PlannedSection>())
        .and_then(|value| u64::try_from(value).ok())
        .ok_or_else(|| {
            Error::limit(
                "operation_memory_bytes",
                u64::MAX,
                Limit::OperationMemoryBytes.maximum(),
            )
        })?;
    ensure_at_most(Limit::OperationMemoryBytes, bytes)?;
    planned.push(candidate);
    Ok(())
}

pub(crate) fn validate_after(
    previous: Option<TableDirectoryEntry>,
    current: TableDirectoryEntry,
) -> Result<()> {
    if previous.is_some_and(|entry| {
        entry.min_ts() >= current.min_ts() || entry.max_ts() >= current.min_ts()
    }) {
        return Err(Error::corruption(
            "query plan",
            "table sections are unordered or overlap",
        ));
    }
    Ok(())
}

#[cfg(test)]
#[path = "plan_tests.rs"]
mod tests;
