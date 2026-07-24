// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{CellValue, FieldId, SeriesId, TableVersion};
use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObservationEntry {
    series: SeriesId,
    field: FieldId,
    value: CellValue,
}

impl ObservationEntry {
    #[must_use]
    pub const fn new(series: SeriesId, field: FieldId, value: CellValue) -> Self {
        Self {
            series,
            field,
            value,
        }
    }

    #[must_use]
    pub const fn series(self) -> SeriesId {
        self.series
    }

    #[must_use]
    pub const fn field(self) -> FieldId {
        self.field
    }

    #[must_use]
    pub const fn value(self) -> CellValue {
        self.value
    }

    const fn key(self) -> (SeriesId, FieldId) {
        (self.series, self.field)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Observation {
    timestamp: i64,
    entries: ObservationEntries,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum ObservationEntries {
    One(ObservationEntry),
    Many(Box<[ObservationEntry]>),
}

impl Observation {
    pub fn new(timestamp: i64, entries: Vec<ObservationEntry>) -> Result<Self> {
        if entries.is_empty() {
            return Err(Error::invalid("entries", "observation must not be empty"));
        }
        if entries
            .windows(2)
            .any(|pair| pair[0].key() >= pair[1].key())
        {
            return Err(Error::invalid(
                "entries",
                "series and field keys must be strictly increasing",
            ));
        }
        let entries = match entries.as_slice() {
            [entry] => ObservationEntries::One(*entry),
            _ => ObservationEntries::Many(entries.into_boxed_slice()),
        };
        Ok(Self { timestamp, entries })
    }

    #[must_use]
    pub const fn timestamp(&self) -> i64 {
        self.timestamp
    }

    #[must_use]
    pub fn entries(&self) -> &[ObservationEntry] {
        match &self.entries {
            ObservationEntries::One(entry) => std::slice::from_ref(entry),
            ObservationEntries::Many(entries) => entries,
        }
    }

    pub(crate) const fn from_single(timestamp: i64, entry: ObservationEntry) -> Self {
        Self {
            timestamp,
            entries: ObservationEntries::One(entry),
        }
    }

    #[allow(
        dead_code,
        reason = "called by append before WAL mutation in a later milestone"
    )]
    pub(crate) fn validate_after(&self, previous: Option<i64>) -> Result<()> {
        if previous.is_some_and(|timestamp| self.timestamp <= timestamp) {
            return Err(Error::invalid(
                "timestamp",
                "table clock must be strictly increasing",
            ));
        }
        Ok(())
    }

    #[allow(
        dead_code,
        reason = "called by append before WAL mutation in a later milestone"
    )]
    pub(crate) fn validate_schema(&self, version: &TableVersion) -> Result<()> {
        for entry in self.entries() {
            let field = version
                .fields()
                .binary_search_by_key(&entry.field(), |schema| schema.field())
                .map_err(|_| Error::invalid("entries", "field is absent from table version"))?;
            if !entry.value().matches(version.fields()[field].value_type()) {
                return Err(Error::invalid("entries", "value does not match field type"));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
#[path = "observation_tests.rs"]
mod tests;
