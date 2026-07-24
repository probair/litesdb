// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by WAL recovery and snapshots later in M3/M5"
)]

use std::collections::BTreeMap;

use crate::{
    Error, FieldId, Observation, Result, SeriesId, TableId, TableVersion, ValueType,
    limits::MAX_TAIL_INDEX_BYTES,
    wal::{record::RecordBody, tail_validate},
};

pub(crate) trait ReplayTarget {
    fn field_type(&self, table: TableId, field: FieldId) -> Option<ValueType>;

    fn apply(&mut self, seq: u64, body: RecordBody) -> Result<()>;
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TailRow {
    timestamp: i64,
    version_no: u32,
    entry_start: u32,
    entry_len: u32,
}

impl TailRow {
    pub(crate) const fn version_no(&self) -> u32 {
        self.version_no
    }

    pub(crate) const fn timestamp(&self) -> i64 {
        self.timestamp
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TailTable {
    versions: Vec<TableVersion>,
    rows: Vec<TailRow>,
    entries: Vec<crate::ObservationEntry>,
    per_stream: BTreeMap<(SeriesId, FieldId), Vec<u32>>,
    retired_series: BTreeMap<SeriesId, i64>,
    retired_fields: BTreeMap<FieldId, i64>,
    last_ts: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredTable {
    table: TableId,
    last_ts: Option<i64>,
    versions: Vec<TableVersion>,
    retired_series: Vec<(SeriesId, i64)>,
    retired_fields: Vec<(FieldId, i64)>,
}

impl RecoveredTable {
    pub(crate) const fn new(
        table: TableId,
        last_ts: Option<i64>,
        versions: Vec<TableVersion>,
        retired_series: Vec<(SeriesId, i64)>,
        retired_fields: Vec<(FieldId, i64)>,
    ) -> Self {
        Self {
            table,
            last_ts,
            versions,
            retired_series,
            retired_fields,
        }
    }
}

impl TailTable {
    pub(crate) fn versions(&self) -> &[TableVersion] {
        &self.versions
    }

    pub(crate) fn rows(&self) -> &[TailRow] {
        &self.rows
    }

    pub(crate) fn stream_rows(&self, series: SeriesId, field: FieldId) -> Option<&[u32]> {
        self.per_stream.get(&(series, field)).map(Vec::as_slice)
    }

    pub(crate) fn streams(&self) -> impl ExactSizeIterator<Item = ((SeriesId, FieldId), &[u32])> {
        self.per_stream
            .iter()
            .map(|(key, rows)| (*key, rows.as_slice()))
    }

    pub(crate) fn row_entries(&self, row: &TailRow) -> Option<&[crate::ObservationEntry]> {
        let start = usize::try_from(row.entry_start).ok()?;
        let length = usize::try_from(row.entry_len).ok()?;
        let end = start.checked_add(length)?;
        self.entries.get(start..end)
    }

    pub(crate) const fn last_ts(&self) -> Option<i64> {
        self.last_ts
    }

    pub(crate) fn retired_series_at(&self, series: SeriesId) -> Option<i64> {
        self.retired_series.get(&series).copied()
    }

    pub(crate) fn retired_field_at(&self, field: FieldId) -> Option<i64> {
        self.retired_fields.get(&field).copied()
    }

    pub(crate) fn retired_series(&self) -> impl Iterator<Item = (SeriesId, i64)> + '_ {
        self.retired_series
            .iter()
            .map(|(series, timestamp)| (*series, *timestamp))
    }

    pub(crate) fn retired_fields(&self) -> impl Iterator<Item = (FieldId, i64)> + '_ {
        self.retired_fields
            .iter()
            .map(|(field, timestamp)| (*field, *timestamp))
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TailIndex {
    tables: BTreeMap<TableId, TailTable>,
    table_high_water: u32,
    next_seq: u64,
    estimated_bytes: u64,
}

impl TailIndex {
    pub(crate) const fn new(table_high_water: u32, next_seq: u64) -> Self {
        Self {
            tables: BTreeMap::new(),
            table_high_water,
            next_seq,
            estimated_bytes: 0,
        }
    }

    pub(crate) fn restore(
        table_high_water: u32,
        next_seq: u64,
        tables: Vec<RecoveredTable>,
    ) -> Result<Self> {
        let mut restored = BTreeMap::new();
        let mut previous = None;
        for table in tables {
            if table.table.get() > table_high_water
                || previous.is_some_and(|prior| prior >= table.table)
                || table.versions.is_empty()
            {
                return Err(Error::corruption(
                    "WAL replay",
                    "checkpoint tables contradict ordering or high water",
                ));
            }
            let retired_series = table.retired_series.into_iter().collect();
            let retired_fields = table.retired_fields.into_iter().collect();
            let id = table.table;
            restored.insert(
                id,
                TailTable {
                    versions: table.versions,
                    rows: Vec::new(),
                    entries: Vec::new(),
                    per_stream: BTreeMap::new(),
                    retired_series,
                    retired_fields,
                    last_ts: table.last_ts,
                },
            );
            previous = Some(id);
        }
        Ok(Self {
            tables: restored,
            table_high_water,
            next_seq,
            estimated_bytes: 0,
        })
    }

    pub(crate) fn table(&self, table: TableId) -> Option<&TailTable> {
        self.tables.get(&table)
    }

    pub(crate) fn tables(&self) -> impl ExactSizeIterator<Item = (TableId, &TailTable)> {
        self.tables.iter().map(|(table, state)| (*table, state))
    }

    pub(crate) const fn next_seq(&self) -> u64 {
        self.next_seq
    }

    pub(crate) const fn estimated_bytes(&self) -> u64 {
        self.estimated_bytes
    }

    pub(crate) const fn table_high_water(&self) -> u32 {
        self.table_high_water
    }

    pub(crate) fn validate(&self, seq: u64, body: &RecordBody) -> Result<()> {
        tail_validate::validate(self, seq, body)
    }

    fn apply_inner(&mut self, seq: u64, body: RecordBody) -> Result<()> {
        self.validate(seq, &body)?;
        let following_seq = seq
            .checked_add(1)
            .ok_or_else(|| Error::corruption("WAL replay", "sequence overflow"))?;
        match body {
            RecordBody::AppendObservation { table, observation } => {
                self.apply_observation(table, &observation)?;
            }
            RecordBody::CreateTable { table, spec } => {
                let expected = self
                    .table_high_water
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("WAL replay", "table id overflow"))?;
                if table.get() != expected || self.tables.contains_key(&table) {
                    return Err(Error::corruption(
                        "WAL replay",
                        "table id is reused or not continuous",
                    ));
                }
                self.tables.insert(
                    table,
                    TailTable {
                        versions: vec![TableVersion::initial(spec)],
                        rows: Vec::new(),
                        entries: Vec::new(),
                        per_stream: BTreeMap::new(),
                        retired_series: BTreeMap::new(),
                        retired_fields: BTreeMap::new(),
                        last_ts: None,
                    },
                );
                self.table_high_water = expected;
            }
            RecordBody::NewTableVersion { table, spec } => {
                let state = self
                    .tables
                    .get_mut(&table)
                    .ok_or_else(|| Error::corruption("WAL replay", "version table is absent"))?;
                let previous = state
                    .versions
                    .last()
                    .ok_or_else(|| Error::corruption("WAL replay", "table has no version"))?;
                if previous.effective_from().is_none() {
                    return Err(Error::corruption(
                        "WAL replay",
                        "cannot supersede an inactive version",
                    ));
                }
                let version = previous
                    .successor(spec)
                    .map_err(|_| Error::corruption("WAL replay", "invalid version successor"))?;
                state.versions.push(version);
            }
            RecordBody::DropTable { table } => {
                if self.tables.remove(&table).is_none() {
                    return Err(Error::corruption("WAL replay", "dropped table is absent"));
                }
            }
            RecordBody::RetireSeries {
                table,
                series,
                retire_ts,
            } => {
                let state = self.tables.get_mut(&table).ok_or_else(|| {
                    Error::corruption("WAL replay", "retired series table is absent")
                })?;
                state.retired_series.insert(series, retire_ts);
            }
            RecordBody::RetireField {
                table,
                field,
                retire_ts,
            } => {
                let state = self.tables.get_mut(&table).ok_or_else(|| {
                    Error::corruption("WAL replay", "retired field table is absent")
                })?;
                if lookup_field(state, field).is_none() {
                    return Err(Error::corruption("WAL replay", "retired field is absent"));
                }
                state.retired_fields.insert(field, retire_ts);
            }
        }
        self.next_seq = following_seq;
        Ok(())
    }

    fn apply_observation(&mut self, table: TableId, observation: &Observation) -> Result<()> {
        let increment = tail_validate::estimate_row_bytes(observation.entries().len())?;
        let estimated_bytes = self.estimated_bytes.checked_add(increment).ok_or_else(|| {
            Error::limit(
                "tail_index_bytes",
                u64::MAX,
                u64::from(MAX_TAIL_INDEX_BYTES),
            )
        })?;
        if estimated_bytes > u64::from(MAX_TAIL_INDEX_BYTES) {
            return Err(Error::limit(
                "tail_index_bytes",
                estimated_bytes,
                u64::from(MAX_TAIL_INDEX_BYTES),
            ));
        }

        let state = self
            .tables
            .get_mut(&table)
            .ok_or_else(|| Error::corruption("WAL replay", "observation table disappeared"))?;
        let row_index = u32::try_from(state.rows.len())
            .map_err(|_| Error::limit("tail_rows", u64::MAX, u64::from(u32::MAX)))?;
        let entry_start = u32::try_from(state.entries.len())
            .map_err(|_| Error::limit("tail_entries", u64::MAX, u64::from(u32::MAX)))?;
        let entry_len = u32::try_from(observation.entries().len())
            .map_err(|_| Error::limit("tail_entries", u64::MAX, u64::from(u32::MAX)))?;
        let version = state
            .versions
            .last_mut()
            .ok_or_else(|| Error::corruption("WAL replay", "table version disappeared"))?;
        if version.effective_from().is_none() {
            *version = version
                .clone()
                .activate(observation.timestamp())
                .map_err(|_| Error::corruption("WAL replay", "version activation failed"))?;
        }
        let version_no = version.version_no();
        for entry in observation.entries() {
            state
                .per_stream
                .entry((entry.series(), entry.field()))
                .or_default()
                .push(row_index);
            if state
                .retired_series
                .get(&entry.series())
                .is_some_and(|retire_ts| observation.timestamp() > *retire_ts)
            {
                state.retired_series.remove(&entry.series());
            }
            if state
                .retired_fields
                .get(&entry.field())
                .is_some_and(|retire_ts| observation.timestamp() > *retire_ts)
            {
                state.retired_fields.remove(&entry.field());
            }
        }
        state.entries.extend_from_slice(observation.entries());
        state.last_ts = Some(observation.timestamp());
        state.rows.push(TailRow {
            timestamp: observation.timestamp(),
            version_no,
            entry_start,
            entry_len,
        });
        self.estimated_bytes = estimated_bytes;
        Ok(())
    }
}

impl ReplayTarget for TailIndex {
    fn field_type(&self, table: TableId, field: FieldId) -> Option<ValueType> {
        self.tables
            .get(&table)
            .and_then(|state| lookup_field(state, field))
    }

    fn apply(&mut self, seq: u64, body: RecordBody) -> Result<()> {
        self.apply_inner(seq, body)
    }
}

fn lookup_field(state: &TailTable, field: FieldId) -> Option<ValueType> {
    let version = state.versions.last()?;
    let index = version
        .fields()
        .binary_search_by_key(&field, |schema| schema.field())
        .ok()?;
    Some(version.fields()[index].value_type())
}

#[cfg(test)]
#[path = "tail_tests.rs"]
mod tests;
