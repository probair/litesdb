// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by MANIFEST format and database open later in M3/M6"
)]

use std::collections::BTreeSet;

#[path = "catalog_validate.rs"]
mod validate;

use crate::{
    Error, FieldId, Result, SeriesId, TableId, TableVersion,
    limits::{MAX_LIVE_UNITS, MAX_TABLES, MAX_UNIT_FILE_BYTES, MAX_UNIT_SECTIONS},
    wal::{Checkpoint, RecoveredTable, TailIndex},
};

use validate::{validate_count, validate_versions};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct ManifestIdentity {
    generation: u64,
    unit_high_water: u64,
    table_high_water: u32,
    shard_id: u64,
    writer_epoch: u64,
}

impl ManifestIdentity {
    pub(crate) const fn new(
        generation: u64,
        unit_high_water: u64,
        table_high_water: u32,
        shard_id: u64,
        writer_epoch: u64,
    ) -> Self {
        Self {
            generation,
            unit_high_water,
            table_high_water,
            shard_id,
            writer_epoch,
        }
    }

    pub(crate) const fn generation(self) -> u64 {
        self.generation
    }

    pub(crate) const fn unit_high_water(self) -> u64 {
        self.unit_high_water
    }

    pub(crate) const fn table_high_water(self) -> u32 {
        self.table_high_water
    }

    pub(crate) const fn shard_id(self) -> u64 {
        self.shard_id
    }

    pub(crate) const fn writer_epoch(self) -> u64 {
        self.writer_epoch
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct RetentionState {
    floor: Option<i64>,
    heads_generation: Option<u64>,
}

impl RetentionState {
    pub(crate) const fn new(floor: Option<i64>, heads_generation: Option<u64>) -> Self {
        Self {
            floor,
            heads_generation,
        }
    }

    pub(crate) const fn floor(self) -> Option<i64> {
        self.floor
    }

    pub(crate) const fn heads_generation(self) -> Option<u64> {
        self.heads_generation
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SeriesRetirement {
    series: SeriesId,
    retire_ts: i64,
}

impl SeriesRetirement {
    pub(crate) const fn new(series: SeriesId, retire_ts: i64) -> Self {
        Self { series, retire_ts }
    }

    pub(crate) const fn series(self) -> SeriesId {
        self.series
    }

    pub(crate) const fn retire_ts(self) -> i64 {
        self.retire_ts
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct FieldRetirement {
    field: FieldId,
    retire_ts: i64,
}

impl FieldRetirement {
    pub(crate) const fn new(field: FieldId, retire_ts: i64) -> Self {
        Self { field, retire_ts }
    }

    pub(crate) const fn field(self) -> FieldId {
        self.field
    }

    pub(crate) const fn retire_ts(self) -> i64 {
        self.retire_ts
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct TableCatalog {
    table: TableId,
    last_ts: Option<i64>,
    versions: Vec<TableVersion>,
    retired_series: Vec<SeriesRetirement>,
    retired_fields: Vec<FieldRetirement>,
}

impl TableCatalog {
    pub(crate) fn restore(
        table: TableId,
        last_ts: Option<i64>,
        versions: Vec<TableVersion>,
        retired_series: Vec<SeriesRetirement>,
        retired_fields: Vec<FieldRetirement>,
    ) -> Result<Self> {
        validate_versions(last_ts, &versions)?;
        if retired_series
            .windows(2)
            .any(|pair| pair[0].series() >= pair[1].series())
        {
            return Err(Error::corruption(
                "MANIFEST table",
                "series retirements are not strictly ordered",
            ));
        }
        if retired_fields
            .windows(2)
            .any(|pair| pair[0].field() >= pair[1].field())
        {
            return Err(Error::corruption(
                "MANIFEST table",
                "field retirements are not strictly ordered",
            ));
        }
        let latest = versions
            .last()
            .ok_or_else(|| Error::corruption("MANIFEST table", "version history is empty"))?;
        for retirement in &retired_fields {
            if latest
                .fields()
                .binary_search_by_key(&retirement.field(), |field| field.field())
                .is_err()
            {
                return Err(Error::corruption(
                    "MANIFEST table",
                    "retired field is absent from latest version",
                ));
            }
        }
        Ok(Self {
            table,
            last_ts,
            versions,
            retired_series,
            retired_fields,
        })
    }

    pub(crate) const fn table(&self) -> TableId {
        self.table
    }

    pub(crate) const fn last_ts(&self) -> Option<i64> {
        self.last_ts
    }

    pub(crate) fn versions(&self) -> &[TableVersion] {
        &self.versions
    }

    pub(crate) fn retired_series(&self) -> &[SeriesRetirement] {
        &self.retired_series
    }

    pub(crate) fn retired_fields(&self) -> &[FieldRetirement] {
        &self.retired_fields
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct UnitMeta {
    unit_id: u64,
    level: u8,
    min_ts: i64,
    max_ts: i64,
    section_count: u32,
    total_rows: u64,
    file_len: u64,
    body_crc32: u32,
}

impl UnitMeta {
    #[allow(
        clippy::too_many_arguments,
        reason = "fields mirror one fixed on-disk unit entry"
    )]
    pub(crate) fn new(
        unit_id: u64,
        level: u8,
        min_ts: i64,
        max_ts: i64,
        section_count: u32,
        total_rows: u64,
        file_len: u64,
        body_crc32: u32,
    ) -> Result<Self> {
        if level > 2
            || min_ts > max_ts
            || section_count == 0
            || section_count > MAX_UNIT_SECTIONS
            || total_rows == 0
        {
            return Err(Error::corruption(
                "MANIFEST unit",
                "level, time range, section count, or row count is invalid",
            ));
        }
        if file_len > MAX_UNIT_FILE_BYTES {
            return Err(Error::limit(
                "unit_file_bytes",
                file_len,
                MAX_UNIT_FILE_BYTES,
            ));
        }
        Ok(Self {
            unit_id,
            level,
            min_ts,
            max_ts,
            section_count,
            total_rows,
            file_len,
            body_crc32,
        })
    }

    pub(crate) const fn unit_id(self) -> u64 {
        self.unit_id
    }

    pub(crate) const fn level(self) -> u8 {
        self.level
    }

    pub(crate) const fn min_ts(self) -> i64 {
        self.min_ts
    }

    pub(crate) const fn max_ts(self) -> i64 {
        self.max_ts
    }

    pub(crate) const fn section_count(self) -> u32 {
        self.section_count
    }

    pub(crate) const fn total_rows(self) -> u64 {
        self.total_rows
    }

    pub(crate) const fn file_len(self) -> u64 {
        self.file_len
    }

    pub(crate) const fn body_crc32(self) -> u32 {
        self.body_crc32
    }

    const fn order_key(self) -> (i64, u64) {
        (self.min_ts, self.unit_id)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Manifest {
    identity: ManifestIdentity,
    checkpoint: Checkpoint,
    retention: RetentionState,
    tables: Vec<TableCatalog>,
    units: Vec<UnitMeta>,
}

impl Manifest {
    pub(crate) fn restore(
        identity: ManifestIdentity,
        checkpoint: Checkpoint,
        retention: RetentionState,
        tables: Vec<TableCatalog>,
        units: Vec<UnitMeta>,
    ) -> Result<Self> {
        validate_count("tables", tables.len(), MAX_TABLES)?;
        validate_count("live_units", units.len(), MAX_LIVE_UNITS)?;
        if tables
            .windows(2)
            .any(|pair| pair[0].table() >= pair[1].table())
        {
            return Err(Error::corruption(
                "MANIFEST",
                "tables are not strictly ordered",
            ));
        }
        if units
            .windows(2)
            .any(|pair| pair[0].order_key() >= pair[1].order_key())
        {
            return Err(Error::corruption(
                "MANIFEST",
                "units are not ordered by minimum time and identifier",
            ));
        }
        let mut unit_ids = BTreeSet::new();
        if units.iter().any(|unit| !unit_ids.insert(unit.unit_id())) {
            return Err(Error::corruption(
                "MANIFEST",
                "unit identifier is duplicated",
            ));
        }
        if tables
            .last()
            .is_some_and(|table| table.table().get() > identity.table_high_water())
            || units
                .iter()
                .any(|unit| unit.unit_id() > identity.unit_high_water())
        {
            return Err(Error::corruption(
                "MANIFEST",
                "allocation high water is below a live identifier",
            ));
        }
        Ok(Self {
            identity,
            checkpoint,
            retention,
            tables,
            units,
        })
    }

    pub(crate) const fn identity(&self) -> ManifestIdentity {
        self.identity
    }

    pub(crate) const fn checkpoint(&self) -> Checkpoint {
        self.checkpoint
    }

    pub(crate) const fn retention(&self) -> RetentionState {
        self.retention
    }

    pub(crate) fn tables(&self) -> &[TableCatalog] {
        &self.tables
    }

    pub(crate) fn units(&self) -> &[UnitMeta] {
        &self.units
    }

    pub(crate) fn replay_target(&self) -> Result<TailIndex> {
        let tables = self
            .tables
            .iter()
            .map(|table| {
                RecoveredTable::new(
                    table.table(),
                    table.last_ts(),
                    table.versions().to_vec(),
                    table
                        .retired_series()
                        .iter()
                        .map(|retirement| (retirement.series(), retirement.retire_ts()))
                        .collect(),
                    table
                        .retired_fields()
                        .iter()
                        .map(|retirement| (retirement.field(), retirement.retire_ts()))
                        .collect(),
                )
            })
            .collect();
        TailIndex::restore(
            self.identity.table_high_water(),
            self.checkpoint.next_seq(),
            tables,
        )
    }
}

#[cfg(test)]
#[path = "catalog_tests.rs"]
mod tests;
