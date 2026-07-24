// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{ops::Range, sync::Arc};

use crate::{
    Bucket, Error, FactCursor, Lookup, Result, Slot, StreamKey, TableId, TableVersion, agg,
    lifecycle_gc::Generation, manifest::UnitMeta, query::primitives, retention::RetentionHeads,
    unit::FileUnitSource, wal::TailIndex,
};

#[derive(Clone)]
pub struct Snapshot {
    state: Arc<SnapshotState>,
}

pub(crate) struct SnapshotState {
    units: Arc<[UnitMeta]>,
    source: Arc<FileUnitSource>,
    tail: Arc<TailIndex>,
    retention_floor: Option<i64>,
    heads: Option<Arc<RetentionHeads>>,
    _generation: Generation,
}

impl Snapshot {
    pub(crate) fn new(
        units: Arc<[UnitMeta]>,
        source: Arc<FileUnitSource>,
        tail: Arc<TailIndex>,
        retention_floor: Option<i64>,
        heads: Option<Arc<RetentionHeads>>,
        generation: Generation,
    ) -> Self {
        Self {
            state: Arc::new(SnapshotState {
                units,
                source,
                tail,
                retention_floor,
                heads,
                _generation: generation,
            }),
        }
    }

    pub fn scan(&self, key: StreamKey, range: Range<i64>) -> Result<FactCursor<'_>> {
        self.ensure_range(&range)?;
        FactCursor::new(
            &self.state.units,
            self.state.source.as_ref(),
            &self.state.tail,
            key,
            range.start,
            range.end,
        )
    }

    pub fn value_at(&self, keys: &[StreamKey], timestamp: i64) -> Result<Vec<Lookup>> {
        self.ensure_point(timestamp)?;
        primitives::value_at_with_heads(
            &self.state.units,
            self.state.source.as_ref(),
            &self.state.tail,
            self.state.heads.as_deref(),
            keys,
            timestamp,
        )
    }

    pub fn latest(&self, keys: &[StreamKey]) -> Result<Vec<Lookup>> {
        primitives::latest_with_heads(
            &self.state.units,
            self.state.source.as_ref(),
            &self.state.tail,
            self.state.heads.as_deref(),
            keys,
        )
    }

    pub fn aggregate(
        &self,
        keys: &[StreamKey],
        range: Range<i64>,
        bucket_width: u32,
    ) -> Result<Vec<Vec<Bucket>>> {
        self.ensure_range(&range)?;
        agg::aggregate(
            &self.state.units,
            self.state.source.as_ref(),
            &self.state.tail,
            keys,
            range.start,
            range.end,
            bucket_width,
            self.state.retention_floor,
        )
    }

    pub fn sample(
        &self,
        keys: &[StreamKey],
        range: Range<i64>,
        step: u32,
    ) -> Result<Vec<Vec<Slot>>> {
        self.ensure_range(&range)?;
        primitives::sample_with_heads(
            &self.state.units,
            self.state.source.as_ref(),
            &self.state.tail,
            self.state.heads.as_deref(),
            keys,
            range.start,
            range.end,
            step,
        )
    }

    pub fn table_versions(&self, table: TableId) -> Result<&[TableVersion]> {
        self.state
            .tail
            .table(table)
            .map(crate::wal::TailTable::versions)
            .ok_or_else(|| Error::invalid("table", "table is absent"))
    }

    #[must_use]
    pub fn retention_floor(&self) -> Option<i64> {
        self.state.retention_floor
    }

    fn ensure_point(&self, timestamp: i64) -> Result<()> {
        if self
            .state
            .retention_floor
            .is_some_and(|floor| timestamp < floor)
        {
            Err(Error::invalid(
                "timestamp",
                "query is below retention floor",
            ))
        } else {
            Ok(())
        }
    }

    fn ensure_range(&self, range: &Range<i64>) -> Result<()> {
        if range.start > range.end {
            return Err(Error::invalid("range", "range is inverted"));
        }
        self.ensure_point(range.start)
    }
}
