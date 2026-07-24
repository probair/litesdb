// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    Error, Result,
    manifest::catalog::{
        FieldRetirement, Manifest, ManifestIdentity, RetentionState, SeriesRetirement,
        TableCatalog, UnitMeta,
    },
    wal::{Checkpoint, TailIndex},
};

impl Manifest {
    pub(crate) fn initial(checkpoint: Checkpoint) -> Result<Self> {
        Self::restore(
            ManifestIdentity::new(0, 0, 0, 0, 0),
            checkpoint,
            RetentionState::default(),
            Vec::new(),
            Vec::new(),
        )
    }

    pub(crate) fn successor_from_tail(
        &self,
        checkpoint: Checkpoint,
        tail: &TailIndex,
        units: Vec<UnitMeta>,
        unit_high_water: u64,
    ) -> Result<Self> {
        let tables = tail
            .tables()
            .map(|(table, state)| {
                TableCatalog::restore(
                    table,
                    state.last_ts(),
                    state.versions().to_vec(),
                    state
                        .retired_series()
                        .map(|(series, timestamp)| SeriesRetirement::new(series, timestamp))
                        .collect(),
                    state
                        .retired_fields()
                        .map(|(field, timestamp)| FieldRetirement::new(field, timestamp))
                        .collect(),
                )
            })
            .collect::<Result<Vec<_>>>()?;
        Self::restore(
            self.next_identity(unit_high_water, tail.table_high_water())?,
            checkpoint,
            self.retention(),
            tables,
            units,
        )
    }

    pub(crate) fn successor_catalog(
        &self,
        retention: RetentionState,
        units: Vec<UnitMeta>,
        unit_high_water: u64,
    ) -> Result<Self> {
        Self::restore(
            self.next_identity(unit_high_water, self.identity().table_high_water())?,
            self.checkpoint(),
            retention,
            self.tables().to_vec(),
            units,
        )
    }

    pub(crate) fn successor_writer_epoch(&self) -> Result<Self> {
        let current = self.identity();
        let generation = current
            .generation()
            .checked_add(1)
            .ok_or_else(|| Error::limit("manifest_generation", u64::MAX, u64::MAX))?;
        let writer_epoch = current
            .writer_epoch()
            .checked_add(1)
            .ok_or_else(|| Error::limit("writer_epoch", u64::MAX, u64::MAX))?;
        Self::restore(
            ManifestIdentity::new(
                generation,
                current.unit_high_water(),
                current.table_high_water(),
                current.shard_id(),
                writer_epoch,
            ),
            self.checkpoint(),
            self.retention(),
            self.tables().to_vec(),
            self.units().to_vec(),
        )
    }

    fn next_identity(
        &self,
        unit_high_water: u64,
        table_high_water: u32,
    ) -> Result<ManifestIdentity> {
        let current = self.identity();
        let generation = current
            .generation()
            .checked_add(1)
            .ok_or_else(|| Error::limit("manifest_generation", u64::MAX, u64::MAX))?;
        if unit_high_water < current.unit_high_water()
            || table_high_water < current.table_high_water()
        {
            return Err(Error::corruption(
                "MANIFEST",
                "allocation high water regressed",
            ));
        }
        Ok(ManifestIdentity::new(
            generation,
            unit_high_water,
            table_high_water,
            current.shard_id(),
            current.writer_epoch(),
        ))
    }
}
