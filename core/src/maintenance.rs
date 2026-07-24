// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, sync::Arc};

use crate::{
    Db, Error, Result,
    fsutil::Area,
    lifecycle_gc,
    manifest::{self, RetentionState, UnitMeta},
    retention::{fold, head_name, publish_heads},
    unit::{FileUnitSource, compact_and_publish, seal_and_publish},
    wal::Checkpoint,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum CompactLevel {
    Level0To1,
    Level1To2,
}

impl CompactLevel {
    pub(crate) const fn source(self) -> u8 {
        match self {
            Self::Level0To1 => 0,
            Self::Level1To2 => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SealReport {
    unit_id: Option<u64>,
    checkpointed_records: u64,
}

impl SealReport {
    #[must_use]
    pub const fn unit_id(self) -> Option<u64> {
        self.unit_id
    }

    #[must_use]
    pub const fn checkpointed_records(self) -> u64 {
        self.checkpointed_records
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct CompactReport {
    input_units: u32,
    output_unit: Option<u64>,
}

impl CompactReport {
    #[must_use]
    pub const fn input_units(self) -> u32 {
        self.input_units
    }

    #[must_use]
    pub const fn output_unit(self) -> Option<u64> {
        self.output_unit
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RetentionReport {
    removed_units: u32,
    floor: Option<i64>,
}

impl RetentionReport {
    #[must_use]
    pub const fn removed_units(self) -> u32 {
        self.removed_units
    }

    #[must_use]
    pub const fn floor(self) -> Option<i64> {
        self.floor
    }
}

impl Db {
    pub fn seal(&self) -> Result<SealReport> {
        let mut engine = self.lock_engine()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let start_seq = engine.manifest.checkpoint().next_seq();
        let end_seq = engine.tail.next_seq();
        if start_seq == end_seq {
            return Ok(SealReport::default());
        }
        let durable = if engine.writer.unsynced_bytes() == 0 {
            engine.writer.durable_position()
        } else {
            engine.writer.sync()?
        };
        engine.last_sync = std::time::Instant::now();
        let checkpoint = Checkpoint::new(durable.segment(), durable.offset(), end_seq)?;
        let has_rows = engine
            .tail
            .tables()
            .any(|(_, table)| !table.rows().is_empty());
        let mut units = engine.manifest.units().to_vec();
        let mut high_water = engine.manifest.identity().unit_high_water();
        let unit_id = if has_rows {
            high_water = high_water
                .checked_add(1)
                .ok_or_else(|| Error::limit("unit_id", u64::MAX, u64::MAX))?;
            remove_orphan(self, Area::Units, &unit_file_name(high_water))?;
            let meta = seal_and_publish(&self.directory, &engine.tail, high_water)?;
            units.push(meta);
            units.sort_unstable_by_key(|unit| (unit.min_ts(), unit.unit_id()));
            Some(high_water)
        } else {
            None
        };
        let next =
            engine
                .manifest
                .successor_from_tail(checkpoint, &engine.tail, units, high_water)?;
        let source = Arc::new(FileUnitSource::open(
            &self.directory.path(Area::Units),
            next.units(),
        )?);
        manifest::publish(
            &self.directory,
            Some(engine.manifest.identity().generation()),
            &next,
        )?;
        let tail = Arc::new(next.replay_target()?);
        engine.manifest = next;
        engine.tail = tail;
        engine.source = source;
        engine.maintenance_due = false;
        engine.last_seal = std::time::Instant::now();
        self.refresh_visible(&engine);
        engine.writer.checkpoint()?;
        Ok(SealReport {
            unit_id,
            checkpointed_records: end_seq.saturating_sub(start_seq),
        })
    }

    pub fn compact(&self, level: CompactLevel) -> Result<CompactReport> {
        let mut engine = self.lock_engine()?;
        engine.writer.ensure_healthy()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let Some((start, end)) = select_run(engine.manifest.units(), level.source()) else {
            return Ok(CompactReport::default());
        };
        self.compact_locked(&mut engine, start, end)
    }

    pub(crate) fn compact_locked(
        &self,
        engine: &mut crate::db::Engine,
        start: usize,
        end: usize,
    ) -> Result<CompactReport> {
        if start >= end || end > engine.manifest.units().len() {
            return Err(Error::corruption(
                "compaction",
                "selected unit range is invalid",
            ));
        }
        let inputs = &engine.manifest.units()[start..end];
        let unit_id = engine
            .manifest
            .identity()
            .unit_high_water()
            .checked_add(1)
            .ok_or_else(|| Error::limit("unit_id", u64::MAX, u64::MAX))?;
        remove_orphan(self, Area::Units, &unit_file_name(unit_id))?;
        let compacted = compact_and_publish(
            &self.directory,
            inputs,
            engine.source.as_ref(),
            &engine.tail,
            unit_id,
        )?;
        let replaced: std::collections::BTreeSet<u64> =
            inputs.iter().map(|unit| unit.unit_id()).collect();
        let obsolete = inputs
            .iter()
            .map(|unit| (Area::Units, unit_file_name(unit.unit_id())))
            .collect::<Vec<_>>();
        let mut units: Vec<UnitMeta> = engine
            .manifest
            .units()
            .iter()
            .copied()
            .filter(|unit| !replaced.contains(&unit.unit_id()))
            .collect();
        units.push(compacted);
        units.sort_unstable_by_key(|unit| (unit.min_ts(), unit.unit_id()));
        let next =
            engine
                .manifest
                .successor_catalog(engine.manifest.retention(), units, unit_id)?;
        let source = Arc::new(FileUnitSource::open(
            &self.directory.path(Area::Units),
            next.units(),
        )?);
        manifest::publish(
            &self.directory,
            Some(engine.manifest.identity().generation()),
            &next,
        )?;
        engine.retire(obsolete);
        engine.manifest = next;
        engine.source = source;
        self.refresh_visible(engine);
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        Ok(CompactReport {
            input_units: u32::try_from(end.saturating_sub(start)).unwrap_or(u32::MAX),
            output_unit: Some(unit_id),
        })
    }

    pub fn retain(&self, cutoff: i64) -> Result<RetentionReport> {
        let mut engine = self.lock_engine()?;
        engine.writer.ensure_healthy()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let expired_count = engine
            .manifest
            .units()
            .iter()
            .take_while(|unit| unit.max_ts() < cutoff)
            .count();
        if expired_count == 0 {
            return Ok(RetentionReport {
                removed_units: 0,
                floor: engine.manifest.retention().floor(),
            });
        }
        let expired = &engine.manifest.units()[..expired_count];
        let mut obsolete = expired
            .iter()
            .map(|unit| (Area::Units, unit_file_name(unit.unit_id())))
            .collect::<Vec<_>>();
        if let Some(generation) = engine.manifest.retention().heads_generation() {
            obsolete.push((Area::Heads, head_name(generation)));
        }
        let maximum = expired
            .iter()
            .map(|unit| unit.max_ts())
            .max()
            .ok_or_else(|| Error::corruption("retention", "expired prefix is empty"))?;
        let floor = maximum
            .checked_add(1)
            .ok_or_else(|| Error::limit("retention_floor", u64::MAX, i64::MAX as u64))?;
        if engine
            .manifest
            .retention()
            .floor()
            .is_some_and(|current| floor <= current)
        {
            return Err(Error::invalid("cutoff", "retention floor must advance"));
        }
        let heads = fold(
            engine.heads.as_deref(),
            expired,
            engine.source.as_ref(),
            &engine.tail,
            floor,
        )?;
        let generation = engine
            .manifest
            .identity()
            .generation()
            .checked_add(1)
            .ok_or_else(|| Error::limit("manifest_generation", u64::MAX, u64::MAX))?;
        remove_orphan(self, Area::Heads, &head_name(generation))?;
        publish_heads(&self.directory, generation, &heads)?;
        let remaining = engine.manifest.units()[expired_count..].to_vec();
        let next = engine.manifest.successor_catalog(
            RetentionState::new(Some(floor), Some(generation)),
            remaining,
            engine.manifest.identity().unit_high_water(),
        )?;
        let source = Arc::new(FileUnitSource::open(
            &self.directory.path(Area::Units),
            next.units(),
        )?);
        manifest::publish(
            &self.directory,
            Some(engine.manifest.identity().generation()),
            &next,
        )?;
        engine.retire(obsolete);
        engine.manifest = next;
        engine.source = source;
        engine.heads = Some(Arc::new(heads));
        self.refresh_visible(&engine);
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        Ok(RetentionReport {
            removed_units: u32::try_from(expired_count).unwrap_or(u32::MAX),
            floor: Some(floor),
        })
    }
}

fn select_run(units: &[UnitMeta], level: u8) -> Option<(usize, usize)> {
    let mut start = 0_usize;
    while start < units.len() {
        if units[start].level() != level {
            start = start.saturating_add(1);
            continue;
        }
        let mut end = start.saturating_add(1);
        while end < units.len() && units[end].level() == level {
            end = end.saturating_add(1);
        }
        if end.saturating_sub(start) >= 2 {
            return Some((start, end));
        }
        start = end;
    }
    None
}

fn remove_orphan(db: &Db, area: Area, name: &str) -> Result<()> {
    let path = db.directory.file(area, name);
    if path.try_exists()? {
        fs::remove_file(path)?;
        db.directory.sync(area)?;
    }
    Ok(())
}

fn unit_file_name(unit_id: u64) -> String {
    format!("{unit_id:016x}.lsu")
}
