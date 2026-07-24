// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::time::Duration;

use crate::{
    CompactLevel, CompactReport, Db, DurablePosition, Error, Result, SealReport, manifest::UnitMeta,
};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceReport {
    synced: Option<DurablePosition>,
    sealed: Option<SealReport>,
    compacted_l1: Option<CompactReport>,
    compacted_l2: Option<CompactReport>,
    more_due: bool,
}

impl MaintenanceReport {
    #[must_use]
    pub const fn synced(self) -> Option<DurablePosition> {
        self.synced
    }

    #[must_use]
    pub const fn sealed(self) -> Option<SealReport> {
        self.sealed
    }

    #[must_use]
    pub const fn compacted_l1(self) -> Option<CompactReport> {
        self.compacted_l1
    }

    #[must_use]
    pub const fn compacted_l2(self) -> Option<CompactReport> {
        self.compacted_l2
    }

    #[must_use]
    pub const fn more_due(self) -> bool {
        self.more_due
    }
}

impl Db {
    pub fn maintain(&self) -> Result<MaintenanceReport> {
        let status = self.maintenance_status()?;
        let synced = if status.sync_due() {
            Some(self.sync()?)
        } else {
            None
        };
        let sealed = if status.seal_due() {
            Some(self.seal()?)
        } else {
            None
        };
        let compacted_l1 =
            self.compact_closed_window(CompactLevel::Level0To1, self.options.compaction.l1_window)?;
        let compacted_l2 =
            self.compact_closed_window(CompactLevel::Level1To2, self.options.compaction.l2_window)?;
        Ok(MaintenanceReport {
            synced,
            sealed,
            compacted_l1,
            compacted_l2,
            more_due: self.closed_window_due()?,
        })
    }

    fn compact_closed_window(
        &self,
        level: CompactLevel,
        width: Duration,
    ) -> Result<Option<CompactReport>> {
        let mut engine = self.lock_engine()?;
        engine.writer.ensure_healthy()?;
        let high_water = high_water_ts(engine.manifest.units(), &engine.tail);
        let Some((start, end)) =
            select_closed_window(engine.manifest.units(), level.source(), width, high_water)?
        else {
            return Ok(None);
        };
        self.compact_locked(&mut engine, start, end).map(Some)
    }

    fn closed_window_due(&self) -> Result<bool> {
        let engine = self.lock_engine()?;
        let high_water = high_water_ts(engine.manifest.units(), &engine.tail);
        Ok(select_closed_window(
            engine.manifest.units(),
            CompactLevel::Level0To1.source(),
            self.options.compaction.l1_window,
            high_water,
        )?
        .is_some()
            || select_closed_window(
                engine.manifest.units(),
                CompactLevel::Level1To2.source(),
                self.options.compaction.l2_window,
                high_water,
            )?
            .is_some())
    }
}

fn high_water_ts(units: &[UnitMeta], tail: &crate::wal::TailIndex) -> Option<i64> {
    units
        .iter()
        .map(|unit| unit.max_ts())
        .chain(tail.tables().filter_map(|(_, table)| table.last_ts()))
        .max()
}

fn select_closed_window(
    units: &[UnitMeta],
    level: u8,
    width: Duration,
    high_water: Option<i64>,
) -> Result<Option<(usize, usize)>> {
    let Some(high_water) = high_water else {
        return Ok(None);
    };
    let width = i128::from(width.as_secs());
    if width == 0 {
        return Err(Error::invalid(
            "compaction window",
            "window must be positive",
        ));
    }
    let Some(start) = units.iter().position(|unit| unit.level() == level) else {
        return Ok(None);
    };
    let first_ts = i128::from(units[start].min_ts());
    let window_start = first_ts
        .checked_sub(first_ts.rem_euclid(width))
        .ok_or_else(|| Error::corruption("compaction window", "window start overflow"))?;
    let window_end = window_start
        .checked_add(width)
        .ok_or_else(|| Error::corruption("compaction window", "window end overflow"))?;
    if i128::from(high_water) < window_end {
        return Ok(None);
    }
    let mut end = start.saturating_add(1);
    while end < units.len()
        && units[end].level() == level
        && i128::from(units[end].min_ts()) < window_end
    {
        end = end.saturating_add(1);
    }
    Ok(Some((start, end)))
}

#[cfg(test)]
#[path = "scheduler_tests.rs"]
mod tests;
