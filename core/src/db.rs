// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#[cfg(feature = "bench-metrics")]
use crate::bench_metrics::{self, Counter, Span, Stage};

use std::{
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{
    Error, FieldId, MaintenanceStatus, Observation, OpenReport, Result, SeriesId, Snapshot,
    TableId, TableSpec, VersionSpec,
    fsutil::{Area, DbDir},
    lifecycle_gc::{self, DeferredDelete, Generation},
    manifest::Manifest,
    retention::RetentionHeads,
    shared_wal::SharedMember,
    unit::FileUnitSource,
    wal::{DurablePosition, RecordBody, ReplayTarget, TailIndex},
};

#[cfg(test)]
pub(crate) use crate::options::SealPolicy;
pub(crate) use crate::options::{OpenOptions, SyncPolicy};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Seq(u64);

impl Seq {
    #[must_use]
    pub const fn get(self) -> u64 {
        self.0
    }
}

pub(crate) struct Engine {
    pub(crate) manifest: Manifest,
    pub(crate) tail: Arc<TailIndex>,
    pub(crate) writer: SharedMember,
    pub(crate) source: Arc<FileUnitSource>,
    pub(crate) heads: Option<Arc<RetentionHeads>>,
    pub(crate) generation: Generation,
    pub(crate) garbage: Vec<DeferredDelete>,
    pub(crate) maintenance_due: bool,
    pub(crate) last_sync: Instant,
    pub(crate) last_seal: Instant,
}

impl Engine {
    pub(crate) fn retire(&mut self, artifacts: impl IntoIterator<Item = (Area, String)>) {
        let Self {
            generation,
            garbage,
            ..
        } = self;
        lifecycle_gc::advance(generation, garbage, artifacts);
    }
}

pub struct Db {
    pub(crate) directory: Arc<DbDir>,
    pub(crate) engine: Mutex<Engine>,
    pub(crate) visible: Mutex<Option<Snapshot>>,
    pub(crate) options: OpenOptions,
}

impl Db {
    pub fn open(root: &Path, options: OpenOptions) -> Result<Self> {
        Self::open_with_report(root, options).map(|(database, _)| database)
    }

    pub fn open_with_report(root: &Path, options: OpenOptions) -> Result<(Self, OpenReport)> {
        Self::open_configured(
            root,
            options,
            #[cfg(feature = "archive")]
            None,
            #[cfg(feature = "archive")]
            false,
        )
    }

    pub(crate) fn open_configured(
        root: &Path,
        options: OpenOptions,
        #[cfg(feature = "archive")] archive_options: Option<crate::archive::ArchiveOptions>,
        #[cfg(feature = "archive")] restoring: bool,
    ) -> Result<(Self, OpenReport)> {
        #[cfg(not(feature = "archive"))]
        let restoring = false;
        #[cfg(feature = "archive")]
        let archive_requested = archive_options.is_some();
        #[cfg(not(feature = "archive"))]
        let archive_requested = false;
        let opened = crate::db_open::open_single(root, options, restoring, archive_requested)?;
        #[cfg(feature = "archive")]
        if let Some(options) = archive_options {
            opened.0.enable_archive(options)?;
        }
        Ok(opened)
    }

    pub fn estimated_append_growth(entry_count: usize) -> Result<u64> {
        crate::wal::estimate_row_bytes(entry_count)
    }

    #[must_use]
    pub const fn tail_limit_bytes() -> u64 {
        crate::limits::MAX_TAIL_INDEX_BYTES as u64
    }

    pub fn append(&self, table: TableId, observation: &Observation) -> Result<Seq> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::Append);
        #[cfg(feature = "bench-metrics")]
        {
            bench_metrics::count(Counter::ObservationAttempts, 1);
            bench_metrics::count(
                Counter::ObservationEntries,
                u64::try_from(observation.entries().len()).unwrap_or(u64::MAX),
            );
        }
        self.mutate(&RecordBody::AppendObservation {
            table,
            observation: observation.clone(),
        })
    }

    pub fn create_table(&self, spec: TableSpec) -> Result<TableId> {
        let mut engine = self.lock_engine()?;
        let raw = engine
            .tail
            .table_high_water()
            .checked_add(1)
            .ok_or_else(|| Error::limit("tables", u64::MAX, u64::from(u32::MAX)))?;
        let table = TableId::new(raw);
        self.mutate_locked(&mut engine, &RecordBody::CreateTable { table, spec })?;
        Ok(table)
    }

    pub fn new_table_version(&self, table: TableId, spec: VersionSpec) -> Result<u32> {
        let mut engine = self.lock_engine()?;
        let version = engine
            .tail
            .table(table)
            .and_then(|state| state.versions().last())
            .ok_or_else(|| Error::invalid("table", "table is absent"))?
            .version_no()
            .checked_add(1)
            .ok_or_else(|| Error::limit("table_version_no", u64::MAX, u64::from(u32::MAX)))?;
        self.mutate_locked(&mut engine, &RecordBody::NewTableVersion { table, spec })?;
        Ok(version)
    }

    pub fn drop_table(&self, table: TableId) -> Result<()> {
        self.mutate(&RecordBody::DropTable { table }).map(|_| ())
    }

    pub fn retire_series(&self, table: TableId, series: SeriesId, retire_ts: i64) -> Result<()> {
        self.mutate(&RecordBody::RetireSeries {
            table,
            series,
            retire_ts,
        })
        .map(|_| ())
    }

    pub fn retire_field(&self, table: TableId, field: FieldId, retire_ts: i64) -> Result<()> {
        self.mutate(&RecordBody::RetireField {
            table,
            field,
            retire_ts,
        })
        .map(|_| ())
    }

    pub fn sync(&self) -> Result<DurablePosition> {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::DbSync);
        let mut engine = self.lock_engine()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let durable = engine.writer.sync()?;
        engine.last_sync = Instant::now();
        Ok(durable)
    }

    pub fn maintenance_status(&self) -> Result<MaintenanceStatus> {
        let engine = self.lock_engine()?;
        engine.writer.ensure_healthy()?;
        let now = Instant::now();
        let unsynced_bytes = engine.writer.unsynced_bytes();
        let sync_due_in = match self.options.sync_policy {
            SyncPolicy::Manual => None,
            SyncPolicy::Interval { every, bytes } => (unsynced_bytes > 0).then(|| {
                if unsynced_bytes >= u64::from(bytes) {
                    Duration::ZERO
                } else {
                    every.saturating_sub(now.duration_since(engine.last_sync))
                }
            }),
        };
        let tail_bytes = engine.tail.estimated_bytes();
        let pending_records = engine
            .tail
            .next_seq()
            .saturating_sub(engine.manifest.checkpoint().next_seq());
        let seal_due_in = (pending_records > 0).then(|| {
            if engine.maintenance_due
                || engine.writer.wal_bytes() >= u64::from(self.options.seal_policy.bytes)
                || tail_bytes >= u64::from(self.options.seal_policy.memory_bytes)
            {
                Duration::ZERO
            } else {
                self.options
                    .seal_policy
                    .interval
                    .saturating_sub(now.duration_since(engine.last_seal))
            }
        });
        let mut level_units = [0_u32; 3];
        for unit in engine.manifest.units() {
            if let Some(count) = level_units.get_mut(usize::from(unit.level())) {
                *count = count.saturating_add(1);
            }
        }
        Ok(MaintenanceStatus {
            sync_due: sync_due_in == Some(Duration::ZERO),
            seal_due: seal_due_in == Some(Duration::ZERO),
            sync_due_in,
            seal_due_in,
            unsynced_bytes,
            wal_bytes: engine.writer.wal_bytes(),
            wal_storage_bytes: engine.writer.storage_bytes(),
            directory_cache_bytes: engine.source.directory_cache_bytes()?,
            tail_bytes,
            visible_seq: engine.tail.next_seq().saturating_sub(1),
            durable_seq: engine.writer.durable_position().seq(),
            pending_records,
            level_units,
            retention_floor: engine.manifest.retention().floor(),
        })
    }

    #[must_use]
    pub fn snapshot(&self) -> Snapshot {
        {
            let visible = match self.visible.lock() {
                Ok(visible) => visible,
                Err(poisoned) => poisoned.into_inner(),
            };
            if let Some(snapshot) = visible.as_ref() {
                return snapshot.clone();
            }
        }
        let engine = match self.engine.lock() {
            Ok(engine) => engine,
            Err(poisoned) => poisoned.into_inner(),
        };
        let mut visible = match self.visible.lock() {
            Ok(visible) => visible,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(snapshot) = visible.as_ref() {
            return snapshot.clone();
        }
        let snapshot = Self::snapshot_from_engine(&engine);
        *visible = Some(snapshot.clone());
        snapshot
    }

    #[must_use]
    pub fn retention_floor(&self) -> Option<i64> {
        self.snapshot().retention_floor()
    }

    fn mutate(&self, body: &RecordBody) -> Result<Seq> {
        #[cfg(feature = "bench-metrics")]
        let lock_profile = Span::new(Stage::MutationLock);
        let mut engine = self.lock_engine()?;
        #[cfg(feature = "bench-metrics")]
        drop(lock_profile);
        self.mutate_locked(&mut engine, body)
    }

    fn mutate_locked(&self, engine: &mut Engine, body: &RecordBody) -> Result<Seq> {
        engine.writer.ensure_healthy()?;
        #[cfg(feature = "bench-metrics")]
        let gc_profile = Span::new(Stage::MutationGc);
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        #[cfg(feature = "bench-metrics")]
        drop(gc_profile);
        let seq = engine.tail.next_seq();
        #[cfg(feature = "bench-metrics")]
        let validate_profile = Span::new(Stage::MutationValidate);
        engine.tail.validate(seq, body).map_err(caller_error)?;
        #[cfg(feature = "bench-metrics")]
        drop(validate_profile);
        if engine.writer.append_requires_checkpoint(body)? {
            self.seal_locked(engine)?;
        }
        let outcome = engine.writer.append(body)?;
        if outcome.seq() != seq {
            return Err(Error::corruption(
                "Db append",
                "WAL sequence disagrees with tail",
            ));
        }
        #[cfg(feature = "bench-metrics")]
        let _tail_profile = Span::new(Stage::TailApply);
        self.invalidate_visible();
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(Counter::TailBytesBeforeApply, engine.tail.estimated_bytes());
        Arc::make_mut(&mut engine.tail)
            .apply(seq, body.clone())
            .map_err(|_| Error::corruption("Db append", "validated tail mutation failed"))?;
        engine.maintenance_due |=
            engine.tail.estimated_bytes() >= u64::from(self.options.seal_policy.memory_bytes);
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(Counter::AppliedRecords, 1);
        Ok(Seq(seq))
    }

    pub(crate) fn lock_engine(&self) -> Result<MutexGuard<'_, Engine>> {
        self.engine.lock().map_err(|_| Error::Poisoned)
    }

    pub(crate) fn refresh_visible(&self, engine: &Engine) {
        let snapshot = Self::snapshot_from_engine(engine);
        match self.visible.lock() {
            Ok(mut visible) => *visible = Some(snapshot),
            Err(poisoned) => *poisoned.into_inner() = Some(snapshot),
        }
    }

    fn snapshot_from_engine(engine: &Engine) -> Snapshot {
        #[cfg(feature = "bench-metrics")]
        let _profile = Span::new(Stage::SnapshotBuild);
        #[cfg(feature = "bench-metrics")]
        bench_metrics::count(
            Counter::SnapshotUnits,
            u64::try_from(engine.manifest.units().len()).unwrap_or(u64::MAX),
        );
        Snapshot::new(
            Arc::from(engine.manifest.units()),
            Arc::clone(&engine.source),
            Arc::clone(&engine.tail),
            engine.manifest.retention().floor(),
            engine.heads.clone(),
            engine.generation.clone(),
        )
    }

    fn invalidate_visible(&self) {
        match self.visible.lock() {
            Ok(mut visible) => *visible = None,
            Err(poisoned) => *poisoned.into_inner() = None,
        }
    }
}

fn caller_error(error: Error) -> Error {
    match error {
        Error::Corruption(_) => Error::invalid("mutation", "mutation violates current state"),
        other => other,
    }
}

#[cfg(test)]
#[path = "db_tests.rs"]
mod tests;

#[cfg(test)]
#[path = "lifecycle_fault_tests.rs"]
mod fault_tests;

#[cfg(test)]
#[path = "lifecycle_matrix_tests.rs"]
mod matrix_tests;

#[cfg(test)]
#[path = "bounded_wal_tests.rs"]
mod bounded_wal_tests;

#[cfg(feature = "archive")]
#[path = "db_archive.rs"]
mod archive_support;

#[path = "db_batch.rs"]
mod batch;
