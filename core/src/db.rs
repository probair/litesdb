// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{
    fs,
    path::Path,
    sync::{Arc, Mutex, MutexGuard},
    time::{Duration, Instant},
};

use crate::{
    Error, FieldId, MaintenanceStatus, Observation, OpenReport, Result, SeriesId, Snapshot,
    TableId, TableSpec, VersionSpec,
    db_open::{finalize_open, initialize_new, open_existing},
    fsutil::{Area, DbDir, DbLock},
    lifecycle_gc::{self, DeferredDelete, Generation},
    manifest::Manifest,
    options::validate,
    retention::RetentionHeads,
    unit::FileUnitSource,
    wal::{DurablePosition, RecordBody, ReplayTarget, TailIndex, WalWriter},
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
    pub(crate) writer: WalWriter,
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
    visible: Mutex<Option<Snapshot>>,
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
        #[cfg(feature = "archive")]
        let archive_requested = archive_options.is_some();
        #[cfg(not(feature = "archive"))]
        let archive_requested = false;
        #[cfg(feature = "archive")]
        if let Some(options) = archive_options {
            options.validate()?;
        }
        #[cfg(not(feature = "archive"))]
        let restoring = false;
        crate::db_open::ensure_open_mode(root, archive_requested, restoring)?;
        let started = Instant::now();
        let config = validate(options)?;
        fs::create_dir_all(root)?;
        let lock = Arc::new(DbLock::acquire(root)?);
        let directory = Arc::new(DbDir::initialize(root)?);
        directory.clear_temporary()?;
        #[cfg(feature = "archive")]
        let loaded_archive = crate::archive::preflight(&directory, archive_options)?;
        let manifest_path = directory.file(Area::Root, "MANIFEST");
        let opened = if manifest_path.try_exists()? {
            open_existing(&directory, config, options.directory_cache_bytes)?
        } else {
            initialize_new(&directory, config, options.directory_cache_bytes)?
        };
        #[cfg(feature = "archive")]
        let opened = {
            let mut opened = opened;
            if let Some(options) = archive_options {
                opened
                    .writer
                    .attach_archive(&directory, options, loaded_archive)?;
            }
            opened
        };
        let tail = Arc::new(opened.tail);
        let source = Arc::new(opened.source);
        let generation = Generation::new(lock);
        let opened_at = Instant::now();
        let snapshot = Snapshot::new(
            Arc::from(opened.catalog.units()),
            Arc::clone(&source),
            Arc::clone(&tail),
            opened.catalog.retention().floor(),
            opened.heads.clone(),
            generation.clone(),
        );
        let database = Self {
            directory,
            engine: Mutex::new(Engine {
                manifest: opened.catalog,
                tail,
                writer: opened.writer,
                source,
                heads: opened.heads,
                generation,
                garbage: Vec::new(),
                maintenance_due: false,
                last_sync: opened_at,
                last_seal: opened_at,
            }),
            visible: Mutex::new(Some(snapshot)),
            options,
        };
        let recovery_seal = finalize_open(&database, options.takeover)?;
        let (wal_bytes, wal_storage_bytes) = {
            let engine = database.lock_engine()?;
            (engine.writer.wal_bytes(), engine.writer.storage_bytes())
        };
        let report = OpenReport {
            elapsed: started.elapsed(),
            replayed_records: opened.replayed_records,
            tail_repairs: opened.tail_repairs,
            repaired_bytes: opened.repaired_bytes,
            recovery_checkpointed_records: recovery_seal.checkpointed_records(),
            recovery_unit_id: recovery_seal.unit_id(),
            wal_bytes,
            wal_storage_bytes,
        };
        Ok((database, report))
    }

    pub fn append(&self, table: TableId, observation: &Observation) -> Result<Seq> {
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
        let mut engine = self.lock_engine()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let durable = engine.writer.sync()?;
        engine.last_sync = Instant::now();
        Ok(durable)
    }

    pub fn maintenance_status(&self) -> Result<MaintenanceStatus> {
        let engine = self.lock_engine()?;
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
        let mut engine = self.lock_engine()?;
        self.mutate_locked(&mut engine, body)
    }

    fn mutate_locked(&self, engine: &mut Engine, body: &RecordBody) -> Result<Seq> {
        engine.writer.ensure_healthy()?;
        lifecycle_gc::reap(&self.directory, &mut engine.garbage);
        let seq = engine.tail.next_seq();
        engine.tail.validate(seq, body).map_err(caller_error)?;
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
        self.invalidate_visible();
        Arc::make_mut(&mut engine.tail)
            .apply(seq, body.clone())
            .map_err(|_| Error::corruption("Db append", "validated tail mutation failed"))?;
        engine.maintenance_due |= outcome.seal_recommended()
            || engine.tail.estimated_bytes() >= u64::from(self.options.seal_policy.memory_bytes);
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
