// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{fs, sync::Arc};

use crate::{
    Db, Error, Result, SealReport,
    fsutil::{Area, DbDir},
    lifecycle_gc,
    limits::MAX_OPERATION_MEMORY_BYTES,
    manifest::{self, Manifest},
    retention::{RetentionHeads, decode_heads, head_name},
    unit::FileUnitSource,
    wal::{Checkpoint, TailIndex, WalWriter, WriterConfig, recover},
};

pub(crate) struct Opened {
    pub(crate) catalog: Manifest,
    pub(crate) tail: TailIndex,
    pub(crate) writer: WalWriter,
    pub(crate) source: FileUnitSource,
    pub(crate) heads: Option<Arc<RetentionHeads>>,
    pub(crate) replayed_records: u64,
    pub(crate) tail_repairs: u64,
    pub(crate) repaired_bytes: u64,
}

pub(crate) fn open_existing(directory: &DbDir, config: WriterConfig) -> Result<Opened> {
    let catalog = manifest::load(directory)?;
    let source = FileUnitSource::open(&directory.path(Area::Units), catalog.units())?;
    let heads = load_heads(directory, &catalog)?;
    let mut tail = catalog.replay_target()?;
    let identity = catalog.identity();
    let recovery = recover(
        directory,
        catalog.checkpoint(),
        identity.shard_id(),
        identity.writer_epoch(),
        &mut tail,
    )?;
    let writer = WalWriter::resume(
        directory,
        config,
        identity.shard_id(),
        identity.writer_epoch(),
        recovery,
    )?;
    lifecycle_gc::cleanup_unreferenced(directory, &catalog);
    Ok(Opened {
        catalog,
        tail,
        writer,
        source,
        heads,
        replayed_records: recovery.replayed_records(),
        tail_repairs: recovery.tail_repairs(),
        repaired_bytes: recovery.repaired_bytes(),
    })
}

pub(crate) fn initialize_new(directory: &DbDir, config: WriterConfig) -> Result<Opened> {
    clear_unowned_areas(directory)?;
    let mut writer = WalWriter::create(directory, config, 0, 0, 1)?;
    let durable = writer.sync()?;
    let checkpoint = Checkpoint::new(durable.segment(), durable.offset(), 1)?;
    let catalog = Manifest::initial(checkpoint)?;
    manifest::publish(directory, None, &catalog)?;
    let source = FileUnitSource::open(&directory.path(Area::Units), &[])?;
    Ok(Opened {
        catalog,
        tail: TailIndex::new(0, 1),
        writer,
        source,
        heads: None,
        replayed_records: 0,
        tail_repairs: 0,
        repaired_bytes: 0,
    })
}

pub(crate) fn finalize_open(database: &Db, takeover: bool) -> Result<SealReport> {
    let (target_epoch, seal_required) = {
        let mut engine = database.lock_engine()?;
        let current_epoch = engine.manifest.identity().writer_epoch();
        let target_epoch = if takeover {
            current_epoch
                .checked_add(1)
                .ok_or_else(|| Error::limit("writer_epoch", u64::MAX, u64::MAX))?
        } else {
            current_epoch
        };
        let tail_nonempty = engine.tail.next_seq() != engine.manifest.checkpoint().next_seq();
        let seal_required = engine
            .writer
            .recovery_seal_required(target_epoch, tail_nonempty)?;
        (target_epoch, seal_required)
    };

    let recovery_seal = if seal_required {
        database.seal()?
    } else {
        SealReport::default()
    };

    let mut engine = database.lock_engine()?;
    let tail_nonempty = engine.tail.next_seq() != engine.manifest.checkpoint().next_seq();
    if engine
        .writer
        .recovery_seal_required(target_epoch, tail_nonempty)?
    {
        return Err(Error::corruption(
            "WAL takeover",
            "one recovery Seal did not normalize WAL capacity",
        ));
    }
    engine.writer.ensure_epoch_adoptable(target_epoch)?;
    if takeover {
        let next = engine.manifest.successor_writer_epoch()?;
        if next.identity().writer_epoch() != target_epoch {
            return Err(Error::corruption(
                "WAL takeover",
                "MANIFEST successor disagrees with target epoch",
            ));
        }
        if let Err(error) = manifest::publish(
            &database.directory,
            Some(engine.manifest.identity().generation()),
            &next,
        ) {
            engine.writer.mark_poisoned();
            return Err(error);
        }
        engine.manifest = next;
    }
    engine
        .writer
        .adopt_epoch(&database.directory, target_epoch)?;
    Ok(recovery_seal)
}

fn load_heads(directory: &DbDir, catalog: &Manifest) -> Result<Option<Arc<RetentionHeads>>> {
    match (
        catalog.retention().floor(),
        catalog.retention().heads_generation(),
    ) {
        (None, None) => Ok(None),
        (Some(floor), Some(generation)) => {
            let path = directory.file(Area::Heads, &head_name(generation));
            let metadata = fs::metadata(&path)?;
            if metadata.len() > u64::from(MAX_OPERATION_MEMORY_BYTES) {
                return Err(Error::limit(
                    "operation_memory_bytes",
                    metadata.len(),
                    u64::from(MAX_OPERATION_MEMORY_BYTES),
                ));
            }
            let bytes = fs::read(path)?;
            Ok(Some(Arc::new(decode_heads(&bytes, floor)?)))
        }
        _ => Err(Error::corruption(
            "MANIFEST retention",
            "floor and head generation must appear together",
        )),
    }
}

fn clear_unowned_areas(directory: &DbDir) -> Result<()> {
    for entry in fs::read_dir(directory.path(Area::Root))? {
        let name = entry?.file_name();
        if !matches!(
            name.to_str(),
            Some("LOCK" | "wal" | "units" | "heads" | "agg" | "tmp")
        ) {
            return Err(Error::corruption(
                "database root",
                "nonempty root without MANIFEST contains a foreign entry",
            ));
        }
    }
    for area in [Area::Wal, Area::Units, Area::Heads, Area::Aggregates] {
        for entry in fs::read_dir(directory.path(area))? {
            let entry = entry?;
            if entry.file_type()?.is_dir() {
                fs::remove_dir_all(entry.path())?;
            } else {
                fs::remove_file(entry.path())?;
            }
        }
        directory.sync(area)?;
    }
    Ok(())
}
