// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    Db, Error, OpenOptions, OpenReport, Result, SharedDbId, SharedWal, SharedWalOptions, Snapshot,
    db::Engine,
    fsutil::{Area, DbDir, DbLock},
    lifecycle_gc::{self, Generation},
    manifest::{self, Manifest},
    retention::{RetentionHeads, decode_heads, head_name},
    shared_wal::{self, SharedMember},
    unit::FileUnitSource,
    wal::Checkpoint,
};
use std::{
    fs::{self, File},
    io::Read,
    path::Path,
    sync::{Arc, Mutex},
    time::Instant,
};

pub(crate) fn open_single(
    root: &Path,
    options: OpenOptions,
    restoring: bool,
    archive_requested: bool,
) -> Result<(Db, OpenReport)> {
    crate::options::validate(options)?;
    ensure_open_mode(root, archive_requested, restoring)?;
    validate_new_root(root)?;
    let binding = shared_wal::read_binding(root)?;
    #[cfg(feature = "archive")]
    if binding.is_none() && root.join("SEALED").try_exists()? {
        let descriptor = crate::archive::sealed::verify(root)?;
        let id = SharedDbId::new(descriptor.cursor.database, descriptor.cursor.generation);
        let owner = SharedWal::open(&root.join("_shared"), SharedWalOptions::default())?;
        return crate::archive::sealed::install(owner, root, id, options);
    }
    if root.join("MANIFEST").try_exists()? && binding.is_none() {
        return Err(Error::unsupported(
            "open",
            "old standalone format has no shared binding",
        ));
    }
    let owner = SharedWal::open(&root.join("_shared"), SharedWalOptions::default())?;
    let id = if let Some((_, id)) = binding {
        id
    } else if let Some(id) = owner.registered_id(root)? {
        id
    } else {
        let mut database = [0; 16];
        let mut generation = [0; 16];
        let mut random = File::open("/dev/urandom")?;
        random.read_exact(&mut database)?;
        random.read_exact(&mut generation)?;
        SharedDbId::new(database, generation)
    };
    open_shared_configured(owner, root, id, options, archive_requested)
}
pub(crate) fn open_shared(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
) -> Result<(Db, OpenReport)> {
    open_shared_configured(owner, root, id, options, false)
}

pub(crate) fn open_shared_configured(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
    archive_requested: bool,
) -> Result<(Db, OpenReport)> {
    open_shared_inner(owner, root, id, options, archive_requested, false)
}

#[cfg(feature = "archive")]
pub(crate) fn open_imported(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
) -> Result<(Db, OpenReport)> {
    open_shared_inner(owner, root, id, options, false, true)
}

fn open_shared_inner(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
    archive_requested: bool,
    imported: bool,
) -> Result<(Db, OpenReport)> {
    ensure_open_mode(root, archive_requested, imported)?;
    let started = Instant::now();
    crate::options::validate(options)?;
    owner.validate_open(root, id)?;
    validate_new_root(root)?;
    if root.join("MANIFEST").try_exists()?
        && !imported
        && !shared_wal::verify_binding(root, owner.identity(), id)?
    {
        return Err(Error::unsupported(
            "open",
            "old or unbound MANIFEST is not supported",
        ));
    }
    fs::create_dir_all(root)?;
    let lock = Arc::new(DbLock::acquire(root)?);
    let directory = Arc::new(DbDir::initialize(root)?);
    if imported {
        owner.register_imported(&directory, id)?;
    } else {
        owner.register(&directory, id)?;
    }
    directory.clear_temporary()?;
    let catalog = if root.join("MANIFEST").try_exists()? {
        manifest::load(&directory)?
    } else {
        let catalog = Manifest::initial(Checkpoint::new(1, 32, 1)?)?;
        if let Err(error) = manifest::publish(&directory, None, &catalog) {
            owner.poison();
            return Err(error);
        }
        catalog
    };
    owner.initialized(id, catalog.checkpoint().next_seq().saturating_sub(1))?;
    let source = Arc::new(FileUnitSource::open(
        &directory.path(Area::Units),
        catalog.units(),
        options.directory_cache_bytes,
    )?);
    let heads = load_heads(&directory, &catalog)?;
    let mut tail = catalog.replay_target()?;
    let (writer, replayed) = SharedMember::recover(owner, id, &directory, &mut tail)?;
    lifecycle_gc::cleanup_unreferenced(&directory, &catalog);
    let generation = Generation::new(lock);
    let tail = Arc::new(tail);
    let snapshot = Snapshot::new(
        Arc::from(catalog.units()),
        Arc::clone(&source),
        Arc::clone(&tail),
        catalog.retention().floor(),
        heads.clone(),
        generation.clone(),
    );
    let now = Instant::now();
    let database = Db {
        directory,
        engine: Mutex::new(Engine {
            manifest: catalog,
            tail,
            writer,
            source,
            heads,
            generation,
            garbage: Vec::new(),
            maintenance_due: false,
            last_sync: now,
            last_seal: now,
        }),
        visible: Mutex::new(Some(snapshot)),
        options,
    };
    if options.takeover {
        let mut engine = database.lock_engine()?;
        let next = engine.manifest.successor_writer_epoch()?;
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
    let state = database.maintenance_status()?;
    Ok((
        database,
        OpenReport {
            elapsed: started.elapsed(),
            replayed_records: replayed,
            tail_repairs: 0,
            repaired_bytes: 0,
            recovery_checkpointed_records: 0,
            recovery_unit_id: None,
            wal_bytes: state.wal_bytes(),
            wal_storage_bytes: state.wal_storage_bytes(),
        },
    ))
}
fn load_heads(directory: &DbDir, catalog: &Manifest) -> Result<Option<Arc<RetentionHeads>>> {
    match (
        catalog.retention().floor(),
        catalog.retention().heads_generation(),
    ) {
        (None, None) => Ok(None),
        (Some(floor), Some(generation)) => {
            let path = directory.file(Area::Heads, &head_name(generation));
            let length = fs::metadata(&path)?.len();
            if length > u64::from(crate::limits::MAX_OPERATION_MEMORY_BYTES) {
                return Err(Error::limit(
                    "operation_memory_bytes",
                    length,
                    crate::limits::MAX_OPERATION_MEMORY_BYTES.into(),
                ));
            }
            Ok(Some(Arc::new(decode_heads(&fs::read(path)?, floor)?)))
        }
        _ => Err(Error::corruption(
            "MANIFEST retention",
            "floor and head generation must appear together",
        )),
    }
}
pub(crate) fn ensure_open_mode(
    root: &Path,
    archive_requested: bool,
    restoring: bool,
) -> Result<()> {
    if root.join("EXPORT-WORK").try_exists()? {
        return Err(Error::unsupported(
            "open",
            "database is an unfinished sealed export",
        ));
    }
    if !archive_requested
        && (root.join("ARCHIVE").try_exists()? || root.join("archive").try_exists()?)
    {
        return Err(Error::unsupported(
            "open",
            "database requires archive-enabled open",
        ));
    }
    if !restoring && root.join("RESTORE-WORK").try_exists()? {
        return Err(Error::unsupported(
            "open",
            "database is an unfinished restore generation",
        ));
    }
    Ok(())
}

fn validate_new_root(root: &Path) -> Result<()> {
    if !root.try_exists()?
        || root.join("MANIFEST").try_exists()?
        || root.join("SHARED").try_exists()?
    {
        return Ok(());
    }
    for entry in fs::read_dir(root)? {
        let entry = entry?;
        if !matches!(
            entry.file_name().to_str(),
            Some("LOCK" | "wal" | "units" | "heads" | "agg" | "tmp" | "_shared" | "RESTORE-WORK")
        ) {
            return Err(Error::corruption(
                "database root",
                "unbound root contains foreign artifacts",
            ));
        }
    }
    if root.join("wal").try_exists()? && fs::read_dir(root.join("wal"))?.next().is_some() {
        return Err(Error::unsupported(
            "open",
            "unbound legacy WAL is not supported",
        ));
    }
    Ok(())
}
