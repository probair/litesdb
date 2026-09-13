// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{
    Db, Error, Result, SharedWal, SharedWalOptions,
    fsutil::{Area, DbDir, publish_atomically, sync_directory},
    manifest,
};
use std::{fs, path::Path};
impl Db {
    pub fn export_sealed(&self, export_dir: &Path) -> Result<()> {
        if export_dir.try_exists()? {
            return Err(Error::invalid("export directory", "must be new"));
        }
        let parent = export_dir
            .parent()
            .ok_or_else(|| Error::invalid("export directory", "parent missing"))?
            .canonicalize()?;
        let root = self.directory.path(Area::Root).canonicalize()?;
        if parent.starts_with(&root) {
            return Err(Error::invalid(
                "export directory",
                "must be outside source database",
            ));
        }
        let mut engine = self.lock_engine()?;
        if parent.starts_with(engine.writer.owner.root_path()) {
            return Err(Error::invalid(
                "export directory",
                "must be outside shared owner",
            ));
        }
        self.seal_locked(&mut engine)?;
        engine.source.validate_all()?;
        fs::create_dir(export_dir)?;
        let directory = DbDir::initialize(export_dir)?;
        publish_atomically(&directory, Area::Root, "EXPORT-WORK", b"LSSE\x02\0\0\0")?;
        let owner = SharedWal::open(&export_dir.join("_shared"), SharedWalOptions::default())?;
        owner.register(&directory, engine.writer.id)?;
        for unit in engine.manifest.units() {
            let name = format!("{:016x}.lsu", unit.unit_id());
            fs::hard_link(
                self.directory.file(Area::Units, &name),
                directory.file(Area::Units, &name),
            )?;
        }
        if let Some(generation) = engine.manifest.retention().heads_generation() {
            let name = crate::retention::head_name(generation);
            fs::hard_link(
                self.directory.file(Area::Heads, &name),
                directory.file(Area::Heads, &name),
            )?;
        }
        directory.sync(Area::Units)?;
        directory.sync(Area::Heads)?;
        manifest::publish(
            &directory,
            engine.manifest.identity().generation().checked_sub(1),
            &engine.manifest,
        )?;
        owner.initialized(
            engine.writer.id,
            engine.manifest.checkpoint().next_seq().saturating_sub(1),
        )?;
        publish_atomically(&directory, Area::Root, "EXPORTED", b"LSSE\x02\0\0\0")?;
        fs::remove_file(directory.file(Area::Root, "EXPORT-WORK"))?;
        directory.sync(Area::Root)?;
        sync_directory(&parent)?;
        Ok(())
    }
}
