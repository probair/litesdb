// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveCursor, BaseDescriptor, BaseFile, base::hash_file};
use crate::{
    Db, Error, OpenOptions, OpenReport, Result, SharedDbId, SharedWal,
    fsutil::{Area, DbDir, publish_atomically},
    manifest,
    unit::FileUnitSource,
};
use std::{fs, path::Path};
pub(crate) fn publish(root: &Path, cursor: ArchiveCursor, id: [u8; 16]) -> Result<()> {
    let directory = DbDir::existing(root);
    let catalog = manifest::load(&directory)?;
    if catalog.checkpoint().next_seq().saturating_sub(1) != cursor.seq {
        return Err(super::invalid("sealed cursor and catalog disagree"));
    }
    let mut paths = vec!["MANIFEST".to_owned()];
    for unit in catalog.units() {
        paths.push(format!("units/{:016x}.lsu", unit.unit_id()));
    }
    if let Some(generation) = catalog.retention().heads_generation() {
        paths.push(format!("heads/{}", crate::retention::head_name(generation)));
    }
    paths.sort_unstable();
    let mut files = Vec::new();
    for path in paths {
        let (length, sha256) = hash_file(&root.join(&path))?;
        files.push(BaseFile {
            path,
            length,
            sha256,
        });
    }
    let descriptor = BaseDescriptor { id, cursor, files };
    publish_atomically(&directory, Area::Root, "SEALED", &descriptor.to_bytes())
}
pub(crate) fn verify(root: &Path) -> Result<BaseDescriptor> {
    let descriptor = super::restore_io::read_descriptor(&root.join("SEALED"))?;
    for file in &descriptor.files {
        if hash_file(&root.join(&file.path))? != (file.length, file.sha256) {
            return Err(super::invalid("sealed file digest mismatch"));
        }
    }
    let directory = DbDir::existing(root);
    let catalog = manifest::load(&directory)?;
    if catalog.checkpoint().next_seq().saturating_sub(1) != descriptor.cursor.seq {
        return Err(super::invalid("sealed checkpoint differs from cursor"));
    }
    let mut expected = vec!["MANIFEST".to_owned()];
    expected.extend(
        catalog
            .units()
            .iter()
            .map(|unit| format!("units/{:016x}.lsu", unit.unit_id())),
    );
    if let Some(generation) = catalog.retention().heads_generation() {
        expected.push(format!("heads/{}", crate::retention::head_name(generation)));
    }
    expected.sort_unstable();
    if expected
        .iter()
        .map(String::as_str)
        .ne(descriptor.files.iter().map(|file| file.path.as_str()))
    {
        return Err(super::invalid("sealed descriptor omits or adds files"));
    }
    FileUnitSource::open(&directory.path(Area::Units), catalog.units(), 0)?.validate_all()?;
    Ok(descriptor)
}
pub(crate) fn install(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
) -> Result<(Db, OpenReport)> {
    install_configured(owner, root, id, options, None)
}
fn install_configured(
    owner: SharedWal,
    root: &Path,
    id: SharedDbId,
    options: OpenOptions,
    archive: Option<crate::ArchiveOptions>,
) -> Result<(Db, OpenReport)> {
    if root.join("RESTORE-WORK").try_exists()? {
        return Err(Error::unsupported(
            "install",
            "restore product is not finished",
        ));
    }
    if crate::shared_wal::verify_binding(root, owner.identity(), id)? {
        let opened =
            crate::db_open::open_shared_configured(owner, root, id, options, archive.is_some())?;
        if let Some(options) = archive {
            opened.0.enable_archive(options)?;
        }
        if root.join("SEALED").try_exists()? {
            fs::remove_file(root.join("SEALED"))?;
            opened.0.directory.sync(Area::Root)?;
        }
        return Ok(opened);
    }
    let descriptor = verify(root)?;
    if descriptor.cursor.database != id.database()
        || descriptor.cursor.generation != id.generation()
    {
        return Err(Error::invalid(
            "sealed identity",
            "product belongs to another logical database generation",
        ));
    }
    let opened = crate::db_open::open_imported(owner, root, id, options)?;
    if let Some(options) = archive {
        opened.0.enable_archive(options)?;
    }
    fs::remove_file(root.join("SEALED"))?;
    opened.0.directory.sync(Area::Root)?;
    Ok(opened)
}
impl SharedWal {
    pub fn install_restored_with_archive(
        &self,
        root: &Path,
        id: SharedDbId,
        options: OpenOptions,
        archive: crate::ArchiveOptions,
    ) -> Result<Db> {
        install_configured(self.clone(), root, id, options, Some(archive)).map(|(db, _)| db)
    }

    pub fn install_restored(
        &self,
        root: &Path,
        id: SharedDbId,
        options: OpenOptions,
    ) -> Result<Db> {
        install(self.clone(), root, id, options).map(|(db, _)| db)
    }
}
