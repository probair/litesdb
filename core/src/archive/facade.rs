// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveCursor, ArchiveOptions, ArchiveStatus, ArchiveStore, ExportChunk};
use crate::{Db, OpenOptions, Result, fsutil::DbDir};
use std::{path::Path, sync::Arc};
impl Db {
    pub fn open_with_archive(
        root: &Path,
        options: OpenOptions,
        archive: ArchiveOptions,
    ) -> Result<Self> {
        Self::open_configured(root, options, Some(archive), false).map(|(db, _)| db)
    }
    pub fn archive_status(&self) -> Result<ArchiveStatus> {
        self.lock_engine()?.writer.archive_status()
    }
    pub fn export_durable(&self, after: ArchiveCursor, max_bytes: u32) -> Result<ExportChunk> {
        let mut engine = self.lock_engine()?;
        let result = engine.writer.export_durable(after, max_bytes);
        if result.as_ref().is_err_and(|error| {
            matches!(
                error.kind(),
                crate::ErrorKind::Io | crate::ErrorKind::Corruption
            )
        }) {
            engine.writer.mark_poisoned();
        }
        result
    }
    pub fn release_archive(&self, through: ArchiveCursor) -> Result<()> {
        self.lock_engine()?.writer.release_archive(through)
    }
}
pub(crate) fn preflight(
    directory: &Arc<DbDir>,
    options: Option<ArchiveOptions>,
) -> Result<Option<ArchiveStore>> {
    let Some(options) = options else {
        return Ok(None);
    };
    let store = ArchiveStore::load(Arc::clone(directory), options)?;
    if let Some(store) = &store {
        let manifest = crate::manifest::load(directory)?;
        store.ensure_protected(manifest.checkpoint().next_seq().saturating_sub(1))?;
    }
    Ok(store)
}
