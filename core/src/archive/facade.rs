// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ArchiveCursor, ArchiveOptions, ArchiveStatus, ExportChunk};
use crate::{Db, OpenOptions, Result};
use std::path::Path;
impl Db {
    pub(crate) fn enable_archive(&self, options: ArchiveOptions) -> Result<()> {
        let mut engine = self.lock_engine()?;
        if engine
            .writer
            .archive_status()
            .is_err_and(|error| error.kind() == crate::ErrorKind::Unsupported)
        {
            self.seal_locked(&mut engine)?;
        }
        engine.writer.enable_archive(options)
    }

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
