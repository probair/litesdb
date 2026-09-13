// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

pub(crate) mod base;
mod base_types;
mod facade;
pub use base_types::{BaseDescriptor, BaseFile, FrozenBase};
mod restore;
mod restore_io;
pub(crate) mod sealed;
pub use restore::{RestoreBuilder, RestoredDbDescriptor};
pub(crate) mod format;
pub(crate) mod types;

pub use types::{ArchiveCursor, ArchiveOptions, ArchiveStatus, ExportChunk};

pub(crate) fn invalid(reason: &'static str) -> crate::Error {
    crate::Error::corruption("archive", reason)
}

#[cfg(test)]
mod suffix_tests;
#[cfg(test)]
mod tests;
