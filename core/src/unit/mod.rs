// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

mod compact;
mod decode;
mod dircache;
mod format;
mod seal;
mod section;
mod section_decode;
mod source;
mod spool;

pub(crate) use compact::assemble_and_publish as compact_and_publish;
pub(crate) use dircache::DirectoryCache;
pub(crate) use format::TableDirectoryEntry;
pub(crate) use seal::assemble_and_publish as seal_and_publish;
pub(crate) use section_decode::{
    DecodedSection, decode as decode_section, decode_stream, decode_streams,
};
pub(crate) use source::{FileUnitSource, UnitSource};
pub(crate) use spool::UnitSpool;

#[cfg(test)]
pub(crate) use compact::assemble as compact_units;
#[cfg(test)]
pub(crate) use format::decode as decode_layout;
#[cfg(test)]
pub(crate) use seal::{SealedUnit, assemble as seal_snapshot, publish as publish_unit};
