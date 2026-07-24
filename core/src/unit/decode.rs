// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "called by queries and database open later in M5/M6"
)]

use crate::{
    Error, Result, TableVersion,
    unit::{format, section_decode::DecodedSection},
    wal::TailIndex,
};

pub(crate) fn scan<F>(
    bytes: &[u8],
    name_unit_id: u64,
    schemas: &TailIndex,
    mut visit: F,
) -> Result<()>
where
    F: FnMut(format::TableDirectoryEntry, &DecodedSection) -> Result<()>,
{
    let layout = format::decode(bytes, name_unit_id)?;
    for entry in layout.sections() {
        let table = schemas
            .table(entry.table())
            .ok_or_else(|| Error::corruption("unit scan", "table is absent from catalog"))?;
        let version_index = table
            .versions()
            .binary_search_by_key(&entry.version_no(), TableVersion::version_no)
            .map_err(|_| Error::corruption("unit scan", "table version is absent from catalog"))?;
        let start = usize::try_from(entry.section_offset())
            .map_err(|_| Error::corruption("unit scan", "section offset does not fit usize"))?;
        let length = usize::try_from(entry.section_len())
            .map_err(|_| Error::corruption("unit scan", "section length does not fit usize"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| Error::corruption("unit scan", "section end overflow"))?;
        let section_bytes = bytes
            .get(start..end)
            .ok_or_else(|| Error::corruption("unit scan", "section extent is invalid"))?;
        let section =
            super::section_decode::decode(section_bytes, *entry, &table.versions()[version_index])?;
        visit(*entry, &section)?;
    }
    Ok(())
}
