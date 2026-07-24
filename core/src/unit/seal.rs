// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::cmp::min;

use crate::{
    Error, ErrorKind, Result, TableId, TableVersion,
    fsutil::DbDir,
    limits::{Limit, MAX_SECTION_ROWS, SECTION_WORKING_MEMORY_BYTES, ensure_at_most},
    manifest::UnitMeta,
    unit::{UnitSpool, section},
    wal::{TailIndex, TailTable},
};

#[cfg(test)]
use crate::{
    fsutil::{Area, publish_atomically},
    unit::format,
};

#[cfg(test)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct SealedUnit {
    name: String,
    bytes: Vec<u8>,
    meta: UnitMeta,
}

pub(crate) struct EncodedSection {
    pub(crate) table: TableId,
    pub(crate) version_no: u32,
    pub(crate) min_ts: i64,
    pub(crate) max_ts: i64,
    pub(crate) row_count: u32,
    pub(crate) payload: Vec<u8>,
}

impl EncodedSection {
    pub(crate) fn new(
        table: TableId,
        version_no: u32,
        min_ts: i64,
        max_ts: i64,
        row_count: u32,
        payload: Vec<u8>,
    ) -> Result<Self> {
        ensure_at_most(Limit::SectionRows, u64::from(row_count))?;
        if payload.len() > usize::try_from(SECTION_WORKING_MEMORY_BYTES).unwrap_or(usize::MAX) {
            return Err(Error::limit(
                "section_working_memory_bytes",
                u64::try_from(payload.len()).unwrap_or(u64::MAX),
                u64::from(SECTION_WORKING_MEMORY_BYTES),
            ));
        }
        if version_no == 0 || min_ts > max_ts || row_count == 0 || payload.is_empty() {
            return Err(Error::corruption(
                "unit assembly",
                "encoded section metadata is invalid",
            ));
        }
        Ok(Self {
            table,
            version_no,
            min_ts,
            max_ts,
            row_count,
            payload,
        })
    }
}

#[cfg(test)]
impl SealedUnit {
    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(crate) const fn meta(&self) -> UnitMeta {
        self.meta
    }
}

#[derive(Clone, Copy)]
struct SectionSource<'a> {
    table_id: TableId,
    table: &'a TailTable,
    version: &'a TableVersion,
    start: usize,
    end: usize,
}

#[cfg(test)]
pub(crate) fn assemble(snapshot: &TailIndex, unit_id: u64) -> Result<SealedUnit> {
    let sources = collect_sources(snapshot)?;
    let mut sections = Vec::with_capacity(sources.len());
    for source in sources {
        encode_source(source, &mut |section| {
            ensure_at_most(
                Limit::UnitSections,
                u64::try_from(sections.len().saturating_add(1)).unwrap_or(u64::MAX),
            )?;
            sections.push(section);
            Ok(())
        })?;
    }
    assemble_sections(0, unit_id, sections)
}

pub(crate) fn assemble_and_publish(
    directory: &DbDir,
    snapshot: &TailIndex,
    unit_id: u64,
) -> Result<UnitMeta> {
    let sources = collect_sources(snapshot)?;
    let mut spool = UnitSpool::new(directory, 0, unit_id)?;
    for source in sources {
        encode_source(source, &mut |section| spool.push(&section))?;
    }
    spool.publish(directory)
}

fn encode_source<F>(source: SectionSource<'_>, emit: &mut F) -> Result<()>
where
    F: FnMut(EncodedSection) -> Result<()>,
{
    let payload = match section::encode(source.table, source.start, source.end, source.version) {
        Ok(payload) => payload,
        Err(error)
            if error.kind() == ErrorKind::ResourceExhausted
                && source.end.saturating_sub(source.start) > 1 =>
        {
            let midpoint = source
                .start
                .checked_add(source.end.saturating_sub(source.start) / 2)
                .ok_or_else(|| {
                    Error::limit("section_rows", u64::MAX, Limit::SectionRows.maximum())
                })?;
            encode_source(
                SectionSource {
                    end: midpoint,
                    ..source
                },
                emit,
            )?;
            return encode_source(
                SectionSource {
                    start: midpoint,
                    ..source
                },
                emit,
            );
        }
        Err(error) => return Err(error),
    };
    let rows = source
        .table
        .rows()
        .get(source.start..source.end)
        .ok_or_else(|| Error::corruption("Seal", "section row range disappeared"))?;
    let min_ts = rows
        .first()
        .map(super::super::wal::TailRow::timestamp)
        .ok_or_else(|| Error::corruption("Seal", "section is empty"))?;
    let max_ts = rows
        .last()
        .map(super::super::wal::TailRow::timestamp)
        .ok_or_else(|| Error::corruption("Seal", "section is empty"))?;
    let row_count = u32::try_from(rows.len())
        .map_err(|_| Error::limit("section_rows", u64::MAX, Limit::SectionRows.maximum()))?;
    emit(EncodedSection::new(
        source.table_id,
        source.version.version_no(),
        min_ts,
        max_ts,
        row_count,
        payload,
    )?)
}

#[cfg(test)]
pub(crate) fn assemble_sections(
    level: u8,
    unit_id: u64,
    sections: Vec<EncodedSection>,
) -> Result<SealedUnit> {
    let section_count = u32::try_from(sections.len())
        .map_err(|_| Error::limit("unit_sections", u64::MAX, Limit::UnitSections.maximum()))?;
    ensure_at_most(Limit::UnitSections, u64::from(section_count))?;
    if sections.is_empty()
        || sections.windows(2).any(|pair| {
            (pair[0].table, pair[0].min_ts) >= (pair[1].table, pair[1].min_ts)
                || (pair[0].table == pair[1].table && pair[0].max_ts >= pair[1].min_ts)
        })
    {
        return Err(Error::corruption(
            "unit assembly",
            "sections are empty, unordered, or overlapping",
        ));
    }
    let directory_start = format::UNIT_HEADER_BYTES
        .checked_add(
            sections
                .len()
                .checked_mul(format::TABLE_DIRECTORY_ENTRY_BYTES)
                .ok_or_else(|| {
                    Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
                })?,
        )
        .and_then(|offset| u64::try_from(offset).ok())
        .ok_or_else(|| Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum()))?;

    let mut offset = directory_start;
    let mut total_rows = 0_u64;
    let mut global_min = i64::MAX;
    let mut global_max = i64::MIN;
    let mut entries = Vec::with_capacity(sections.len());
    let mut payloads = Vec::with_capacity(sections.len());
    for section in sections {
        let section_len = u32::try_from(section.payload.len()).map_err(|_| {
            Error::limit(
                "section_bytes",
                u64::try_from(section.payload.len()).unwrap_or(u64::MAX),
                u64::from(u32::MAX),
            )
        })?;
        entries.push(format::TableDirectoryEntry::new(
            section.table,
            section.version_no,
            section.min_ts,
            section.max_ts,
            section.row_count,
            offset,
            section_len,
        )?);
        offset = offset.checked_add(u64::from(section_len)).ok_or_else(|| {
            Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum())
        })?;
        total_rows = total_rows
            .checked_add(u64::from(section.row_count))
            .ok_or_else(|| Error::limit("unit_rows", u64::MAX, u64::MAX))?;
        global_min = global_min.min(section.min_ts);
        global_max = global_max.max(section.max_ts);
        payloads.push(section.payload);
    }

    let header = format::UnitHeader::new(
        level,
        unit_id,
        global_min,
        global_max,
        section_count,
        total_rows,
    )?;
    let file_len = offset
        .checked_add(u64::try_from(format::UNIT_FOOTER_BYTES).unwrap_or(u64::MAX))
        .ok_or_else(|| Error::limit("unit_file_bytes", u64::MAX, Limit::UnitFileBytes.maximum()))?;
    ensure_at_most(Limit::OperationMemoryBytes, file_len)?;
    let borrowed: Vec<&[u8]> = payloads.iter().map(Vec::as_slice).collect();
    let bytes = format::encode(header, &entries, &borrowed)?;
    let layout = format::decode(&bytes, unit_id)?;
    let meta = UnitMeta::new(
        unit_id,
        header.level(),
        header.min_ts(),
        header.max_ts(),
        header.section_count(),
        header.total_rows(),
        layout.file_len(),
        layout.body_crc32(),
    )?;
    Ok(SealedUnit {
        name: format::unit_name(unit_id),
        bytes,
        meta,
    })
}

#[cfg(test)]
pub(crate) fn publish(directory: &DbDir, sealed: &SealedUnit) -> Result<()> {
    let target = directory.file(Area::Units, sealed.name());
    if target.try_exists()? {
        return Err(Error::invalid(
            "unit_id",
            "immutable unit file already exists",
        ));
    }
    publish_atomically(directory, Area::Units, sealed.name(), sealed.bytes())
}

fn collect_sources(snapshot: &TailIndex) -> Result<Vec<SectionSource<'_>>> {
    let mut sources = Vec::new();
    for (table_id, table) in snapshot.tables() {
        let rows = table.rows();
        let mut start = 0_usize;
        while start < rows.len() {
            let version_no = rows
                .get(start)
                .map(super::super::wal::TailRow::version_no)
                .ok_or_else(|| Error::corruption("Seal", "row start is absent"))?;
            let version_index = table
                .versions()
                .binary_search_by_key(&version_no, TableVersion::version_no)
                .map_err(|_| Error::corruption("Seal", "row table version is absent"))?;
            let mut run_end = start;
            while rows
                .get(run_end)
                .is_some_and(|row| row.version_no() == version_no)
            {
                run_end = run_end.checked_add(1).ok_or_else(|| {
                    Error::limit("section_rows", u64::MAX, Limit::SectionRows.maximum())
                })?;
            }
            while start < run_end {
                let maximum_end = start
                    .checked_add(usize::try_from(MAX_SECTION_ROWS).unwrap_or(usize::MAX))
                    .unwrap_or(run_end);
                let end = min(maximum_end, run_end);
                sources.push(SectionSource {
                    table_id,
                    table,
                    version: &table.versions()[version_index],
                    start,
                    end,
                });
                ensure_at_most(
                    Limit::UnitSections,
                    u64::try_from(sources.len()).unwrap_or(u64::MAX),
                )?;
                start = end;
            }
        }
    }
    if sources.is_empty() {
        return Err(Error::invalid("Seal", "snapshot contains no rows"));
    }
    Ok(sources)
}

#[cfg(test)]
#[path = "seal_tests.rs"]
mod tests;
