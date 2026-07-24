// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "consumed by the retention publication runner later in M6"
)]

use std::collections::BTreeMap;

use crate::{
    Error, Result, StreamKey, TableVersion,
    manifest::UnitMeta,
    query::VersionContract,
    retention::{RetentionHead, RetentionHeads},
    unit::{DecodedSection, UnitSource, decode_section},
    wal::{TailIndex, TailTable},
};

pub(crate) fn fold(
    prior: Option<&RetentionHeads>,
    expired: &[UnitMeta],
    source: &dyn UnitSource,
    schemas: &TailIndex,
    floor: i64,
) -> Result<RetentionHeads> {
    if expired.is_empty()
        || prior.is_some_and(|heads| heads.floor() >= floor)
        || expired.iter().any(|unit| unit.max_ts() >= floor)
        || expired.windows(2).any(|pair| {
            (pair[0].min_ts(), pair[0].unit_id()) >= (pair[1].min_ts(), pair[1].unit_id())
        })
    {
        return Err(Error::invalid(
            "retention",
            "expired units and floor do not form an advancing ordered prefix",
        ));
    }
    let mut latest = BTreeMap::new();
    if let Some(heads) = prior {
        for head in heads.entries() {
            latest.insert(
                StreamKey::new(head.table(), head.series(), head.field()),
                *head,
            );
        }
    }
    let mut seen_sections = vec![0_u32; expired.len()];
    let mut seen_rows = vec![0_u64; expired.len()];
    for (table, state) in schemas.tables() {
        let mut previous_max = None;
        for (unit_index, unit) in expired.iter().copied().enumerate() {
            for entry in source.table_sections(unit, table)? {
                if entry.table() != table
                    || entry.min_ts() < unit.min_ts()
                    || entry.max_ts() > unit.max_ts()
                    || previous_max.is_some_and(|maximum| maximum >= entry.min_ts())
                {
                    return Err(Error::corruption(
                        "retention fold",
                        "source section contradicts unit or table ordering",
                    ));
                }
                let version_index = state
                    .versions()
                    .binary_search_by_key(&entry.version_no(), TableVersion::version_no)
                    .map_err(|_| Error::corruption("retention fold", "version is absent"))?;
                seen_sections[unit_index] = seen_sections[unit_index]
                    .checked_add(1)
                    .ok_or_else(|| Error::corruption("retention fold", "section count overflow"))?;
                seen_rows[unit_index] = seen_rows[unit_index]
                    .checked_add(u64::from(entry.row_count()))
                    .ok_or_else(|| Error::corruption("retention fold", "row count overflow"))?;
                let bytes = source.section_bytes(unit, entry)?;
                let decoded = decode_section(&bytes, entry, &state.versions()[version_index])?;
                apply_section(table, &decoded, &mut latest)?;
                previous_max = Some(entry.max_ts());
            }
        }
    }
    for (index, unit) in expired.iter().enumerate() {
        if seen_sections[index] != unit.section_count() || seen_rows[index] != unit.total_rows() {
            return Err(Error::corruption(
                "retention fold",
                "source directory totals disagree with MANIFEST",
            ));
        }
    }
    let mut entries = Vec::with_capacity(latest.len());
    for (key, head) in latest {
        let Some(table) = schemas.table(key.table()) else {
            continue;
        };
        if is_live(table, key, head, floor)? {
            entries.push(head);
        }
    }
    RetentionHeads::new(floor, entries)
}

fn apply_section(
    table: crate::TableId,
    decoded: &DecodedSection,
    latest: &mut BTreeMap<StreamKey, RetentionHead>,
) -> Result<()> {
    for (row, timestamp) in decoded.timestamps().iter().enumerate() {
        for column in decoded.columns() {
            if let Some(value) = column.cells().get(row).copied().flatten() {
                let key = StreamKey::new(table, column.series(), column.field());
                if latest
                    .get(&key)
                    .is_some_and(|head| head.fact_ts() >= *timestamp)
                {
                    return Err(Error::corruption(
                        "retention fold",
                        "Facts are duplicate or out of order",
                    ));
                }
                latest.insert(
                    key,
                    RetentionHead::new(table, column.series(), column.field(), *timestamp, value),
                );
            }
        }
    }
    Ok(())
}

fn is_live(table: &TailTable, key: StreamKey, head: RetentionHead, floor: i64) -> Result<bool> {
    let contract = VersionContract::resolve(table.versions(), key.field(), head.fact_ts())?;
    if !head.value().matches(contract.interpretation()) {
        return Err(Error::corruption(
            "retention fold",
            "head payload disagrees with historical schema",
        ));
    }
    let retired = table
        .retired_series_at(key.series())
        .into_iter()
        .chain(table.retired_field_at(key.field()))
        .max();
    Ok(
        !retired.is_some_and(|cutoff| floor > cutoff && head.fact_ts() <= cutoff)
            && contract.is_live(head.fact_ts(), floor),
    )
}

#[cfg(test)]
#[path = "fold_tests.rs"]
mod tests;
