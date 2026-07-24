// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{cell::Cell, collections::BTreeMap};

use super::range;
use crate::{ErrorKind, TableId, manifest::UnitMeta, unit::TableDirectoryEntry};

fn unit(id: u64, min_ts: i64, max_ts: i64) -> UnitMeta {
    UnitMeta::new(id, 0, min_ts, max_ts, 1, 1, 1, 0)
        .unwrap_or_else(|_| unreachable!("valid unit metadata rejected"))
}

fn section(table: u32, min_ts: i64, max_ts: i64) -> TableDirectoryEntry {
    TableDirectoryEntry::new(TableId::new(table), 1, min_ts, max_ts, 1, 100, 1)
        .unwrap_or_else(|_| unreachable!("valid section rejected"))
}

fn loader(
    directories: &BTreeMap<u64, Vec<TableDirectoryEntry>>,
) -> impl FnMut(UnitMeta, TableId) -> crate::Result<Vec<TableDirectoryEntry>> + '_ {
    move |unit, table| {
        Ok(directories
            .get(&unit.unit_id())
            .map(|entries| {
                entries
                    .iter()
                    .copied()
                    .filter(|entry| entry.table() == table)
                    .collect()
            })
            .unwrap_or_default())
    }
}

#[test]
fn range_path_filters_units_then_table_sections() {
    let units = [unit(9, 0, 19), unit(2, 20, 39), unit(5, 40, 59)];
    let directories = BTreeMap::from([
        (9, vec![section(1, 0, 9), section(1, 10, 19)]),
        (2, vec![section(1, 20, 39), section(2, 20, 39)]),
        (5, vec![section(1, 40, 59)]),
    ]);
    let planned = range(&units, TableId::new(1), 15, 45, loader(&directories))
        .unwrap_or_else(|_| unreachable!("valid range plan failed"));
    assert_eq!(
        planned
            .iter()
            .map(|entry| (entry.unit().unit_id(), entry.section().min_ts()))
            .collect::<Vec<_>>(),
        [(9, 10), (2, 20), (5, 40)]
    );
}

#[test]
fn range_arguments_are_checked_before_loading() {
    let units = [unit(1, 0, 9)];
    let calls = Cell::new(0_u32);
    let empty = range(&units, TableId::new(1), 5, 5, |_, _| {
        calls.set(calls.get().saturating_add(1));
        Ok(vec![])
    })
    .unwrap_or_else(|_| unreachable!());
    assert!(empty.is_empty());
    assert_eq!(calls.get(), 0);
    assert_eq!(
        range(&units, TableId::new(1), 6, 5, |_, _| Ok(vec![]))
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
}

#[test]
fn contradictory_routing_metadata_is_rejected() {
    let unordered = [unit(2, 20, 29), unit(1, 0, 9)];
    let duplicate = [unit(1, 0, 9), unit(1, 20, 29)];
    for units in [&unordered[..], &duplicate] {
        assert_eq!(
            range(units, TableId::new(1), 0, 30, |_, _| Ok(vec![]))
                .err()
                .map(|error| error.kind()),
            Some(ErrorKind::Corruption)
        );
    }

    let units = [unit(1, 0, 20), unit(2, 15, 30)];
    let directories = BTreeMap::from([(1, vec![section(1, 0, 20)]), (2, vec![section(1, 15, 30)])]);
    assert_eq!(
        range(&units, TableId::new(1), 0, 31, loader(&directories))
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );

    let wrong_range = BTreeMap::from([(1, vec![section(1, -1, 9)])]);
    assert_eq!(
        range(
            &[unit(1, 0, 9)],
            TableId::new(1),
            0,
            10,
            loader(&wrong_range),
        )
        .err()
        .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}
