// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{DirectoryCache, page_charge};
use crate::{
    ErrorKind, TableId, limits::MAX_DIRECTORY_CACHE_BYTES, unit::format::TableDirectoryEntry,
};

fn entry(table: u32, timestamp: i64, offset: u64) -> TableDirectoryEntry {
    TableDirectoryEntry::new(TableId::new(table), 1, timestamp, timestamp, 1, offset, 1)
        .unwrap_or_else(|_| unreachable!("valid directory entry rejected"))
}

#[test]
fn exact_budget_uses_true_lru_order() {
    let one = page_charge(1).unwrap_or(u64::MAX);
    let budget = u32::try_from(one * 2).unwrap_or(u32::MAX);
    let mut cache = DirectoryCache::new(budget).unwrap_or_else(|_| unreachable!());
    assert!(cache.insert(1, vec![entry(1, 1, 100)]).unwrap_or(false));
    assert!(cache.insert(2, vec![entry(2, 2, 200)]).unwrap_or(false));
    assert_eq!(cache.used_bytes(), one * 2);
    assert!(cache.get(1).is_some());
    assert!(cache.insert(3, vec![entry(3, 3, 300)]).unwrap_or(false));
    assert!(cache.get(1).is_some());
    assert!(cache.get(2).is_none());
    assert!(cache.get(3).is_some());
    assert_eq!(cache.len(), 2);
}

#[test]
fn oversized_page_is_not_cached() {
    let one = page_charge(1).unwrap_or(u64::MAX);
    let mut cache =
        DirectoryCache::new(u32::try_from(one).unwrap_or(0)).unwrap_or_else(|_| unreachable!());
    assert!(cache.insert(1, vec![entry(1, 1, 100)]).unwrap_or(false));
    assert!(
        !cache
            .insert(2, vec![entry(2, 2, 200), entry(3, 3, 201)])
            .unwrap_or(true)
    );
    assert!(cache.get(1).is_some());
    assert_eq!(cache.len(), 1);
}

#[test]
fn table_lookup_uses_ordered_directory_slice() {
    let mut cache = DirectoryCache::with_default_budget();
    let entries = vec![entry(1, 1, 100), entry(1, 2, 101), entry(2, 1, 102)];
    assert!(cache.insert(7, entries).unwrap_or(false));
    let sections = cache
        .table_sections(7, TableId::new(1))
        .unwrap_or_else(|| unreachable!());
    assert_eq!(sections.len(), 2);
    assert_eq!((sections[0].min_ts(), sections[1].min_ts()), (1, 2));
    assert_eq!(
        cache
            .table_sections(7, TableId::new(9))
            .map(<[TableDirectoryEntry]>::is_empty),
        Some(true)
    );
}

#[test]
fn limits_and_immutable_identity_are_enforced() {
    assert_eq!(
        DirectoryCache::new(MAX_DIRECTORY_CACHE_BYTES + 1)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
    let mut cache = DirectoryCache::with_default_budget();
    assert_eq!(
        cache.insert(1, vec![]).map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
    assert!(cache.insert(1, vec![entry(1, 1, 100)]).unwrap_or(false));
    assert_eq!(
        cache
            .insert(1, vec![entry(1, 2, 100)])
            .map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
    assert!(!cache.is_empty());
}
