// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::{collections::BTreeMap, mem::size_of};

use crate::{
    Error, Result, TableId,
    limits::{Limit, ensure_at_most},
    unit::format::TableDirectoryEntry,
};

const PAGE_OVERHEAD_BYTES: u64 = 128;

struct CachedDirectory {
    entries: Box<[TableDirectoryEntry]>,
    charge: u64,
    last_used: u64,
}

pub(crate) struct DirectoryCache {
    pages: BTreeMap<u64, CachedDirectory>,
    budget: u64,
    used: u64,
    clock: u64,
}

impl DirectoryCache {
    pub(crate) fn new(budget: u32) -> Result<Self> {
        ensure_at_most(Limit::DirectoryCacheBytes, u64::from(budget))?;
        Ok(Self {
            pages: BTreeMap::new(),
            budget: u64::from(budget),
            used: 0,
            clock: 1,
        })
    }

    #[cfg(test)]
    pub(crate) fn with_default_budget() -> Self {
        Self {
            pages: BTreeMap::new(),
            budget: u64::from(crate::limits::DEFAULT_DIRECTORY_CACHE_BYTES),
            used: 0,
            clock: 1,
        }
    }

    pub(crate) const fn used_bytes(&self) -> u64 {
        self.used
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.pages.len()
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.pages.is_empty()
    }

    pub(crate) fn insert(
        &mut self,
        unit_id: u64,
        entries: Vec<TableDirectoryEntry>,
    ) -> Result<bool> {
        validate_entries(&entries)?;
        if let Some(existing) = self.pages.get(&unit_id) {
            if existing.entries.as_ref() != entries.as_slice() {
                return Err(Error::corruption(
                    "directory cache",
                    "immutable unit directory changed",
                ));
            }
            let _ = self.get(unit_id);
            return Ok(true);
        }
        let charge = page_charge(entries.len())?;
        if charge > self.budget {
            return Ok(false);
        }
        while self
            .used
            .checked_add(charge)
            .is_none_or(|total| total > self.budget)
        {
            self.evict_oldest()?;
        }
        let last_used = self.next_tick();
        self.used = self
            .used
            .checked_add(charge)
            .ok_or_else(|| Error::corruption("directory cache", "used-byte overflow"))?;
        self.pages.insert(
            unit_id,
            CachedDirectory {
                entries: entries.into_boxed_slice(),
                charge,
                last_used,
            },
        );
        Ok(true)
    }

    pub(crate) fn get(&mut self, unit_id: u64) -> Option<&[TableDirectoryEntry]> {
        let tick = self.next_tick();
        let page = self.pages.get_mut(&unit_id)?;
        page.last_used = tick;
        Some(&page.entries)
    }

    pub(crate) fn table_sections(
        &mut self,
        unit_id: u64,
        table: TableId,
    ) -> Option<&[TableDirectoryEntry]> {
        let entries = self.get(unit_id)?;
        let first = entries.partition_point(|entry| entry.table() < table);
        let last = entries.partition_point(|entry| entry.table() <= table);
        entries.get(first..last)
    }

    fn evict_oldest(&mut self) -> Result<()> {
        let unit_id = self
            .pages
            .iter()
            .min_by_key(|(unit_id, page)| (page.last_used, **unit_id))
            .map(|(unit_id, _)| *unit_id)
            .ok_or_else(|| Error::corruption("directory cache", "eviction found no page"))?;
        let removed = self
            .pages
            .remove(&unit_id)
            .ok_or_else(|| Error::corruption("directory cache", "eviction page disappeared"))?;
        self.used = self
            .used
            .checked_sub(removed.charge)
            .ok_or_else(|| Error::corruption("directory cache", "used-byte underflow"))?;
        Ok(())
    }

    fn next_tick(&mut self) -> u64 {
        if self.clock == u64::MAX {
            self.compact_ticks();
        }
        let tick = self.clock;
        self.clock = self.clock.saturating_add(1);
        tick
    }

    fn compact_ticks(&mut self) {
        let mut order: Vec<(u64, u64)> = self
            .pages
            .iter()
            .map(|(unit_id, page)| (page.last_used, *unit_id))
            .collect();
        order.sort_unstable();
        for (index, (_, unit_id)) in order.into_iter().enumerate() {
            if let Some(page) = self.pages.get_mut(&unit_id) {
                page.last_used = u64::try_from(index).unwrap_or(u64::MAX);
            }
        }
        self.clock = u64::try_from(self.pages.len())
            .unwrap_or(u64::MAX)
            .saturating_add(1);
    }
}

fn validate_entries(entries: &[TableDirectoryEntry]) -> Result<()> {
    if entries.is_empty()
        || entries
            .windows(2)
            .any(|pair| (pair[0].table(), pair[0].min_ts()) >= (pair[1].table(), pair[1].min_ts()))
    {
        return Err(Error::corruption(
            "directory cache",
            "directory is empty or unordered",
        ));
    }
    ensure_at_most(
        Limit::UnitSections,
        u64::try_from(entries.len()).unwrap_or(u64::MAX),
    )
}

fn page_charge(entry_count: usize) -> Result<u64> {
    let entries = entry_count
        .checked_mul(size_of::<TableDirectoryEntry>())
        .and_then(|bytes| u64::try_from(bytes).ok())
        .ok_or_else(|| {
            Error::limit(
                "directory_cache_bytes",
                u64::MAX,
                Limit::DirectoryCacheBytes.maximum(),
            )
        })?;
    entries.checked_add(PAGE_OVERHEAD_BYTES).ok_or_else(|| {
        Error::limit(
            "directory_cache_bytes",
            u64::MAX,
            Limit::DirectoryCacheBytes.maximum(),
        )
    })
}

#[cfg(test)]
#[path = "dircache_tests.rs"]
mod tests;
