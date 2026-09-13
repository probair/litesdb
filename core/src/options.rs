// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::time::Duration;

use crate::{
    Error, Result,
    limits::{
        DEFAULT_DIRECTORY_CACHE_BYTES, DEFAULT_L1_WINDOW_SECS, DEFAULT_L2_WINDOW_SECS,
        DEFAULT_SEAL_BYTES, DEFAULT_SEAL_INTERVAL_SECS, DEFAULT_SEAL_MEMORY_BYTES, Limit,
        MAX_SEAL_MEMORY_BYTES, MIN_COMPACTION_WINDOW_SECS, ensure_at_most,
    },
};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SyncPolicy {
    Manual,
    Interval { every: Duration, bytes: u32 },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SealPolicy {
    pub bytes: u32,
    pub interval: Duration,
    pub memory_bytes: u32,
}

impl Default for SealPolicy {
    fn default() -> Self {
        Self {
            bytes: DEFAULT_SEAL_BYTES,
            interval: Duration::from_secs(u64::from(DEFAULT_SEAL_INTERVAL_SECS)),
            memory_bytes: DEFAULT_SEAL_MEMORY_BYTES,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct CompactionPolicy {
    pub l1_window: Duration,
    pub l2_window: Duration,
}

impl Default for CompactionPolicy {
    fn default() -> Self {
        Self {
            l1_window: Duration::from_secs(DEFAULT_L1_WINDOW_SECS),
            l2_window: Duration::from_secs(DEFAULT_L2_WINDOW_SECS),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct OpenOptions {
    pub sync_policy: SyncPolicy,
    pub seal_policy: SealPolicy,
    pub compaction: CompactionPolicy,
    pub directory_cache_bytes: u32,
    pub takeover: bool,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            sync_policy: SyncPolicy::Interval {
                every: Duration::from_secs(1),
                bytes: 262_144,
            },
            seal_policy: SealPolicy::default(),
            compaction: CompactionPolicy::default(),
            directory_cache_bytes: DEFAULT_DIRECTORY_CACHE_BYTES,
            takeover: false,
        }
    }
}

pub(crate) fn validate(options: OpenOptions) -> Result<()> {
    ensure_at_most(
        Limit::DirectoryCacheBytes,
        u64::from(options.directory_cache_bytes),
    )?;
    if let SyncPolicy::Interval { every, bytes } = options.sync_policy
        && (every.is_zero() || bytes == 0)
    {
        return Err(Error::invalid(
            "sync policy",
            "interval and byte threshold must be positive",
        ));
    }
    if options.seal_policy.bytes == 0
        || options.seal_policy.interval.is_zero()
        || options.seal_policy.memory_bytes == 0
        || options.seal_policy.memory_bytes > MAX_SEAL_MEMORY_BYTES
    {
        return Err(Error::invalid(
            "seal policy",
            "interval or memory threshold is invalid",
        ));
    }
    validate_compaction(options.compaction)?;
    Ok(())
}

fn validate_compaction(policy: CompactionPolicy) -> Result<()> {
    let l1 = policy.l1_window;
    let l2 = policy.l2_window;
    let whole_seconds = l1.subsec_nanos() == 0 && l2.subsec_nanos() == 0;
    let fit_timestamps = i64::try_from(l1.as_secs()).is_ok() && i64::try_from(l2.as_secs()).is_ok();
    if !whole_seconds || !fit_timestamps || l1.as_secs() < MIN_COMPACTION_WINDOW_SECS || l1 > l2 {
        return Err(Error::invalid(
            "compaction policy",
            "windows must be whole seconds with 1s <= l1_window <= l2_window",
        ));
    }
    Ok(())
}
