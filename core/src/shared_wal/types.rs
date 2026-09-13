// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use crate::{Error, Result};

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SharedDbId {
    database: [u8; 16],
    generation: [u8; 16],
}
impl SharedDbId {
    #[must_use]
    pub const fn new(database: [u8; 16], generation: [u8; 16]) -> Self {
        Self {
            database,
            generation,
        }
    }
    #[must_use]
    pub const fn database(self) -> [u8; 16] {
        self.database
    }
    #[must_use]
    pub const fn generation(self) -> [u8; 16] {
        self.generation
    }
    pub(crate) fn bytes(self) -> [u8; 32] {
        let mut result = [0; 32];
        result[..16].copy_from_slice(&self.database);
        result[16..].copy_from_slice(&self.generation);
        result
    }
    pub(crate) fn from_bytes(bytes: &[u8]) -> Result<Self> {
        Ok(Self::new(
            super::format::array(bytes, 0)?,
            super::format::array(bytes, 16)?,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedWalOptions {
    pub max_bytes: u64,
    pub segment_bytes: u32,
    pub buffer_bytes: u32,
    pub max_databases: u32,
    pub index_bytes: u64,
}
impl Default for SharedWalOptions {
    fn default() -> Self {
        Self {
            max_bytes: 1_073_741_824,
            segment_bytes: 67_108_864,
            buffer_bytes: 1_048_576,
            max_databases: 100_000,
            index_bytes: 67_108_864,
        }
    }
}
impl SharedWalOptions {
    pub(crate) fn validate(self) -> Result<()> {
        if self.segment_bytes < 256
            || u64::from(self.segment_bytes).saturating_mul(2) > self.max_bytes
            || self.max_bytes > 1_099_511_627_776
            || self.buffer_bytes < 128
            || self.buffer_bytes > 67_108_864
            || self.max_databases == 0
            || self.max_databases > 1_000_000
            || self.index_bytes < 1024
            || self.index_bytes > 1_073_741_824
        {
            return Err(Error::invalid(
                "shared WAL options",
                "invalid bounded storage policy",
            ));
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SharedDurablePosition {
    pub(crate) lsn: u64,
}
impl SharedDurablePosition {
    #[must_use]
    pub const fn lsn(self) -> u64 {
        self.lsn
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SharedWalStatus {
    pub(crate) storage: u64,
    pub(crate) buffered: u64,
    pub(crate) durable: u64,
    pub(crate) visible: u64,
    pub(crate) writes: u64,
    pub(crate) syncs: u64,
}
impl SharedWalStatus {
    #[must_use]
    pub const fn storage_bytes(self) -> u64 {
        self.storage
    }
    #[must_use]
    pub const fn buffered_bytes(self) -> u64 {
        self.buffered
    }
    #[must_use]
    pub const fn durable_lsn(self) -> u64 {
        self.durable
    }
    #[must_use]
    pub const fn visible_lsn(self) -> u64 {
        self.visible
    }
    #[must_use]
    pub const fn write_calls(self) -> u64 {
        self.writes
    }
    #[must_use]
    pub const fn sync_calls(self) -> u64 {
        self.syncs
    }
}
