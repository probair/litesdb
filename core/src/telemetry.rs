// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::time::Duration;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct OpenReport {
    pub(crate) elapsed: Duration,
    pub(crate) replayed_records: u64,
    pub(crate) tail_repairs: u64,
    pub(crate) repaired_bytes: u64,
    pub(crate) recovery_checkpointed_records: u64,
    pub(crate) recovery_unit_id: Option<u64>,
    pub(crate) wal_bytes: u64,
    pub(crate) wal_storage_bytes: u64,
}

impl OpenReport {
    #[must_use]
    pub const fn elapsed(self) -> Duration {
        self.elapsed
    }

    #[must_use]
    pub const fn replayed_records(self) -> u64 {
        self.replayed_records
    }

    #[must_use]
    pub const fn tail_repairs(self) -> u64 {
        self.tail_repairs
    }

    #[must_use]
    pub const fn repaired_bytes(self) -> u64 {
        self.repaired_bytes
    }

    #[must_use]
    pub const fn recovery_checkpointed_records(self) -> u64 {
        self.recovery_checkpointed_records
    }

    #[must_use]
    pub const fn recovery_unit_id(self) -> Option<u64> {
        self.recovery_unit_id
    }

    #[must_use]
    pub const fn wal_bytes(self) -> u64 {
        self.wal_bytes
    }

    #[must_use]
    pub const fn wal_storage_bytes(self) -> u64 {
        self.wal_storage_bytes
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct MaintenanceStatus {
    pub(crate) sync_due: bool,
    pub(crate) seal_due: bool,
    pub(crate) sync_due_in: Option<Duration>,
    pub(crate) seal_due_in: Option<Duration>,
    pub(crate) unsynced_bytes: u64,
    pub(crate) wal_bytes: u64,
    pub(crate) wal_storage_bytes: u64,
    pub(crate) tail_bytes: u64,
    pub(crate) visible_seq: u64,
    pub(crate) durable_seq: u64,
    pub(crate) pending_records: u64,
    pub(crate) level_units: [u32; 3],
    pub(crate) retention_floor: Option<i64>,
}

impl MaintenanceStatus {
    #[must_use]
    pub const fn sync_due(self) -> bool {
        self.sync_due
    }

    #[must_use]
    pub const fn seal_due(self) -> bool {
        self.seal_due
    }

    #[must_use]
    pub const fn sync_due_in(self) -> Option<Duration> {
        self.sync_due_in
    }

    #[must_use]
    pub const fn seal_due_in(self) -> Option<Duration> {
        self.seal_due_in
    }

    #[must_use]
    pub const fn unsynced_bytes(self) -> u64 {
        self.unsynced_bytes
    }

    #[must_use]
    pub const fn wal_bytes(self) -> u64 {
        self.wal_bytes
    }

    #[must_use]
    pub const fn wal_storage_bytes(self) -> u64 {
        self.wal_storage_bytes
    }

    #[must_use]
    pub const fn tail_bytes(self) -> u64 {
        self.tail_bytes
    }

    #[must_use]
    pub const fn visible_seq(self) -> u64 {
        self.visible_seq
    }

    #[must_use]
    pub const fn durable_seq(self) -> u64 {
        self.durable_seq
    }

    #[must_use]
    pub const fn pending_records(self) -> u64 {
        self.pending_records
    }

    #[must_use]
    pub const fn level_units(self) -> [u32; 3] {
        self.level_units
    }

    #[must_use]
    pub const fn retention_floor(self) -> Option<i64> {
        self.retention_floor
    }
}
