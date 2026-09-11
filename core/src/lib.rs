// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![forbid(unsafe_code)]
#![deny(rust_2018_idioms, rust_2021_compatibility, unused_must_use)]
#![deny(
    clippy::all,
    clippy::pedantic,
    clippy::unwrap_used,
    clippy::expect_used
)]
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
#![cfg_attr(not(test), deny(clippy::arithmetic_side_effects))]

mod agg;
#[cfg(feature = "archive")]
pub mod archive;
mod codec;
mod db;
mod db_open;
mod error;
mod fsutil;
mod lifecycle_gc;
mod limits;
mod maintenance;
mod manifest;
mod options;
mod query;
mod retention;
mod scheduler;
mod telemetry;
mod types;
mod unit;
mod wal;

pub use agg::{Bucket, SumResult};
#[cfg(feature = "archive")]
pub use archive::{
    ArchiveCursor, ArchiveOptions, ArchiveStatus, BaseDescriptor, BaseFile, ExportChunk,
    FrozenBase, RestoreBuilder, RestoredDbDescriptor,
};
pub use db::{Db, Seq};
pub use error::{ArgDetail, CorruptionDetail, Error, ErrorKind, LimitDetail, OpDetail};
pub use maintenance::{CompactLevel, CompactReport, RetentionReport, SealReport};
pub use options::{CompactionPolicy, OpenOptions, SealPolicy, SyncPolicy};
pub use query::{Fact, FactCursor, Lookup, Slot, Snapshot};
pub use scheduler::MaintenanceReport;
pub use telemetry::{MaintenanceStatus, OpenReport};
pub use types::{
    CellValue, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, Sq1,
    StreamKey, TableId, TableSpec, TableVersion, Validity, ValueType, VersionSpec,
};
pub use wal::DurablePosition;

pub type Result<T> = std::result::Result<T, Error>;

fn assert_send_sync<T: Send + Sync>() {}

const _: fn() = assert_send_sync::<db::Db>;
const _: fn() = assert_send_sync::<query::Snapshot>;

#[cfg(test)]
#[path = "production_contract_tests.rs"]
mod production_contract_tests;

#[cfg(test)]
#[path = "caller_extension_tests.rs"]
mod caller_extension_tests;

#[cfg(test)]
#[path = "takeover_tests.rs"]
mod takeover_tests;

#[cfg(test)]
#[path = "takeover_fault_tests.rs"]
mod takeover_fault_tests;

#[cfg(test)]
mod archive_feature_tests;
