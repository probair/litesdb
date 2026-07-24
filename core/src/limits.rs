// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

#![allow(
    dead_code,
    reason = "limits are consumed incrementally by later modules"
)]

use crate::{Error, Result};

pub(crate) const MAX_WAL_RECORD_PAYLOAD: u32 = 65_536;
pub(crate) const DEFAULT_WAL_SEGMENT_BYTES: u32 = 1_048_576;
pub(crate) const MAX_WAL_SEGMENT_BYTES: u32 = 67_108_864;
pub(crate) const DEFAULT_WAL_BYTES: u32 = 5_242_880;
pub(crate) const MAX_WAL_BYTES: u32 = 26_214_400;
pub(crate) const DEFAULT_SEAL_BYTES: u32 = 2_621_440;
pub(crate) const MAX_SEAL_BYTES: u32 = MAX_WAL_BYTES;
pub(crate) const DEFAULT_SEAL_INTERVAL_SECS: u32 = 300;
pub(crate) const HOSTED_SEAL_INTERVAL_SECS: u32 = 1_800;
pub(crate) const DEFAULT_SEAL_MEMORY_BYTES: u32 = 33_554_432;
pub(crate) const MAX_SEAL_MEMORY_BYTES: u32 = 67_108_864;
pub(crate) const DEFAULT_L1_WINDOW_SECS: u64 = 7_200;
pub(crate) const DEFAULT_L2_WINDOW_SECS: u64 = 86_400;
pub(crate) const MIN_COMPACTION_WINDOW_SECS: u64 = 1;
pub(crate) const MAX_TABLES: u32 = 65_536;
pub(crate) const MAX_UNIT_SECTIONS: u32 = 65_536;
pub(crate) const MAX_SECTION_ROWS: u32 = 65_536;
pub(crate) const MAX_UNIT_FILE_BYTES: u64 = 8_589_934_592;
pub(crate) const MAX_LOGICAL_STREAMS: u32 = 262_144;
pub(crate) const MAX_RETENTION_HEADS: u32 = 262_144;
pub(crate) const MAX_LIVE_UNITS: u32 = 8_192;
pub(crate) const MAX_MANIFEST_BODY_BYTES: u32 = 67_108_864;
pub(crate) const MAX_AGGREGATION_LEVELS: u8 = 3;
pub(crate) const MAX_QUERY_SLOTS: u32 = 1_048_576;
pub(crate) const MAX_OPERATION_MEMORY_BYTES: u32 = 16_777_216;
pub(crate) const SECTION_WORKING_MEMORY_BYTES: u32 = 4_194_304;
pub(crate) const SECTION_DECODE_MEMORY_BYTES: u32 = 8_388_608;
pub(crate) const MAX_OPEN_FILES: u16 = 64;
pub(crate) const DEFAULT_DIRECTORY_CACHE_BYTES: u32 = 1_048_576;
pub(crate) const MAX_DIRECTORY_CACHE_BYTES: u32 = 16_777_216;
pub(crate) const MAX_TAIL_INDEX_BYTES: u32 = 67_108_864;
pub(crate) const MAX_THREADS: u8 = 0;
pub(crate) const MAX_STRIPPED_BINARY_BYTES: u32 = 2_097_152;
pub(crate) const MAX_IDLE_RSS_BYTES: u32 = 2_097_152;
pub(crate) const MAX_OPERATION_RSS_BYTES: u32 = 16_777_216;
pub(crate) const MAX_RECOVERY_RSS_BYTES: u32 = 16_777_216;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Limit {
    WalRecordPayload,
    WalSegmentBytes,
    WalBytes,
    SealBytes,
    SealMemoryBytes,
    Tables,
    UnitSections,
    SectionRows,
    UnitFileBytes,
    LogicalStreams,
    RetentionHeads,
    LiveUnits,
    ManifestBodyBytes,
    AggregationLevels,
    QuerySlots,
    OperationMemoryBytes,
    OpenFiles,
    DirectoryCacheBytes,
    TailIndexBytes,
    Threads,
    StrippedBinaryBytes,
    IdleRssBytes,
    OperationRssBytes,
    RecoveryRssBytes,
}

impl Limit {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::WalRecordPayload => "wal_record_payload",
            Self::WalSegmentBytes => "wal_segment_bytes",
            Self::WalBytes => "wal_bytes",
            Self::SealBytes => "seal_bytes",
            Self::SealMemoryBytes => "seal_memory_bytes",
            Self::Tables => "tables",
            Self::UnitSections => "unit_sections",
            Self::SectionRows => "section_rows",
            Self::UnitFileBytes => "unit_file_bytes",
            Self::LogicalStreams => "logical_streams",
            Self::RetentionHeads => "retention_heads",
            Self::LiveUnits => "live_units",
            Self::ManifestBodyBytes => "manifest_body_bytes",
            Self::AggregationLevels => "aggregation_levels",
            Self::QuerySlots => "query_slots",
            Self::OperationMemoryBytes => "operation_memory_bytes",
            Self::OpenFiles => "open_files",
            Self::DirectoryCacheBytes => "directory_cache_bytes",
            Self::TailIndexBytes => "tail_index_bytes",
            Self::Threads => "threads",
            Self::StrippedBinaryBytes => "stripped_binary_bytes",
            Self::IdleRssBytes => "idle_rss_bytes",
            Self::OperationRssBytes => "operation_rss_bytes",
            Self::RecoveryRssBytes => "recovery_rss_bytes",
        }
    }

    pub(crate) const fn maximum(self) -> u64 {
        match self {
            Self::WalRecordPayload => MAX_WAL_RECORD_PAYLOAD as u64,
            Self::WalSegmentBytes => MAX_WAL_SEGMENT_BYTES as u64,
            Self::WalBytes => MAX_WAL_BYTES as u64,
            Self::SealBytes => MAX_SEAL_BYTES as u64,
            Self::SealMemoryBytes => MAX_SEAL_MEMORY_BYTES as u64,
            Self::Tables => MAX_TABLES as u64,
            Self::UnitSections => MAX_UNIT_SECTIONS as u64,
            Self::SectionRows => MAX_SECTION_ROWS as u64,
            Self::UnitFileBytes => MAX_UNIT_FILE_BYTES,
            Self::LogicalStreams => MAX_LOGICAL_STREAMS as u64,
            Self::RetentionHeads => MAX_RETENTION_HEADS as u64,
            Self::LiveUnits => MAX_LIVE_UNITS as u64,
            Self::ManifestBodyBytes => MAX_MANIFEST_BODY_BYTES as u64,
            Self::AggregationLevels => MAX_AGGREGATION_LEVELS as u64,
            Self::QuerySlots => MAX_QUERY_SLOTS as u64,
            Self::OperationMemoryBytes => MAX_OPERATION_MEMORY_BYTES as u64,
            Self::OpenFiles => MAX_OPEN_FILES as u64,
            Self::DirectoryCacheBytes => MAX_DIRECTORY_CACHE_BYTES as u64,
            Self::TailIndexBytes => MAX_TAIL_INDEX_BYTES as u64,
            Self::Threads => MAX_THREADS as u64,
            Self::StrippedBinaryBytes => MAX_STRIPPED_BINARY_BYTES as u64,
            Self::IdleRssBytes => MAX_IDLE_RSS_BYTES as u64,
            Self::OperationRssBytes => MAX_OPERATION_RSS_BYTES as u64,
            Self::RecoveryRssBytes => MAX_RECOVERY_RSS_BYTES as u64,
        }
    }
}

pub(crate) const fn ensure_at_most(limit: Limit, actual: u64) -> Result<()> {
    let maximum = limit.maximum();
    if actual <= maximum {
        Ok(())
    } else {
        Err(Error::limit(limit.name(), actual, maximum))
    }
}

#[cfg(test)]
#[path = "limits_tests.rs"]
mod tests;
