// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    DEFAULT_DIRECTORY_CACHE_BYTES, DEFAULT_SEAL_BYTES, DEFAULT_SEAL_MEMORY_BYTES,
    DEFAULT_WAL_BYTES, Limit, ensure_at_most,
};
use crate::{Error, ErrorKind};

const ALL: [Limit; 24] = [
    Limit::WalRecordPayload,
    Limit::WalSegmentBytes,
    Limit::WalBytes,
    Limit::SealBytes,
    Limit::SealMemoryBytes,
    Limit::Tables,
    Limit::UnitSections,
    Limit::SectionRows,
    Limit::UnitFileBytes,
    Limit::LogicalStreams,
    Limit::RetentionHeads,
    Limit::LiveUnits,
    Limit::ManifestBodyBytes,
    Limit::AggregationLevels,
    Limit::QuerySlots,
    Limit::OperationMemoryBytes,
    Limit::OpenFiles,
    Limit::DirectoryCacheBytes,
    Limit::TailIndexBytes,
    Limit::Threads,
    Limit::StrippedBinaryBytes,
    Limit::IdleRssBytes,
    Limit::OperationRssBytes,
    Limit::RecoveryRssBytes,
];

#[test]
fn every_hard_limit_accepts_its_boundary_and_rejects_one_more() {
    for limit in ALL {
        let maximum = limit.maximum();
        assert!(ensure_at_most(limit, maximum).is_ok(), "{limit:?}");

        let error = match ensure_at_most(limit, maximum + 1) {
            Ok(()) => unreachable!("{} accepted an over-limit value", limit.name()),
            Err(error) => error,
        };
        assert_eq!(error.kind(), ErrorKind::ResourceExhausted);
        let Error::ResourceExhausted(detail) = error else {
            unreachable!();
        };
        assert_eq!(detail.name(), limit.name());
        assert_eq!(detail.actual(), maximum + 1);
        assert_eq!(detail.maximum(), maximum);
    }
}

#[test]
fn default_soft_watermarks_fit_their_hard_budgets() {
    assert_eq!(DEFAULT_SEAL_BYTES, DEFAULT_WAL_BYTES / 2);
    assert!(u64::from(DEFAULT_WAL_BYTES) <= Limit::WalBytes.maximum());
    assert!(u64::from(DEFAULT_SEAL_BYTES) <= Limit::SealBytes.maximum());
    assert!(u64::from(DEFAULT_SEAL_MEMORY_BYTES) <= Limit::SealMemoryBytes.maximum());
    assert!(u64::from(DEFAULT_DIRECTORY_CACHE_BYTES) <= Limit::DirectoryCacheBytes.maximum());
}
