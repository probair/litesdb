// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::VersionContract;
use crate::{ErrorKind, FieldId, FieldSchema, TableVersion, Validity, ValueType};

fn version(
    number: u32,
    effective: i64,
    validity: Validity,
    fields: &[(u16, ValueType)],
) -> TableVersion {
    TableVersion::restore(
        number,
        validity,
        fields
            .iter()
            .map(|(field, kind)| FieldSchema::new(FieldId::new(*field), *kind))
            .collect(),
        Some(effective),
    )
    .unwrap_or_else(|_| unreachable!("valid version rejected"))
}

fn history() -> Vec<TableVersion> {
    vec![
        version(
            1,
            10,
            Validity::duration_seconds(5).unwrap_or(Validity::Forever),
            &[(1, ValueType::UInt)],
        ),
        version(
            2,
            20,
            Validity::Forever,
            &[(1, ValueType::UInt), (2, ValueType::F32Bits)],
        ),
    ]
}

#[test]
fn activation_boundaries_resolve_historical_versions() {
    let versions = history();
    let old =
        VersionContract::resolve(&versions, FieldId::new(1), 19).unwrap_or_else(|_| unreachable!());
    let current =
        VersionContract::resolve(&versions, FieldId::new(1), 20).unwrap_or_else(|_| unreachable!());
    assert_eq!(old.version().version_no(), 1);
    assert_eq!(old.effective_range(), (10, Some(20)));
    assert_eq!(current.version().version_no(), 2);
    assert_eq!(current.effective_range(), (20, None));
    assert_eq!(current.interpretation(), ValueType::UInt);
}

#[test]
fn finite_validity_has_exact_overflow_free_boundary() {
    let versions = history();
    let contract =
        VersionContract::resolve(&versions, FieldId::new(1), 10).unwrap_or_else(|_| unreachable!());
    assert!(contract.is_live(10, 14));
    assert!(!contract.is_live(10, 15));
    assert!(!contract.is_live(10, 9));

    let extreme = vec![version(
        1,
        i64::MIN,
        Validity::duration_seconds(u32::MAX).unwrap_or(Validity::Forever),
        &[(1, ValueType::UInt)],
    )];
    let contract = VersionContract::resolve(&extreme, FieldId::new(1), i64::MIN)
        .unwrap_or_else(|_| unreachable!());
    assert!(!contract.is_live(i64::MIN, i64::MAX));
}

#[test]
fn impossible_fact_version_pairs_are_corruption() {
    let versions = history();
    for result in [
        VersionContract::resolve(&versions, FieldId::new(1), 9),
        VersionContract::resolve(&versions, FieldId::new(2), 19),
    ] {
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(ErrorKind::Corruption)
        );
    }
    let introduced =
        VersionContract::resolve(&versions, FieldId::new(2), 20).unwrap_or_else(|_| unreachable!());
    assert_eq!(introduced.interpretation(), ValueType::F32Bits);
    assert!(introduced.is_live(20, i64::MAX));
}
