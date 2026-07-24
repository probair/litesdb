// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{
    FieldRetirement, Manifest, ManifestIdentity, RetentionState, SeriesRetirement, TableCatalog,
    UnitMeta,
};
use crate::{
    ErrorKind, FieldId, FieldSchema, SeriesId, TableId, TableVersion, Validity, ValueType,
    wal::Checkpoint,
};

fn version(number: u32, effective: Option<i64>, fields: &[(u16, ValueType)]) -> TableVersion {
    TableVersion::restore(
        number,
        Validity::Forever,
        fields
            .iter()
            .map(|(field, value_type)| FieldSchema::new(FieldId::new(*field), *value_type))
            .collect(),
        effective,
    )
    .unwrap_or_else(|_| unreachable!("valid version fixture rejected"))
}

fn table() -> TableCatalog {
    TableCatalog::restore(
        TableId::new(1),
        Some(20),
        vec![
            version(1, Some(10), &[(1, ValueType::UInt)]),
            version(
                2,
                Some(20),
                &[(1, ValueType::UInt), (2, ValueType::F32Bits)],
            ),
        ],
        vec![SeriesRetirement::new(SeriesId::new(3), 19)],
        vec![FieldRetirement::new(FieldId::new(2), 20)],
    )
    .unwrap_or_else(|_| unreachable!("valid table catalog rejected"))
}

#[test]
fn complete_manifest_state_is_accepted() {
    let identity = ManifestIdentity::new(7, 9, 4, 0, 0);
    let checkpoint = Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!());
    let unit =
        UnitMeta::new(9, 2, 10, 20, 1, 2, 100, 0x1234_5678).unwrap_or_else(|_| unreachable!());
    let manifest = Manifest::restore(
        identity,
        checkpoint,
        RetentionState::new(Some(5), Some(6)),
        vec![table()],
        vec![unit],
    )
    .unwrap_or_else(|_| unreachable!("valid MANIFEST rejected"));
    assert_eq!(manifest.identity(), identity);
    assert_eq!(manifest.checkpoint(), checkpoint);
    assert_eq!(manifest.retention().floor(), Some(5));
    assert_eq!(manifest.retention().heads_generation(), Some(6));
    assert_eq!(manifest.tables()[0].last_ts(), Some(20));
    assert_eq!(manifest.tables()[0].retired_series()[0].retire_ts(), 19);
    assert_eq!(manifest.tables()[0].retired_fields()[0].retire_ts(), 20);
    assert_eq!(manifest.units()[0], unit);
}

#[test]
fn contradictory_version_histories_are_rejected() {
    let cases = [
        vec![version(2, Some(1), &[(1, ValueType::UInt)])],
        vec![
            version(1, None, &[(1, ValueType::UInt)]),
            version(2, Some(2), &[(1, ValueType::UInt)]),
        ],
        vec![
            version(1, Some(2), &[(1, ValueType::UInt)]),
            version(2, Some(1), &[(1, ValueType::UInt)]),
        ],
        vec![
            version(1, Some(1), &[(1, ValueType::UInt)]),
            version(2, Some(2), &[(1, ValueType::F32Bits)]),
        ],
    ];
    for versions in cases {
        let Err(error) = TableCatalog::restore(TableId::new(1), Some(2), versions, vec![], vec![])
        else {
            unreachable!("contradictory version history accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
    }
}

#[test]
fn malformed_retirements_are_rejected() {
    let versions = vec![version(1, Some(1), &[(1, ValueType::UInt)])];
    let cases = [
        TableCatalog::restore(
            TableId::new(1),
            Some(1),
            versions.clone(),
            vec![
                SeriesRetirement::new(SeriesId::new(2), 1),
                SeriesRetirement::new(SeriesId::new(1), 1),
            ],
            vec![],
        ),
        TableCatalog::restore(
            TableId::new(1),
            Some(1),
            versions,
            vec![],
            vec![FieldRetirement::new(FieldId::new(2), 1)],
        ),
    ];
    for result in cases {
        assert_eq!(
            result.map_err(|error| error.kind()),
            Err(ErrorKind::Corruption)
        );
    }
}

#[test]
fn ordering_and_high_waters_are_authoritative() {
    let checkpoint = Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!());
    let unit = UnitMeta::new(2, 0, 1, 1, 1, 1, 1, 0).unwrap_or_else(|_| unreachable!());
    let low_unit = Manifest::restore(
        ManifestIdentity::new(0, 1, 1, 0, 0),
        checkpoint,
        RetentionState::default(),
        vec![
            TableCatalog::restore(
                TableId::new(1),
                None,
                vec![version(1, None, &[(1, ValueType::UInt)])],
                vec![],
                vec![],
            )
            .unwrap_or_else(|_| unreachable!()),
        ],
        vec![unit],
    );
    assert_eq!(
        low_unit.map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );

    let invalid_unit = UnitMeta::new(1, 3, 2, 1, 1, 1, 1, 0);
    assert_eq!(
        invalid_unit.map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}

#[test]
fn units_are_time_ordered_with_unique_ids() {
    let checkpoint = Checkpoint::new(1, 32, 1).unwrap_or_else(|_| unreachable!());
    let early = UnitMeta::new(2, 0, 10, 19, 1, 1, 100, 0).unwrap_or_else(|_| unreachable!());
    let late = UnitMeta::new(1, 0, 20, 29, 1, 1, 100, 0).unwrap_or_else(|_| unreachable!());
    assert!(
        Manifest::restore(
            ManifestIdentity::new(0, 2, 0, 0, 0),
            checkpoint,
            RetentionState::default(),
            vec![],
            vec![early, late],
        )
        .is_ok()
    );
    let unordered = Manifest::restore(
        ManifestIdentity::new(0, 2, 0, 0, 0),
        checkpoint,
        RetentionState::default(),
        vec![],
        vec![late, early],
    );
    assert_eq!(
        unordered.map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
    let duplicate = UnitMeta::new(2, 0, 30, 39, 1, 1, 100, 0).unwrap_or_else(|_| unreachable!());
    let duplicate_id = Manifest::restore(
        ManifestIdentity::new(0, 2, 0, 0, 0),
        checkpoint,
        RetentionState::default(),
        vec![],
        vec![early, duplicate],
    );
    assert_eq!(
        duplicate_id.map_err(|error| error.kind()),
        Err(ErrorKind::Corruption)
    );
}
