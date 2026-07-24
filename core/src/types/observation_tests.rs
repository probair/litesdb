// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Observation, ObservationEntry};
use crate::{
    CellValue, ErrorKind, F32Bits, FieldId, FieldSchema, SeriesId, TableVersion, Validity,
    ValueType, VersionSpec,
};

fn entry(series: u64, field: u16, value: CellValue) -> ObservationEntry {
    ObservationEntry::new(SeriesId::new(series), FieldId::new(field), value)
}

fn version() -> TableVersion {
    let Ok(spec) = VersionSpec::new(
        Validity::Forever,
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::Sq1),
            FieldSchema::new(FieldId::new(3), ValueType::F32Bits),
        ],
    ) else {
        unreachable!("test schema is valid");
    };
    TableVersion::initial(spec)
}

#[test]
fn observation_must_be_non_empty_and_strictly_ordered() {
    let Err(error) = Observation::new(0, Vec::new()) else {
        unreachable!();
    };
    assert_eq!(error.kind(), ErrorKind::InvalidArgument);

    for entries in [
        vec![entry(1, 2, CellValue::Null), entry(1, 1, CellValue::Null)],
        vec![
            entry(1, 1, CellValue::Null),
            entry(1, 1, CellValue::UInt(1)),
        ],
        vec![entry(2, 1, CellValue::Null), entry(1, 2, CellValue::Null)],
    ] {
        let Err(error) = Observation::new(0, entries) else {
            unreachable!();
        };
        assert_eq!(error.kind(), ErrorKind::InvalidArgument);
    }
}

#[test]
fn omission_and_explicit_null_remain_distinct() {
    let Ok(observation) = Observation::new(
        i64::MIN,
        vec![
            entry(0, 1, CellValue::Null),
            entry(0, 3, CellValue::F32Bits(F32Bits::from_bits(0x8000_0000))),
        ],
    ) else {
        unreachable!("ordered non-empty observation rejected");
    };
    assert_eq!(observation.entries().len(), 2);
    assert_eq!(observation.entries()[0].value(), CellValue::Null);
    assert_eq!(observation.entries()[0].field(), FieldId::new(1));
    assert_eq!(observation.entries()[1].field(), FieldId::new(3));
    assert!(observation.validate_schema(&version()).is_ok());
}

#[test]
fn schema_rejects_unknown_fields_and_wrong_payload_types_but_accepts_null() {
    let schema = version();
    let cases = [
        entry(0, 4, CellValue::UInt(1)),
        entry(0, 1, CellValue::sq1(7)),
        entry(0, 2, CellValue::F32Bits(F32Bits::from_bits(1))),
    ];
    for rejected in cases {
        let Ok(observation) = Observation::new(1, vec![rejected]) else {
            unreachable!("single entry is ordered");
        };
        assert_eq!(
            observation
                .validate_schema(&schema)
                .map_err(|error| error.kind()),
            Err(ErrorKind::InvalidArgument)
        );
    }

    let Ok(null) = Observation::new(1, vec![entry(0, 2, CellValue::Null)]) else {
        unreachable!("single null entry is valid");
    };
    assert!(null.validate_schema(&schema).is_ok());
}

#[test]
fn table_clock_is_strictly_increasing_across_the_full_i64_domain() {
    let Ok(first) = Observation::new(i64::MIN, vec![entry(0, 1, CellValue::UInt(1))]) else {
        unreachable!();
    };
    assert!(first.validate_after(None).is_ok());
    assert!(first.validate_after(Some(i64::MIN)).is_err());

    let Ok(last) = Observation::new(i64::MAX, vec![entry(0, 1, CellValue::UInt(2))]) else {
        unreachable!();
    };
    assert!(last.validate_after(Some(i64::MAX - 1)).is_ok());
    assert!(last.validate_after(Some(i64::MAX)).is_err());
}
