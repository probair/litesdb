// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::aggregate;
use crate::{
    CellValue, ErrorKind, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry, Result,
    SeriesId, Sq1, StreamKey, TableId, Validity, ValueType, VersionSpec,
    agg::bucket::SumResult,
    manifest::UnitMeta,
    query::UnitSource,
    unit::TableDirectoryEntry,
    wal::{RecordBody, ReplayTarget, TailIndex},
};

struct EmptySource;

impl UnitSource for EmptySource {
    fn table_sections(&self, _unit: UnitMeta, _table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        unreachable!("tail-only aggregation loaded a unit directory")
    }

    fn section_bytes(&self, _unit: UnitMeta, _section: TableDirectoryEntry) -> Result<Vec<u8>> {
        unreachable!("tail-only aggregation loaded section bytes")
    }
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::Sq1),
            FieldSchema::new(FieldId::new(3), ValueType::F32Bits),
            FieldSchema::new(FieldId::new(4), ValueType::UInt),
        ],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
}

fn observation(timestamp: i64, values: &[(u16, CellValue)]) -> Observation {
    Observation::new(
        timestamp,
        values
            .iter()
            .map(|(field, value)| {
                ObservationEntry::new(SeriesId::new(7), FieldId::new(*field), *value)
            })
            .collect(),
    )
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

fn tail() -> TailIndex {
    let table = TableId::new(1);
    let mut tail = TailIndex::new(0, 1);
    tail.apply(
        1,
        RecordBody::CreateTable {
            table,
            spec: spec(),
        },
    )
    .unwrap_or_else(|_| unreachable!());
    let low = Sq1::new(50).unwrap_or_else(|| unreachable!());
    let high = Sq1::new(78).unwrap_or_else(|| unreachable!());
    let rows = [
        (
            0,
            vec![
                (1, CellValue::UInt(5)),
                (2, CellValue::Sq1(low)),
                (3, CellValue::F32Bits(F32Bits::from_bits(0x8000_0000))),
                (4, CellValue::Null),
            ],
        ),
        (
            5,
            vec![
                (1, CellValue::Null),
                (2, CellValue::Null),
                (3, CellValue::Null),
                (4, CellValue::Null),
            ],
        ),
        (
            10,
            vec![
                (1, CellValue::UInt(7)),
                (2, CellValue::Sq1(high)),
                (3, CellValue::F32Bits(F32Bits::from_bits(0))),
            ],
        ),
        (
            15,
            vec![(3, CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)))],
        ),
    ];
    for (index, (timestamp, values)) in rows.into_iter().enumerate() {
        let seq = u64::try_from(index).unwrap_or(u64::MAX).saturating_add(2);
        tail.apply(
            seq,
            RecordBody::AppendObservation {
                table,
                observation: observation(timestamp, &values),
            },
        )
        .unwrap_or_else(|_| unreachable!("valid row rejected"));
    }
    tail
}

fn key(field: u16) -> StreamKey {
    StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(field))
}

#[test]
fn mixed_types_fold_into_exact_nonempty_buckets() {
    let tail = tail();
    let source = EmptySource;
    let rows = aggregate(
        &[],
        &source,
        &tail,
        &[key(1), key(2), key(3), key(4)],
        0,
        20,
        10,
        None,
    )
    .unwrap_or_else(|_| unreachable!("valid aggregation failed"));
    assert_eq!(rows[0].len(), 2);
    assert_eq!((rows[0][0].sample_count(), rows[0][0].null_count()), (2, 1));
    assert_eq!(rows[0][0].sum(), SumResult::UInt(5));
    assert!(!rows[0][0].is_partial());
    assert_eq!(rows[0][1].sum(), SumResult::UInt(7));
    assert!(rows[0][1].is_partial());

    assert_eq!(rows[1][0].sum(), SumResult::Sq1Fp4(50_000));
    assert_eq!(rows[1][1].sum(), SumResult::Sq1Fp4(120_000));
    assert_eq!(rows[2][0].sum(), SumResult::NotProvided);
    assert_eq!(
        rows[2][0].min(),
        Some(CellValue::F32Bits(F32Bits::from_bits(0x8000_0000)))
    );
    assert_eq!(
        rows[2][1].max(),
        Some(CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)))
    );
    assert_eq!(rows[3].len(), 1);
    assert_eq!((rows[3][0].min(), rows[3][0].max()), (None, None));
    assert_eq!((rows[3][0].sample_count(), rows[3][0].null_count()), (2, 2));
}

#[test]
fn clipped_and_duplicate_rows_are_exact() {
    let tail = tail();
    let source = EmptySource;
    let rows = aggregate(&[], &source, &tail, &[key(1), key(1)], 0, 12, 10, None)
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(rows[0], rows[1]);
    assert_eq!((rows[0][1].start_ts(), rows[0][1].end_ts()), (10, 12));
    assert!(rows[0][1].is_partial());
}

#[test]
fn aggregate_inputs_are_preflighted() {
    let tail = tail();
    let source = EmptySource;
    let cases = [
        aggregate(&[], &source, &tail, &[key(1)], 10, 0, 1, None),
        aggregate(&[], &source, &tail, &[key(1)], 0, 10, 0, None),
        aggregate(&[], &source, &tail, &[key(1)], 0, 10, 1, Some(1)),
    ];
    for result in cases {
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(ErrorKind::InvalidArgument)
        );
    }
    assert_eq!(
        aggregate(&[], &source, &tail, &[key(1)], 0, 1_048_577, 1, None)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
    assert_eq!(
        aggregate(&[], &source, &tail, &[key(9)], 0, 10, 1, None)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
}
