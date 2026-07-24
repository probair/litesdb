// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{ReplayTarget, TailIndex};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, SeriesId, TableId,
    Validity, ValueType, VersionSpec, wal::record::RecordBody,
};

fn spec(fields: &[(u16, ValueType)]) -> VersionSpec {
    VersionSpec::new(
        Validity::duration_seconds(9).unwrap_or(Validity::Forever),
        fields
            .iter()
            .map(|(field, value_type)| FieldSchema::new(FieldId::new(*field), *value_type))
            .collect(),
    )
    .unwrap_or_else(|_| unreachable!("valid tail schema rejected"))
}

fn observation(timestamp: i64, values: &[(u64, u16, CellValue)]) -> Observation {
    Observation::new(
        timestamp,
        values
            .iter()
            .map(|(series, field, value)| {
                ObservationEntry::new(SeriesId::new(*series), FieldId::new(*field), *value)
            })
            .collect(),
    )
    .unwrap_or_else(|_| unreachable!("valid tail observation rejected"))
}

fn apply(index: &mut TailIndex, seq: u64, body: RecordBody) {
    index
        .apply(seq, body)
        .unwrap_or_else(|_| unreachable!("valid tail mutation rejected"));
}

#[test]
fn observations_activate_versions_and_build_postings() {
    let mut index = TailIndex::new(0, 1);
    apply(
        &mut index,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    assert_eq!(
        index.field_type(TableId::new(1), FieldId::new(1)),
        Some(ValueType::UInt)
    );
    apply(
        &mut index,
        2,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(10, &[(4, 1, CellValue::Null), (5, 1, CellValue::UInt(7))]),
        },
    );
    let table = index
        .table(TableId::new(1))
        .unwrap_or_else(|| unreachable!());
    assert_eq!(table.versions()[0].effective_from(), Some(10));
    assert_eq!(table.last_ts(), Some(10));
    assert_eq!(table.rows()[0].version_no(), 1);
    assert_eq!(table.rows()[0].timestamp(), 10);
    assert_eq!(
        table.stream_rows(SeriesId::new(4), FieldId::new(1)),
        Some([0].as_slice())
    );
    assert!(index.estimated_bytes() > 0);
}

#[test]
fn new_version_preserves_fields_and_activates_lazily() {
    let mut index = TailIndex::new(0, 1);
    apply(
        &mut index,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    apply(
        &mut index,
        2,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(1, &[(1, 1, CellValue::UInt(1))]),
        },
    );
    apply(
        &mut index,
        3,
        RecordBody::NewTableVersion {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt), (2, ValueType::F32Bits)]),
        },
    );
    assert_eq!(
        index
            .table(TableId::new(1))
            .unwrap_or_else(|| unreachable!())
            .versions()[1]
            .effective_from(),
        None
    );
    apply(
        &mut index,
        4,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(2, &[(1, 1, CellValue::UInt(2))]),
        },
    );
    let table = index
        .table(TableId::new(1))
        .unwrap_or_else(|| unreachable!());
    let versions = table.versions();
    assert_eq!((versions[0].version_no(), versions[1].version_no()), (1, 2));
    assert_eq!(versions[1].effective_from(), Some(2));
    assert_eq!(
        table
            .rows()
            .iter()
            .map(super::TailRow::version_no)
            .collect::<Vec<_>>(),
        [1, 2]
    );
    assert_eq!(
        index.tables().map(|(table, _)| table).collect::<Vec<_>>(),
        [TableId::new(1)]
    );
}

#[test]
fn retirement_revival_and_drop_are_semantic_mutations() {
    let mut index = TailIndex::new(0, 1);
    apply(
        &mut index,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    apply(
        &mut index,
        2,
        RecordBody::RetireSeries {
            table: TableId::new(1),
            series: SeriesId::new(9),
            retire_ts: 5,
        },
    );
    apply(
        &mut index,
        3,
        RecordBody::RetireField {
            table: TableId::new(1),
            field: FieldId::new(1),
            retire_ts: 5,
        },
    );
    apply(
        &mut index,
        4,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(6, &[(9, 1, CellValue::UInt(1))]),
        },
    );
    apply(
        &mut index,
        5,
        RecordBody::DropTable {
            table: TableId::new(1),
        },
    );
    assert!(index.table(TableId::new(1)).is_none());
    assert_eq!(index.next_seq(), 6);
}

#[test]
fn semantic_errors_are_corruption_and_atomic() {
    let cases = [
        RecordBody::CreateTable {
            table: TableId::new(2),
            spec: spec(&[(1, ValueType::UInt)]),
        },
        RecordBody::DropTable {
            table: TableId::new(1),
        },
        RecordBody::RetireField {
            table: TableId::new(1),
            field: FieldId::new(1),
            retire_ts: 0,
        },
    ];
    for body in cases {
        let mut index = TailIndex::new(0, 1);
        let Err(error) = index.apply(1, body) else {
            unreachable!("invalid complete WAL mutation accepted");
        };
        assert_eq!(error.kind(), ErrorKind::Corruption);
        assert_eq!(index.next_seq(), 1);
    }

    let mut index = TailIndex::new(0, 1);
    apply(
        &mut index,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(&[(1, ValueType::UInt)]),
        },
    );
    apply(
        &mut index,
        2,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(5, &[(1, 1, CellValue::UInt(1))]),
        },
    );
    let invalid = RecordBody::AppendObservation {
        table: TableId::new(1),
        observation: observation(5, &[(1, 1, CellValue::UInt(2))]),
    };
    let Err(error) = index.apply(3, invalid) else {
        unreachable!("non-increasing table clock accepted");
    };
    assert_eq!(error.kind(), ErrorKind::Corruption);
    assert_eq!(index.next_seq(), 3);
    assert_eq!(
        index
            .table(TableId::new(1))
            .unwrap_or_else(|| unreachable!())
            .rows()
            .len(),
        1
    );
}
