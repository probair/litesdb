// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Lookup, Slot, latest, sample, value_at};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, Result, SeriesId,
    StreamKey, TableId, Validity, ValueType, VersionSpec,
    manifest::UnitMeta,
    unit::TableDirectoryEntry,
    wal::{RecordBody, ReplayTarget, TailIndex},
};

struct EmptySource;

impl super::UnitSource for EmptySource {
    fn table_sections(&self, _unit: UnitMeta, _table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        unreachable!("tail-only query loaded a unit directory")
    }

    fn section_bytes(&self, _unit: UnitMeta, _section: TableDirectoryEntry) -> Result<Vec<u8>> {
        unreachable!("tail-only query loaded section bytes")
    }
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::duration_seconds(5).unwrap_or(Validity::Forever),
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::UInt),
        ],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
}

fn observation(timestamp: i64, field: u16, value: CellValue) -> Observation {
    Observation::new(
        timestamp,
        vec![ObservationEntry::new(
            SeriesId::new(7),
            FieldId::new(field),
            value,
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

fn apply(tail: &mut TailIndex, seq: u64, body: RecordBody) {
    tail.apply(seq, body)
        .unwrap_or_else(|_| unreachable!("valid mutation rejected"));
}

fn tail() -> TailIndex {
    let table = TableId::new(1);
    let mut tail = TailIndex::new(0, 1);
    apply(
        &mut tail,
        1,
        RecordBody::CreateTable {
            table,
            spec: spec(),
        },
    );
    for (seq, timestamp, field, value) in [
        (2, 10, 1, CellValue::UInt(10)),
        (3, 12, 2, CellValue::UInt(12)),
        (4, 14, 1, CellValue::Null),
        (5, 20, 1, CellValue::UInt(20)),
    ] {
        apply(
            &mut tail,
            seq,
            RecordBody::AppendObservation {
                table,
                observation: observation(timestamp, field, value),
            },
        );
    }
    tail
}

fn keys() -> (StreamKey, StreamKey) {
    (
        StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(1)),
        StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(2)),
    )
}

#[test]
fn value_at_and_latest_apply_historical_validity() {
    let tail = tail();
    let source = EmptySource;
    let (primary, secondary) = keys();
    assert_eq!(
        value_at(&[], &source, &tail, &[primary], 13).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(10),
            at_ts: 10,
        }])
    );
    assert_eq!(
        value_at(&[], &source, &tail, &[primary], 14).ok(),
        Some(vec![Lookup::Null { at_ts: 14 }])
    );
    assert_eq!(
        value_at(&[], &source, &tail, &[primary], 19).ok(),
        Some(vec![Lookup::Missing])
    );
    assert_eq!(
        latest(&[], &source, &tail, &[primary, secondary]).ok(),
        Some(vec![
            Lookup::Value {
                value: CellValue::UInt(20),
                at_ts: 20,
            },
            Lookup::Missing,
        ])
    );
}

#[test]
fn sample_is_validity_filled_and_batch_equivalent() {
    let tail = tail();
    let source = EmptySource;
    let (primary, _) = keys();
    let expected = vec![
        Slot::Gap,
        Slot::Value {
            value: CellValue::UInt(10),
            source_ts: 10,
            carried: true,
        },
        Slot::Value {
            value: CellValue::UInt(10),
            source_ts: 10,
            carried: true,
        },
        Slot::Null {
            source_ts: 14,
            carried: true,
        },
        Slot::Null {
            source_ts: 14,
            carried: true,
        },
        Slot::Gap,
        Slot::Value {
            value: CellValue::UInt(20),
            source_ts: 20,
            carried: true,
        },
    ];
    assert_eq!(
        sample(&[], &source, &tail, &[primary], 9, 22, 2).ok(),
        Some(vec![expected.clone()])
    );
    assert_eq!(
        sample(&[], &source, &tail, &[primary, primary], 9, 22, 2).ok(),
        Some(vec![expected.clone(), expected])
    );
}

#[test]
fn retirement_and_revival_are_applied_at_query_time() {
    let mut tail = tail();
    let source = EmptySource;
    let (primary, _) = keys();
    apply(
        &mut tail,
        6,
        RecordBody::RetireSeries {
            table: TableId::new(1),
            series: SeriesId::new(7),
            retire_ts: 20,
        },
    );
    assert_eq!(
        value_at(&[], &source, &tail, &[primary], 21).ok(),
        Some(vec![Lookup::Missing])
    );
    apply(
        &mut tail,
        7,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(22, 1, CellValue::UInt(22)),
        },
    );
    assert_eq!(
        latest(&[], &source, &tail, &[primary]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(22),
            at_ts: 22,
        }])
    );
}

#[test]
fn batch_inputs_and_slot_limits_are_preflighted() {
    let tail = tail();
    let source = EmptySource;
    let (primary, _) = keys();
    let unknown = StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(9));
    assert_eq!(
        value_at(&[], &source, &tail, &[primary, unknown], 20)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    for result in [
        sample(&[], &source, &tail, &[primary], 0, 10, 0),
        sample(&[], &source, &tail, &[primary], 10, 0, 1),
    ] {
        assert_eq!(
            result.err().map(|error| error.kind()),
            Some(ErrorKind::InvalidArgument)
        );
    }
    assert_eq!(
        sample(&[], &source, &tail, &[primary], 0, 1_048_577, 1)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::ResourceExhausted)
    );
}
