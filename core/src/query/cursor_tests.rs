// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::{Fact, FactCursor, UnitSource};
use crate::query::primitives::{Lookup, Slot, latest, sample, value_at};
use crate::{
    CellValue, ErrorKind, FieldId, FieldSchema, Observation, ObservationEntry, Result, SeriesId,
    StreamKey, TableId, Validity, ValueType, VersionSpec,
    agg::{SumResult, aggregate},
    manifest::UnitMeta,
    unit::{SealedUnit, TableDirectoryEntry, decode_layout, seal_snapshot},
    wal::{RecordBody, RecoveredTable, ReplayTarget, TailIndex},
};
use std::cell::Cell;

struct MemoryUnit {
    meta: UnitMeta,
    bytes: Vec<u8>,
    sections: Vec<TableDirectoryEntry>,
    directory_reads: Cell<u32>,
    section_reads: Cell<u32>,
}

impl MemoryUnit {
    fn new(sealed: &SealedUnit) -> Self {
        let layout = decode_layout(sealed.bytes(), sealed.meta().unit_id())
            .unwrap_or_else(|_| unreachable!("valid unit rejected"));
        Self {
            meta: sealed.meta(),
            bytes: sealed.bytes().to_vec(),
            sections: layout.sections().to_vec(),
            directory_reads: Cell::new(0),
            section_reads: Cell::new(0),
        }
    }

    fn take_section_reads(&self) -> u32 {
        self.section_reads.replace(0)
    }

    fn take_directory_reads(&self) -> u32 {
        self.directory_reads.replace(0)
    }
}

impl UnitSource for MemoryUnit {
    fn table_sections(&self, unit: UnitMeta, table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        if unit != self.meta {
            return Err(crate::Error::corruption(
                "test unit source",
                "unknown unit metadata",
            ));
        }
        self.directory_reads
            .set(self.directory_reads.get().saturating_add(1));
        Ok(self
            .sections
            .iter()
            .copied()
            .filter(|entry| entry.table() == table)
            .collect())
    }

    fn section_bytes(&self, unit: UnitMeta, section: TableDirectoryEntry) -> Result<Vec<u8>> {
        if unit != self.meta {
            return Err(crate::Error::corruption(
                "test unit source",
                "unknown unit metadata",
            ));
        }
        self.section_reads
            .set(self.section_reads.get().saturating_add(1));
        let start = usize::try_from(section.section_offset())
            .map_err(|_| crate::Error::corruption("test unit source", "offset overflow"))?;
        let length = usize::try_from(section.section_len())
            .map_err(|_| crate::Error::corruption("test unit source", "length overflow"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| crate::Error::corruption("test unit source", "end overflow"))?;
        self.bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| crate::Error::corruption("test unit source", "extent is invalid"))
    }
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::UInt),
        ],
    )
    .unwrap_or_else(|_| unreachable!("valid schema rejected"))
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
    .unwrap_or_else(|_| unreachable!("valid observation rejected"))
}

fn apply(tail: &mut TailIndex, seq: u64, body: RecordBody) {
    tail.apply(seq, body)
        .unwrap_or_else(|_| unreachable!("valid replay mutation rejected"));
}

fn fixture() -> (MemoryUnit, TailIndex) {
    let table = TableId::new(1);
    let mut sealed_tail = TailIndex::new(0, 1);
    apply(
        &mut sealed_tail,
        1,
        RecordBody::CreateTable {
            table,
            spec: spec(),
        },
    );
    apply(
        &mut sealed_tail,
        2,
        RecordBody::AppendObservation {
            table,
            observation: observation(10, &[(7, 1, CellValue::UInt(11))]),
        },
    );
    apply(
        &mut sealed_tail,
        3,
        RecordBody::AppendObservation {
            table,
            observation: observation(20, &[(7, 2, CellValue::UInt(99))]),
        },
    );
    let sealed = seal_snapshot(&sealed_tail, 1).unwrap_or_else(|_| unreachable!("Seal failed"));
    let versions = sealed_tail
        .table(table)
        .unwrap_or_else(|| unreachable!())
        .versions()
        .to_vec();
    let mut tail = TailIndex::restore(
        1,
        4,
        vec![RecoveredTable::new(
            table,
            Some(20),
            versions,
            vec![],
            vec![],
        )],
    )
    .unwrap_or_else(|_| unreachable!("restore failed"));
    apply(
        &mut tail,
        4,
        RecordBody::AppendObservation {
            table,
            observation: observation(30, &[(7, 1, CellValue::Null)]),
        },
    );
    apply(
        &mut tail,
        5,
        RecordBody::AppendObservation {
            table,
            observation: observation(40, &[(7, 1, CellValue::UInt(44))]),
        },
    );
    (MemoryUnit::new(&sealed), tail)
}

fn collect(cursor: &mut FactCursor<'_>) -> Vec<Fact> {
    let mut facts = Vec::new();
    while let Some(fact) = cursor
        .next_fact()
        .unwrap_or_else(|_| unreachable!("valid cursor failed"))
    {
        facts.push(fact);
    }
    facts
}

#[test]
fn cursor_concatenates_seal_and_tail_bit_exactly() {
    let (source, tail) = fixture();
    let key = StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(1));
    let mut cursor = FactCursor::new(&[source.meta], &source, &tail, key, 0, 50)
        .unwrap_or_else(|_| unreachable!("cursor construction failed"));
    let facts = collect(&mut cursor);
    assert_eq!(
        facts
            .iter()
            .map(|fact| (fact.timestamp(), fact.value()))
            .collect::<Vec<_>>(),
        [
            (10, CellValue::UInt(11)),
            (30, CellValue::Null),
            (40, CellValue::UInt(44)),
        ]
    );
}

#[test]
fn batch_primitives_share_same_table_section_reads() {
    let (source, tail) = fixture();
    let keys = [
        StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(1)),
        StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(2)),
    ];
    let units = [source.meta];

    let _ = value_at(&units, &source, &tail, &keys, 20)
        .unwrap_or_else(|_| unreachable!("batch value_at failed"));
    assert_eq!(source.take_directory_reads(), 1);
    assert_eq!(source.take_section_reads(), 1);
    let _ = latest(&units, &source, &tail, &keys)
        .unwrap_or_else(|_| unreachable!("batch latest failed"));
    assert_eq!(source.take_directory_reads(), 1);
    assert_eq!(source.take_section_reads(), 1);
    let _ = sample(&units, &source, &tail, &keys, 0, 25, 5)
        .unwrap_or_else(|_| unreachable!("batch sample failed"));
    assert_eq!(source.take_directory_reads(), 1);
    assert_eq!(source.take_section_reads(), 1);
    let _ = aggregate(&units, &source, &tail, &keys, 0, 25, 5, None)
        .unwrap_or_else(|_| unreachable!("batch aggregate failed"));
    assert_eq!(source.take_directory_reads(), 1);
    assert_eq!(source.take_section_reads(), 1);
}

#[test]
fn ranges_and_keys_are_strict() {
    let (source, tail) = fixture();
    let key = StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(1));
    let mut cursor = FactCursor::new(&[source.meta], &source, &tail, key, 30, 40)
        .unwrap_or_else(|_| unreachable!());
    assert_eq!(
        collect(&mut cursor),
        [Fact {
            timestamp: 30,
            value: CellValue::Null
        }]
    );
    for key in [
        StreamKey::new(TableId::new(2), SeriesId::new(7), FieldId::new(1)),
        StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(9)),
    ] {
        assert_eq!(
            FactCursor::new(&[source.meta], &source, &tail, key, 0, 50)
                .err()
                .map(|error| error.kind()),
            Some(ErrorKind::InvalidArgument)
        );
    }
}

#[test]
fn seal_tail_overlap_is_corruption() {
    let (source, tail) = fixture();
    let table = TableId::new(1);
    let versions = tail
        .table(table)
        .unwrap_or_else(|| unreachable!())
        .versions()
        .to_vec();
    let mut overlap = TailIndex::restore(
        1,
        4,
        vec![RecoveredTable::new(
            table,
            Some(10),
            versions,
            vec![],
            vec![],
        )],
    )
    .unwrap_or_else(|_| unreachable!());
    apply(
        &mut overlap,
        4,
        RecordBody::AppendObservation {
            table,
            observation: observation(15, &[(7, 1, CellValue::UInt(1))]),
        },
    );
    let key = StreamKey::new(table, SeriesId::new(7), FieldId::new(1));
    assert_eq!(
        FactCursor::new(&[source.meta], &source, &overlap, key, 0, 50)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}

#[test]
fn five_primitives_match_one_snapshot_oracle() {
    let (source, tail) = fixture();
    let key = StreamKey::new(TableId::new(1), SeriesId::new(7), FieldId::new(1));
    let units = [source.meta];

    let mut cursor =
        FactCursor::new(&units, &source, &tail, key, 0, 50).unwrap_or_else(|_| unreachable!());
    assert_eq!(
        collect(&mut cursor),
        [
            Fact {
                timestamp: 10,
                value: CellValue::UInt(11),
            },
            Fact {
                timestamp: 30,
                value: CellValue::Null,
            },
            Fact {
                timestamp: 40,
                value: CellValue::UInt(44),
            },
        ]
    );
    assert_eq!(
        value_at(&units, &source, &tail, &[key], 35).ok(),
        Some(vec![Lookup::Null { at_ts: 30 }])
    );
    assert_eq!(
        latest(&units, &source, &tail, &[key]).ok(),
        Some(vec![Lookup::Value {
            value: CellValue::UInt(44),
            at_ts: 40,
        }])
    );
    assert_eq!(
        sample(&units, &source, &tail, &[key], 10, 50, 10).ok(),
        Some(vec![vec![
            Slot::Value {
                value: CellValue::UInt(11),
                source_ts: 10,
                carried: false,
            },
            Slot::Value {
                value: CellValue::UInt(11),
                source_ts: 10,
                carried: true,
            },
            Slot::Null {
                source_ts: 30,
                carried: false,
            },
            Slot::Value {
                value: CellValue::UInt(44),
                source_ts: 40,
                carried: false,
            },
        ]])
    );
    let buckets = aggregate(&units, &source, &tail, &[key], 0, 50, 20, None)
        .unwrap_or_else(|_| unreachable!("valid aggregation failed"));
    assert_eq!(buckets[0].len(), 3);
    assert_eq!(buckets[0][0].sum(), SumResult::UInt(11));
    assert_eq!(
        (buckets[0][1].sample_count(), buckets[0][1].null_count()),
        (1, 1)
    );
    assert_eq!(buckets[0][2].sum(), SumResult::UInt(44));
    assert!(buckets[0][2].is_partial());
}
