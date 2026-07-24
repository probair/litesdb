// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use super::fold;
use crate::{
    CellValue, Error, ErrorKind, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry,
    Result, SeriesId, Sq1, StreamKey, TableId, Validity, ValueType, VersionSpec,
    manifest::UnitMeta,
    retention::{RetentionHead, RetentionHeads},
    unit::{SealedUnit, TableDirectoryEntry, UnitSource, decode_layout, seal_snapshot},
    wal::{RecordBody, ReplayTarget, TailIndex},
};

struct MemoryUnit {
    meta: UnitMeta,
    bytes: Vec<u8>,
    sections: Vec<TableDirectoryEntry>,
}

impl MemoryUnit {
    fn new(sealed: &SealedUnit) -> Self {
        let layout = decode_layout(sealed.bytes(), sealed.meta().unit_id())
            .unwrap_or_else(|_| unreachable!("valid unit rejected"));
        Self {
            meta: sealed.meta(),
            bytes: sealed.bytes().to_vec(),
            sections: layout.sections().to_vec(),
        }
    }
}

impl UnitSource for MemoryUnit {
    fn table_sections(&self, unit: UnitMeta, table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        if unit != self.meta {
            return Err(Error::corruption("test source", "metadata changed"));
        }
        Ok(self
            .sections
            .iter()
            .copied()
            .filter(|entry| entry.table() == table)
            .collect())
    }

    fn section_bytes(&self, unit: UnitMeta, section: TableDirectoryEntry) -> Result<Vec<u8>> {
        if unit != self.meta {
            return Err(Error::corruption("test source", "metadata changed"));
        }
        let start = usize::try_from(section.section_offset())
            .map_err(|_| Error::corruption("test source", "offset overflow"))?;
        let length = usize::try_from(section.section_len())
            .map_err(|_| Error::corruption("test source", "length overflow"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| Error::corruption("test source", "extent overflow"))?;
        self.bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Error::corruption("test source", "extent is invalid"))
    }
}

fn spec(validity: Validity, fields: &[(u16, ValueType)]) -> VersionSpec {
    VersionSpec::new(
        validity,
        fields
            .iter()
            .map(|(field, value_type)| FieldSchema::new(FieldId::new(*field), *value_type))
            .collect(),
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
        .unwrap_or_else(|_| unreachable!("valid mutation rejected"));
}

fn fixture() -> (MemoryUnit, TailIndex, RetentionHeads) {
    let mut tail = TailIndex::new(0, 1);
    apply(
        &mut tail,
        1,
        RecordBody::CreateTable {
            table: TableId::new(1),
            spec: spec(
                Validity::Forever,
                &[(1, ValueType::UInt), (2, ValueType::F32Bits)],
            ),
        },
    );
    apply(
        &mut tail,
        2,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(
                10,
                &[
                    (1, 1, CellValue::UInt(7)),
                    (1, 2, CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234))),
                ],
            ),
        },
    );
    apply(
        &mut tail,
        3,
        RecordBody::AppendObservation {
            table: TableId::new(1),
            observation: observation(20, &[(1, 1, CellValue::Null)]),
        },
    );
    apply(
        &mut tail,
        4,
        RecordBody::CreateTable {
            table: TableId::new(2),
            spec: spec(
                Validity::duration_seconds(1).unwrap_or(Validity::Forever),
                &[(1, ValueType::Sq1)],
            ),
        },
    );
    apply(
        &mut tail,
        5,
        RecordBody::AppendObservation {
            table: TableId::new(2),
            observation: observation(
                20,
                &[(
                    1,
                    1,
                    CellValue::Sq1(Sq1::new(42).unwrap_or_else(|| unreachable!())),
                )],
            ),
        },
    );
    let sealed = seal_snapshot(&tail, 1).unwrap_or_else(|_| unreachable!("Seal failed"));
    let prior = RetentionHeads::new(
        15,
        vec![RetentionHead::new(
            TableId::new(1),
            SeriesId::new(9),
            FieldId::new(1),
            10,
            CellValue::UInt(99),
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid prior heads rejected"));
    (MemoryUnit::new(&sealed), tail, prior)
}

#[test]
fn fold_preserves_only_latest_live_typed_facts() {
    let (source, schemas, prior) = fixture();
    let heads = fold(Some(&prior), &[source.meta], &source, &schemas, 21)
        .unwrap_or_else(|_| unreachable!("fold failed"));
    let actual: Vec<(StreamKey, i64, CellValue)> = heads
        .entries()
        .iter()
        .map(|head| {
            (
                StreamKey::new(head.table(), head.series(), head.field()),
                head.fact_ts(),
                head.value(),
            )
        })
        .collect();
    assert_eq!(
        actual,
        [
            (
                StreamKey::new(TableId::new(1), SeriesId::new(1), FieldId::new(1)),
                20,
                CellValue::Null,
            ),
            (
                StreamKey::new(TableId::new(1), SeriesId::new(1), FieldId::new(2)),
                10,
                CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)),
            ),
            (
                StreamKey::new(TableId::new(1), SeriesId::new(9), FieldId::new(1)),
                10,
                CellValue::UInt(99),
            ),
        ]
    );
}

#[test]
fn fold_rejects_invalid_floor_and_missing_sections() {
    let (mut source, schemas, prior) = fixture();
    assert_eq!(
        fold(Some(&prior), &[source.meta], &source, &schemas, 15)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    source.sections.pop();
    assert_eq!(
        fold(Some(&prior), &[source.meta], &source, &schemas, 21)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}
