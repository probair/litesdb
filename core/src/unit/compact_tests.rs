// Copyright (c) 2026 LiteSDB contributors.
// SPDX-License-Identifier: LGPL-3.0-only
// This file is part of LiteSDB. See LICENSE for license details.
// Project: https://github.com/probair/litesdb

use std::collections::BTreeMap;

use super::assemble;
use crate::{
    CellValue, Error, ErrorKind, F32Bits, FieldId, FieldSchema, Observation, ObservationEntry,
    Result, SeriesId, TableId, Validity, ValueType, VersionSpec,
    manifest::UnitMeta,
    unit::{SealedUnit, TableDirectoryEntry, UnitSource, decode, decode_layout, seal_snapshot},
    wal::{RecordBody, RecoveredTable, ReplayTarget, TailIndex},
};

type FactKey = (u32, u32, i64, u64, u16);

#[derive(Default)]
struct MemoryUnits {
    units: BTreeMap<u64, (UnitMeta, Vec<u8>, Vec<TableDirectoryEntry>)>,
}

impl MemoryUnits {
    fn insert(&mut self, sealed: &SealedUnit) {
        let layout = decode_layout(sealed.bytes(), sealed.meta().unit_id())
            .unwrap_or_else(|_| unreachable!("valid unit rejected"));
        self.units.insert(
            sealed.meta().unit_id(),
            (
                sealed.meta(),
                sealed.bytes().to_vec(),
                layout.sections().to_vec(),
            ),
        );
    }
}

impl UnitSource for MemoryUnits {
    fn table_sections(&self, unit: UnitMeta, table: TableId) -> Result<Vec<TableDirectoryEntry>> {
        let (stored, _, entries) = self
            .units
            .get(&unit.unit_id())
            .ok_or_else(|| Error::corruption("test source", "unit is absent"))?;
        if *stored != unit {
            return Err(Error::corruption("test source", "metadata changed"));
        }
        Ok(entries
            .iter()
            .copied()
            .filter(|entry| entry.table() == table)
            .collect())
    }

    fn section_bytes(&self, unit: UnitMeta, section: TableDirectoryEntry) -> Result<Vec<u8>> {
        let (stored, bytes, _) = self
            .units
            .get(&unit.unit_id())
            .ok_or_else(|| Error::corruption("test source", "unit is absent"))?;
        if *stored != unit {
            return Err(Error::corruption("test source", "metadata changed"));
        }
        let start = usize::try_from(section.section_offset())
            .map_err(|_| Error::corruption("test source", "offset overflow"))?;
        let length = usize::try_from(section.section_len())
            .map_err(|_| Error::corruption("test source", "length overflow"))?;
        let end = start
            .checked_add(length)
            .ok_or_else(|| Error::corruption("test source", "extent overflow"))?;
        bytes
            .get(start..end)
            .map(<[u8]>::to_vec)
            .ok_or_else(|| Error::corruption("test source", "extent is invalid"))
    }
}

fn spec() -> VersionSpec {
    VersionSpec::new(
        Validity::Forever,
        vec![
            FieldSchema::new(FieldId::new(1), ValueType::UInt),
            FieldSchema::new(FieldId::new(2), ValueType::F32Bits),
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
        .unwrap_or_else(|_| unreachable!("valid mutation rejected"));
}

fn fixture() -> (MemoryUnits, Vec<UnitMeta>, TailIndex) {
    let table = TableId::new(1);
    let mut first = TailIndex::new(0, 1);
    apply(
        &mut first,
        1,
        RecordBody::CreateTable {
            table,
            spec: spec(),
        },
    );
    apply(
        &mut first,
        2,
        RecordBody::AppendObservation {
            table,
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
        &mut first,
        3,
        RecordBody::AppendObservation {
            table,
            observation: observation(20, &[(1, 2, CellValue::Null)]),
        },
    );
    let first_unit = seal_snapshot(&first, 1).unwrap_or_else(|_| unreachable!("Seal failed"));
    let versions = first
        .table(table)
        .unwrap_or_else(|| unreachable!())
        .versions()
        .to_vec();
    let mut second = TailIndex::restore(
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
        &mut second,
        4,
        RecordBody::AppendObservation {
            table,
            observation: observation(30, &[(1, 1, CellValue::Null)]),
        },
    );
    apply(
        &mut second,
        5,
        RecordBody::AppendObservation {
            table,
            observation: observation(
                40,
                &[
                    (1, 1, CellValue::UInt(9)),
                    (1, 2, CellValue::F32Bits(F32Bits::from_bits(0x8000_0000))),
                ],
            ),
        },
    );
    let second_unit = seal_snapshot(&second, 2).unwrap_or_else(|_| unreachable!("Seal failed"));
    let metas = vec![first_unit.meta(), second_unit.meta()];
    let mut source = MemoryUnits::default();
    source.insert(&first_unit);
    source.insert(&second_unit);
    (source, metas, second)
}

fn facts(
    source: &MemoryUnits,
    units: &[UnitMeta],
    schemas: &TailIndex,
) -> BTreeMap<FactKey, CellValue> {
    let mut facts = BTreeMap::new();
    for unit in units {
        let (_, bytes, _) = source
            .units
            .get(&unit.unit_id())
            .unwrap_or_else(|| unreachable!("fixture unit absent"));
        decode::scan(bytes, unit.unit_id(), schemas, |entry, section| {
            for (row, timestamp) in section.timestamps().iter().enumerate() {
                for column in section.columns() {
                    if let Some(value) = column.cells().get(row).copied().flatten() {
                        facts.insert(
                            (
                                entry.table().get(),
                                entry.version_no(),
                                *timestamp,
                                column.series().get(),
                                column.field().get(),
                            ),
                            value,
                        );
                    }
                }
            }
            Ok(())
        })
        .unwrap_or_else(|_| unreachable!("valid unit scan failed"));
    }
    facts
}

#[test]
fn compaction_is_bitwise_fact_equivalent() {
    let (mut source, inputs, schemas) = fixture();
    let expected = facts(&source, &inputs, &schemas);
    let compacted = assemble(&inputs, &source, &schemas, 3)
        .unwrap_or_else(|_| unreachable!("compaction failed"));
    assert_eq!(compacted.meta().level(), 1);
    assert_eq!(compacted.meta().section_count(), 1);
    assert_eq!(compacted.meta().total_rows(), 4);
    source.insert(&compacted);
    assert_eq!(facts(&source, &[compacted.meta()], &schemas), expected);
    assert_eq!(
        expected.get(&(1, 1, 10, 1, 2)),
        Some(&CellValue::F32Bits(F32Bits::from_bits(0x7fc0_1234)))
    );
}

#[test]
fn compaction_selection_is_strict() {
    let (source, inputs, schemas) = fixture();
    let one = assemble(&inputs[..1], &source, &schemas, 3)
        .unwrap_or_else(|_| unreachable!("singleton promotion failed"));
    let reversed = assemble(&[inputs[1], inputs[0]], &source, &schemas, 3);
    let reused = assemble(&inputs, &source, &schemas, 2);
    assert_eq!(one.meta().level(), 1);
    assert_eq!(
        reversed.err().map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
    assert_eq!(
        reused.err().map(|error| error.kind()),
        Some(ErrorKind::InvalidArgument)
    );
}

#[test]
fn compaction_cross_checks_manifest_totals() {
    let (mut source, inputs, schemas) = fixture();
    if let Some((_, _, sections)) = source.units.get_mut(&2) {
        sections.clear();
    }
    assert_eq!(
        assemble(&inputs, &source, &schemas, 3)
            .err()
            .map(|error| error.kind()),
        Some(ErrorKind::Corruption)
    );
}

fn append_sparse_rows(tail: &mut TailIndex, table: TableId, start: i64, rows: i64) {
    for timestamp in start..start + rows {
        let bits = match timestamp % 4 {
            0 => 0x7fc0_1234,
            1 => 0x8000_0000,
            2 => 0x0000_0001,
            _ => 0xff80_0000,
        };
        let integer = if timestamp % 7 == 0 {
            CellValue::Null
        } else {
            CellValue::UInt(timestamp.unsigned_abs())
        };
        let mut values = vec![
            (1, 1, integer),
            (1, 2, CellValue::F32Bits(F32Bits::from_bits(bits))),
        ];
        if timestamp == 20_000 {
            values.push((2, 1, CellValue::UInt(17)));
        }
        if timestamp == 39_999 {
            values.push((3, 2, CellValue::F32Bits(F32Bits::from_bits(0xffc0_5678))));
        }
        let sequence = timestamp.unsigned_abs() + 2;
        apply(
            tail,
            sequence,
            RecordBody::AppendObservation {
                table,
                observation: observation(timestamp, &values),
            },
        );
    }
}

#[test]
fn compaction_sparse_facts_survive_memory_split_inside_source() {
    let table = TableId::new(1);
    let mut first = TailIndex::new(0, 1);
    apply(
        &mut first,
        1,
        RecordBody::CreateTable {
            table,
            spec: spec(),
        },
    );
    append_sparse_rows(&mut first, table, 0, 20_000);
    let first_unit =
        seal_snapshot(&first, 1).unwrap_or_else(|_| unreachable!("first valid L0 Seal rejected"));
    let versions = first
        .table(table)
        .unwrap_or_else(|| unreachable!("created table is absent"))
        .versions()
        .to_vec();
    let mut second = TailIndex::restore(
        1,
        20_002,
        vec![RecoveredTable::new(
            table,
            Some(19_999),
            versions,
            vec![],
            vec![],
        )],
    )
    .unwrap_or_else(|_| unreachable!("valid restored schema rejected"));
    append_sparse_rows(&mut second, table, 20_000, 20_000);
    let second_unit =
        seal_snapshot(&second, 2).unwrap_or_else(|_| unreachable!("second valid L0 Seal rejected"));
    assert_eq!(first_unit.meta().section_count(), 1);
    assert_eq!(second_unit.meta().section_count(), 1);
    let inputs = [first_unit.meta(), second_unit.meta()];
    let mut source = MemoryUnits::default();
    source.insert(&first_unit);
    source.insert(&second_unit);
    let expected = facts(&source, &inputs, &second);
    assert_eq!(expected.len(), 80_002);

    let compacted = assemble(&inputs, &source, &second, 3)
        .unwrap_or_else(|_| unreachable!("valid sparse compaction rejected"));
    assert_eq!(compacted.meta().total_rows(), 40_000);
    assert_eq!(compacted.meta().section_count(), 2);
    let layout = decode_layout(compacted.bytes(), 3)
        .unwrap_or_else(|_| unreachable!("valid compacted layout rejected"));
    assert!(layout.sections()[0].max_ts() > 20_000);
    assert!(layout.sections()[0].row_count() < crate::limits::MAX_SECTION_ROWS);
    assert!(layout.sections()[1].min_ts() < 39_999);
    source.insert(&compacted);
    assert_eq!(facts(&source, &[compacted.meta()], &second), expected);
}
